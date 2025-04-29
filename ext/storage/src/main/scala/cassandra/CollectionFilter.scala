package fauna.storage.cassandra

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.storage.{ Cell => _, _ }
import fauna.storage.cassandra.comparators._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import io.netty.buffer.Unpooled
import org.apache.cassandra.db.{ BufferCell, Cell, OnDiskAtom }
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.db.compaction.CompactionController
import org.apache.cassandra.utils.FastByteOperations
import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

object CollectionFilter {
  type CollectionMVT = PartialFunction[(ScopeID, CollectionID), Timestamp]
  type Builder = Function3[
    OnDiskAtomIterator,
    CompactionController,
    CollectionMVT,
    CollectionFilter]

  object Stats {
    final class Builder {
      // The number of overlaps _outside_ of this compaction, which
      // prevent collection.
      private[this] var overlaps: Long = 0

      // The number of cells/columns filtered out during compaction.
      private[this] var filtered: Long = 0

      // The number of cells/columns which _would_ be filtered, if not
      // for overlapping SSTables outside the compaction.
      private[this] var ignored: Long = 0

      def setOverlaps(n: Long): Unit = overlaps = n

      def incrFiltered(): Unit =
        filtered += 1

      def incrIgnored(): Unit =
        ignored += 1

      def build(): Stats = Stats(overlaps, filtered, ignored)
    }

    def newBuilder: Builder = new Builder()
  }

  final case class Stats(overlaps: Long, filtered: Long, ignored: Long)
}

abstract class CollectionFilter(
  iterator: OnDiskAtomIterator,
  controller: CompactionController,
  val stats: CollectionFilter.Stats.Builder)
    extends CellFilter(iterator) {

  require(
    getColumnFamily.metadata.cfName == schema.name,
    s"${getColumnFamily.metadata.cfName} does not equal ${schema.name}")

  def schema: ColumnFamilySchema

  /** Returns true iff droppable cells should be preserved.
    *
    * Droppable cells are:
    *  - CREATE cells with TTL < gcBefore;
    *  - DELETE cells with validTS < gcBefore;
    *  - REMOVE cells with validTS < gcBefore.
    *
    * Expunging droppable data requires that this compaction contains the only files
    * covering this partition.
    */
  final lazy val keepDroppableCells: Boolean = {
    // overlaps outside this compaction
    val overlaps = controller.overlappingKey(getKey)
    if (overlaps > 0) {
      stats.setOverlaps(overlaps)
      true
    } else {
      false
    }
  }

  protected def predicate[T: CassandraDecoder](cell: Cell) = {
    val name = Unpooled.wrappedBuffer(cell.name.toByteBuffer)
    val prefix = schema.nameComparator.bytesToCValues(name)
    val key = CValue(schema.keyComparator, Unpooled.wrappedBuffer(getKey.getKey))
    Predicate(key, prefix).as[T]
  }

  override def computeNext(): OnDiskAtom = {
    // Define a local tailrec method so scalac may TCO this method,
    // but computeNext() may still be overridden.
    @tailrec
    def compute0(): OnDiskAtom = {
      if (iterator.hasNext()) {
        val atom = iterator.next()
        incrTotalCells()

        require(atom.isInstanceOf[Cell], "Range tombstones should not exist.")

        if (filter(atom.asInstanceOf[Cell])) {
          incrCells()
          atom
        } else {
          stats.incrFiltered()
          compute0()
        }
      } else {
        null
      }
    }

    compute0()
  }
}

object NopFilter {
  def apply(
    name: String,
    iterator: OnDiskAtomIterator,
    controller: CompactionController): NopFilter =
    new NopFilter(name, iterator, controller, CollectionFilter.Stats.newBuilder)
}

final class NopFilter(
  name: String,
  iterator: OnDiskAtomIterator,
  controller: CompactionController,
  stats: CollectionFilter.Stats.Builder)
    extends CollectionFilter(iterator, controller, stats) {
  def schema = ColumnFamilySchema(name, Nil)
  def filter(cell: Cell): Boolean = true
}

object VersionsFilter {
  def apply(
    iterator: OnDiskAtomIterator,
    controller: CompactionController,
    collect: CollectionFilter.CollectionMVT): VersionsFilter =
    new VersionsFilter(iterator, controller, collect, CollectionFilter.Stats.newBuilder)
}

/** This filter selects only those live cells which have valid times
  * at-or-later than their Collection's minimum valid time (MVT), or
  * are the first cell earlier than the MVT.
  *
  * Continuing to retain the first cell earlier than MVT is important
  * to maintain consistent history during reads, in light of these
  * filters not (necessarily) observing all cells from each row they
  * process. By retaining this "extra" cell, each row may have up to
  * one cell in each SSTable in storage which lies before MVT. As
  * these SSTables are compacted together, they will converge toward
  * leaving a single cell - dead or alive - for any row.
  *
  * Similar to tombstones, this final cell may be removed if:
  *   - no newer cells from the same row exist AND EITHER
  *     + the cell is a "delete" event AND
  *       the cell's valid time is lower than MVT
  *     + the cell is a "create" and its TTL is lower than MVT
  */
final class VersionsFilter(
  iterator: OnDiskAtomIterator,
  controller: CompactionController,
  collect: CollectionFilter.CollectionMVT,
  stats: CollectionFilter.Stats.Builder)
    extends CollectionFilter(iterator, controller, stats)
    with ExceptionLogging {

  require(
    !controller.keepTombstones,
    s"can't collect ${Tables.Versions.CFName} when tombstone purges are disabled")

  @inline def schema = Tables.Versions.Schema

  private[this] var expire = false

  private[this] val (scopeID, docID, isCollectible) = {
    val (scopeID, docID) =
      Tables.Versions.decodeRowKey(Unpooled.wrappedBuffer(getKey.getKey))
    (scopeID, docID, collect.isDefinedAt((scopeID, docID.collID)))
  }

  // This is lazy because it may not be needed at all if this row
  // isn't collectible.
  private[this] lazy val gcBefore =
    collect((scopeID, docID.collID))

  // Implements CellFilter
  def filter(cell: Cell): Boolean =
    if (!cell.isLive || !isCollectible) {
      true
    } else if (expire) {
      false
    } else {
      val (_, validTS, action, _) = predicate[Tables.Versions.Key](cell)

      if (validTS < gcBefore) {
        // Begin expiring cells after this one.
        expire = true

        if (action.isCreate) {
          ttl(cell) match {
            // Drop this cell if it's TTL'd, below MVT, and no
            // overlapping SSTables exist outside this compaction.
            case Some(ttl) if ttl < gcBefore =>
              val keep = keepDroppableCells

              if (keep) {
                stats.incrIgnored()
              }

              keep

            // Keep a cell with a TTL greater than MVT, or is the
            // first "create" below MVT.
            case Some(_) | None => true
          }
        } else {
          // If there are no overlapping SSTables for this row, and
          // this is a delete, drop this cell too.
          val keep = keepDroppableCells

          if (keep) {
            stats.incrIgnored()
          }

          keep
        }

      } else {
        true // Keep it, this one is live.
      }
    }

  private def ttl(cell: Cell): Option[Timestamp] =
    try {
      require(cell.isLive, "Tombstones cannot be decoded.")

      val buf = Unpooled.wrappedBuffer(cell.value)
      val parser = CBORParser(buf)
      val codec = new PartialIRCodec(List("ttl"))

      // There are two formats for the cell value.
      // Old: 2-array [data] [diff]
      // New: 4-array [schema version] [ttl] [data] [diff]
      val a = parser.read(CBORParser.ArrayStartSwitch)
      if (a == 2) {
        // Reading the array state put the index at the start of data.
        parser.read(codec) map { ir =>
          ir.asInstanceOf[TimeV].value
        }
      } else if (a == 4) {
        CBOR.decode[SchemaVersion](buf)
        CBOR.decode[Option[Timestamp]](buf)
      } else {
        throw new AssertionError("Version cell tuple was not size 2 or 4")
      }

    } catch {
      case ex: InvalidType =>
        logException(ex.copy(context = Some((scopeID, docID))))
        None

      // Do not halt compaction if bad data is discovered. Say
      // something, and move on.
      case NonFatal(ex) =>
        logException(ex)
        None
    }
}

object SortedIndexFilter {

  type OldKey = (Array[Byte], Array[Byte], Timestamp, SetAction)

  def apply(
    iterator: OnDiskAtomIterator,
    controller: CompactionController,
    collect: CollectionFilter.CollectionMVT): SortedIndexFilter =
    new SortedIndexFilter(iterator, controller, collect, CollectionFilter.Stats.newBuilder)
}

/** This filter selects only those live cells which have valid times
  * at-or-later than each cell's respective Collection minimum valid
  * time (MVT).
  *
  * Additionally, the first cell below MVT for each tuple may be
  * retained if it is an "add" event and its TTL is either unset
  * (equiv. to Timestamp.MaxMicros) or higher than MVT.
  */
final class SortedIndexFilter(
  iterator: OnDiskAtomIterator,
  controller: CompactionController,
  collect: CollectionFilter.CollectionMVT,
  stats: CollectionFilter.Stats.Builder)
    extends CollectionFilter(iterator, controller, stats)
    with ExceptionLogging {

  require(
    !controller.keepTombstones,
    s"can't collect ${Tables.SortedIndex.CFName} when tombstone purges are disabled")

  @inline def schema = Tables.SortedIndex.Schema

  private[this] val (scopeID, indexID, terms) =
    Tables.Indexes.decode(Unpooled.wrappedBuffer(getKey.getKey))

  private[this] val logger = getLogger()
  private[this] var prevExpired = false
  private[this] var prevValidTS: Timestamp = _
  private[this] var prevBytes: Array[Byte] = _

  @inline private def reset(): Unit = {
    prevExpired = false
    prevValidTS = null
    prevBytes = null
  }

  def filter(cell: Cell): Boolean = {
    if (!cell.isLive) {
      return true
    }

    if (keepDroppableCells) {
      stats.incrIgnored()
      return true
    }

    val cur = maybeUpgrade(cell)
    val (_, bytes, validTS, action, txnTS) =
      predicate[Tables.SortedIndex.Key](cur)

    if ((prevBytes ne null) && equiv(bytes)) {
      if (logger.isDebugEnabled && prevValidTS == validTS) {
        logger.debug(
          s"Compaction found conflicting cells in SortedIndex for $scopeID $indexID $terms: " +
            s"${bytes.toHexString} at $validTS/$action/$txnTS (expired=$prevExpired)")
      }
      // NOTE: If the previews equivalent cell is preserved, conflicting cells must
      // also be preserved so that conflict resolution can choose which entry to
      // surfice to reads (see IndexValue.ByValueEquiv). When the previews cell
      // expires, then it's safe to drop all equivalent cells.
      if (!prevExpired && prevValidTS == validTS) {
        return true
      } else {
        logger.debug(
          s"Eliding cell for $scopeID $indexID $terms based on tuple equivalence: " +
            s"${prevBytes.toHexString} shadows ${bytes.toHexString} at " +
            s"$validTS/$action/$txnTS (expired=$prevExpired)")
        return false
      }
    }

    reset() // New tuple; reset.

    val values = CBOR.parse[Vector[IndexTerm]](bytes)
    val docID = values.last.value.asInstanceOf[DocIDV].value

    val gcBefore =
      collect.applyOrElse((scopeID, docID.collID), Function.const(Timestamp.Epoch))

    // Not expired.
    if (validTS >= gcBefore) {
      return true
    }

    // Begin looking for equiv. cells.
    prevValidTS = validTS
    prevBytes = bytes

    // This is the first "add" event below MVT. Keep it if it isn't
    // expired.
    if (action.isCreate && !expired(cur, gcBefore)) {
      true
    } else {
      // This is the first "remove" event below MVT or a TTL'd "add". Drop it and the
      // remaining equivalent cells (conflicting or not).
      prevExpired = true

      // NOTE: A "remove" event below MVT with a TTL doesn't
      // change anything here. There must be one of:
      //     * an "add" above MVT with an updated TTL
      //     * an "add" without a TTL
      //     * no event at all
      // In each of these cases, this event is expungable.

      if (logger.isDebugEnabled) {
        logger.debug(
          s"Eliding event for $scopeID $indexID $terms: " +
            s"${bytes.toHexString} at $validTS/$action/$txnTS")
      }

      false
    }
  }

  private def equiv(curBytes: Array[Byte]): Boolean = {
    val cmp = FastByteOperations.compareUnsigned(
      prevBytes,
      0,
      prevBytes.length,
      curBytes,
      0,
      curBytes.length)

    cmp == 0
  }

  private def expired(cell: Cell, gcBefore: Timestamp): Boolean = {
    // C* stores tombstone timestamps in the cell value, where we
    // store TTLs in live cells.
    require(cell.isLive, "Tombstones cannot be expired.")

    // Short-circuit if the cell value is empty
    if (cell.value.remaining <= 0) {
      return false
    }

    val (_, bytes, validTS, action, txnTS) =
      predicate[Tables.SortedIndex.Key](cell)

    // Short-circuit if this is not an "add" event.
    if (action.isDelete) {
      return false
    }

    val values = CBOR.parse[Vector[IndexTerm]](bytes)
    val docID = values.last.value.asInstanceOf[DocIDV].value

    // Sanity check
    if (validTS > gcBefore) {
      return false
    }

    val data = Unpooled.wrappedBuffer(cell.value)
    Try(CBOR.decode[ValueModifier](data)) match {
      case Success(TTLModifier(ttl)) => ttl < gcBefore
      case Success(_)                => false

      case Failure(ex) =>
        val (scope, index, terms) =
          Tables.Indexes.decode(Unpooled.wrappedBuffer(getKey.getKey))
        // This used to go to the exception log but then they stick around, so moving
        // to core log instead.
        logger.warn(
          s"Unable to decode ValueModifier: ${cell.value.toHexString} " +
            s"(scope=$scope index=$index terms=$terms values=$values " +
            s"doc=$docID validTS=$validTS action=$action txnTS=$txnTS)",
          ex)
        false
    }
  }

  private def maybeUpgrade(cell: Cell): Cell = {
    val name = Unpooled.wrappedBuffer(cell.name.toByteBuffer)
    val prefix = schema.nameComparator.bytesToCValues(name)
    val key = CValue(schema.keyComparator, Unpooled.wrappedBuffer(getKey.getKey))
    val pred = Predicate(key, prefix)
    val value = new Value(
      pred,
      Unpooled.wrappedBuffer(cell.value),
      transactionTS = Timestamp.ofMicros(cell.timestamp))

    if (value.keyPredicate.columnName.size == 3) {
      // Rewrite to the current schema, using the transactionTS from
      // the Cell so the current codec can process it.
      val (rowKey, bytes, validTS, action) =
        value.keyPredicate.as[SortedIndexFilter.OldKey]

      val newValue = new Value[Tables.SortedIndex.Key](
        (rowKey, bytes, validTS, action, cell.timestamp),
        value.data,
        value.transactionTS)

      val newName = newValue.keyPredicate.uncheckedColumnNameBytes
      new BufferCell(
        controller.cfs.metadata.comparator.cellFromByteBuffer(newName.nioBuffer),
        cell.value,
        cell.timestamp)
    } else {
      cell
    }
  }
}

object HistoricalIndexFilter {

  type OldKey = (Array[Byte], Timestamp, SetAction, Array[Byte])

  def apply(
    iterator: OnDiskAtomIterator,
    controller: CompactionController,
    collect: CollectionFilter.CollectionMVT): HistoricalIndexFilter =
    new HistoricalIndexFilter(
      iterator,
      controller,
      collect,
      CollectionFilter.Stats.newBuilder)
}

/** This filter drops all cells with a valid timestamp earlier than
  * their respective Collection's MVT.
  *
  * Reads at the MVT must switch to the SortedIndex column family to
  * correctly return the history of a set at that snapshot time.
  */
final class HistoricalIndexFilter(
  iterator: OnDiskAtomIterator,
  controller: CompactionController,
  collect: CollectionFilter.CollectionMVT,
  stats: CollectionFilter.Stats.Builder)
    extends CollectionFilter(iterator, controller, stats) {

  import HistoricalIndexFilter._

  require(
    !controller.keepTombstones,
    s"can't collect ${Tables.HistoricalIndex.CFName} when tombstone purges are disabled")

  @inline def schema = Tables.HistoricalIndex.Schema

  private[this] val scopeID = {
    val (scopeID, _, _) =
      Tables.Indexes.decode(Unpooled.wrappedBuffer(getKey.getKey))
    scopeID
  }

  override def computeNext(): OnDiskAtom = {
    @tailrec
    def compute0(): OnDiskAtom = {
      if (iterator.hasNext()) {
        val atom = iterator.next()
        incrTotalCells()

        require(atom.isInstanceOf[Cell], "Range tombstones should not exist.")

        val cell = maybeUpgrade(atom.asInstanceOf[Cell])

        if (filter(cell)) {
          incrCells()
          cell
        } else {
          stats.incrFiltered()
          compute0()
        }
      } else {
        null
      }
    }

    compute0()
  }

  // Implements CellFilter
  def filter(cell: Cell): Boolean =
    if (!cell.isLive) {
      true
    } else {
      val (_, validTS, _, bytes, _) =
        predicate[Tables.HistoricalIndex.Key](cell)
      val values = CBOR.parse[Vector[IndexTerm]](bytes)
      val docID = values.head.value.asInstanceOf[DocIDV].value

      val gcBefore =
        collect.applyOrElse(
          (scopeID, docID.collID),
          Function.const(Timestamp.Epoch))

      // Drop all cells prior to the MVT.
      validTS >= gcBefore
    }

  private def maybeUpgrade(cell: Cell): Cell = {
    val name = Unpooled.wrappedBuffer(cell.name.toByteBuffer)
    val prefix = schema.nameComparator.bytesToCValues(name)
    val key = CValue(schema.keyComparator, Unpooled.wrappedBuffer(getKey.getKey))
    val pred = Predicate(key, prefix)
    val value = new Value(
      pred,
      Unpooled.wrappedBuffer(cell.value),
      transactionTS = Timestamp.ofMicros(cell.timestamp))

    if (value.keyPredicate.columnName.size == 3) {
      // Rewrite to the current schema, using the transactionTS from
      // the Cell so the current codec can process it.
      val (rowKey, validTS, action, bytes) =
        value.keyPredicate.as[OldKey]

      val newValue = new Value[Tables.HistoricalIndex.Key](
        (rowKey, validTS, action, bytes, cell.timestamp),
        value.data,
        value.transactionTS)

      val newName = newValue.keyPredicate.uncheckedColumnNameBytes
      new BufferCell(
        controller.cfs.metadata.comparator.cellFromByteBuffer(newName.nioBuffer),
        cell.value,
        cell.timestamp)
    } else {
      cell
    }
  }

}
