package fauna.repo.store

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.{ ConsoleControl, Timestamp }
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query._
import fauna.storage._
import fauna.storage.api.scan._
import fauna.storage.api.set._
import fauna.storage.cassandra.comparators._
import fauna.storage.cassandra.ColumnFamilySchema
import fauna.storage.index._
import fauna.storage.ops._
import fauna.trace._
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.collection.mutable.Builder
import scala.util.Try

object AbstractIndex {
  type Col = (IndexKey, IndexValue)
  type Row = Iterable[Col]

  // Introduce 20% over fetch per partitioned read to reduce the possibility of
  // finding a short page while merging streams which would cause additional serial
  // reads. The chosen over fetch presents peak performance in the
  // `DocumentsIndexReads` QA workload.
  val OverFetchFactor = 1.2

  val IndexConsistencyCheckMaxEntries = 128
}

abstract class AbstractIndex[K: CassandraCodec] {
  type Key = K

  val schema: ColumnFamilySchema

  def clear(idx: IndexKey): Query[Unit] =
    clear(Predicate(rowKey(idx)))

  def clear(keyBytes: ByteBuf): Query[Unit] =
    clear(Predicate(keyBytes))

  private def clear(key: Predicate): Query[Unit] = {
    val k = key.uncheckedRowKeyBytes
    val write = RemoveAllWrite(schema.name, k)
    Query.write(write)
  }

  /** Write a cell tombstone to storage over the provided entry.
    *
    * !!!! WARNING !!!!
    *
    * This is _NOT_ transactional! Use a Write op to remove a cell
    * transactionally if at all possible.
    */
  def tombstone(entry: ElementScan.Entry): Query[Unit] =
    NonTransactionalStore.zapEntry(schema.name, entry)

  def invalidIndexRowsForDocID(scope: ScopeID, docID: DocID)(
    deriveMVT: (ScopeID, CollectionID) => Query[Timestamp])
    : Query[Seq[(Timestamp, IndexRow)]] =
    cachedIndexRowsForDocID(scope, docID) flatMap { rows =>
        val adds = rows filter { _.value.isCreate }
        val invalidQs = adds map { r =>
          deriveMVT(scope, docID.collID) flatMap { mvt =>
            val snapshotTS = mvt max r.value.ts.validTS
            Store.getUnmigrated(
              r.value.scopeID,
              r.value.docID,
              snapshotTS) flatMap {
              case Some(_) => Query.none
              case None => Query.some((mvt, r))
            }
          }
        }
        // Run the check in a separate query context to avoid hitting the cache from
        // the original query.
        Query.repo flatMap { repo =>
          Query.future(repo.result(invalidQs.sequence)) map { res =>
            val adds = res.value.flatten

            // XXX: extra info for troubleshooting MVT-related failures.
            if (adds.nonEmpty) {
              getLogger.warn(rows.mkString("HISTORY: [", ",", "]"))
            }

            adds
          }
        }
    }

  def cachedIndexRowsForDocID(scope: ScopeID, docID: DocID) =
    cachedIndexRows(AbstractIndex.IndexConsistencyCheckMaxEntries) map {
      _ filter { r => r.value.scopeID == scope && r.value.docID == docID }
    }

  def cachedIndexRows(max: Int): Query[Seq[IndexRow]] = Query.context map { c =>
    val b = Seq.newBuilder[IndexRow]
    cachedIndexRows(max, c, b)
    b.result()
  }

  private def cachedIndexRows(
    max: Int,
    c: QueryContext,
    b: Builder[IndexRow, Seq[IndexRow]]) = {
    c.inspectReadCache { cache =>
      val iter = cache.iterator
      var count = 0
      while (iter.hasNext && count < max) {
        // Ignore the writes. For new reads, pending writes affecting the doc
        // should affect the index entries consistently, and should be reflected
        // in cached reads. Well, this is a good test of it, anyway...
        import scala.language.existentials
        val ((_, read), fut) = iter.next()
        if (read.columnFamily == Tables.SortedIndex.CFName) {
          count += 1

          val (scopeID, id, terms) =
            CBOR.parse[(ScopeID, IndexID, Vector[IndexTerm])](read.rowKey)
          val key = IndexKey(scopeID, id, terms)

          if (fut.isCompleted && fut.value.get.isSuccess) {
            val vs: Vector[Element] = fut.value.get.get match {
              case r: SetSnapshot.Result       => r.values
              case r: SparseSetSnapshot.Result => r.values
              case r: SetSortedValues.Result   => r.values
              case _ => throw new IllegalStateException("unexpected set result")
            }
            b ++= vs map { v => IndexRow(key, v.toIndexValue, Indexer.Empty) }
          }
        }
      }
    }
  }

  def decodeIndex(rowKey: ByteBuf, cell: Cell): Value[K] = {
    val value = Tables.decodeValue(schema, rowKey, cell)

    // Check for ancient history: index cells did not always include
    // the transactionTS in the cell name.
    if (value.keyPredicate.columnName.size == 3) {
      upgradeIndex(value, cell.ts.micros)
    } else {
      value
    }
  }

  protected def upgradeIndex(value: Value[K], transactionTS: Long): Value[K]

  protected def rowKey(idx: IndexKey): Array[Byte] = Tables.Indexes.rowKey(idx)

  protected def assertValidRange(from: IndexValue, to: IndexValue, ascending: Boolean): Try[Unit]

  protected def toKey(row: IndexRow): Key

  protected def toValue(row: IndexRow): Value[Key]

  protected def sparseScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[KeyScan.Entry]]

  protected def elementScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[ElementScan.Entry]]

  def fromValue(scope: ScopeID, v: Value[K]): IndexValue

  /** A sparse scan returns each row key within an index column family
    * within the scan bounds.
    *
    * NOTE: The unit element in each tuple satisfies the Mapper
    * pagination API.
    */
  def sparseScan(bounds: ScanBounds): PagedQuery[Iterable[(KeyScan.Entry, Unit)]] =
    sparseScan(bounds, DefaultPageSize) mapValuesT { e =>
      (e, ())
    }

  /** An element scan returns each value within an index column family
    * with the scan bounds.
    *
    * NOTE: The unit element in each tuple satisfies the Mapper
    * pagination API.
    */
  def elementScan(
    bounds: ScanBounds): PagedQuery[Iterable[(ElementScan.Entry, Unit)]] =
    elementScan(bounds, DefaultPageSize) mapValuesT { e =>
      (e, ())
    }

  protected def encodeTTL(ttl: Option[Timestamp]): ByteBuf = ttl match {
    case Some(ttl) => CBOR.encode(TTLModifier(ttl))
    case None      => Unpooled.EMPTY_BUFFER
  }
}

/**
  * HistoricalIndex is a bitemporal secondary index on Versions.
  */
object HistoricalIndex extends AbstractHistoricalIndex {
  val schema = Tables.HistoricalIndex.Schema
}

/**
 * HistoricalIndex2 is a version of HistoricalIndex that is temporarily populated
 * with rebuilt index data that is being evaluated to see whether it should replace
 * the original, live data.
 */
object HistoricalIndex2 extends AbstractHistoricalIndex {
  val schema = Tables.HistoricalIndex.Schema2
}

abstract class AbstractHistoricalIndex extends AbstractIndex[Tables.HistoricalIndex.Key] {
  implicit val ordering = IndexValue.ByEventOrdering

  protected def upgradeIndex(value: Value[Key], transactionTS: Long): Value[Key] = {
    // Rewrite to the current schema, using the transactionTS from
    // the Cell so the current codec can process it.
    val (rowKey, validTS, action, bytes) =
      value.keyPredicate.as[(Array[Byte], Timestamp, SetAction, Array[Byte])]

    new Value[Key](
      (rowKey, validTS, action, bytes, transactionTS),
      value.data,
      value.transactionTS
    )
  }

  protected def assertValidRange(from: IndexValue, to: IndexValue, ascending: Boolean) =
    fauna.repo.assertValidRange(from.event, to.event, ascending)

  protected def toKey(row: IndexRow) =
    Tables.HistoricalIndex.toKey(row.key, row.value)

  protected def toValue(row: IndexRow) =
    new Value(toKey(row), encodeTTL(row.value.tuple.ttl))

  protected def sparseScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[KeyScan.Entry]] =
    Store.historicalKeyScan(bounds, pageSize)

  protected def elementScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[ElementScan.Entry]] =
    Store.historicalElementScan(bounds, pageSize)

  def fromValue(scope: ScopeID, value: Value[Key]) =
    IndexValue.fromHistoricalValue(scope, value)
}

/**
  * SortedIndex is similar to HistoricalIndex, ordered by user-defined
  * values. It also contains an optional set of user-defined covered
  * values.
  */
object SortedIndex extends AbstractSortedIndex {
  val schema = Tables.SortedIndex.Schema
}

/**
 * SortedIndex2 is a version of SortedIndex that is temporarily populated
 * with rebuilt index data that is being evaluated to see whether it should replace
 * the original, live data.
 */
object SortedIndex2 extends AbstractSortedIndex {
  val schema = Tables.SortedIndex.Schema2
}

abstract class AbstractSortedIndex extends AbstractIndex[Tables.SortedIndex.Key] {
  implicit val ordering = IndexValue.ByValueOrdering

  protected def upgradeIndex(value: Value[Key], transactionTS: Long): Value[Key] = {
    // Rewrite to the current schema, using the transactionTS from
    // the Cell so the current codec can process it.
    val (rowKey, bytes, validTS, action) =
      value.keyPredicate.as[(Array[Byte], Array[Byte], Timestamp, SetAction)]

    new Value[Key](
      (rowKey, bytes, validTS, action, transactionTS),
      value.data,
      value.transactionTS
    )
  }

  protected def assertValidRange(from: IndexValue, to: IndexValue, ascending: Boolean) =
    fauna.repo.assertValidRange(ByInstanceID(from.event), ByInstanceID(to.event), ascending)

  protected def toKey(row: IndexRow) =
    Tables.SortedIndex.toKey(row.key, row.value)

  protected def toValue(row: IndexRow) =
    new Value(toKey(row), encodeTTL(row.value.tuple.ttl))

  protected def sparseScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[KeyScan.Entry]] =
    Store.sortedKeyScan(bounds, pageSize)

  protected def elementScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[ElementScan.Entry]] =
    Store.sortedElementScan(bounds, pageSize)

  def fromValue(scope: ScopeID, value: Value[Key]) =
    IndexValue.fromSortedValue(scope, value)
}

// NOTE: Methods on this object should not be used outside of
// fauna.repo! They should be exposed via Store/NewStore. This object
// remains public to facilitate access to methods with a
// ConsoleControl.
object IndexStore {
  private val IndexConsistencyCheckMaxDocIDs = 128

  def invalidIndexRowsForDocID(scope: ScopeID, docID: DocID)(
    deriveMVT: (ScopeID, CollectionID) => Query[Timestamp])
    : Query[Seq[(Timestamp, IndexRow)]] =
    Query.context flatMap { c =>
      if (
        c.indexConsistencyCheckDocIDs.size >= IndexConsistencyCheckMaxDocIDs ||
        !c.addIndexConsistencyCheckDocID(docID)
      ) {
        Query.value(Seq.empty)
      } else {
        SortedIndex.invalidIndexRowsForDocID(scope, docID)(deriveMVT)
      }
    }

  // single row reads

  // Opt out of optimistic locked reads based in the index config.
  // Temporarily public so the new API store code can use it.
  def setReadIsolation[T](cfg: IndexConfig, page: PagedQuery[T]): PagedQuery[T] =
    Query.state flatMap { state =>
      if (!state.enabledConcurrencyChecks || cfg.serialReads) {
        page
      } else {
        Query.disableConcurrencyChecksForPage(page)
      }
    }

  def historicalIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    fromVal: IndexValue,
    toVal: IndexValue,
    size: Int,
    ascending: Boolean)(
    implicit ctl: ConsoleControl): PagedQuery[Iterable[IndexValue]] =
    Store.historicalIndex(
      cfg,
      terms map { _.value },
      fromVal,
      toVal,
      size,
      ascending,
      MVTProvider.Default) mapValuesT { _.toIndexValue }

  def sortedIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    fromVal: IndexValue,
    toVal: IndexValue,
    size: Int)(
    implicit ctl: ConsoleControl): PagedQuery[Iterable[IndexValue]] =
    Store.sortedIndexDebug(
      cfg,
      terms map { _.value },
      fromVal,
      toVal,
      size)

  // Creates properly padded sparse sorted index queries.
  // Sparse queries are used in intersections and aren't
  // appropriate to use generally.
  def sparseBound(cfg: IndexConfig, key: IndexTuple, asc: Boolean) = {
    val (from, to) = if (asc) {
      (IndexValue.MinValue, IndexValue.MaxValue)
    } else {
      (IndexValue.MaxValue, IndexValue.MinValue)
    }

    if (key.values.isEmpty) {
      (from.copy(tuple = cfg.pad(key, asc)),
        to.copy(tuple = cfg.pad(key, !asc)))
    } else {
      (from.copy(tuple = cfg.pad(key.copy(docID = from.docID), asc)),
        to.copy(tuple = cfg.pad(key.copy(docID = to.docID), !asc)))
    }
  }

  // writes

  private def uniqueIDForKey(
    cfg: IndexConfig,
    row: IndexRow): Query[Option[DocID]] = {

    val docIDQ = Store.uniqueIDForKey(cfg, row.key.terms)

    docIDQ map {
      case Some(id) => Option.when(id != row.value.docID)(id)
      case _        => None
    }
  }

  private def uniqueID(
    cfg: IndexConfig,
    row: IndexRow): Query[Option[DocID]] = {

    val docIDQ =
      Store.uniqueID(cfg, row.key.terms map { _.value }, row.value.tuple)

    docIDQ map {
      case Some(id) => Option.when(id != row.value.docID)(id)
      case _        => None
    }
  }

  private[repo] def add(
    row: IndexRow,
    cfg: Option[IndexConfig],
    backfill: Boolean = false): Query[Unit] = {

    // When backfilling, use the SetBackfill op, which writes at the
    // same timestamp as the Version from which this row was
    // generated, rather than the timestamp of the transaction
    // comitting the backfill.
    val op = if (backfill) {
      SetBackfill(row.key, row.value, missingSorted = true, missingHistorical = true)
    } else {
      SetAdd(row.key, row.value)
    }

    val write = Query.write(op) map { _ =>
      if (isTracingEnabled) {
        traceMsg(s"  INSERT Indexes -> $row")
      }
    }

    cfg match {
      case Some(idx) if row.value.isCreate =>
        idx.constraint match {
          case Unconstrained => write
          case UniqueTerms =>
            uniqueIDForKey(idx, row) flatMap {
              case Some(id) =>
                throw UniqueConstraintViolation(List((idx.scopeID, idx.id, id, row)))
              case None => write
            }

          case UniqueValues =>
            uniqueID(idx, row) flatMap {
              case Some(id) =>
                throw UniqueConstraintViolation(List((idx.scopeID, idx.id, id, row)))
                case None => write
            }
        }
      case _ => write
    }
  }

  def update(
    indexer: Indexer,
    rev: Revision,
    applyConstraints: Boolean): Query[Unit] = {
    val versions = rev.source flatMap { c => c.canonical :: c.conflicts }
    val source = versions map { indexer.rows(_) }

    val before = source.accumulate(Set.newBuilder[IndexRow]) { (b, rows) =>
      b ++= rows
    } map { _.result() }

    val afterQs = Seq.newBuilder[Query[Seq[IndexRow]]]
    rev.target.foldLeft(rev.root) { (prev, cur) =>
      val diff = prev match {
        case prev: Version.Live => Some(cur.fields diffTo prev.fields)
        case _: Version.Deleted => None
      }

      val v = cur.withDiff(diff)
      afterQs += indexer.rows(v)
      v
    }

    val after = afterQs.result().accumulate(Set.newBuilder[IndexRow]) { (b, rows) =>
      b ++= rows
    } map { _.result() }

    (after, before) par { case (after, before) =>
      val toRemove = before diff after
      val toAdd = after diff before

      val removesQ = toRemove.view.map(remove).join

      val addsQ = toAdd.view
        .map { row =>
          val cfg = if (applyConstraints) Some(row.indexer.config) else None
          add(row, cfg) map { Right(_) } recover {
            case UniqueConstraintViolation(idxs) => Left(idxs)
          }
        }
        .sequence
        .map { rvs =>
          val indexes = rvs collect { case Left(idxs) => idxs } flatten

          if (indexes.nonEmpty) {
            throw UniqueConstraintViolation(indexes.toList)
          }
        }

      Seq(removesQ, addsQ).join
    }
  }

  def remove(row: IndexRow): Query[Unit] =
    Query.write(SetRemove(row.key, row.value)) map { _ =>
      if (isTracingEnabled) {
        traceMsg(s"  REMOVE Indexes -> $row")
      }
    }

  def clear(key: IndexKey): Query[Unit] =
    Seq(HistoricalIndex.clear(key), SortedIndex.clear(key)).join

  def clear(keyBytes: ByteBuf): Query[Unit] =
    Seq(HistoricalIndex.clear(keyBytes), SortedIndex.clear(keyBytes)).join

  /**
    * Given a Version and an Indexer, computes the necessary index
    * rows, and inserts them regardless of existing state. Returns
    * the number of rows inserted.
    */
  def build(version: Version, indexer: Indexer): Query[Int] =
    indexer.rows(version) flatMap { rows =>
      (rows map { add(_, None, backfill = true) } accumulate 0) { (t, _) => t + 1 }
    }
}
