package fauna.model.tasks

import fauna.atoms._
import fauna.lang.{ Timestamp, Timing }
import fauna.lang.syntax._
import fauna.model.RuntimeEnv
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import io.netty.buffer.Unpooled
import java.util.concurrent.TimeoutException
import scala.collection.immutable.Queue
import scala.concurrent.duration._

/** Thrown by `Paginator.paginate()` when reducing either page size or
  * group size further would not result in progress.
  */
final class PaginatorProgressException extends Exception

object Paginator {
  val MinPageSize = 1

  val SizeInterval = 2

  // FIXME: remove this and let the scheduler push back. Since most
  // work is done in parallel, without weighted scheduler pushback,
  // the latency of each page's work will be a function of the
  // system's overall latency. IE with a per-page latency target of
  // five seconds, global latency will track towards 5 seconds when
  // tasks are running.
  val LatencyTarget = 1500.millis

  sealed abstract class Cursor

  case class VersionCursor(
    scope: ScopeID,
    id: DocID,
    version: VersionID,
    txnTS: Long)
      extends Cursor {
    val toVersion: Version =
      if (version.action.isCreate) {
        Version.Live(
          scope,
          id,
          BiTimestamp.decode(version.validTS, Timestamp.ofMicros(txnTS)),
          version.action,
          SchemaVersion.Max,
          Data.empty)
      } else {
        Version.Deleted(
          scope,
          id,
          BiTimestamp.decode(version.validTS, Timestamp.ofMicros(txnTS)),
          SchemaVersion.Max)
      }
  }

  case class IndexCursor(scope: ScopeID, key: IndexKey, value: IndexValue)
      extends Cursor

  implicit val DocActionT = FieldType.Alias[DocAction, Boolean](
    _.isCreate,
    {
      case true  => Create
      case false => Delete
    })
  implicit val VersionIDT = FieldType.RecordCodec[VersionID]

  implicit val VersionCursorT = FieldType.RecordCodec[VersionCursor]

  implicit val SetActionT = FieldType.Alias[SetAction, Boolean](
    _.isCreate,
    {
      case true  => Add
      case false => Remove
    })

  implicit val TermT = FieldType.RecordCodec[IndexTerm]
  implicit val TupleT = FieldType.RecordCodec[IndexTuple]

  // FIXME: this is hardly a paragon of pulchritude - clean it up
  implicit val IdxValueT = FieldType[IndexValue]("IndexValue") { idx =>
    MapV(
      "tuple" -> TupleT.encode(idx.tuple).get,
      "validTS" -> FieldType.TimestampT.encode(idx.ts.validTS).get,
      "action" -> SetActionT.encode(idx.action).get,
      "transactionTS" -> FieldType.TimestampT.encode(idx.ts.transactionTS).get
    )
  } { case m: MapV =>
    val tuple = TupleT.decode(m.get(List("tuple")), Queue.empty).toOption.get

    // TODO: remove support for "ts"
    val validTSOpt = m.get(List("ts")) orElse m.get(List("validTS"))
    val validTS = FieldType.TimestampT.decode(validTSOpt, Queue.empty).toOption.get
    val action = SetActionT.decode(m.get(List("action")), Queue.empty).toOption.get

    // default to Timestamp.Epoch for encoded values lacking a txnTS; it is maximally
    // inclusive
    val txnTS = FieldType.TimestampT
      .decode(m.get(List("transactionTS")), Queue.empty)
      .toOption getOrElse Timestamp.Epoch

    IndexValue(tuple, Resolved(validTS, txnTS), action)
  }

  implicit val IndexKeyT = FieldType.RecordCodec[IndexKey]
  implicit val IndexCursorT = FieldType.RecordCodec[IndexCursor]
}

trait Paginator[Key] {
  import Paginator._

  type RowID
  type Col
  type Cursor <: Paginator.Cursor

  type Row = Iterable[(RowID, Col)]

  private[this] val log = getLogger

  sealed abstract class Result

  /** Some data may have been processed, but there is more work to
    * do. Continue from cursor on the next iteration.
    */
  case class Continue(cursor: Cursor, pageSize: Int, groupSize: Int) extends Result

  /** No more data is available.
    */
  case object Finished extends Result

  /** An error or timeout occurred while processing from the current
    * cursor. Retry with the same cursor and a reduced page/group
    * size.
    */
  case class Retry(pageSize: Int, groupSize: Int) extends Result

  /** Define along with the implementation of `toCursor()` to the
    * concrete cursor type.
    */
  implicit val CursorT: FieldType[Cursor]

  def name: String

  /** The maximum number of columns per page yielded to iteratees
    * using this Paginator.
    */
  def maxPageSize: Int = 512

  protected def toCursor(col: (RowID, Col)): Cursor

  protected def paged(
    key: Key,
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    pageSize: Int,
    selector: Selector): PagedQuery[Row]

  /** Processes a page of size `pageSize` in groups of `groupSize`
    * using a `RangeIteratee`, starting at `key` and an optional offset
    * `from`.
    *
    * See the comments on the variants of Result for caller expectations.
    *
    * Throws `PaginatorProgressException` if reducing the page and
    * group size will not result in further progress.
    */
  final def paginate(
    gauge: String,
    key: Key,
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    iter: RangeIteratee[RowID, Col],
    pageSize: Option[Int] = None,
    groupSize: Option[Int] = None,
    selector: Selector = Selector.All): Query[Result] = {
    val perPage = pageSize getOrElse maxPageSize
    val perGroup = perPage min groupSize.getOrElse(maxPageSize)

    Query.stats flatMap { stats =>
      getPage(key, snapshotOverride, from, perPage, selector) flatMap {
        case (row, cursor) =>
          val pages = row grouped perGroup
          val timing = Timing.start

          process(gauge, iter, pages, perPage) map {
            case (cur, processed, newPageSize) =>
              val size = if (newPageSize == perPage) {
                (perPage + SizeInterval) min maxPageSize
              } else {
                newPageSize
              }

              (cur.orElse(cursor), size, timing.elapsed / (processed max 1))
          }
      } map {
        case (Some(cur), pageSize, meanTime) =>
          stats.timing(s"$gauge.Transaction.Time", meanTime.toMillis)

          val newGroupSize = if (meanTime < LatencyTarget) {
            (perGroup + SizeInterval) min pageSize
          } else {
            (perGroup - SizeInterval) max MinPageSize
          }

          Continue(cur, pageSize, newGroupSize)
        case (None, _, meanTime) =>
          stats.timing(s"$gauge.Transaction.Time", meanTime.toMillis)
          Finished
      } recoverWith { case _: TimeoutException | _: MaxQueryWidthExceeded =>
        newPageSize(gauge, perPage) map { newPageSize =>
          Retry(newPageSize, perGroup min newPageSize)
        }
      }
    }
  }

  private def newPageSize(gauge: String, pageSize: Int): Query[Int] =
    Query.stats map { stats =>
      val newPageSize = (pageSize / 2) max MinPageSize

      // If we already hit the minimum page size, there's nothing
      // we can do. Bail and yell about it.
      if (newPageSize == pageSize) {
        throw new PaginatorProgressException
      } else {
        stats.incr(s"$gauge.Backoffs")
        log.info(s"Backing off page size to $newPageSize due to timeout ($name)")
        newPageSize
      }
    }

  /** Executes the iteratee on each iterable yielded by pages,
    * returning the next cursor (if any), the number of pages
    * processed, and an adjusted page size for the next iteration.
    */
  private def process(
    gauge: String,
    iter: RangeIteratee[RowID, Col],
    pages: Iterator[Iterable[(RowID, Col)]],
    pageSize: Int,
    processed: Int = 0): Query[(Option[Cursor], Int, Int)] =
    Query.stats flatMap { stats =>
      if (pages.hasNext) {
        val page = pages.next()

        iter.run(PagedQuery(page)) flatMap { _ =>
          stats.incr(s"$gauge.Pages.Processed")
          process(gauge, iter, pages, pageSize, processed + 1)
        } recoverWith {
          case _: ContentionException | _: TimeoutException |
              _: UnretryableException =>
            newPageSize(gauge, pageSize) map { newPageSize =>
              (page.headOption map toCursor, processed, newPageSize)
            }
        }
      } else {
        Query.value((None, processed, pageSize))
      }
    }

  /** Returns a page of the row given by `key` of up to `pageSize`
    * columns, optionally starting at `from`, and a cursor to fetch
    * the next page, if any additional columns exist.
    */
  def getPage(
    key: Key,
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    pageSize: Int,
    selector: Selector): Query[(Row, Option[Cursor])] = {
    val padded = pageSize + 1
    val pageQ =
      paged(key, snapshotOverride, from, padded, selector).takeT(padded).flattenT

    pageQ map { elems =>
      val page = elems
      val next = page.lift(pageSize) map toCursor
      (page.take(pageSize), next)
    }
  }
}

trait VersionPaginator[K] extends Paginator[K] {
  type RowID = (ScopeID, DocID)
  type Cursor = Paginator.VersionCursor

  implicit val CursorT = Paginator.VersionCursorT
}

trait VersionRowPaginator extends VersionPaginator[(ScopeID, DocID)] {

  type Col = Version

  final protected def toCursor(col: (RowID, Col)) = {
    val ((scope, id), v) = col
    Paginator.VersionCursor(scope, id, v.versionID, v.ts.transactionTS.micros)
  }

  final protected def paged(
    key: (ScopeID, DocID),
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    pageSize: Int,
    selector: Selector): PagedQuery[Row] = {

    if (snapshotOverride.isDefined) {
      getLogger.warn("Ignoring specified snapshot override in VersionRowPaginator")
    }

    val (scope, id) = key
    val cursor = from map { _.version }

    val versions = RuntimeEnv.Default
      .Store(scope)
      .versions(
        id,
        cursor getOrElse VersionID.MaxValue,
        VersionID.MinValue,
        pageSize,
        reverse = false) mapValuesT {
      (key, _)
    }

    Query.withBytesReadDelta(versions) flatMap { case (vs, delta) =>
      Query.incrDocuments(delta getOrElse 0) map { _ => vs }
    }
  }
}

trait VersionSegmentMVTPaginator extends VersionPaginator[Segment] {

  type Col = Version

  override val maxPageSize: Int = 16384

  final protected def toCursor(col: (RowID, Col)) = {
    val ((scope, id), v) = col
    Paginator.VersionCursor(scope, id, v.versionID, v.ts.transactionTS.micros)
  }

  final protected def paged(
    key: Segment,
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    pageSize: Int,
    selector: Selector): PagedQuery[Row] = {

    if (snapshotOverride.isDefined) {
      getLogger.warn("Ignoring specified snapshot override in VersionRowPaginator")
    }

    Query.repo flatMap { repo =>
      val bounds = from match {
        case None => ScanBounds(key)
        case Some(from) =>
          val rowKey =
            Unpooled.wrappedBuffer(Tables.Versions.rowKey(from.scope, from.id))

          val loc = repo.keyspace.locateKey(Unpooled.wrappedBuffer(rowKey))
          require(key.contains(loc), s"segment $key does not contain $loc! ($from)")

          ScanBounds(rowKey, key.right)
      }

      Store
        .docScanRaw(bounds, Unpooled.EMPTY_BUFFER, selector, pageSize)
        .flatMapPagesT { doc =>
          val maxValue = from.fold(VersionID.MaxValue) { f =>
            if (f.scope == doc.scopeID && f.id == doc.docID) {
              f.version
            } else {
              VersionID.MaxValue
            }
          }

          val versions = RuntimeEnv.Default
            .Store(doc.scopeID)
            .versions(
              doc.docID,
              maxValue,
              VersionID.MinValue,
              maxPageSize,
              false) mapValuesT { v =>
            ((v.parentScopeID, v.id), v)
          }

          // Don't count read ops on the 'spine', but accumulate them
          // for each 'leaf' document.
          Query.withBytesReadDelta(versions) flatMap { case (vs, delta) =>
            Query.incrDocuments(delta getOrElse 0) map { _ => vs }
          }
        }
    }
  }
}

/** Iterates over the latest versions (or as of an optional snapshot time) of
  * all documents matching the selector.
  *
  * Warning: This scanner does not apply doc GC rules/rewrites. GC roots will
  * not be rewritten, and if the chosen snapshot override is below the MVT a
  * returned document, this scanner will violate doc GC rules.
  */
trait SnapshotSegmentMVTPaginator extends VersionPaginator[Segment] {

  type Col = Version

  override val maxPageSize: Int = 16384

  final protected def toCursor(col: (RowID, Col)) = {
    val ((scope, id), v) = col
    Paginator.VersionCursor(scope, id, v.versionID, v.ts.transactionTS.micros)
  }

  final protected def paged(
    key: Segment,
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    pageSize: Int,
    selector: Selector): PagedQuery[Row] =
    Query.repo flatMap { repo =>
      val bounds = from match {
        case None => ScanBounds(key)
        case Some(from) =>
          val rowKey =
            Unpooled.wrappedBuffer(Tables.Versions.rowKey(from.scope, from.id))

          val loc = repo.keyspace.locateKey(Unpooled.wrappedBuffer(rowKey))
          require(key.contains(loc), s"segment $key does not contain $loc! ($from)")

          ScanBounds(rowKey, key.right)
      }

      val scan = Store
        .docScan(
          (sc, coll) => RuntimeEnv.Default.getCollection(sc, coll).mapT(_.Schema),
          bounds,
          Unpooled.EMPTY_BUFFER,
          selector,
          pageSize,
          snapshotOverride
        )
        .mapValuesT(v => ((v.parentScopeID, v.id), v))

      // FIXME: this doesn't work correctly
      // Query.withBytesReadDelta(scan) flatMap { case (vs, delta) =>
      //   Query.incrDocuments(delta getOrElse 0) map { _ => vs }
      // }
      scan
    }
}

trait SortedRowPaginator extends Paginator[(IndexConfig, Vector[IndexTerm])] {

  type RowID = IndexKey
  type Col = IndexValue
  type Cursor = Paginator.IndexCursor

  implicit val CursorT = Paginator.IndexCursorT

  final protected def toCursor(col: (RowID, Col)) = {
    val (key, value) = col
    val ts = value.ts.resolve(Timestamp.Epoch) // Max. inclusive in case unresolved.
    Paginator.IndexCursor(key.scope, key, value.copy(ts = ts))
  }

  final protected def paged(
    key: (IndexConfig, Vector[IndexTerm]),
    snapshotOverride: Option[Timestamp],
    from: Option[Cursor],
    pageSize: Int,
    selector: Selector) = {

    if (snapshotOverride.isDefined) {
      getLogger.warn("Ignoring specified snapshot override in VersionRowPaginator")
    }

    val (cfg, terms) = key
    val cursor = from map { c => c.value }
    val idxKey = IndexKey(cfg.scopeID, cfg.id, terms)

    val events = Store.sortedIndex(
      cfg,
      terms,
      cursor getOrElse IndexValue.MinValue,
      IndexValue.MaxValue,
      pageSize,
      ascending = true) mapValuesT { (idxKey, _) }

    Query.withBytesReadDelta(events) flatMap { case (evs, delta) =>
      Query.addSets(1, cfg.partitions.toInt, delta getOrElse 0) map { _ =>
        evs
      }
    }
  }
}
