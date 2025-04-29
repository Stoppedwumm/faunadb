package fauna.model.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.tasks._
import fauna.prop._
import fauna.prop.test.PropSpec
import fauna.repo.{ IndexConfig => _, _ }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.repo.test.{ CassandraHelper, DefaultExtractor, IndexConfig }
import fauna.stats.{ QueryMetrics, StatsRequestBuffer }
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.Selector
import org.scalatest.matchers.should.Matchers

class PaginatorSpec extends PropSpec(5, 200) with Matchers {

  val MaxPageSize = 512

  val ctx = CassandraHelper.context("model")

  val collection = CollectionID(1024)

  def newScope = Prop.const(ScopeID(ctx.nextID()))
  def ID(id: Long) = DocID(SubID(id), collection)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  abstract class Collector[T, U, R] extends RangeIteratee[T, U] {

    private[this] val cols = Seq.newBuilder[R]

    def toCol(col: U): R

    def apply(key: T) =
      Query.some(colIter())

    def result: Seq[R] = cols.result()

    private def colIter(): ColIteratee[U] =
      ColIteratee {
        case None => Query.none
        case Some(page) =>
          cols ++= page map toCol
          Query.some(colIter())
      }
  }

  final class VersionCollector
      extends Collector[(ScopeID, DocID), Version, Timestamp] {

    def toCol(col: Version) = col.ts.validTS
  }

  final class IndexCollector extends Collector[IndexKey, IndexValue, Timestamp] {

    def toCol(col: IndexValue) = col.ts.validTS
  }

  final class VersionsByRow extends VersionRowPaginator {
    val name = "VersionsByRow"

    override val maxPageSize = MaxPageSize
  }

  final class VersionsBySegment extends VersionSegmentMVTPaginator {
    val name = "VersionsBySegment"

    override val maxPageSize = MaxPageSize
  }

  final class IndexByRow extends SortedRowPaginator {
    val name = "IndexByRow"

    override val maxPageSize = MaxPageSize
  }

  once("versions by row") {
    for {
      scope <- newScope
      cols  <- Prop.int(MaxPageSize to MaxPageSize * 2)
    } {
      val id = ID(1)

      for {
        ts <- 1 to cols
      } yield {
        val q = if (Prop.boolean.sample) {
          Store.insertCreateUnmigrated(scope, id, TS(ts), Data.empty)
        } else {
          Store.insertDeleteUnmigrated(scope, id, TS(ts))
        }

        ctx ! q
      }

      val iter = new VersionCollector
      val pager = new VersionsByRow
      val stats = new StatsRequestBuffer

      var cur: Option[pager.Cursor] = None

      do {
        val q = pager.paginate("Versions", (scope, id), None, cur, iter) map {
          case pager.Continue(cur, _, _) => Some(cur)
          case pager.Finished            => None
          case _: pager.Retry            => cur
        }
        cur = ctx.withStats(stats) ! q
      } while (cur.nonEmpty)

      iter.result should have size (cols)

      // Each page is a separate doc read.
      val json = stats.toJson
      (json / QueryMetrics.ReadDocument).as[Int] should be(2)
    }
  }

  once("versions by segment") {
    for {
      scope <- newScope
      cols  <- Prop.int(MaxPageSize to MaxPageSize * 2)
    } {
      val id = ID(1)
      val schema = CollectionSchema.empty(scope, collection)

      for {
        ts <- 1 to cols
      } yield {
        val q = if (Prop.boolean.sample) {
          Store.insertCreate(schema, id, TS(ts), Data.empty)
        } else {
          Store.insertDelete(schema, id, TS(ts))
        }

        ctx ! q
      }

      val iter = new VersionCollector
      val pager = new VersionsBySegment
      val selector = Selector.from(scope, Seq(collection))
      val stats = new StatsRequestBuffer

      var cur: Option[pager.Cursor] = None

      do {
        val q = pager.paginate(
          "Versions",
          Segment.All,
          None,
          cur,
          iter,
          selector = selector) map {
          case pager.Continue(cur, _, _) => Some(cur)
          case pager.Finished            => None
          case _: pager.Retry            => cur
        }
        cur = ctx.withStats(stats) ! q
      } while (cur.nonEmpty)

      iter.result should have size (cols)

      // Each page is a separate doc read.
      val json = stats.toJson
      (json / QueryMetrics.ReadDocument).as[Int] should be(2)
    }
  }

  once("index by row") {
    for {
      scope <- newScope
      cols  <- Prop.int(MaxPageSize to MaxPageSize * 2)
    } {
      val id = ID(1)
      val cfg = IndexConfig(
        scope,
        IndexID(1024),
        collection,
        Vector.empty,
        Vector(DefaultExtractor(List("ts"))))
      val schema =
        CollectionSchema.empty(scope, collection).copy(indexes = List(cfg))

      for {
        _ <- 1 to cols
      } yield {
        ctx ! Store.insert(schema, id, Data.empty)
      }

      val iter = new IndexCollector
      val pager = new IndexByRow
      val stats = new StatsRequestBuffer

      var cur: Option[pager.Cursor] = None
      var pages: Int = 0

      do {
        pages += 1
        val q = pager.paginate("Indexes", (cfg, Vector.empty), None, cur, iter) map {
          case pager.Continue(cur, _, _) => Some(cur)
          case pager.Finished            => None
          case _: pager.Retry            => cur
        }
        cur = ctx.withStats(stats) ! q
      } while (cur.nonEmpty)

      // single "add" for the first version, then an "add"/"remove"
      // pair for each subsequent
      iter.result should have size ((cols * 2) - 1)

      // Each page is a separate set read.
      val json = stats.toJson
      (json / QueryMetrics.ReadSet).as[Int] should be(pages)
    }
  }
}
