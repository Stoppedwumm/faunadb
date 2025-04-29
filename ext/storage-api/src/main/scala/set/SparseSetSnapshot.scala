package fauna.storage.api.set

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang._
import fauna.lang.syntax._
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.api._
import fauna.storage.index._
import fauna.storage.ops.Write
import scala.collection.immutable.SortedSet
import scala.concurrent.{ ExecutionContext, Future }

object SparseSetSnapshot {

  // To prevent bad queries from crushing the node, a sparse snapshot won't ever
  // return more than this many results.
  val MaxResults = 65536

  implicit val Codec = CBOR.TupleCodec[SparseSetSnapshot]

  /** A sparse set snapshot result, which is a snapshot of a few tuples in
    * sorted-index-order or reversed-sorted-index-order.
    */
  final case class Result(
    values: Vector[Element.Live],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }
}

/** A sparse snapshot of a set at a particular valid time.
  *
  * A sparse set snapshot is a set snapshot of a particular set of tuples.
  */
final case class SparseSetSnapshot(
  scopeID: ScopeID,
  indexID: IndexID,
  terms: Vector[Term],
  slices: Vector[(IndexValue, IndexValue)],
  snapshotTS: Timestamp,
  minValidTS: MVTMap = MVTMap.Default,
  validTS: Option[Timestamp] = None,
  order: Order = Order.Descending)
    extends Read[SparseSetSnapshot.Result] {
  import SparseSetSnapshot._

  require(slices.nonEmpty, "sparse snapshot requires at least one slice")
  require(
    minValidTS.all forall { _ <= snapshotTS },
    "all min. valid times must be greater than snapshot time"
  )

  def codec = Result.Codec

  def name = "Set.Snapshot.Sparse"

  lazy val rowKey = Tables.Indexes.rowKey(scopeID, indexID, terms map { _.unwrap })

  override def toString: String =
    s"SparseSetSnapshot($scopeID, $indexID, $terms, slices=$slices, " +
      s"snapshotTS=$snapshotTS, minValidTS=$minValidTS, validTS=$validTS, order=$order)"

  def columnFamily = Tables.SortedIndex.CFName

  lazy val subSnapshots =
    slices map { case (from, to) =>
      SetSnapshot(
        scopeID,
        indexID,
        terms,
        SetSnapshot.Cursor(from.tuple, to.tuple, order),
        snapshotTS,
        minValidTS,
        validTS,
        MaxResults
      )
    }

  def isRelevant(write: Write): Boolean =
    subSnapshots exists { op => op.isRelevant(write) }

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[Result] = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    implicit val ordering = order match {
      case Order.Ascending  => Element.Live.ByValueOrdering
      case Order.Descending => Element.Live.ByValueOrdering.reverse
    }
    val subResFs = subSnapshots map { op =>
      op.run(source, ctx, priority, writes filter op.isRelevant, deadline)
    }
    val resF = subResFs.accumulate(
      (SortedSet.empty, Timestamp.Epoch, Timestamp.MaxMicros, 0)) {
      case ((valSet, lmt, lat, bs), snapRes) =>
        (
          valSet ++ snapRes.values,
          lmt max snapRes.lastModifiedTS,
          lat min snapRes.lastAppliedTS,
          bs + snapRes.bytesRead)
    }
    resF map { case (values, lastModifiedTS, lastAppliedTS, bytesRead) =>
      Result(values.toVector, lastModifiedTS, lastAppliedTS, bytesRead)
    }
  }
}
