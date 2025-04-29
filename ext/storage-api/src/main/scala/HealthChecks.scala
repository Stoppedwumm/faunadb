package fauna.storage.api

import fauna.atoms.{ HostID, ScopeID }
import fauna.codex.cbor.CBOR
import fauna.exec.ImmediateExecutionContext
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Storage => _, _ }
import fauna.storage.ops.Write
import io.netty.buffer.Unpooled
import scala.concurrent.{ ExecutionContext, Future }

object HealthChecks {

  implicit val Codec = CBOR.TupleCodec[HealthChecks]

  final case class Result(
    values: Vector[(HostID, Timestamp)],
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

  val Root = "HealthCheckRoot"
}

/** Get all health checks. */
final case class HealthChecks(snapshotTS: Timestamp)
    extends Read[HealthChecks.Result] {
  import HealthChecks._

  type Key = Tables.HealthChecks.Key

  def name = "HealthChecks"

  override def toString = s"HealthChecks(snapshotTS=$snapshotTS)"

  def codec = Result.Codec

  def scopeID = ScopeID.RootID

  def from: Predicate = Root
  def rowKey = from.rowKey.bytes
  def columnFamily = Tables.HealthChecks.CFName

  def isRelevant(write: Write): Boolean = false

  private def parseValue(v: Value[Key]): (HostID, Timestamp) = {
    val id = CBOR.parse[HostID](v.key._2)
    val ts = if (v.data.isReadable) {
      CBOR.parse[Timestamp](v.data)
    } else {
      Timestamp.Epoch
    }

    (id, ts)
  }

  private def cellToHealthCheck(cell: Cell) = {
    val prefix = Tables.HealthChecks.Schema.nameComparator.bytesToCValues(cell.name)
    val pred = Predicate(from.rowKey, prefix)
    parseValue(new Value[Key](pred, cell.value, cell.ts))
  }

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[Result] = {

    val readF = read(ctx.engine, priority, writes, deadline)

    implicit val iec = ImmediateExecutionContext
    readF flatMap { case (page, merger) =>
      merger.flatten() map { result =>
        val bytesRead = rowKey.readableBytes + result.bytesEmitted

        Result(
          result.values,
          page.lastModifiedTS,
          ctx.engine.appliedTimestamp,
          bytesRead
        )
      }
    }
  }

  private def read(
    engine: StorageEngine,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext) = {
    require(writes forall { isRelevant(_) }, "irrelevant writes found")

    val pageF =
      RowPaginator(
        engine,
        columnFamily,
        rowKey,
        Unpooled.EMPTY_BUFFER,
        Unpooled.EMPTY_BUFFER,
        1024, // Based on the old API's page size.
        Order.Descending,
        priority,
        snapshotTS,
        deadline
      )

    implicit val iec = ImmediateExecutionContext
    pageF map { page =>
      val merger =
        RowMerger(columnFamily, page.cells, writes)
          .mapT { cellToHealthCheck(_) }

      (page, merger)
    }
  }
}
