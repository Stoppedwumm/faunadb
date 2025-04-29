package fauna.storage.api

import fauna.atoms.{ HostID, SchemaVersion, ScopeID }
import fauna.codex.cbor.CBOR
import fauna.lang.{ TimeBound, Timestamp }
import fauna.scheduler.PriorityGroup
import fauna.storage.{ Row, Tables }
import fauna.storage.ops.Write
import scala.concurrent.{ ExecutionContext, Future }

object SchemaVersionSnapshot {

  implicit val Codec = CBOR.TupleCodec[SchemaVersionSnapshot]

  /** The SchemaVersion CF is a bit strange. When the schema version is written,
    * an empty row for each scope ID is written into the SchemaVersion CF. This
    * has the side effect of updating the row timestamps, which is where the
    * schema version is actually stored. So, reading that empty row effectively
    * returns (Unit, Timestamp), where the Timestamp is the last modified time
    * (ie, the schema version).
    *
    * So, the result of all this is that we can just build a SchemaVersion off
    * of the `lastModifiedTime` on this class.
    */
  final case class Result(
    lastModifiedTS: Timestamp,
    lastAppliedTS: Timestamp,
    bytesRead: Int)
      extends Read.Result {
    def schemaVersion: Option[SchemaVersion] =
      Option.when(lastModifiedTS != Timestamp.Epoch) {
        SchemaVersion(lastModifiedTS)
      }
  }

  object Result {
    implicit val Codec = CBOR.TupleCodec[Result]
  }

}

/** SchemaVersionSnapshot reads the version of a schema, which is the last modified
  * timestamp of the schema's row in C*.
  */
final case class SchemaVersionSnapshot(scopeID: ScopeID, snapshotTS: Timestamp)
    extends Read[SchemaVersionSnapshot.Result] {
  import SchemaVersionSnapshot._

  def name = "SchemaVersion"

  override def toString: String =
    s"SchemaVersionSnapshot(scopeID=$scopeID, snapshotTS=$snapshotTS)"

  def codec = Result.Codec

  lazy val rowKey = Tables.SchemaVersions.rowKey(scopeID)
  def columnFamily = Tables.SchemaVersions.CFName

  def isRelevant(write: Write): Boolean = false

  private[api] def run(
    source: HostID,
    ctx: Storage.Context,
    priority: PriorityGroup,
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[Result] = {
    ctx.engine.readRow(snapshotTS, rowKey, Vector.empty, priority, deadline) map {
      case Row(_, ts) => Result(ts, ctx.engine.appliedTimestamp, 0)
    }
  }
}
