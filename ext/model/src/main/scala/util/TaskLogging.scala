package fauna.model.util

import fauna.atoms.GlobalID
import fauna.codex.json.JSValue
import fauna.codex.json2._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.{ Database, Task }
import fauna.repo.cassandra.CassandraService
import fauna.stats.QueryMetrics
import io.netty.buffer.ByteBufAllocator

object TaskLogging {
  lazy val log = getLogger("tasks")

  /** Top-level key containing the replica name of the host which
    * executed the task.
    */
  final val ReplicaField = JSON.Escaped("replica")

  /** Top-level key containing the ID of the executed task.
    */
  final val IDField = JSON.Escaped("task_id")

  /** Top-level key containing the tenant database which caused the
    * task execution.
    */
  final val ScopeField = JSON.Escaped("scope_db")

  /** Top-level key containing the time of the task execution in
    * ISO-8601 format.
    */
  final val TSField = JSON.Escaped("ts")
}

trait TaskLogging {
  import TaskLogging._

  protected def logTask(
    task: Task,
    auth: JSValue,
    metrics: QueryMetrics,
    globalIDPath: Seq[GlobalID]): Unit = {
    val alloc = ByteBufAllocator.DEFAULT
    val buf = alloc.ioBuffer

    try {
      val out = JSONWriter(buf)

      out.writeObjectStart()

      out.writeObjectField(
        ReplicaField,
        out.writeString(CassandraService.instance.replicaName))
      out.writeObjectField(IDField, out.writeNumber(task.id.toLong))
      out.writeObjectField(TSField, out.writeString(Clock.time.toString))

      out.writeObjectField(
        ScopeField, {
          out.writeDelimiter()
          auth.writeTo(buf, false)
        })

      out.writeObjectField(
        CommonLogging.GlobalIDPathField, {
          out.writeArrayStart()
          globalIDPath.map(Database.encodeGlobalID(_)) foreach { out.writeString(_) }
          out.writeArrayEnd()
        })

      CommonLogging.Stats.writeStats(out, metrics, httpCode = None) { _ =>
        // no extra stats to log
        ()
      }

      task.trace foreach { traceCtx =>
        CommonLogging.Trace.writeTrace(out, traceCtx, isTasksLog = true)
      }

      out.writeObjectEnd()

      log.info(buf.toUTF8String)
    } finally {
      buf.release()
    }
  }
}
