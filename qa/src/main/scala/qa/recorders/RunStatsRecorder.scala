package fauna.qa.recorders

import fauna.codex.json._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.prop.api.DefaultQueryHelpers
import java.io.{ File, FileOutputStream }
import scala.concurrent.Future
import scala.util.Try

object RunStatsRecorder {

  val Percentiles =
    Array[Double](50.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.999, 99.9999)
}

/**
  * RunStatsRecorder receives a collection of stats from a particular snapshot in time
  * aggregates them per host and overall then writes them out. The record function is
  * synchronized, subclasses can expect calls to `writeOut` to be protected from having
  * underlying structures mutated.
  */
sealed abstract class RunStatsRecorder {

  def record(ts: Timestamp, stats: Seq[HostStat]): Future[Unit]
  def close(): Future[Unit]
}

// Do nothing with stats
object NullRunStatsRecorder extends RunStatsRecorder {

  def record(ts: Timestamp, stats: Seq[HostStat]): Future[Unit] = Future.unit
  def close() = Future.unit
}

object FaunaRunStatsRecorder extends DefaultQueryHelpers {
  val CollectionName = "snapshots"
  val SnapshotsRef = ClassRef(CollectionName)
}

/**
  * Record stats to FaunaDB. See `statToEntry` for the schema used to record
  * stats.
  */
class FaunaRunStatsRecorder(startTime: Timestamp, rec: FaunaRecorder)
    extends RunStatsRecorder
    with DefaultQueryHelpers {
  import FaunaRunStatsRecorder._

  rec.ensureCollection(CollectionName)

  def close() = Future.unit

  private def statToEntry(ts: Timestamp, stat: HostStat): JSObject = {
    val reqsPerSec = JSObject.newBuilder
    val latencies = JSObject.newBuilder
    RunStatsRecorder.Percentiles foreach { p =>
      reqsPerSec += (p.toString -> stat.reqsPerSec.getValueAtPercentile(p))
      latencies += (p.toString -> stat.latencies.getValueAtPercentile(p))
    }

    MkObject(
      "startTS" -> Time(startTime.toString),
      "timestamp" -> Time(ts.toString),
      "replica" -> stat.host.dc,
      "address" -> stat.host.addr,
      "reqsPerSec" -> JSObject("object" -> reqsPerSec.result()),
      "latencies" -> JSObject("object" -> latencies.result())
    )
  }

  def record(ts: Timestamp, stats: Seq[HostStat]): Future[Unit] = {
    val creates = stats map { stat =>
      CreateF(SnapshotsRef, MkObject("data" -> statToEntry(ts, stat)))
    }
    rec.query(Do(creates: _*)).unit
  }
}

object CSVRunStatsRecorder {
  val NewLine = "\n".toUTF8Bytes

  val Header = {
    val out = Vector.newBuilder[String]
    out += "startTS"
    out += "timestamp"
    out += "replica"
    out += "address"
    RunStatsRecorder.Percentiles foreach { p =>
      out += s"p$p latency"
    }
    RunStatsRecorder.Percentiles foreach { p =>
      out += s"p$p throughput"
    }
    out.result().mkString(",").toUTF8Bytes
  }
}

/**
  * Write stats to a CSV file. See `Header` in the companion object for schema.
  */
class CSVRunStatsRecorder(startTime: Timestamp, path: String)
    extends RunStatsRecorder {
  import CSVRunStatsRecorder._

  private[this] val outFile = {
    val f = new FileOutputStream(new File(path))
    f.write(Header)
    f.write(NewLine)
    f.flush()

    getLogger().info(s"Sending stats to file: $path")

    f
  }

  def close() = Future.fromTry(Try(outFile.close()))

  private def writeStat(ts: Timestamp, stat: HostStat): Unit = {
    val out = Vector.newBuilder[String]
    out += startTime.toString
    out += ts.toString
    out += stat.host.dc
    out += stat.host.addr
    RunStatsRecorder.Percentiles foreach { p =>
      out += stat.latencies.getValueAtPercentile(p).toString
    }
    RunStatsRecorder.Percentiles foreach { p =>
      out += stat.reqsPerSec.getValueAtPercentile(p).toString
    }

    outFile.write(out.result().mkString(",").toUTF8Bytes)
    outFile.write(NewLine)
    outFile.flush()
  }

  def record(ts: Timestamp, stats: Seq[HostStat]): Future[Unit] = {
    stats foreach { writeStat(ts, _) }
    Future.unit
  }
}
