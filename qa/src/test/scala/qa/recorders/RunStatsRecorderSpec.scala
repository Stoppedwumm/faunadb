package fauna.qa.test.recorders

import fauna.api.test.APISpecMatchers
import fauna.atoms.APIVersion
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.net.http._
import fauna.prop.api._
import fauna.qa._
import fauna.qa.recorders._
import java.io.File
import org.HdrHistogram._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Random

class RunStatsRecorderSpec
    extends AnyFreeSpec
    with Matchers
    with DefaultQueryHelpers
    with APIResponseHelpers
    with BeforeAndAfterAll
    with Eventually
    with APISpecMatchers {

  private var client: FaunaDB.Client = _

  override protected def beforeAll() = {
    val CoreLauncher.IDAndClient(_, cl) = CoreLauncher.launchOneNodeCluster(APIVersion.Default.toString)
    client = cl

    eventually(timeout(15.seconds), interval(1.second)) {
      val res = client.api.get("/ping?scope=write", FaunaDB.rootKey)
      res should respond(OK)
    }
  }

  override protected def afterAll() = CoreLauncher.terminateAll()

  val startTime = Clock.time
  val overall = HostStat(Host("Overall", "Overall"))

  val readings = Seq(
    (HostStat(Host("test-replica", "addr-1")), 100),
    (HostStat(Host("test-replica", "addr-2")), 1000)
  ) map {
    case (hostStat, limit) =>
      val latencies = new Histogram(StatsSnapshot.SignificantDigits)
      val snap = System.nanoTime
      (0 until 100) foreach { _ =>
        latencies.recordValue(Random.nextLong(limit))
      }
      overall.add(snap, latencies, 0)
      hostStat.add(snap, latencies, 0)
      hostStat
  }

  "FaunaRunStatsRecorder" - {
    "records stats to Fauna" in {
      val hp = s"${client.cfg.host}:8443"
      val faunaClient = HttpClient(hp)
      val faunaRecorder = new FaunaRecorder(hp, "secret")
      val recorder = new FaunaRunStatsRecorder(startTime, faunaRecorder)

      val delQ = MapF(
        Lambda("v" -> DeleteF(Var("v"))),
        Paginate(Documents(FaunaRunStatsRecorder.SnapshotsRef))
      )
      faunaClient.query(delQ, "secret").json

      val recordTime = Clock.time
      Await.result(recorder.record(recordTime, overall +: readings), 5.seconds)

      val getQ = MapF(
        Lambda("v" -> Get(Var("v"))),
        Paginate(Documents(FaunaRunStatsRecorder.SnapshotsRef))
      )
      val raw =
        (faunaClient.query(getQ, "secret").resource / "data").as[JSArray].value

      // This is gross but gives us a list sorted the way we expect
      val entries = {
        val vs = Vector.newBuilder[JSValue]
        Seq("Overall", "addr-1", "addr-2") foreach { address =>
          raw foreach { r =>
            if ((r.as[JSObject] / "data" / "address").as[String] == address) {
              vs += r
            }
          }
        }
        vs.result().iterator
      }

      def checkEntry(
        replica: String,
        addr: String,
        latencies: Histogram,
        value: JSValue
      ): Unit = {
        val entry = (value.as[JSObject] / "data").as[JSObject]

        Timestamp.parse((entry / "startTS" / "@ts").as[String]) should equal(
          startTime
        )
        Timestamp.parse((entry / "timestamp" / "@ts").as[String]) should equal(
          recordTime
        )
        (entry / "replica").as[String] should equal(replica)
        (entry / "address").as[String] should equal(addr)
        RunStatsRecorder.Percentiles foreach { p =>
          (entry / "latencies" / p.toString).as[Long] should equal(
            latencies.getValueAtPercentile(p)
          )
          // no throughput data, but the readings should still be there.
          (entry / "reqsPerSec" / p.toString).as[Long] should equal(0)
        }
      }

      checkEntry("Overall", "Overall", overall.latencies, entries.next())
      readings foreach { stat =>
        checkEntry(stat.host.dc, stat.host.addr, stat.latencies, entries.next())
      }
    }
  }

  "CSVRunStatsRecorder" - {
    "records stats to a CSV file" in {
      val f = File.createTempFile("stats-", ".csv")
      f.deleteOnExit()

      val recorder = new CSVRunStatsRecorder(startTime, f.toPath.toString)

      val recordTime = Clock.time
      Await.result(recorder.record(recordTime, overall +: readings), 5.seconds)

      def checkLine(
        replica: String,
        addr: String,
        latencies: Histogram,
        line: String
      ): Unit = {
        val fields = line.split(',').iterator
        Timestamp.parse(fields.next()) should equal(startTime)
        Timestamp.parse(fields.next()) should equal(recordTime)
        fields.next() should equal(replica)
        fields.next() should equal(addr)
        RunStatsRecorder.Percentiles foreach { p =>
          fields.next().toLong should equal(latencies.getValueAtPercentile(p))
        }
        // no throughput data, but the readings should still be there.
        RunStatsRecorder.Percentiles foreach { _ =>
          fields.next().toLong should equal(0)
        }
      }

      val lines = Source.fromFile(f).getLines()
      lines.next() // drop the header line

      checkLine("Overall", "Overall", overall.latencies, lines.next())
      readings foreach { stat =>
        checkLine(stat.host.dc, stat.host.addr, stat.latencies, lines.next())
      }
    }
  }
}
