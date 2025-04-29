package fauna.qa.recorders

import fauna.prop.api.DefaultQueryHelpers
import fauna.codex.cbor.CBOR
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.net.http._
import fauna.qa._
import java.net.URL
import org.HdrHistogram._
import scala.concurrent.duration._
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

// Serializable structure to hold stats info from Workers
case class StatsSnapshot(nanoTime: Long, latencies: Histogram, errors: Long) {

  override def toString: String =
    s"StatsSnapshot($nanoTime, Histogram(size:${latencies.getTotalCount}), $errors)"
}

object StatsSnapshot {
  val SignificantDigits = 5
  implicit val Codec = CBOR.RecordCodec[StatsSnapshot]
}

/**
  * HostStat is an internal structure used to hold aggregate stats per host.
  */
case class HostStat(host: Host) {
  val latencies = new Histogram(StatsSnapshot.SignificantDigits)
  val reqsPerSec = new Histogram(StatsSnapshot.SignificantDigits)

  var lastSnap: Long = 0
  var errors: Long = 0

  def add(time: Long, histo: Histogram, errs: Long): Unit = {
    // we can't calculate throughput if we don't have the first snapTime
    val timeDelta = (time - lastSnap).nanos.toSeconds
    if (timeDelta > 0) {
      val throughput = histo.getTotalCount / timeDelta
      reqsPerSec.recordValue(throughput)
    }
    lastSnap = time

    latencies.add(histo)
    errors += errs
  }
}

class FaunaRecorder(url: String, secret: String) extends DefaultQueryHelpers {
  private[this] val client = HttpClient(url)

  def close() = Future.fromTry(Try(client.die()))

  def query(json: JSValue): Future[HttpResponse] =
    client.query(
      Body(json.toByteBuf, ContentType.JSON),
      secret
    )

  def ensureCollection(name: String): Unit = {
    val stmnt = If(
      Not(Exists(ClassRef(name))),
      CreateCollection(MkObject("name" -> name)),
      JSNull
    )
    val res = Await.result(query(stmnt), 30.seconds)
    require(
      res.code == 200 || res.code == 201,
      s"could not ensure collection '$name' exists"
    )
  }
}

class RunRecorder(startTime: Timestamp, config: QAConfig)
    extends DefaultQueryHelpers {

  private[this] val log = getLogger()

  private[this] val faunaRecorder = {
    val rec = for {
      url <- config.opt("fauna.recorder.url")
      key <- config.opt("fauna.recorder.secret")
    } yield new FaunaRecorder(url, key)

    rec foreach { fauna =>
      fauna.ensureCollection("summary")
      val res = fauna.query(
        CreateF(
          ClassRef("summary"),
          MkObject(
            "data" ->
              MkObject(
                "startTS" -> Time(startTime.toString),
                "cluster" -> config.clusterName
              )
          )
        )
      )
      Await.result(res, 30.seconds)
    }

    rec
  }

  private[this] val annotationRecorders = {
    val recs = Seq.newBuilder[AnnotationRecorder]

    for {
      url <- config.opt("fauna.datadog.events-url", new URL(_))
      apiKey <- config.opt("fauna.datadog.api-key")
      appKey <- config.opt("fauna.datadog.app-key")
    } recs += new DatadogAnnotationRecorder(
      config.clusterName,
      url,
      apiKey,
      appKey,
      config.annotationTags
    )

    faunaRecorder foreach { fRec =>
      recs += new FaunaAnnotationRecorder(startTime, fRec)
    }

    recs.result()
  }

  private[this] val runStatsRecorders = {
    val recs = Seq.newBuilder[RunStatsRecorder]

    faunaRecorder foreach { fRec =>
      recs += new FaunaRunStatsRecorder(startTime, fRec)
    }

    for {
      path <- config.opt("fauna.stats.csv-path")
    } recs += new CSVRunStatsRecorder(startTime, path)

    recs.result()
  }

  private[this] val overall = HostStat(Host("Overall", "Overall"))
  private[this] val perHost = MMap.empty[Host, HostStat]

  def close(): Future[Unit] = {
    val anns = annotationRecorders map { _.close() }
    val stats = runStatsRecorders map { _.close() }

    implicit val ec = ExecutionContext.parasitic
    (anns ++ stats).join
  }

  def annotate(
    title: String,
    fields: Seq[(String, String)],
    extraTags: Seq[(String, String)]
  ): Future[Unit] = {
    implicit val ec = ExecutionContext.parasitic
    annotationRecorders.map { _.record(title, fields, extraTags) }.join
  }

  def recordStat(getStats: => Future[Seq[(Host, StatsSnapshot)]]): Future[Unit] =
    if (runStatsRecorders.isEmpty) {
      Future.unit
    } else {
      implicit val ec = ExecutionContext.parasitic
      getStats flatMap { stats =>
        synchronized {
          val aggLatencies = new Histogram(StatsSnapshot.SignificantDigits)
          val (aggTS, aggErrs) = stats.foldLeft((0L, 0L)) {
            case ((lastTS, aggErrs), (host, StatsSnapshot(ts, histo, errs))) =>
              val stat = perHost.getOrElseUpdate(host, HostStat(host))
              stat.add(ts, histo, errs)
              aggLatencies.add(histo)
              (lastTS max ts, aggErrs + errs)
          }

          overall.add(aggTS, aggLatencies, aggErrs)

          val snap = overall +: perHost.values.toSeq
          val ts = Clock.time
          runStatsRecorders.map { _.record(ts, snap) }.join
        }
      }
    }

  def slackMsg(
    title: String,
    fields: Seq[(String, String)] = Seq.empty
  ): Future[Unit] = {
    val res = for {
      url <- config.opt("fauna.slack-url", new URL(_))
    } yield {
      val fullTitle = s"*[${config.clusterName}] $title*"

      val blocks = JSArray.newBuilder
      blocks += JSObject(
        "type" -> "section",
        "text" -> JSObject(
          "type" -> "mrkdwn",
          "text" -> fullTitle
        )
      )

      if (fields.nonEmpty) {
        val jsFields = JSArray.newBuilder
        fields foreach {
          case (title, body) =>
            jsFields += JSObject(
              "type" -> "mrkdwn",
              "text" -> s"*${title}*:\n${body}"
            )
        }
        blocks += JSObject(
          "type" -> "section",
          "fields" -> jsFields.result()
        )
      }

      val body = JSObject("blocks" -> blocks.result()).toString

      val rep = HttpClient(s"${url.getProtocol}://${url.getHost}")
        .post(url.getPath, Body(body, "application/json"))

      implicit val ec = ExecutionContext.parasitic
      HttpClient.discard(rep) recover {
        case err =>
          log.error("Could not send Slack Message", err)
      }
    }
    res getOrElse Future.unit
  }
}
