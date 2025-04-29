package fauna.qa.recorders

import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.net.http._
import fauna.net.http.{ Body, HttpClient }
import fauna.prop.api.DefaultQueryHelpers
import java.net.URL
import scala.concurrent.Future

sealed trait AnnotationRecorder {
  def close(): Future[Unit]

  def record(
    title: String,
    fields: Seq[(String, String)],
    extraTags: Seq[(String, String)]
  ): Future[Unit]
}

class DatadogAnnotationRecorder(
  clusterName: String,
  url: URL,
  apiKey: String,
  appKey: String,
  tags: Seq[(String, String)]
) extends AnnotationRecorder {

  def record(
    title: String,
    fields: Seq[(String, String)],
    extraTags: Seq[(String, String)]
  ): Future[Unit] = {
    val text = fields map { case (k, v) => s"$k:\n$v" } mkString "\n\n"

    val js =
      JSObject(
        "title" -> title,
        "text" -> text,
        "tags" -> new JSArray((tags ++ extraTags) map {
          case (k, v) => JSString(s"$k:$v")
        }),
        "date_happened" -> Clock.time.seconds,
        "aggregation_key" -> s"qa-$clusterName"
      )

    val query = s"api_key=$apiKey&application_key=$appKey"
    val rep = HttpClient(s"${url.getProtocol}://${url.getHost}")
      .post(url.getPath, Body(js.toByteBuf, ContentType.JSON), null, query)
    HttpClient.discard(rep)
  }

  def close() = Future.unit
}

object FaunaAnnotationsRecorder extends DefaultQueryHelpers {
  val CollectionName = "annotations"
  val AnnotationsRef = ClassRef(CollectionName)
}

class FaunaAnnotationRecorder(startTime: Timestamp, rec: FaunaRecorder)
    extends AnnotationRecorder
    with DefaultQueryHelpers {
  import FaunaAnnotationsRecorder._

  rec.ensureCollection(CollectionName)

  def close() = Future.unit

  def record(
    title: String,
    fields: Seq[(String, String)],
    extraTags: Seq[(String, String)]
  ): Future[Unit] = {
    val fieldsJS = fields map { case (t, v) => JSArray(t, v) }
    rec
      .query(
        CreateF(
          AnnotationsRef,
          MkObject(
            "data" ->
              MkObject(
                "startTS" -> Time(startTime.toString),
                "title" -> title,
                "fields" -> JSArray(fieldsJS: _*)
              )
          )
        )
      )
      .unit
  }
}
