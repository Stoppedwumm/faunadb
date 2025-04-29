package fauna.prop.api

import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.net.http._
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.util.AsciiString
import language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

trait HttpResponseHelpers {

  val DefaultTimeout = Duration.Inf

  implicit class HttpBodyOps(body: HttpBody) {

    def resolved = body match {
      case _: NoBody => None
      case c: Body =>
        Await.result(c.data map { buf =>
          Some((buf, c.contentType))
        }, DefaultTimeout)
      case c: Chunked =>
        Await.result(c.data map { buf =>
          Some((buf, c.contentType))
        }, DefaultTimeout)
    }

    def resolvedDataOpt: Option[ByteBuf] = resolved map { _._1 }
    def resolvedContentTypeOpt: Option[String] = resolved map { _._2 }

    def resolvedData = resolvedDataOpt getOrElse Unpooled.EMPTY_BUFFER

    def resolvedContentType =
      resolvedContentTypeOpt getOrElse ContentType.OctetStream
  }

  implicit class HttpResponseOps(f: Future[HttpResponse]) {
    def res = Await.result(f, DefaultTimeout)

    def status = res.status

    def statusCode = status.code

    def headers = res.raw.headers

    def header(name: String) = res.header(new AsciiString(name))

    def hasBody = res.body.resolved.isDefined

    def bodyOpt = res.body.resolvedDataOpt

    def body = res.body.resolvedData

    def contentTypeOpt = res.body.resolvedContentTypeOpt

    def contentType = res.body.resolvedContentType

    def json = JSON.parse[JSValue](body).as[JSObject]
  }

  implicit final def jsDataToHttpContent(data: JSValue) =
    Body(data.toByteBuf, ContentType.JSON)
}

trait APIResponseHelpers extends HttpResponseHelpers {

  // JSON conversions

  implicit class JSValueOps(json: JSValue) {
    def ref = (json / "ref" / "@ref").orElse((json / "ref").as[String])

    def refObj = (json / "ref").as[JSObject]

    def collRefObj = (refObj / "@ref" / "collection").as[JSObject]

    def faunaClass = parentRef

    def ts = (json / "ts").as[Long]

    def tsObj = Timestamp.parse((json / "ts" / "@ts").as[String])

    def before = (json / "before").as[String]

    def after = (json / "after").as[String]

    def id = (refObj / "@ref").asOpt[String] match {
      case Some(ref) => ref.split("/").reverse.head
      case None => (refObj / "@ref" / "id").as[String]
    }

    def parentRef = (json / "ref").asOpt[String] match {
      case Some(ref) => ref.split("/").dropRight(1) mkString "/"
      case None => refObj / "@ref" / "class"
    }

    def path = "/" + ref

    def parentPath = "/" + parentRef
  }

  implicit class JSObjectOps(json: JSObject) {
    def keys = json.value map { _._1 }

    def slice(fields: String*) =
      JSObject(json.value filter { case (k, _) => fields contains k }: _*)

    def +(pair: (String, JSValue)) = this ++ Seq(pair)

    def ++(seq: Seq[(String, JSValue)]) =
      JSObject((this -- (seq map { _._1 })).value ++ seq: _*)

    def ++(other: JSObject): JSObject = this ++ other.value

    def --(excluded: Seq[String]) =
      JSObject(json.value filterNot { case (k, _) => excluded contains k }: _*)

    def --(other: JSObject) = {
      val map = other.as[Map[String, JSValue]]
      JSObject(json.value filterNot { case (k, v) => map get k contains v }: _*)
    }
  }

  // API responses

  implicit class APIResponseOps(res: Future[HttpResponse]) {
    def resource = (res.json / "resource").as[JSObject]
    def errors = (res.json / "errors").as[Seq[JSObject]]
    def ref = resource.ref
    def refObj = resource.refObj
    def path = resource.path
    def ts = resource.ts
    def beforeParam = resource.before
    def afterParam = resource.after
    def events = (resource / "events").as[Seq[JSObject]]
    def resources = (resource / "resources").as[Seq[String]]
    def sets = (resource / "sets").as[Seq[String]]
  }
}
