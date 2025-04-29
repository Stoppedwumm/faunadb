package fauna.qa

import fauna.atoms.APIVersion
import fauna.codex.json._
import fauna.lang.syntax._
import fauna.net.http._
import io.netty.util.AsciiString
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

case class BadResponseException(q: FaunaQuery, rep: HttpResponse)
    extends Exception(s"Non-2xx Response: $rep from $q")

case class QAClient(node: CoreNode) {

  private[this] val client = HttpClient(
    node.host.addr,
    node.port,
    responseTimeout = 15.seconds,
    connectTimeout = 10.seconds,
    pooledBuffers = true,
    pooledConnections = true
  )

  private def okCode(code: Int): Boolean = code >= 200 && code < 300

  def lastSeenTxnTime: Long = client.lastSeenTxnTime

  def query(req: FaunaQuery): Future[HttpResponse] =
    client.query(
      Body(req.query.toString, ContentType.JSON),
      req.authKey,
      req.headers) flatMap {
      case rep if okCode(rep.code) => Future.successful(rep)
      case rep                     => Future.failed(BadResponseException(req, rep))
    }
}

sealed abstract class CursorDir(val fieldName: String)

object CursorDir {
  case object After extends CursorDir("after")
  case object Before extends CursorDir("before")
}

case class FaunaQuery(
  authKey: AuthType,
  query: JSValue,
  cursorDir: Option[CursorDir] = None,
  apiVersion: Option[APIVersion] = None,
  maxContentionRetries: Option[Int] = None,
  requestPathOverride: Option[String] = None,
  name: Option[String] = None
) {

  val headers: Seq[(AsciiString, AnyRef)] =
    apiVersion map { v => HTTPHeaders.FaunaDBAPIVersion -> v.toString } toSeq

  def nextPage(cursor: JSObject): FaunaQuery =
    query match {
      case obj: JSObject => copy(query = obj ++ cursor)
      case _ =>
        throw new UnsupportedOperationException(
          "nextPage only supported for JSObject queries"
        )
    }

  def nextReq(code: Int, body: JSObject): Option[FaunaQuery] =
    if (code >= 200 && code < 300) {
      result(body)
    } else {
      errorResult(code, body)
    }

  def result(body: JSObject): Option[FaunaQuery] =
    consumePages(body)

  def errorResult(code: Int, body: JSObject): Option[FaunaQuery] =
    None

  def consumePages(body: JSObject): Option[FaunaQuery] =
    for {
      dir <- cursorDir
      cursor <- (body / "resource" / dir.fieldName).asOpt[JSValue]
    } yield nextPage(JSObject(dir.fieldName -> cursor))
}

object FaunaArrayQuery {
  private val log = getLogger
}

/**
  * A subclass of FaunaQuery that takes an array of functions in the request and
  * expects an array in the response
  */
abstract class FaunaArrayQuery(authKey: String, query: JSArray)
    extends FaunaQuery(authKey, query) {

  override def result(body: JSObject) =
    (body / "resource").tryAs[JSArray] match {
      case Success(arr) => resultArray(arr)
      case Failure(_) =>
        FaunaArrayQuery.log.warn(
          s"Unexpected response (non-array in 'resource'):\n$body\nto query:\n$query"
        )
        None
    }

  def resultArray(arr: JSArray): Option[FaunaQuery]
}

object FaunaQuery {

  def multi(
    key: AuthType,
    query: JSValue,
    next: JSObject => Option[FaunaQuery]
  ): FaunaQuery =
    new FaunaQuery(key, query, None) {
      override def result(body: JSObject): Option[FaunaQuery] = next(body)
    }

  def multi(
    key: AuthType,
    query: FaunaQuery,
    next: JSObject => Option[FaunaQuery]
  ): FaunaQuery =
    multi(key, query.query, next)
}

case class DBResource(
  key: String,
  ref: JSValue,
  data: Option[JSObject] = None,
  update: Boolean = false
) {
  val action = if (update) "update" else "create"

  val body =
    JSObject(action -> ref) ++
      data
        .map { d =>
          JSObject("params" -> JSObject("quote" -> d))
        }
        .getOrElse(JSObject())

  val toRequest = FaunaQuery(key, body)
}

object DBResource {

  def apply(key: String, ref: JSValue, body: JSObject): DBResource =
    DBResource(key, ref, Some(body))
}
