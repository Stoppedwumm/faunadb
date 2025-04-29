package fauna.api

import fauna.atoms.APIVersion
import fauna.auth.Auth
import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.syntax._
import fauna.model.runtime.fql2.RedactedQuery
import fauna.net.http.{ HttpRequest, HttpResponse }
import fauna.repo.query.Query
import fauna.stats.{ QueryMetrics, StatTags }
import fauna.trace.QueryLogTags
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString
import scala.collection.immutable.SeqMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

object APIEndpoint {

  object Metrics {
    private val ResponsePrefix = "HTTP.Responses."

    val RequestsReceived = "Queries.Received"
    val RequestsProcessed = "Queries.Processed"

    /** HTTP.Responses.Time */
    val ResponseTime = s"${ResponsePrefix}Time"

    /** HTTP.Responses.200 */
    def responseCode(code: Int): String = s"$ResponsePrefix$code"

    /** HTTP.Responses.Ping.200 */
    def pingResponseCode(code: Int): String = s"${ResponsePrefix}Ping.$code"
  }

  // This function varies from JSON.tryParse in that it captures
  // StackOverflowError. SOE is normally fatal but we really want to catch all
  // issues with JSON parsing in the request/response cycle.
  def tryParseJSON[T: JSON.Decoder](in: ByteBuf): Try[T] =
    try {
      Success(JSON.parse[T](in))
    } catch {
      case e @ (NonFatal(_) | _: StackOverflowError) => Failure(e)
    }

  /** Captures auxilliary request params derived from the request, pre-execution */
  case class RequestInfo(
    apiVersion: APIVersion,
    minSnapTime: Timestamp,
    timeout: Duration,
    maxContentionRetries: Option[Int]) {
    val deadline = timeout match {
      case d: FiniteDuration => d.bound
      case _                 => TimeBound.Max
    }
  }

  object RequestInfo {
    val Null = RequestInfo(APIVersion.Default, Timestamp.Epoch, Duration.Inf, None)
  }

  /** Captures response info, passed to the render thunk. */
  case class ResponseInfo(
    txnTime: Timestamp,
    metrics: QueryMetrics,
    userTags: QueryLogTags,
    renderPretty: Boolean
  )

  object ResponseInfo {
    val Null = ResponseInfo(
      Timestamp.Epoch,
      QueryMetrics.Null,
      QueryLogTags(SeqMap.empty),
      true)
  }

  case class Response(http: HttpResponse, redacted: Option[RedactedQuery] = None)
}

trait APIEndpoint {
  import APIEndpoint._

  /** The typed Request variant for this endpoint. */
  type Request

  // FIXME: Do something less bad here.
  def tagsHeader: AsciiString

  /** Used to tag endpoint specific metrics.
    * Allows us to do things like separate out 500s by endpoint.
    */
  def endpointMetricName: String

  def metricsTags: StatTags = StatTags(Set(("endpoint", endpointMetricName)))

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: APIEndpoint.RequestInfo,
    body: Future[Option[ByteBuf]])(
    implicit ec: ExecutionContext): Future[Either[Response, Request]]

  def getRequestInfo(
    app: CoreAppContext,
    httpReq: HttpRequest): Either[Response, RequestInfo]

  def exec(
    app: CoreAppContext,
    info: RequestInfo,
    req: Request,
    auth: Option[Auth]): Query[(Option[JSValue], ResponseInfo => Query[Response])]

  def recover(
    app: CoreAppContext,
    reqInfo: RequestInfo,
    errResInfo: ResponseInfo,
    auth: Option[Auth],
    ex: Throwable): Query[Response]
}
