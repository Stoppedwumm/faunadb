package fauna.api.fql1

import fauna.api._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.model.SchemaNames
import fauna.net.http._
import fauna.repo._
import fauna.repo.query.{ QFail, Query, State }
import fauna.stats.StatsRecorder
import io.netty.buffer.ByteBuf
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Temporary bridge endpoint impl. This will be replaced in a followup PR.
  */
object TMPEndpoint extends APIEndpoint {

  type Request = (HttpRequest, Option[ByteBuf])

  def tagsHeader = HTTPHeaders.FaunaTags

  val endpointMetricName = "fql4"

  def apply(app: CoreAppContext, req: APIRequest): Query[APIResponse] =
    if (UtilRoutes.isUtilPath(req.path)) {
      UtilRoutes(req)
    } else {
      FQL1Routes(app.config, req)
    }

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: APIEndpoint.RequestInfo,
    body: Future[Option[ByteBuf]])(implicit ec: ExecutionContext) =
    body map { v => Right((httpReq, v)) }

  def getRequestInfo(app: CoreAppContext, httpReq: HttpRequest) =
    try {
      Right(
        APIEndpoint.RequestInfo(
          RequestParams.APIVersionParam(httpReq),
          RequestParams.LastSeenTxn(httpReq),
          RequestParams.QueryTimeout(httpReq),
          RequestParams.MaxRetriesOnContention(httpReq)
        ))
    } catch {
      case RequestParams.Invalid(msg) =>
        Left(
          APIEndpoint.Response(
            createError0(msg, ProtocolErrorCode.BadRequest).toHTTPResponse
          ))
    }

  def exec(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    req: Request,
    auth: Option[Auth]) = {
    val (httpReq, body) = req
    val apiReq = APIRequest(httpReq, auth, body)

    def emptyPending: SchemaNames.PendingNames = Map.empty

    val q = apply(app, apiReq) flatMap {
      case res: APIResponse.QueryResponse =>
        // this is begging for a better solution. perhaps
        // ENG-XXX.  in the interim, we store away the modified
        // ids from this transaction, and thread them back
        // through the renderer later. it's bad.
        SchemaNames.modifiedNamedCollections map {
          (_, res)
        }
      case res =>
        Query.value((emptyPending, res))
    } recoverWith { case ResponseError(r) =>
      // Previously, ResponseError was allowed to bubble and fail the query
      // execution, causing query metrics to be discarded (search
      // Query.scala for `state.metrics.commitAll`). Recovering from
      // ResponseError here results in a metrics commit, so we need to zero
      // them out before returning to preserve behavior.
      Query.updateState(s => s.copy(metrics = new State.Metrics)) map { _ =>
        (emptyPending, r)
      }
    }

    q map { case (pending, res) =>
      // Render errors as a JSValue for inclusion in the query log. It's a
      // compromise: responses are rendered directly to serialized JSON, which
      // is more efficient than using a JSValue as an intermediate, but makes it
      // difficult to reproduce sections of the rendered response in the query
      // log.
      // TODO: Rationalize this part of query processing.
      val errorsJSON = res match {
        case r: APIResponse.ErrorResponse =>
          Some(r.json / "errors")
        case APIResponse.QueryErrorResponse(errs, _, version, _) =>
          Some(JSArray(errs.map { err =>
            fauna.ast.Error.toJSValue(version, err)
          }))
        case _ =>
          None
      }

      // return a thunk which when called starts rendering. CoreApp uses this to
      // properly trace the rendering stage.
      val thunk = renderThunk(app, pending, res) _
      (errorsJSON, thunk)
    }
  }

  def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable) = {
    QFail.guard {
      val version = reqInfo.apiVersion

      val res = ex match {
        case RequestParams.Invalid(msg) => APIResponse.BadRequest(msg)

        // FIXME: There is some exception handling knots to undo here. CE should
        // really be handled in the general exceptionToResponse method, but
        // cannot, because exceptionToResponse does not run in Query. See notes
        // at def of TransactionContentionResponse, as well.
        case DocContentionException(scope, doc, _) =>
          APIResponse.TransactionContentionResponse(version, scope, doc)
        case _: ContentionException =>
          APIResponse.TransactionContentionNoInfo

        // FIXME: Start returning aggressive timeout errors in FQL2
        case AggressiveTimeoutException(_, _, te)
            if version != APIVersion.Unstable =>
          exceptionToResponse(te, app.stats)
        case e =>
          exceptionToResponse(e, app.stats)
      }

      renderThunk(app, Map.empty, res)(errResInfo)
    }
  }

  private def renderThunk(
    app: CoreAppContext,
    pending: SchemaNames.PendingNames,
    res: APIResponse)(
    info: APIEndpoint.ResponseInfo): Query[APIEndpoint.Response] = {
    QFail.guard {
      val httpResQ = res match {
        case res: APIResponse.StreamResponse =>
          Query.value(res.toRenderedEventsAsHTTPResponse(app.streamCtx))

        case res =>
          res.toRenderedHTTPResponse(
            info.renderPretty,
            pending,
            info.txnTime
          )
      }

      httpResQ map { res =>
        setHeaders(res, info)
        APIEndpoint.Response(res)
      }
    }
  }

  private def setHeaders(res: HttpResponse, info: APIEndpoint.ResponseInfo): Unit = {
    res.setHeader(HTTPHeaders.TxnTime, info.txnTime.micros)

    if (info.userTags.tags.nonEmpty) {
      res.setHeader(HTTPHeaders.FaunaTags, info.userTags.toHTTPHeader)
    }

    if (res.code != 401 && res.code != 500) {
      val bytesOut = res.contentLength.getOrElse(0)

      res.setHeader(HTTPHeaders.ComputeOps, info.metrics.computeOps)
      res.setHeader(HTTPHeaders.ByteReadOps, info.metrics.byteReadOps)
      res.setHeader(HTTPHeaders.ByteWriteOps, info.metrics.byteWriteOps)
      res.setHeader(HTTPHeaders.QueryTime, info.metrics.queryTime)
      res.setHeader(HTTPHeaders.QueryBytesIn, info.metrics.queryBytesIn)
      res.setHeader(HTTPHeaders.QueryBytesOut, bytesOut)
      res.setHeader(HTTPHeaders.StorageBytesRead, info.metrics.storageBytesRead)
      res.setHeader(HTTPHeaders.StorageBytesWrite, info.metrics.storageBytesWrite)
      res.setHeader(HTTPHeaders.TxnRetries, info.metrics.txnRetries)

      var limited = Vector.empty[String]
      if (info.metrics.rateLimitComputeHit) {
        limited :+= "compute"
      }
      if (info.metrics.rateLimitReadHit) {
        limited :+= "read"
      }
      if (info.metrics.rateLimitWriteHit) {
        limited :+= "write"
      }

      if (limited.size > 0) {
        res.setHeader(HTTPHeaders.RateLimitedOps, limited.mkString(","))
      }

      // FIXME: enabling this has stalled, so I didn't bother plumbing through
      // config here.
      // if (config.query_enable_expected_query_time) {
      //   res.setHeader(HTTPHeaders.ExpectedQueryTime, metrics.expectedQueryTime)
      // }
    }
  }

  private def codeStr(code: ProtocolErrorCode): String = {
    import ProtocolErrorCode._
    code match {
      case Unauthorized                => "unauthorized"
      case Forbidden                   => "permission denied"
      case NotFound                    => "not found"
      case MethodNotAllowed            => "method not allowed"
      case Conflict                    => "conflict"
      case Disabled                    => "permission denied"
      case BadRequest                  => "bad request"
      case RequestTooLarge             => "request too large"
      case TooManyRequests             => "too many requests"
      case ProcessingTimeLimitExceeded => "processing time limit exceeded"
      case InternalError               => "internal server error"
      case ServiceTimeout              => "time out"
      case OperatorError               => "operator error"
      case ContendedTransaction        => "contended transaction"
    }
  }

  private def createError0(msg: String, code: ProtocolErrorCode) =
    APIResponse.ErrorResponse(code.httpStatus, codeStr(code), msg)

  def exceptionToResponse(
    e: Throwable,
    stats: StatsRecorder): APIResponse.ErrorResponse = {
    ExceptionResponseHelpers.toResponse(e, stats) { (code, msg) =>
      createError0(msg, code)
    }
  }
}
