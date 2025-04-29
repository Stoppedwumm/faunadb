package fauna.api.fql2

import fauna.api.{ APIEndpoint, CoreAppContext, ExceptionResponseHelpers }
import fauna.atoms.SchemaVersion
import fauna.auth.{ AdminPermissions, Auth, ServerPermissions }
import fauna.codex.json.JSValue
import fauna.lang.syntax._
import fauna.model.Cache
import fauna.net.http.HttpRequest
import fauna.repo.query.Query
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString
import scala.concurrent.{ ExecutionContext, Future }

case class EnvironmentRequest()

object EnvironmentEndpoint extends APIEndpoint {

  override type Request = EnvironmentRequest

  override def tagsHeader: AsciiString = AsciiString.cached("")

  val endpointMetricName = "fql10environment"

  override def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: APIEndpoint.RequestInfo,
    body: Future[Option[ByteBuf]])(
    implicit
    ec: ExecutionContext): Future[Either[APIEndpoint.Response, EnvironmentRequest]] =
    Future.successful(
      Right(EnvironmentRequest())
    )

  override def getRequestInfo(
    app: CoreAppContext,
    httpReq: HttpRequest): Either[APIEndpoint.Response, APIEndpoint.RequestInfo] =
    Right(APIEndpoint.RequestInfo.Null)

  override def exec(
    app: CoreAppContext,
    info: APIEndpoint.RequestInfo,
    req: EnvironmentRequest,
    auth: Option[Auth]): Query[(
    Option[JSValue],
    APIEndpoint.ResponseInfo => Query[APIEndpoint.Response])] = {
    val resQ = auth match {
      case None => Query.value(EnvironmentResponse.Error.Unauthorized)
      case Some(auth) =>
        auth.permissions match {
          case AdminPermissions | ServerPermissions =>
            (
              EnvironmentExporter.exportEnvironment(auth),
              Cache.getLastSeenSchema(auth.scopeID)
            ) par { (env, schemaVersion) =>
              Query.value(
                EnvironmentResponse
                  .Success(env, schemaVersion.getOrElse(SchemaVersion.Min).toMicros))
            }
          case _ => Query.value(EnvironmentResponse.Error.Forbidden)
        }
    }

    val linearizedQ = Query.linearized(resQ)

    Query.value((None, _ => linearizedQ.map(_.toResponse)))
  }

  override def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable): Query[APIEndpoint.Response] = {
    val errRes = ExceptionResponseHelpers.toResponse(ex, app.stats) { (code, msg) =>
      EnvironmentResponse.Error(code, msg)
    }
    Query.value(errRes.toResponse)
  }
}
