package fauna.api.export

import fauna.api.{ APIEndpoint, CoreAppContext, ExceptionResponseHelpers }
import fauna.auth.{
  AdminPermissions,
  Auth,
  ServerPermissions,
  ServerReadOnlyPermissions
}
import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.lang.syntax._
import fauna.model.{ Collection, Task }
import fauna.model.runtime.fql2.serialization.ValueFormat
import fauna.model.tasks.{ ExportDataTask, TaskRouter }
import fauna.model.tasks.export.{ DatafileFormat, ExportInfo }
import fauna.net.http.{ HTTPHeaders, HttpRequest }
import fauna.repo.query.Query
import fauna.util.router._
import fauna.util.BCrypt
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString
import scala.concurrent.{ ExecutionContext, Future }

case class ExportDataRequest(
  httpReq: HttpRequest,
  exportKey: Option[String],
  body: Option[ByteBuf])
case class Authenticated(auth: Auth, body: Option[ByteBuf])

object ExportDataEndpoint
    extends APIEndpoint
    with RouteDSL[Authenticated, Query[AbstractExportDataResponse]] {

  import APIEndpoint._

  type Request = ExportDataRequest

  def tagsHeader = AsciiString.EMPTY_STRING

  def endpointMetricName = "export"

  override def routePrefix = "/export/1"

  PUT / stringP / { (exportID, req) =>
    val body = req.body map {
      JSON.parse[JSValue](_)
    }

    body match {
      case None =>
        Query.value(ExportDataResponse.BadRequest("Missing request body"))

      case Some(json) =>
        val collections =
          (json / "collections").asOpt[Seq[String]]

        def withDefault[T](jsv: JSValue, default: => T, f: String => Option[T]) =
          jsv.asOpt[String] match {
            // default
            case None    => Some(default)
            case Some(s) => f(s)
          }

        // disallow decorated
        val docFormat = withDefault(
          json / "doc_format",
          ValueFormat.Tagged,
          ValueFormat.fromStringOpt(_).filterNot(_ == ValueFormat.Decorated))

        val datafileFormat =
          withDefault(
            json / "datafile_format",
            DatafileFormat.JSON,
            {
              case DatafileFormat.JSON.toString  => Some(DatafileFormat.JSON)
              case DatafileFormat.JSONL.toString => Some(DatafileFormat.JSONL)
              case _                             => None
            }
          )

        val datafileCompression =
          (json / "datafile_compression").asOpt[Boolean].getOrElse(false)

        (docFormat, datafileFormat, collections) match {
          case (None, _, _) =>
            Query.value(
              ExportDataResponse.BadRequest(
                s"Invalid doc_format '${(json / "doc_format").as[String]}'"))

          case (_, None, _) =>
            Query.value(ExportDataResponse.BadRequest(
              s"Invalid datafile_format '${(json / "datafile_format").as[String]}'"))

          case (_, _, Some(collections)) if collections.isEmpty =>
            Query.value(
              ExportDataResponse.BadRequest("'collections' cannot be empty"))

          case (Some(docFormat), Some(datafileFormat), collsParam) =>
            val scope = req.auth.scopeID
            val collsQ = collsParam match {
              case Some(names) =>
                names.sorted.distinct
                  .map(n => ExportInfo.collectionIDForExport(scope, n).map(n -> _))
                  .sequence
              case None =>
                Collection
                  .getAll(scope)
                  .mapValuesT(c => c.name -> Some(c.id))
                  .flattenT
                  .map(_ ++ ExportInfo.NativeAllowed)
            }

            collsQ flatMap { colls =>
              val notFound = colls collect { case (n, None) => n }

              if (notFound.nonEmpty) {
                Query.value(
                  ExportDataResponse.BadRequest(
                    s"Collections not found: ${notFound.mkString(", ")}"))
              } else {
                val collIDs = colls.map(_._2.get)
                val collNames = colls.map(_._1)

                // there should be only a single task running for the given export ID
                findTask(req.auth, exportID, executing = true).flatMap {
                  case Some(running) =>
                    val info = ExportInfo(running)

                    if (docFormat != info.docFormat) {
                      Query.value(ExportDataResponse.BadRequest(
                        s"Requested doc_format `$docFormat` does not match running export's value `${info.docFormat}`"))
                    } else if (datafileFormat != info.datafileFormat) {
                      Query.value(ExportDataResponse.BadRequest(
                        s"Requested datafile_format `$datafileFormat` does not match running export's value `${info.datafileFormat}`"))
                    } else if (datafileCompression != info.datafileCompression) {
                      Query.value(ExportDataResponse.BadRequest(
                        s"Requested datafile_compression `$datafileCompression` does not match running export's value `${info.datafileCompression}`"))

                    } else if (collsParam.nonEmpty && collIDs != info.collIDs) {
                      Query.value(ExportDataResponse.BadRequest(
                        s"Requested collections do not match running export's collections."))
                    } else {
                      Query.value(ExportDataResponse.Ok(running))
                    }

                  case None =>
                    Query.snapshotTime flatMap { ts =>
                      ExportDataTask
                        .indexScan(
                          exportID,
                          req.auth.scopeID,
                          collIDs.toVector,
                          collNames.toVector,
                          ts,
                          docFormat,
                          datafileFormat,
                          datafileCompression)
                        .map(ExportDataResponse.Created(_))
                    }
                }
              }
            }
        }
    }
  }

  GET / stringP / { (exportID, req) =>
    getTaskByExportID(req.auth, exportID) map {
      case Right(task) => ExportDataResponse.Ok(task)
      case Left(resp)  => resp
    }
  }

  DELETE / stringP / { (exportID, req) =>
    getTaskByExportID(req.auth, exportID) flatMap {
      case Right(task) =>
        TaskRouter.cancel(task, None).map(_ => ExportDataResponse.Deleted)
      case Left(resp) => Query.value(resp)
    }
  }

  private def findTask(auth: Auth, exportID: String, executing: Boolean) =
    Task
      .getAllByAccount(auth.accountID, ExportDataTask.Root.name, executing)
      .selectT(_.data.getOpt(ExportDataTask.ExportIDField).contains(exportID))
      .flattenT
      .map(_.lastOption)

  private def getTaskByExportID(auth: Auth, exportID: String) =
    findTask(auth, exportID, executing = true)
      .orElseT(findTask(auth, exportID, executing = false))
      .map {
        case Some(task) if task.data(ExportDataTask.ScopeField) == auth.scopeID =>
          Right(task)
        case _ => Left(ExportDataResponse.NotFound)
      }

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: RequestInfo,
    bodyF: Future[Option[ByteBuf]])(implicit ec: ExecutionContext) = {
    bodyF map { body =>
      val exportKey = httpReq.getHeader(HTTPHeaders.ExportKey)
      Right(ExportDataRequest(httpReq, exportKey, body))
    }
  }

  def getRequestInfo(app: CoreAppContext, httpReq: HttpRequest) = Right(
    RequestInfo.Null)

  private type Render = (Option[JSValue], ResponseInfo => Query[Response])

  def exec(
    app: CoreAppContext,
    info: RequestInfo,
    req: Request,
    auth: Option[Auth]): Query[Render] = {

    def toResponse(sr: AbstractExportDataResponse): Query[Render] = {
      Query.value((None, _ => Query.value(sr.toResponse())))
    }

    def exportKeyIsValid =
      (req.exportKey, app.config.exportSharedKeyHashes) match {
        case (_, Nil)      => true
        case (None, _)     => false
        case (Some(k), hs) => hs.exists(BCrypt.check(k, _))
      }

    // security through obscurity
    if (!exportKeyIsValid) {
      return toResponse(ExportDataResponse.NotFound)
    }

    auth match {
      case None => toResponse(ExportDataResponse.Unauthorized)
      case Some(auth) =>
        auth.permissions match {
          case AdminPermissions | ServerPermissions | ServerReadOnlyPermissions =>
            endpoints(
              req.httpReq.method,
              req.httpReq.path.getOrElse(routePrefix)) match {
              case NotFound => toResponse(ExportDataResponse.NotFound)
              case MethodNotAllowed(_) =>
                toResponse(ExportDataResponse.MethodNotAllowed)
              case Handler(args, handler) =>
                handler(args, Authenticated(auth, req.body)) flatMap {
                  toResponse(_)
                }
            }

          case _ =>
            toResponse(ExportDataResponse.Forbidden)
        }
    }
  }

  def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable) = {

    ExceptionResponseHelpers.toResponse(ex, app.stats) { case (code, msg) =>
      Query.value(ExportDataResponse.ErrorResponse(code, msg).toResponse())
    }
  }
}
