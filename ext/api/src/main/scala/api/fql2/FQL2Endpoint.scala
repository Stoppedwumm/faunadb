package fauna.api.fql2

import com.fasterxml.jackson.core.exc.StreamConstraintsException
import fauna.api._
import fauna.atoms.{ APIVersion, SchemaSourceID, SchemaVersion }
import fauna.auth._
import fauna.codex.json2.JSONParseException
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{
  FQL2Output,
  FQLInterpreter,
  InfoWarns,
  ReadBroker,
  Result,
  SourceContext
}
import fauna.model.runtime.fql2.serialization.{ FQL2Query, ValueFormat }
import fauna.model.Cache
import fauna.net.http._
import fauna.repo.query.{ QFail, Query }
import fauna.repo.service.rateLimits.OpsLimitExceptionForRendering
import fauna.repo.values.Value
import fauna.repo.DocContentionException
import fql.ast.Src
import fql.error.Diagnostic
import io.netty.buffer.ByteBuf
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

object FQL2Endpoint extends APIEndpoint {

  type Request = FQL2Request

  val tagsHeader = HTTPHeaders.QueryTags

  val endpointMetricName = "fql10"

  val FQLv10Path = "/query/1"

  @annotation.nowarn("cat=unused-params")
  private def badRequest(
    code: ProtocolErrorCode,
    msg: String,
    reqInfo: APIEndpoint.RequestInfo) = {
    // FIXME: populate with tags from reqInfo if available.
    val nullRes = APIEndpoint.ResponseInfo.Null
    Left(FQL2Response.ProtocolError(code, msg, Nil).toResponse(nullRes))
  }

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: APIEndpoint.RequestInfo,
    bodyF: Future[Option[ByteBuf]])(implicit ec: ExecutionContext) =
    try {
      val format = httpReq
        .getHeader(HTTPHeaders.Format)
        .map(ValueFormat.fromString(_))
        .getOrElse(ValueFormat.Simple)

      val typecheck = RequestParams.Typecheck(httpReq)
      val linearized = RequestParams.Linearized(httpReq)
      val performanceHints = RequestParams.PerformanceHints(httpReq)

      getRequestBody(bodyF, reqInfo).mapT(
        FQL2Request(_, format, typecheck, linearized, performanceHints))
    } catch {
      case v: ValueFormat.InvalidFormat =>
        Future.successful(
          badRequest(
            ProtocolErrorCode.BadRequest,
            RequestParams
              .InvalidHeader(HTTPHeaders.Format, v.getMessage())
              .message,
            reqInfo))
      case RequestParams.Invalid(msg) =>
        Future.successful(badRequest(ProtocolErrorCode.BadRequest, msg, reqInfo))
    }

  private def getRequestBody(
    bodyF: Future[Option[ByteBuf]],
    reqInfo: APIEndpoint.RequestInfo)(implicit ec: ExecutionContext) =
    bodyF
      .mapT { body =>
        APIEndpoint.tryParseJSON[FQL2RequestBody](body) match {
          case Success(req) => Right(req)
          case Failure(_: StackOverflowError) =>
            badRequest(
              ProtocolErrorCode.BadRequest,
              "Invalid request body: JSON exceeds nesting limit",
              reqInfo)
          case Failure(e: StreamConstraintsException)
              if e.getMessage contains "Document nesting depth" =>
            badRequest(
              ProtocolErrorCode.BadRequest,
              "Invalid request body: JSON exceeds nesting limit",
              reqInfo)
          case Failure(e: JSONParseException) =>
            badRequest(
              ProtocolErrorCode.BadRequest,
              s"Invalid request body: ${e.msg}",
              reqInfo)
          case Failure(_) =>
            badRequest(
              ProtocolErrorCode.BadRequest,
              s"Invalid request body",
              reqInfo)
        }
      }
      .getOrElseT {
        badRequest(ProtocolErrorCode.BadRequest, "No request body provided", reqInfo)
      }

  def getRequestInfo(app: CoreAppContext, httpReq: HttpRequest) =
    try {
      Right(
        APIEndpoint.RequestInfo(
          // TODO: pull FQL2 API version from the request path.
          APIVersion.Default,
          RequestParams.LastTxnTs(httpReq),
          RequestParams.QueryTimeoutMs(httpReq),
          RequestParams.MaxContentionRetries(httpReq)
        ))
    } catch {
      case RequestParams.Invalid(msg) =>
        badRequest(ProtocolErrorCode.BadRequest, msg, APIEndpoint.RequestInfo.Null)
    }

  def exec(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    req: FQL2Request,
    auth: Option[Auth]) = {
    val resQ = auth match {
      case None => Query.value(FQL2Response.ProtocolError.Unauthorized)
      case Some(auth) =>
        val intp = new FQLInterpreter(
          auth,
          performanceDiagnosticsEnabled = req.performanceHints)

        val argQ: Query[Map[String, Value]] = req.body.arguments match {
          case Some(args) =>
            args
              .map { case (key, undecided) =>
                undecided.toValue(intp, req.format) map {
                  case Right(value) => (key, value)
                  case Left(error) =>
                    throw FQL2ResponseError(
                      FQL2Response.ProtocolError(
                        ProtocolErrorCode.BadRequest,
                        s"Invalid argument '$key': ${error.message}",
                        Seq.empty))
                }
              }
              .sequence
              .map { _.toMap }
          case None => Query.value(Map.empty)
        }

        argQ flatMap { arguments =>
          evalQuery(
            intp,
            req.body.query,
            arguments = arguments,
            typecheck = req.typecheck,
            format = req.format
          )
        }
    }

    val linearizedQ = if (req.linearized) Query.linearized(resQ) else resQ

    linearizedQ
      .recover { case FQL2ResponseError(res) => res }
      .map(res => (None, renderThunk(res)))
  }

  private def evalQuery(
    intp: FQLInterpreter,
    query: FQL2Query,
    arguments: Map[String, Value],
    typecheck: Option[Boolean],
    format: ValueFormat): Query[FQL2Response] =
    for {
      output <- FQLInterpreter.evalQuery(intp, query, arguments, typecheck)
      outputFiltered <- intp.auth.checkLoggingPermission(intp.scopeID) map {
        allowLogs =>
          if (allowLogs) {
            val resultFiltered = output.result match {
              case ok @ Result.Ok(_) => ok
              case Result.Err(err)   => err.filterDecodedErrors().toResult
            }
            output.copy(result = resultFiltered)
          } else {
            val logsFiltered = output.infoWarns.filterLogs()
            val resultFiltered = output.result match {
              case ok @ Result.Ok(_) => ok
              case Result.Err(err)   => err.filterErrors().toResult
            }

            output.copy(result = resultFiltered, infoWarns = logsFiltered)
          }
      }
      schemaVersion <- Cache.getLastSeenSchema(intp.scopeID)
      response <- mapToResponse(
        intp.auth,
        query,
        format,
        outputFiltered,
        schemaVersion.getOrElse(SchemaVersion.Min))
    } yield response

  private def mapToResponse(
    auth: Auth,
    query: FQL2Query,
    format: ValueFormat,
    output: FQL2Output,
    schemaVersion: SchemaVersion): Query[FQL2Response] = {
    def checkSrcs(
      srcIds: Set[Src],
      srcs: Map[Src.Id, String],
      infoWarns: InfoWarns,
      errors: Seq[Diagnostic] = Seq.empty) =
      if (srcIds.exists(_ == Src.Null)) {
        getLogger().warn(s"""|found null span for query:
            |$query
            |with errors:
            |${errors.map(_.renderWithSource(srcs)).mkString("\n\n")}
            |check diagnostics:
            |${infoWarns.check.map(_.renderWithSource(srcs)).mkString("\n\n")}
            |runtime diagnostics:
            |${infoWarns.runtime.map(_.renderWithSource(srcs)).mkString("\n\n")}
            |""".stripMargin)
      }

    val schemaVersionMicros = schemaVersion.toMicros
    output.result match {
      case Result.Ok((v, t)) =>
        SourceContext.lookup(
          new FQLInterpreter(auth),
          output.infoWarns.allSrcs) map { srcs =>
          checkSrcs(output.infoWarns.allSrcs, srcs, output.infoWarns)
          FQL2Response.Success(
            result = v,
            staticType = t,
            sourceCtx = srcs,
            infoWarns = output.infoWarns,
            redacted = output.redacted,
            format = format,
            schemaVersion = Some(schemaVersionMicros)
          )
        }
      case Result.Err(e) =>
        // we throw FQL2ResponseErrors that wrap the response so that any
        // writes that happened during the transaction are aborted. The
        // responses are then pulled out of the FQL2ResponseError object in
        // the recover method.
        val srcIds = output.infoWarns.allSrcs ++ e.errors.flatMap { _.allSrcs }
        SourceContext.lookup(new FQLInterpreter(auth), srcIds).map { srcs =>
          checkSrcs(srcIds, srcs, output.infoWarns, e.errors)
          throw FQL2ResponseError(
            FQL2Response
              .QueryError(
                fail = e,
                sourceCtx = srcs,
                infoWarns = output.infoWarns,
                redacted = output.redacted,
                format = format,
                schemaVersion = Some(schemaVersionMicros)))
        }
    }
  }

  def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable) = {

    val errQ =
      ex match {
        case RequestParams.Invalid(msg) =>
          Query.value(
            FQL2Response.ProtocolError(ProtocolErrorCode.BadRequest, msg, Seq.empty))

        case DocContentionException(_, SchemaSourceID(_), _) =>
          Query.value(
            FQL2Response.ProtocolError(
              ProtocolErrorCode.ContendedTransaction,
              s"Transaction was aborted due to detection of concurrent modifications to schema.",
              headers = Nil))

        // NB: Special case for contentions with detectable doc IDs. For these, we
        // want to render the user readable document ID that is causing contention.
        case DocContentionException(scope, docID, _) =>
          // see note on materializeRef
          @annotation.nowarn("cat=deprecation")
          val materializedQ = ReadBroker.materializeRef(scope, Value.Doc(docID))
          materializedQ.map { ref =>
            FQL2Response.ProtocolError(
              ProtocolErrorCode.ContendedTransaction,
              s"Transaction was aborted due to detection of concurrent modifications to ${ref.toRefString}.",
              headers = Nil)
          }
        case OpsLimitExceptionForRendering(_, _) =>
          Query.value(
            FQL2Response.ProtocolError(
              code = ProtocolErrorCode.TooManyRequests,
              message = "Rate limit exceeded",
              withStats = true))

        case _: Throwable =>
          Query.value {
            ExceptionResponseHelpers.toResponse(ex, app.stats) { (code, msg) =>
              FQL2Response.ProtocolError(code, msg, Nil)
            }
          }
      }

    errQ flatMap { renderThunk(_)(errResInfo) }
  }

  private def renderThunk(res: FQL2Response)(
    info: APIEndpoint.ResponseInfo): Query[APIEndpoint.Response] =
    QFail.guard { Query.value(res.toResponse(info)) }
}
