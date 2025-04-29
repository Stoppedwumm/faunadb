package fauna.api.fql2

import fauna.api.{ APIEndpoint, CoreAppContext, ExceptionResponseHelpers }
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.stream.Subscription
import fauna.model.runtime.Effect
import fauna.model.Cache
import fauna.net.http.HttpRequest
import fauna.repo.query.Query
import fauna.repo.values.{ Value, ValueReification, ValueType }
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString
import scala.concurrent.{ ExecutionContext, Future }

object StreamEndpoint extends APIEndpoint {
  import APIEndpoint._
  import Result._

  type Request = StreamRequest

  val Path = "/stream/1"

  def tagsHeader = AsciiString.EMPTY_STRING

  def endpointMetricName = "stream"

  private[this] val NoInfo = Right(RequestInfo.Null)

  private val logger = getLogger()

  def getRequestInfo(app: CoreAppContext, httpReq: HttpRequest) =
    NoInfo

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: RequestInfo,
    body: Future[Option[ByteBuf]])(implicit ec: ExecutionContext) =
    body mapT { buf =>
      StreamRequest.fromBody(buf).toEither.left.map { err =>
        StreamResponse.Error.badRequest(err.getMessage).toResponse
      }
    } getOrElseT {
      Left(StreamResponse.Error.badRequest("No request body provided").toResponse)
    }

  def exec(
    app: CoreAppContext,
    info: RequestInfo,
    req: Request,
    auth: Option[Auth]) = {
    def responseQ =
      auth match {
        case None =>
          Query.value(StreamResponse.Error.Unauthorized)

        case Some(_) if req.cursorOverride exists { _.ts < req.streamIR.start } =>
          Query.value(
            StreamResponse.Error
              .badRequest(
                s"Invalid stream start time: Stream start time can not be before the stream create time."))

        case Some(auth) =>
          val interp =
            new FQLInterpreter(
              auth,
              effectLimit = Effect.Limit(
                Effect.Read,
                "Writes are disallowed in stream subscriptions."
              ))

          interp.withSystemValidTime(Some(req.cursor.ts)) flatMap { ctx =>
            // NOTE: a failure in reification may indicate a stale cache problem,
            // most commonly a collection that was deleted and recreated for the
            // purpose or testing streaming. On that case, host receiving the
            // streaming request may still point to the old collection ID in its by
            // name lookup cache.
            val reifiedSetQ =
              Cache.guardFromStalenessIf(
                auth.scopeID,
                ctx.evalWithTypecheck(
                  req.streamIR.source,
                  ValueReification.vctx(req.streamIR.values),
                  typeMode = FQLInterpreter.TypeMode.Disabled
                )
              ) { _.isErr }

            reifiedSetQ map {
              case Ok((stream: Value.EventSource, _)) =>
                val stream0 =
                  req.cursorOverride.fold(stream) { _ =>
                    stream.copy(cursor = req.cursorOverride)
                  }
                val subscription = new Subscription(auth, app.streamCtx, stream0)
                val events = subscription.subscribe()
                StreamResponse.Success(subscription.streamID, events)

              case Ok((other, _)) =>
                val expected = ValueType.AnyEventSource.displayString
                val actual = other.dynamicType.displayString
                StreamResponse.Error
                  .badRequest(
                    s"Invalid request body: invalid event source provided, Expected $expected, received $actual"
                  )

              case Err(err) =>
                logger.info(s"Error evaluating stream query, $err")
                StreamResponse.Error
                  .badRequest(
                    s"Error evaluating provided stream, ${err.failureMessage}"
                  )
            }
          }
      }

    Query.value((None, _ => responseQ.map(_.toResponse)))
  }

  def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable): Query[Response] =
    ExceptionResponseHelpers.toResponse(ex, app.stats) { case (code, msg) =>
      Query.value(StreamResponse.Error(code, msg).toResponse)
    }
}
