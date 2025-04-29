package fauna.api.fql2

import fauna.api.{
  APIEndpoint,
  CoreAppContext,
  ExceptionResponseHelpers,
  RequestParams
}
import fauna.atoms.APIVersion
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.stream.Feed
import fauna.model.runtime.Effect
import fauna.model.Cache
import fauna.net.http.HttpRequest
import fauna.repo.query.Query
import fauna.repo.values.{ Value, ValueReification, ValueType }
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

object FeedEndpoint extends APIEndpoint {
  import APIEndpoint._
  import Result._

  type Request = FeedRequest

  val Path = "/feed/1"

  def tagsHeader = AsciiString.EMPTY_STRING
  def endpointMetricName = "changefeed"

  private val logger = getLogger()

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
        Left(
          FeedResponse.Error
            .badRequest(s"Invalid headers: $msg")
            .toResponse
        )
    }

  def getRequest(
    app: CoreAppContext,
    httpReq: HttpRequest,
    reqInfo: RequestInfo,
    body: Future[Option[ByteBuf]])(implicit ec: ExecutionContext) =
    body mapT { buf =>
      FeedRequest.fromBody(buf).toEither.left.map { err =>
        FeedResponse.Error.badRequest(err.getMessage).toResponse
      }
    } getOrElseT {
      Left(
        FeedResponse.Error
          .badRequest("No request body provided")
          .toResponse)
    }

  def exec(
    app: CoreAppContext,
    info: RequestInfo,
    req: Request,
    auth: Option[Auth]) = {
    def responseQ =
      auth match {
        case None => Query.value(FeedResponse.Error.Unauthorized)
        case Some(auth) =>
          val interp =
            new FQLInterpreter(
              auth,
              effectLimit = Effect.Limit(
                Effect.Read,
                "Writes are disallowed in change feeds requests."
              ))

          interp.withSystemValidTime(Some(req.sourceIR.start)) flatMap { ctx =>
            // NOTE: a failure in reification may indicate a stale cache problem,
            // most commonly a collection that was deleted and recreated for the
            // purpose or testing streaming. On that case, host receiving the
            // streaming request may still point to the old collection ID in its by
            // name lookup cache.
            val reifiedSetQ =
              Cache.guardFromStalenessIf(
                auth.scopeID,
                ctx.evalWithTypecheck(
                  req.sourceIR.source,
                  ValueReification.vctx(req.sourceIR.values),
                  typeMode = FQLInterpreter.TypeMode.Disabled
                )
              ) { _.isErr }

            reifiedSetQ flatMap {
              case Ok((source: Value.EventSource, _)) =>
                val source0 =
                  req.cursorOverride.fold(source) { _ =>
                    source.copy(cursor = req.cursorOverride)
                  }
                // NB. Uses a best effort approach to return the polled events
                // before the deadline is reached by dedicating 90% of the total
                // query time to polling events, leaving a buffer of 5% of the
                // time deadline for rendering the response back.
                val deadline =
                  (info.deadline.timeLeft * .95) match {
                    case t: FiniteDuration => t.bound
                    case _                 => info.deadline
                  }
                new Feed(auth, source0)
                  .poll(req.pageSize, deadline)
                  .map { FeedResponse.Success(_) }

              case Ok((other, _)) =>
                val expected = ValueType.AnyEventSource.displayString
                val actual = other.dynamicType.displayString
                Query.value(FeedResponse.Error
                  .badRequest(
                    s"Invalid request body: invalid event source provided, Expected $expected, received $actual"
                  ))

              case Err(err) =>
                logger.error(s"Error evaluating event source, $err")
                Query.value(FeedResponse.Error
                  .badRequest(
                    s"Error evaluating provided event source, ${err.failureMessage}"
                  ))
            }
          }
      }

    // we want the responseQ to be executed when the endpoint query is executed so
    // the query metrics are recorded in the query log
    responseQ.map { res =>
      (None, _ => Query.value(res.toResponse))
    }
  }

  def recover(
    app: CoreAppContext,
    reqInfo: APIEndpoint.RequestInfo,
    errResInfo: APIEndpoint.ResponseInfo,
    auth: Option[Auth],
    ex: Throwable): Query[Response] =
    ExceptionResponseHelpers.toResponse(ex, app.stats) { case (code, msg) =>
      Query.value(FeedResponse.Error(code, msg).toResponse)
    }
}
