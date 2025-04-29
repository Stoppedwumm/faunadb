package fauna.model.runtime.stream

import fauna.auth.Auth
import fauna.lang.Timing
import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure }
import fauna.model.runtime.fql2.Result.Ok
import fauna.model.runtime.stream.{ Event, EventFilter, EventTransformer }
import fauna.model.runtime.Effect
import fauna.repo.query.Query

/** This class is intended to provide a shared logic for `StreamValue` translation to
  * the `EventReplayer` and `EventTranslator`. Both classes have a similar set of
  * dependencies and share the same value processing logic.
  */
final class StreamValueTranslator(
  val auth: Auth,
  val filter: EventFilter,
  val transformer: EventTransformer) {

  def translate(value: StreamValue.Unresolved): Query[Event.Processed] = {
    val interp =
      new FQLInterpreter(
        auth,
        effectLimit = Effect.Limit(
          Effect.Read,
          "Writes are disallowed in stream subscriptions."
        ),
        systemValidTime = Some(value.cursor.ts)
      )

    Query
      .withMetrics {
        val queryTimer = Timing.start
        value
          .resolve()
          .flatMap {
            case None => Query.value((None, Ok(None)))
            case Some(value) =>
              filter
                .keep(interp, value)
                .flatMap {
                  case true  => transformer.transform(interp, value)
                  case false => Query.value(Ok(None))
                }
                .map { (Some(value.cursor), _) }
          }
          .map {
            (_, queryTimer.elapsedMillis)
          }
      }
      .map { case (((cursor, evRes), qTime), stateMetrics) =>
        val eventMetrics = Event.Metrics(stateMetrics, qTime)
        evRes
          .map(Event.Processed(_, eventMetrics, cursor))
          .getOr {
            case QueryRuntimeFailure(
                  code,
                  failureMessage,
                  _,
                  _,
                  _,
                  Some(abortReturn)) =>
              Event.Processed(
                Event.AbortError(code, failureMessage, value.cursor.ts, abortReturn),
                eventMetrics,
                cursor
              )

            case err =>
              Event.Processed(
                Event.Error(err.code, err.failureMessage),
                eventMetrics,
                cursor
              )
          }
      }
  }
}
