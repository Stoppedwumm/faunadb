package fauna.model.runtime.fql2

import fauna.model.runtime.fql2.{ QueryRuntimeFailure, Result }
import fauna.model.schema.CollectionConfig
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, WriteHook }

/** Revalidation hooks are registered in the FQLInterpCtx as a post-eval hook. Each
  * hook must return a unique key alongside a query to be executed at the end of
  * query evaluation. Keys are encouraged to be declared as case classes, or have
  * well defined `hashCode` and `equals` implementations. Returned queries should
  * be declared should be wrapped in `Query.defer` so that they only execute during
  * post-eval rather than inline with this function call.
  */
trait PostEvalHook {
  type Self <: PostEvalHook
  def key: AnyRef
  def run(): Query[Result[Unit]]

  // by default later hooks win.
  def merge(last: Self): Self
}

object PostEvalHook {

  type Factory =
    (
      FQLInterpCtx,
      CollectionConfig,
      WriteHook.Event
    ) => PostEvalHook

  sealed trait Revalidation extends PostEvalHook {
    def config: CollectionConfig
    def event: WriteHook.Event
    def stackTrace: FQLInterpreter.StackTrace
    def validate(): Query[Seq[ConstraintFailure]]

    def run() = validate() map {
      case Seq() => Result.Ok(())
      case errs =>
        val prev = event.prevData.map((event.id, _))
        val msg = config.constraintViolationMessage(prev, event.action)
        Result.Err(
          QueryRuntimeFailure
            .SchemaConstraintViolation(msg, errs, stackTrace))
    }
  }

  object Revalidation {
    final case class Default(
      label: String,
      ctx: FQLInterpCtx,
      config: CollectionConfig,
      event: WriteHook.Event,
      stackTrace: FQLInterpreter.StackTrace,
      fn: (
        FQLInterpCtx,
        CollectionConfig,
        WriteHook.Event) => Query[Seq[ConstraintFailure]])
        extends Revalidation {
      def key = (label, event.id)
      def validate() = fn(ctx, config, event)

      // by default later hooks win.
      def merge(last: Self): Self = last
    }

    def apply(label: String)(
      fn: (
        FQLInterpCtx,
        CollectionConfig,
        WriteHook.Event) => Query[Seq[ConstraintFailure]]): Factory = {
      (ctx, cfg, event) =>
        Default(label, ctx, cfg, event, ctx.stackTrace, fn)
    }
  }
}
