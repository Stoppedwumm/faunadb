package fauna.model.runtime

import fauna.model.runtime.fql2.{ FQLInterpCtx, QueryRuntimeFailure, Result }
import fauna.repo.query.Query

// Here is an example effect error at runtime:
// "`all` performs a read, which is not allowed in default values."
//  ^^^^^          ^^^^^^                          ^^^^^^^^^^^^^^
//  The action     The effect                      The reason
//
// - The action is the `name` of an `Effect.Action`
// - The effect is the `name` of an `Effect`
// - The reason is the `reason` of an `Effect.Limit`
//
// This message is generated in `Effect.Action.message`.

sealed trait Effect {
  def +(other: Effect): Effect
  def +(other: Option[Effect]): Effect = this + (other getOrElse Effect.Pure)

  def gteq(other: Effect) = (this + other) == this

  // The name for the effect that was performed in an error message (ex, "a read").
  def name: String
}

object Effect {

  // `reason` is used in an error message. It should be a lowercase, plural term.
  // See the comments at the top of this file.
  final case class Limit(effect: Effect, reason: String) {
    def allows(eff: Effect) = effect gteq eff

    def min(other: Limit) =
      if (allows(other.effect)) other else this
  }

  // `name` is used in an error message. It should be a capitalized term.
  // See the comments at the top of this file.
  sealed abstract class Action(val name: String) {
    def check(ctx: FQLInterpCtx, effect: Effect): Query[Result[Unit]] =
      if (ctx.effectLimit.allows(effect)) {
        Result.Ok(()).toQuery
      } else {
        QueryRuntimeFailure
          .InvalidEffect(ctx.effectLimit, this, effect, ctx.stackTrace)
          .toQuery
      }

    def message(effect: Effect, limit: Limit) =
      s"$name performs ${effect.name}, which is not allowed in ${limit.reason}."
  }

  object Action {
    final case object Field extends Action("Reading a field")
    final case object Projection extends Action("Projection")
    final case object AllFields extends Action("Reading a document")
    final case class Function(val funcName: String) extends Action(s"`$funcName`")

    // Placeholder action for V4, where we don't plumb the calling function through.
    // ex.
    // "Call performs a read, which is not allowed in index bindings."
    //
    // This error shouldn't really come up ever, but we might as well make it
    // readable.
    final case object V4Action extends Action("Call")
  }

  object Pure extends Effect {
    // N/A (pure is always allowed)
    def name = "pure"

    def +(other: Effect) = other
  }
  // This is pure, but it allows `Time.now()` and `newId()` to be called. This is
  // required for default field values, which must be pure, except they allow for
  // Time.now().
  object Observation extends Effect {
    // "`foo` performs an observation"
    def name = "an observation"

    def +(other: Effect) = other match {
      case Pure => this
      case o    => o
    }
  }
  object Read extends Effect {
    // "`foo` performs a read"
    def name = "a read"

    def +(other: Effect) = other match {
      case Observation | Pure => this
      case o                  => o
    }
  }
  object Write extends Effect {
    // "`foo` performs a write"
    def name = "a write"

    def +(other: Effect) = this
  }
}
