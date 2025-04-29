package fql.typer

import fql.ast.Span
import fql.error.{ Hint, TypeError }
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.Buffer

sealed trait TypeConstraintFailure {
  def span: Span
}

object TypeConstraintFailure {

  def toTypeError(
    value: Constraint.Value,
    use: Constraint.Use,
    span: Span,
    _fails: Iterable[TypeConstraintFailure])(
    dispVal: Constraint.Value => String,
    dispUse: Constraint.Use => String): TypeError = {

    def tmpUnion(es: Buffer[Constraint.Use]) = {
      val id = Alt.GrpId(Int.MaxValue)
      val grp = Alt.Grp(id, es.size, false, None, Alt.Level.Zero, Type.Level.Zero)
      Constraint.Union(grp.childAlts.view.zip(es).to(ArraySeq))
    }

    def transitive(fails: List[TypeConstraintFailure]) =
      TypeError(
        s"Type `${dispVal(value)}` is not a subtype of `${dispUse(use)}`",
        span,
        fails.map(toError(_)(dispVal, dispUse))
      )

    val subs = Buffer.empty[NotSubtype]
    val fails = List.newBuilder[TypeConstraintFailure]

    _fails.foreach {
      case f: NotSubtype => subs += f
      case o             => fails += o
    }

    subs.groupBy(_.span).foreach { case (span, es) =>
      es.groupBy(_.value).foreach { case (value, es) =>
        if (es.sizeIs <= 1) {
          fails ++= es
        } else {
          fails += NotSubtype(value, tmpUnion(es.map(_.use)), span)
        }
      }
    }

    fails.result() match {
      case List(f) if f.span == span        => toError(f)(dispVal, dispUse)
      case List(f) if span.contains(f.span) => toError(f)(dispVal, dispUse)
      case fails @ List(_: NotSubtype)      => transitive(fails)
      // If we have a single error, and it's more specific than NotSubtype, use
      // that.
      case List(f) => toError(f)(dispVal, dispUse)
      case fails   => transitive(fails)
    }
  }

  final case class MissingField(
    value: Constraint.Value,
    dotSpan: Span,
    span: Span,
    field: String
  ) extends TypeConstraintFailure

  final case class ExtraField(
    value: Constraint.Value,
    span: Span,
    field: String
  ) extends TypeConstraintFailure

  final case class ExtraWildcard(
    value: Constraint.Value,
    span: Span
  ) extends TypeConstraintFailure

  final case class CannotAccess(
    value: Constraint.Value,
    span: Span
  ) extends TypeConstraintFailure

  final case class CannotProject(
    value: Constraint.Value,
    span: Span
  ) extends TypeConstraintFailure

  final case class NotAFunction(
    value: Constraint.Value,
    span: Span,
    argsSpan: Span
  ) extends TypeConstraintFailure

  final case class InvalidArity(
    value: Constraint.Value,
    span: Span,
    expected: Int,
    provided: Int
  ) extends TypeConstraintFailure

  final case class InvalidTupleArity(
    value: Constraint.Value,
    span: Span,
    expected: Int,
    provided: Int
  ) extends TypeConstraintFailure

  final case class InvalidTupleIndex(
    value: Constraint.Value,
    span: Span,
    arity: Int,
    index: Int
  ) extends TypeConstraintFailure

  final case class NotSameSingleton(
    value: Constraint.Value,
    use: Constraint.Use,
    span: Span
  ) extends TypeConstraintFailure

  final case class InvalidGenericConstraint private (
    value: Constraint.Value,
    use: Constraint.Use,
    span: Span)
      extends TypeConstraintFailure
  object InvalidGenericConstraint {
    def apply(v: Type.Skolem, u: Constraint.Use, sp: Span) =
      new InvalidGenericConstraint(v, u, sp)
    def apply(v: Constraint.Value, u: Type.Skolem, sp: Span) =
      new InvalidGenericConstraint(v, u, sp)
  }

  final case class NotSubtype(
    value: Constraint.Value,
    use: Constraint.Use,
    span: Span
  ) extends TypeConstraintFailure

  final case class Unimplemented(
    value: Constraint.Value,
    use: Constraint.Use,
    span: Span
  ) extends TypeConstraintFailure

  def toError(fail: TypeConstraintFailure)(
    dispVal: Constraint.Value => String,
    dispUse: Constraint.Use => String): TypeError =
    fail match {
      case MissingField(v, dotSpan, sp, field) =>
        val hints = Seq.newBuilder[Hint]
        // If the value ends where the dot starts, then we have a `3.foo` case,
        // so showing where the type was inferred is not helpful.
        //
        // If the value span and apply span are the same, then this hint will
        // show the same location as the main error, and will not be helpful.
        if (v.span != Span.Null && dotSpan.start != v.span.end && v.span != sp) {
          hints += Hint(s"Type `${dispVal(v)}` inferred here", v.span)
        }
        if (v == Type.Null) {
          hints += Hint(
            "Use the ! or ?. operator to handle the null case",
            dotSpan.copy(end = dotSpan.start),
            Some("!")
          )
        }
        TypeError(
          s"Type `${dispVal(v)}` does not have field `$field`",
          sp,
          Nil,
          hints.result()
        )

      case ExtraField(v, sp, field) =>
        TypeError(s"Type `${dispVal(v)}` contains extra field `$field`", sp)

      case ExtraWildcard(v, sp) =>
        TypeError(s"Type `${dispVal(v)}` cannot have a wildcard", sp)

      case CannotAccess(v, sp) =>
        TypeError(s"Cannot use `[]` operator with type `${dispVal(v)}`", sp)

      case CannotProject(v, sp) =>
        TypeError(s"Cannot use projection with type `${dispVal(v)}`", sp)

      case NotAFunction(v, sp, argsSpan) =>
        val dv = dispVal(v)
        TypeError(
          s"Type `$dv` cannot be used as a function",
          sp,
          Seq.empty,
          if (argsSpan == Span.Null) Seq.empty
          else Seq(Hint(s"`$dv` is used as a function here", argsSpan)))

      case InvalidArity(_, sp, expected, provided) =>
        // FIXME: Improve diagnostics here with some hints underneath the
        // specific arguments.
        if (provided > expected) {
          TypeError(
            s"Function was called with too many arguments. Expected $expected, received $provided",
            sp)
        } else {
          TypeError(
            s"Function was not called with enough arguments. Expected $expected, received $provided",
            sp)
        }

      case InvalidTupleArity(_, sp, expected, provided) =>
        if (provided > expected) {
          TypeError(
            s"Tuple contains too many elements. Expected $expected, received $provided",
            sp)
        } else {
          TypeError(
            s"Tuple does not contain enough elements. Expected $expected, received $provided",
            sp)
        }

      case InvalidTupleIndex(_, sp, arity, index) =>
        TypeError(s"Index $index out of bounds for length $arity", sp)

      case NotSameSingleton(v, u, sp) =>
        TypeError(s"`${dispVal(v)}` is not the value `${dispUse(u)}`", sp)

      case InvalidGenericConstraint(v, u, sp) =>
        (v, u) match {
          case (v: Type.Skolem, u) =>
            TypeError(
              s"Generic type `${dispVal(v)}` cannot be used as type `${dispUse(u)}`",
              sp)
          case (v, u: Type.Skolem) =>
            TypeError(
              s"Type `${dispVal(v)}` cannot be used as generic type `${dispUse(u)}`",
              sp)
          case _ => sys.error("unreachable!")
        }

      case NotSubtype(v, u, sp) =>
        TypeError(s"Type `${dispVal(v)}` is not a subtype of `${dispUse(u)}`", sp)

      case Unimplemented(v, u, sp) =>
        TypeError(s"Unimplemented constrain `$v` <: `$u`", sp)
    }
}
