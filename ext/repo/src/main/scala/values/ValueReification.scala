package fauna.repo.values

import fql.ast.{ Expr, Literal, Name, Span }

/** A module for translating an FQL value into an expression and companion
  * closure of _persistable_ values, which when reevaluated, will reproduce the
  * value. This is primarily used in SetCursor serialization.
  */
object ValueReification {

  def vctx(values: Seq[Value]): Map[String, Value] =
    values.iterator.zipWithIndex.map { case (v, i) => s"v$i" -> v }.toMap

  def reify(v: Value): (Expr, Vector[Value]) = {
    val ctx = new ReifyCtx
    val e = ctx.save(v)
    (e, ctx.closure())
  }

  final class ReifyCtx {
    private[this] val values = Vector.newBuilder[Value]
    private[this] var varIdx = 0

    private[ValueReification] def closure() =
      values.result()

    def save(value: Value): Expr =
      reify(value) match {
        case Left(expr) => expr
        case Right(value) =>
          val res = Expr.Id(s"v$varIdx", Span.Null)
          values += value
          varIdx += 1
          res
      }

    private def reify(value: Value): Either[Expr, Value] = {
      value match {
        // FIXME: IDs are the only type which are not persistable, and also
        // do not have an expression representation. Either one needs to be
        // fixed in order to handle it here. In the meantime we convert to a
        // string, since ID's Cast allows it.
        case Value.ID(id) => Right(Value.Str(id.toString))

        // These are cheaper as exprs
        case Value.True   => Left(Expr.Lit(Literal.True, Span.Null))
        case Value.False  => Left(Expr.Lit(Literal.False, Span.Null))
        case Value.Str(s) => Left(Expr.Lit(Literal.Str(s), Span.Null))

        // NOTE: Although not persistable, partials and set cursors are returned as
        // is since their materialization happpens at the model layer. See
        // `MaterializedValue`.
        case _: Value.Persistable | _: Value.Struct.Partial | _: Value.SetCursor =>
          Right(value)

        case stream: Value.EventSource => Left(stream.reify(this))
        case set: Value.Set            => Left(set.reify(this))
        case fn: Value.Func            => Left(fn.reify(this))
        case so: Value.SingletonObject => Left(so.reify(this))

        case arr: Value.Array if arr.isPersistable => Right(arr)
        case Value.Array(arr) =>
          Left(Expr.Array(arr.map(save), Span.Null))

        case str: Value.Struct if str.isPersistable => Right(str)
        case Value.Struct.Full(str, _, _, _) =>
          val fields = str.iterator.map { case (n, v) =>
            (Name(n, Span.Null), save(v))
          }.toSeq

          Left(Expr.Object(fields, Span.Null))
      }
    }
  }
}
