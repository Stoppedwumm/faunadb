package fauna.model.test

import fql.ast.display._
import fql.ast.Expr
import fql.ast.Literal
import fql.ast.Name
import fql.ast.Span

object WrappedExpr {
  val sp = Span.Null

  def id(name: String): Expr = Expr.Id(name, sp)
  def shortLambda(expr: Expr): Expr = Expr.ShortLambda(expr)
  def lambda(args: Seq[String], expr: Expr): Expr =
    Expr.LongLambda(args.map(v => Some(Name(v, sp))), None, expr, sp)

  def ths: Expr = Expr.This(sp)
  def str(s: String): Expr = Expr.Lit(Literal.Str(s), sp)
  def int(n: Int): Expr = Expr.Lit(Literal.Int(n), sp)

  def tuple(exprs: Expr*): Expr = Expr.Tuple(exprs, sp)
  def array(exprs: Expr*): Expr = Expr.Array(exprs, sp)

  implicit class ExprOps(val expr: Expr) {
    def chain(op: Expr.MethodChain.Component): Expr =
      expr match {
        case Expr.MethodChain(lhs, elems, span) =>
          Expr.MethodChain(lhs, elems.appended(op), span)
        case _ =>
          Expr.MethodChain(expr, Seq(op), WrappedExpr.sp)
      }

    def call(name: String, args: Seq[Expr]): Expr = {
      chain(
        Expr.MethodChain
          .MethodCall(
            Span.Null,
            Name(name, WrappedExpr.sp),
            args,
            false,
            None,
            WrappedExpr.sp))
    }

    def op(name: String, args: Option[Expr]): Expr = {
      Expr.OperatorCall(expr, Name(name, WrappedExpr.sp), args, WrappedExpr.sp)
    }

    def select(field: String): Expr = {
      chain(Expr.MethodChain.Select(Span.Null, Name(field, sp), false))
    }

    def access(field: String): Expr = {
      chain(
        Expr.MethodChain.Access(Seq(WrappedExpr.id(field)), None, WrappedExpr.sp))
    }
  }
}

case class WrappedExpr(expr: Expr) {
  override def toString: String = expr.display
}
