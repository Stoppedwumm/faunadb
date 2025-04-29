package fauna.model.test

import fql.ast.Expr
import fql.ast.Literal
import org.scalacheck.Shrink
import scala.annotation.tailrec

trait ExprShrinker {
  def shrink[T](v: T)(implicit shrink: Shrink[T]): LazyList[T] =
    LazyList.from(Shrink.shrink(v))
  def shrinkWithOrig[T](v: T)(implicit shrink: Shrink[T]): LazyList[T] =
    LazyList.from(Shrink.shrinkWithOrig(v))

  implicit val shrinkLiteral: Shrink[Literal] = Shrink.withLazyList {
    // TODO: shink strings? (not sure if this will work or not)
    case Literal.Str(_) => LazyList.empty
    case Literal.Int(v) => shrink(v).map(Literal.Int(_))

    case lit => sys.error(s"cannot shrink lit: $lit")
  }

  implicit val shrinkComponent: Shrink[Expr.MethodChain.Component] =
    Shrink.withLazyList {
      case Expr.MethodChain.Select(_, _, _) => LazyList.empty

      case Expr.MethodChain.MethodCall(dotSpan, name, args, _, _, applySpan) =>
        for {
          args1 <- shrink(args)(Shrink.shrinkContainer).filter(
            _.length == args.length)
        } yield Expr.MethodChain.MethodCall(
          dotSpan,
          name,
          args1,
          false,
          None,
          applySpan)
      case component => sys.error(s"cannot shrink component $component")
    }

  protected def isShrinkableComponent(c: Expr.MethodChain.Component): Boolean = true

  @tailrec
  private def canShrinkComponents(original: Seq[Expr.MethodChain.Component])(
    shrunk: Seq[Expr.MethodChain.Component]): Boolean = {
    val matchRes = original.headOption match {
      case Some(orig) => isShrinkableComponent(orig)
      case None       => true
    }
    val headRes = (original.headOption == shrunk.headOption) || matchRes
    if (original.nonEmpty || shrunk.nonEmpty) {
      headRes && canShrinkComponents(original.drop(1))(shrunk.drop(1))
    } else {
      headRes
    }
  }

  def canShrinkOp(op: String) = Set("+", "-", "*", "/", "%").contains(op)

  implicit val shrinkExpr: Shrink[Expr] = Shrink.withLazyList {
    // cannot shrink ids
    case Expr.Id(_, _) => LazyList.empty

    case Expr.Lit(v, sp) => shrink(v).map(Expr.Lit(_, sp))

    // try with and without parens
    case Expr.Tuple(Seq(elem), sp) =>
      shrinkWithOrig(elem) ++ shrink(elem).map(e => Expr.Tuple(Seq(e), sp))

    case Expr.Array(elems, sp) =>
      shrink(elems)(Shrink.shrinkContainer).map(Expr.Array(_, sp))

    case Expr.LongLambda(args, vari, body, sp) =>
      shrink(body).map { b => Expr.LongLambda(args, vari, b, sp) }

    case Expr.ShortLambda(e) => shrink(e).map(Expr.ShortLambda(_))

    case Expr.OperatorCall(lhs, field, Some(rhs), sp) =>
      val shrunkSides =
        shrink(lhs).map { lhs1 =>
          Expr.OperatorCall(lhs1, field, Some(rhs), sp)
        } ++ shrink(rhs).map { rhs1 =>
          Expr.OperatorCall(lhs, field, Some(rhs1), sp)
        }
      // If this operator is shrinkable, try with just the lhs and rhs
      if (canShrinkOp(field.str)) {
        shrunkSides ++ shrinkWithOrig(lhs) ++ shrinkWithOrig(rhs)
      } else {
        shrunkSides
      }

    case Expr.MethodChain(lhs, elems, sp) =>
      shrink(lhs).map { lhs1 =>
        Expr.MethodChain(lhs1, elems, sp)
      } ++ shrink(elems)(Shrink.shrinkContainer)
        .filter(canShrinkComponents(elems))
        .map { elems1 =>
          Expr.MethodChain(lhs, elems1, sp)
        }

    case expr => sys.error(s"cannot shrink expr: $expr")
  }

  implicit val shrinkExprOps: Shrink[WrappedExpr] = Shrink.withLazyList { e =>
    shrink(e.expr).map(WrappedExpr(_))
  }
}
