package fql.parser

import fastparse._
import fql.ast._
import fql.error.ParseError
import scala.collection.mutable.{ Map => MMap }

object TupleishRepeater extends Implicits.Repeater[Expr, (Seq[Expr], Boolean)] {
  final class Buf {
    val exprs = List.newBuilder[Expr]
    var isIds = true

    def append(expr: Expr): Unit = {
      if (!expr.isInstanceOf[Expr.Id]) {
        isIds = false
      }
      exprs += expr
    }
  }

  type Acc = Buf

  def initial = new Buf
  def accumulate(e: Expr, acc: Buf) = acc.append(e)
  def result(acc: Buf) = (acc.exprs.result(), acc.isIds)
}

object ProjectElemRepeater
    extends Implicits.Repeater[(Name, Expr), Seq[(Name, Expr)]] {
  final class Buf {
    val elems = List.newBuilder[(Name, Expr)]

    def append(t: (Name, Expr)): Unit =
      t match {
        case (l, Expr.ShortLambda(e)) => elems += ((l, e))
        case t                        => elems += t
      }
  }

  type Acc = Buf

  def initial = new Buf
  def accumulate(e: (Name, Expr), acc: Buf) = acc.append(e)
  def result(acc: Buf) = acc.elems.result()
}

/** This is a bit chaotic, but the return type is:
  * - Left(e) => error
  * - Right(seq, _, true) => an interface
  * - Right(seq, Option(wild), false) => an object
  */
object TypeObjectRepeater
    extends Implicits.Repeater[
      PartialTypeExpr.ObjectField,
      Either[ParseError, Seq[PartialTypeExpr.ObjectField]]] {
  final class Buf {
    var seen = MMap.empty[String, Name]
    val fields = Seq.newBuilder[PartialTypeExpr.ObjectField]
    var error = Option.empty[ParseError]

    def append(v: PartialTypeExpr.ObjectField): Unit = {
      if (error.isEmpty) {
        v match {
          case PartialTypeExpr.LiteralField(n, _, _) if seen.contains(n.str) =>
            error = Some(Failure.DuplicateObjectKey(seen(n.str).span, n.span))
          case f =>
            f match {
              case PartialTypeExpr.LiteralField(n, _, _) => seen += (n.str -> n)
              case _                                     => ()
            }
            fields += f
        }
      }
    }
  }

  type Acc = Buf

  def initial = new Buf
  def accumulate(v: PartialTypeExpr.ObjectField, acc: Buf) = acc.append(v)
  def result(acc: Buf) =
    if (acc.error.isDefined) {
      Left(acc.error.get)
    } else {
      Right(acc.fields.result())
    }
}
