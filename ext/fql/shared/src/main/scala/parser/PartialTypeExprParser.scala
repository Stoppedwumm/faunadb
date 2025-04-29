package fql.parser

import fastparse._
import fastparse.NoWhitespace._
import fql.ast._
import scala.collection.mutable.Stack

// This is either a type, or a type prefixed with `...`.
sealed trait TypeOrVariadic {
  def ty: PartialTypeExpr
}

final object TypeOrVariadic {
  final case class Type(ty: PartialTypeExpr) extends TypeOrVariadic
  final case class Variadic(ty: PartialTypeExpr) extends TypeOrVariadic
}

// This is either a type, or a tuple with the last element having a `...` prefix.
sealed trait ExprOrArgs {
  def exprOrFail(p: Parser): PartialTypeExpr = this match {
    case ExprOrArgs.Expr(ty) => ty
    case args: ExprOrArgs.Args =>
      args.emitFail(p)
      // need to return something :shrug:
      PartialTypeExpr.Id("null", p.span(0, 0))
  }
}

final object ExprOrArgs {
  final case class Expr(ty: PartialTypeExpr) extends ExprOrArgs
  final case class Args(
    elems: Seq[(Option[Name], PartialTypeExpr)],
    variadic: Option[(Option[Name], PartialTypeExpr)],
    span: Span)
      extends ExprOrArgs {
    def emitFail(p: Parser) = {
      elems.foreach {
        case (Some(name), _) => p.emit(Failure.InvalidNamedType(name.span))
        case _               => ()
      }
      variadic.foreach { v =>
        p.emit(Failure.InvalidVariadic(v._2.span))
      }
    }
  }
}

object PartialTypeExprParser {

  val TypeOpPrecedences: Map[String, Int] =
    Seq(Seq("=>"), Seq("|"), Seq("&"), Seq("?")).zipWithIndex.flatMap {
      case (ops, i) => ops.map((_, i))
    }.toMap

  val TExprRightAcc = Set("=>")

  private[PartialTypeExprParser] final case class TypeOp(
    prec: Int,
    racc: Boolean,
    name: Name,
    rhs: Option[ExprOrArgs])
}

import PartialTypeExprParser._

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait PartialTypeExprParser { parser: Parser =>

  def parseTypeSchemeExpr[_: P] = P(nl ~~ TypeSchemes.expr ~~ End)

  object TypeSchemes {
    def expr[_: P]: P[TypeExpr.Scheme] = P(tparams.? ~~ Types.expr).map {
      case (tps, expr) => TypeExpr.Scheme(tps.getOrElse(Nil), expr)
    }

    def tparams[_: P] = P(
      `<` ~~ (plainIdentStr ~~ nl).repX(min = 1, sep = `,`) ~~ `,`.? ~~ `>` ~~ nl
    )
  }

  def parseTypeExpr[_: P] = P(nl ~~ Types.expr ~~ End)
  def parseSchemaTypeExpr[_: P] = P(nl ~~ SchemaTypes.expr ~~ End)

  object PartialTypes {
    def expr0[_: P]: P[PartialTypeExpr] = typeTerm
    def expr[_: P] = P(expr0 ~~ nl)

    def exprNoLambda[_: P] = typeTerm

    def typeTerm[_: P] = P(baseType ~~ typeOpChain)
      .map { case (init, ops) =>
        val exprStack = Stack.empty[ExprOrArgs]
        val opStack = Stack.empty[TypeOp]

        exprStack.push(init)

        def buildOp(op: Name) = op.str match {
          case "?" => buildUnaryOp(op)
          case _   => buildBinaryOp(op.str)
        }

        def buildUnaryOp(name: Name) = {
          val l = exprStack.pop()
          l match {
            case ExprOrArgs.Expr(lhs) =>
              exprStack.push(
                ExprOrArgs.Expr(
                  PartialTypeExpr
                    .Nullable(lhs, name.span, span(lhs.span.start, name.span.end))
                ))
            case args: ExprOrArgs.Args =>
              args.emitFail(parser)
              TypeExpr.Id("null", span(0, 0))
          }
        }

        def buildBinaryOp(op: String) = {
          val rhs = exprStack.pop().exprOrFail(parser)
          val l = exprStack.pop()

          val app = (l, op) match {
            case (ExprOrArgs.Expr(lhs), op) =>
              (lhs, op) match {
                case (PartialTypeExpr.Union(ms, s), "|") =>
                  PartialTypeExpr.Union(ms :+ rhs, span(s.start, rhs.span.end))
                case (PartialTypeExpr.Intersect(ms, s), "&") =>
                  PartialTypeExpr.Intersect(ms :+ rhs, span(s.start, rhs.span.end))
                case (lhs, "|") =>
                  PartialTypeExpr.Union(
                    Vector(lhs, rhs),
                    span(lhs.span.start, rhs.span.end))
                case (lhs, "&") =>
                  PartialTypeExpr.Intersect(
                    Vector(lhs, rhs),
                    span(lhs.span.start, rhs.span.end))

                case (PartialTypeExpr.Tuple(elems, _), "=>") =>
                  PartialTypeExpr.Lambda(
                    elems.map(None -> _),
                    None,
                    rhs,
                    span(lhs.span.start, rhs.span.end))
                case (arg, "=>") =>
                  PartialTypeExpr.Lambda(
                    Seq(None -> arg),
                    None,
                    rhs,
                    span(lhs.span.start, rhs.span.end))

                case (_, op) => sys.error(s"unexpected op $op") // see `typeop` below
              }

            case (ExprOrArgs.Args(args, variadic, argsSpan), "=>") =>
              PartialTypeExpr.Lambda(
                args,
                variadic,
                rhs,
                span(argsSpan.start, rhs.span.end))
            case (args: ExprOrArgs.Args, _) =>
              args.emitFail(parser)
              PartialTypeExpr.Id("null", span(0, 0))
          }
          exprStack.push(ExprOrArgs.Expr(app))
        }

        ops.iterator foreach { op =>
          opStack popWhile {
            case op0 if op0.prec > op.prec || (op0.prec == op.prec && !op0.racc) =>
              buildOp(op0.name)
              true
            case _ => false
          }

          // Even though we don't use the op's rhs past this point, we can save an
          // allocation by reusing the op struct instead of creating a tuple.
          opStack.push(op)
          op.rhs map { exprStack.push(_) }
        }

        opStack foreach { op => buildOp(op.name) }
        exprStack.pop().exprOrFail(parser)
      }

    def typeOpChain[_: P] = P(typeOpRhs.repX)

    def typeOpRhs[_: P] = P(unaryTypeop | binaryTypeop)

    def unaryTypeop[_: P] = P(spanned(StringIn("?").!)(Name) ~~/ ws).map { case op =>
      TypeOp(TypeOpPrecedences(op.str), TExprRightAcc(op.str), op, None)
    }

    def binaryTypeop[_: P] =
      P(spanned(StringIn("|", "&", "=>").!)(Name) ~~/ nl ~~ baseType).map {
        case (op, t) =>
          TypeOp(TypeOpPrecedences(op.str), TExprRightAcc(op.str), op, Some(t))
      }

    def baseType[_: P]: P[ExprOrArgs] =
      P((simpleType.map(ExprOrArgs.Expr(_)) | tupleArgs) ~~ ws)
    def simpleType[_: P]: P[PartialTypeExpr] =
      P(kwType | singleType | varType | objectType | arrayTupleType, "type")

    def anyType[_: P] = P(spanned(`Any`)((_, s) => PartialTypeExpr.Any(s)))
    def neverType[_: P] = P(spanned(`Never`)((_, s) => PartialTypeExpr.Never(s)))
    def kwType[_: P] = P(anyType | neverType)

    // def nullType[_: P] = P( spanned(`null`)((_, s) =>
    // TypeExpr.Singleton(Literal.Null, s)) )
    def trueType[_: P] = P(
      spanned(`true`)((_, s) => PartialTypeExpr.Singleton(Literal.True, s)))
    def falseType[_: P] = P(
      spanned(`false`)((_, s) => PartialTypeExpr.Singleton(Literal.False, s)))
    def integerType[_: P] = P(spanned(integer)(PartialTypeExpr.Singleton))
    def stringType[_: P] = P(spanned(singlestring | doublestring)((s, sp) =>
      PartialTypeExpr.Singleton(Literal.Str(s), sp)))
    def singleType[_: P] = P(trueType | falseType | integerType | stringType)

    def varTypeIdentStr[_: P]: P[String] =
      P((plainIdentInit ~~ (".".? ~~ plainIdentRest).repX).!)
        .opaque("type identifier")

    def varType[_: P] = P(
      spanned(varTypeIdentStr)(Name) ~~
        (`<` ~~ expr.repX(min = 1, sep = `,`) ~~ `,`.? ~~ ">" ~~/ Index).?
    ).map {
      case (id, None) => PartialTypeExpr.Id(id.str, id.span)
      case (id, Some((args, i1))) =>
        PartialTypeExpr.Cons(id, args, span(id.span.start, i1))
    }

    def objectType[_: P]: P[PartialTypeExpr] =
      P(Index ~~ `{` ~~ objectBody.? ~~ `}` ~~/ Index).map { case (i1, fields, i2) =>
        PartialTypeExpr.Object(fields.getOrElse(Seq.empty), span(i1, i2))
      }
    def objectBody[_: P] = {
      implicit val repeater = TypeObjectRepeater
      P(
        (literalField | wildcardField | interfaceField)
          .repX(min = 1, sep = objectsep)
          .flatMap {
            case Left(e)  => fail(e)
            case Right(v) => Pass(v)
          } ~~ `,`.? ~~ nl)
    }

    def literalField[_: P] =
      (Exprs.objectFieldName ~~ ws ~~ `:` ~~ expr0 ~~ (`=` ~~/ Exprs.expr0).?)
        .map(PartialTypeExpr.LiteralField.tupled)
    def wildcardField[_: P] =
      (spanned("*")((_, sp) => sp) ~~ ws ~~ `:` ~~ expr0)
        .map(PartialTypeExpr.WildcardField.tupled)
    def interfaceField[_: P] =
      spanned(`...`)((_, sp) => PartialTypeExpr.InterfaceField(sp))

    def removeVariadics(tes: Seq[TypeOrVariadic]): Seq[PartialTypeExpr] = {
      val exprs = Seq.newBuilder[PartialTypeExpr]
      tes.foreach {
        case TypeOrVariadic.Type(t)     => exprs += t
        case TypeOrVariadic.Variadic(t) => emit(Failure.InvalidVariadic(t.span))
      }
      exprs.result()
    }

    def tupleArgs[_: P]: P[ExprOrArgs] =
      P(Index ~~ `(` ~~ parenTuple0 ~~ `)` ~~/ Index)
        .map {
          case (i1, Seq(), i2) =>
            ExprOrArgs.Expr(PartialTypeExpr.Tuple(Seq.empty, span(i1, i2)))
          case (i1, exprs, i2) =>
            exprs.last match {
              case (vName, TypeOrVariadic.Variadic(t)) =>
                ExprOrArgs.Args(
                  exprs.dropRight(1).flatMap {
                    case (name, TypeOrVariadic.Type(t)) => Some(name -> t)
                    case (_, TypeOrVariadic.Variadic(t)) =>
                      emit(Failure.InvalidVariadic(t.span))
                      None
                  },
                  Some((vName, t)),
                  span(i1, i2)
                )
              case (_, TypeOrVariadic.Type(_)) =>
                val elems = removeVariadics(exprs.map(_._2))
                if (exprs.exists(_._1.isDefined)) {
                  ExprOrArgs.Args(
                    exprs.flatMap {
                      case (name, TypeOrVariadic.Type(t)) => Some(name -> t)
                      case (_, TypeOrVariadic.Variadic(t)) =>
                        emit(Failure.InvalidVariadic(t.span))
                        None
                    },
                    None,
                    span(i1, i2)
                  )
                } else if (elems.sizeIs == 1) {
                  ExprOrArgs.Expr(elems.head)
                } else {
                  ExprOrArgs.Expr(PartialTypeExpr.Tuple(elems, span(i1, i2)))
                }
            }
        }

    def parenTuple0[_: P]: P[Seq[(Option[Name], TypeOrVariadic)]] =
      namedTypeOrVariadic.repX(sep = `,`) ~~ `,`.?

    def namedTypeOrVariadic[_: P]: P[(Option[Name], TypeOrVariadic)] =
      P((ident ~~ `:`).? ~~ exprOrVariadic)
    def exprOrVariadic[_: P]: P[TypeOrVariadic] =
      expr.map(TypeOrVariadic.Type) | variadicExpr.map(TypeOrVariadic.Variadic)
    def variadicExpr[_: P] = P(`...` ~~ expr)

    def arrayTupleType[_: P] =
      P(Index ~~ `[` ~~ expr.repX(sep = `,`) ~~ `,`.? ~~ `]` ~~/ Index).map {
        case (i1, ts, i2) => PartialTypeExpr.Tuple(ts, span(i1, i2))
      }
  }
}
