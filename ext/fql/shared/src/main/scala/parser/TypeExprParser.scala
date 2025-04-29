package fql.parser

import fastparse._
import fql.ast._
import fql.error._
import scala.language.implicitConversions

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait TypeExprParser { parser: Parser =>

  object Types {
    def expr0[_: P] = PartialTypes.expr0.map(TypeExprParser.typeExpr(_)(parser))
    def expr[_: P] = expr0 ~~ nl
  }

  object SchemaTypes {
    def expr0[_: P] =
      PartialTypes.expr0.map(TypeExprParser.schemaTypeExpr(_)(parser))
    def expr[_: P] = expr0 ~~ nl
  }
}

sealed trait NoDefaultReason {
  def toHint: Hint
}

object NoDefaultReason {
  case class ParentHasDefault(default: Expr) extends NoDefaultReason {
    def toHint = Hint("Parent has a conflicting default value", default.span)
  }
  final case class ParentIsNullable(qmarkSpan: Span) extends NoDefaultReason {
    def toHint = Hint("Parent is nullable", qmarkSpan)
  }
  final case class ParentIsUnion(span: Span) extends NoDefaultReason {
    def toHint = Hint("Parent is a union", span)
  }
  final case class ParentNotObject(span: Span) extends NoDefaultReason {
    def toHint = Hint("Parent is not an object", span)
  }
}

object TypeExprParser {
  def typeExpr(p: PartialTypeExpr)(
    implicit emitter: DiagnosticEmitter[ParseError],
    reason: Option[NoDefaultReason] = None): TypeExpr = {
    import PartialTypeExpr._

    p match {
      case Hole(sp)             => TypeExpr.Hole(sp)
      case Any(sp)              => TypeExpr.Any(sp)
      case Never(sp)            => TypeExpr.Never(sp)
      case Singleton(value, sp) => TypeExpr.Singleton(value, sp)
      case Id(str, sp)          => TypeExpr.Id(str, sp)
      case Cons(name, targs, sp) =>
        TypeExpr.Cons(name, targs.map(typeExpr), sp)

      case Object(fields, sp) =>
        var wildcard: Option[TypeExpr] = None
        var interface: Option[Span] = None
        val teFields = Seq.newBuilder[(Name, TypeExpr)]

        fields.foreach { f =>
          wildcard.foreach { wc =>
            emitter.emit(
              ParseError(
                "Wildcard must be the last element in an object",
                f.span,
                hints = Seq(Hint("Wildcard defined here", wc.span))))
          }
          interface.foreach { i =>
            emitter.emit(
              ParseError(
                "Interface must be the last element in an object",
                f.span,
                hints = Seq(Hint("Interface defined here", i))))
          }

          f match {
            case f: PartialTypeExpr.LiteralField =>
              f.default.foreach { default =>
                emitter.emit(
                  ParseError(
                    "Default values are not allowed here",
                    default.span,
                    hints = reason match {
                      case Some(reason) => Seq(reason.toHint)
                      case None         => Seq.empty
                    }))
              }

              teFields += f.name -> typeExpr(f.ty)

            case PartialTypeExpr.WildcardField(_, te) =>
              wildcard = Some(typeExpr(te))
            case PartialTypeExpr.InterfaceField(sp) =>
              interface = Some(sp)
          }
        }

        if (interface.isDefined) {
          TypeExpr.Interface(teFields.result(), sp)
        } else {
          TypeExpr.Object(teFields.result(), wildcard, sp)
        }

      case Tuple(elems, sp) => TypeExpr.Tuple(elems.map(typeExpr), sp)
      case Lambda(params, variadic, ret, sp) =>
        TypeExpr.Lambda(
          params.map { case (name, ty) => (name, typeExpr(ty)) },
          variadic.map { case (name, ty) => (name, typeExpr(ty)) },
          typeExpr(ret),
          sp
        )

      case Union(members, sp)     => TypeExpr.Union(members.map(typeExpr), sp)
      case Intersect(members, sp) => TypeExpr.Intersect(members.map(typeExpr), sp)
      case Difference(elem, sub, sp) =>
        TypeExpr.Difference(typeExpr(elem), typeExpr(sub), sp)
      case Recursive(name, in, sp) =>
        TypeExpr.Recursive(name, typeExpr(in), sp)
      case Nullable(base, qmarkSpan, sp) =>
        TypeExpr.Nullable(typeExpr(base), qmarkSpan, sp)
    }
  }

  def schemaTypeExpr(p: PartialTypeExpr, reason: Option[NoDefaultReason] = None)(
    implicit emitter: DiagnosticEmitter[ParseError]): SchemaTypeExpr = {
    import PartialTypeExpr._

    implicit def tyToSchema(te: TypeExpr): SchemaTypeExpr = SchemaTypeExpr.Simple(te)

    p match {
      case Object(fields, _)
          if fields.exists(_.isInstanceOf[PartialTypeExpr.InterfaceField]) =>
        typeExpr(p)(
          emitter,
          Some(reason.getOrElse(NoDefaultReason.ParentNotObject(p.span))))

      case Object(fields, sp) =>
        var wildcard: Option[TypeExpr] = None
        val teFields = Seq.newBuilder[(Name, SchemaTypeExpr, Option[Expr])]

        fields.foreach { f =>
          wildcard.foreach { wc =>
            emitter.emit(
              ParseError(
                "Wildcard must be the last element in an object",
                f.span,
                hints = Seq(Hint("Wildcard defined here", wc.span))))
          }

          f match {
            case f: PartialTypeExpr.LiteralField =>
              reason.foreach { reason =>
                f.default.foreach { d =>
                  emitter.emit(
                    ParseError(
                      "Default values are not allowed here",
                      d.span,
                      hints = Seq(reason.toHint)))
                }
              }

              teFields += ((
                f.name,
                schemaTypeExpr(
                  f.ty,
                  reason.orElse(f.default.map(NoDefaultReason.ParentHasDefault(_)))),
                f.default))

            case PartialTypeExpr.WildcardField(_, te) =>
              wildcard = Some(typeExpr(te))
            case PartialTypeExpr.InterfaceField(_) => sys.error("unreachable")
          }
        }

        SchemaTypeExpr.Object(teFields.result(), wildcard, sp)

      // Special case for a better error.
      case Nullable(_, qmarkSpan, _) =>
        typeExpr(p)(
          emitter,
          Some(reason.getOrElse(NoDefaultReason.ParentIsNullable(qmarkSpan))))
      case Union(_, span) =>
        typeExpr(p)(
          emitter,
          Some(reason.getOrElse(NoDefaultReason.ParentIsUnion(span))))

      case _ =>
        typeExpr(p)(
          emitter,
          Some(reason.getOrElse(NoDefaultReason.ParentNotObject(p.span))))
    }
  }
}
