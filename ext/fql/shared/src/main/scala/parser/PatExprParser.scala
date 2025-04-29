package fql.parser

import fastparse._
import fql.ast._

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait PatExprParser { parser: Parser =>

  def parsePatExpr[_: P] = P(nl ~~ Patterns.expr ~~ End)

  object Patterns {

    def expr0[_: P]: P[PatExpr] = P(
      holePat | litPat | objectPat | tuplePat | arrayPat | varPat)
    def expr[_: P]: P[PatExpr] = P(expr0 ~~ nl)

    def holePat[_: P] = P(Index ~~ `_`./ ~~ Index ~~ nl ~~ suffix.?).map {
      case (i1, i2, None)     => PatExpr.Hole(None, span(i1, i2))
      case (i1, _, Some(pat)) => PatExpr.Hole(Some(pat), span(i1, pat.span.end))
    }

    def varPat[_: P] = P(ident ~~ nl ~~ suffix.?).map {
      case (id, None) => PatExpr.Bind(id, None, id.span)
      case (id, Some(pat)) =>
        PatExpr.Bind(id, Some(pat), span(id.span.start, pat.span.end))
    }

    def suffix[_: P] = P(
      `:` ~~ Types.expr.map(tpe => PatExpr.Type(tpe, tpe.span)) | `@` ~~ expr)

    def nullPat[_: P] = P(spanned(`null`)((_, s) => PatExpr.Lit(Literal.Null, s)))
    def truePat[_: P] = P(spanned(`true`)((_, s) => PatExpr.Lit(Literal.True, s)))
    def falsePat[_: P] = P(spanned(`false`)((_, s) => PatExpr.Lit(Literal.False, s)))
    def integerPat[_: P] = P(spanned(integer)(PatExpr.Lit))
    def stringPat[_: P] = P(
      spanned(singlestring | Exprs.objectFieldDoubleString)((s, sp) =>
        PatExpr.Lit(Literal.Str(s), sp)))
    def litPat[_: P] = P(nullPat | truePat | falsePat | integerPat | stringPat)

    def objectPat[_: P] =
      P(Index ~~ `{` ~~ objectField.repX(sep = `,`) ~~ `,`.? ~~ `}` ~~/ Index)
        .map { case (i1, fields, i2) => PatExpr.Object(fields, span(i1, i2)) }

    def objectField[_: P] = P(objectLitField | objectIdField)
    def objectLitField[_: P] = P(
      spanned(singlestring | doublestring)(Name) ~~ `:` ~~ expr ~~ nl)
    def objectIdField[_: P] = P(ident ~~ ws ~~ (`:` ~~ expr).? ~~ nl).map {
      case (id, None)      => (id, PatExpr.Bind(id, None, id.span))
      case (id, Some(pat)) => (id, pat)
    }

    def tuplePat[_: P] =
      P(Index ~~ `(` ~~ expr.repX(sep = `,`) ~~ `,`.? ~~ `)` ~~/ Index)
        .map {
          case (_, Seq(elem), _) => elem
          case (i1, elems, i2)   => PatExpr.Array(elems, None, span(i1, i2))
        }

    def arrayPat[_: P] = P(Index ~~ `[` ~~ arrayBody ~~ `]` ~~ Index)
      .map { case (i1, (elems, rest), i2) =>
        PatExpr.Array(elems, rest, span(i1, i2))
      }
    def arrayBody[_: P] =
      P(
        (`...` ~~ expr).map(r => (Nil, Some(r))) | (expr.repX(sep =
          `,`) ~~ (`,` ~~ `...` ~~ expr).?))
  }
}
