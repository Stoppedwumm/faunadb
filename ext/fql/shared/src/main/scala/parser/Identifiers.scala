package fql.parser

import fastparse._
import fastparse.NoWhitespace._
import fql.ast.Name

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
/** Parsers for identifiers. */
trait Identifiers { parser: Parser =>
  def KW[_: P](s: String) = P((s ~~ !plainIdentRest).opaque(s))

  def plainIdentInit[_: P] = P(CharIn("a-zA-Z_"))
  def plainIdentRest[_: P] = P(CharsWhileIn("a-zA-Z0-9_'"))
  def plainIdentStr[_: P] =
    P((plainIdentInit ~~ plainIdentRest.?).!).opaque("identifier")

  def ident[_: P] = P(spanned(plainIdentStr)(Name)) flatMap { name =>
    if (Tokens.invalidIdents.contains(name.str)) {
      fail(Failure.InvalidIdent(name))
    } else {
      Pass(name)
    }
  }
}
