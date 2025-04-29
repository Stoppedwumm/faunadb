package fql.test.parser

import fql.ast._
import fql.error.ParseError
import fql.parser._
import fql.test.Spec
import fql.Result
import scala.language.implicitConversions

class FieldPathParserSpec extends Spec {
  val src = Src.Id("*path*")

  def span(start: Int, end: Int) = Span(start, end, src)
  def parsePath(str: String) = Parser.path(str, src)

  def path(elems: PathElem*)(sp: Span) = Path(elems.toList, sp)

  implicit def strToPathElem(t: (String, Span)): PathElem =
    PathElem.Field(t._1, t._2)
  implicit def intToPathElem(t: (Int, Span)): PathElem =
    PathElem.Index(t._1, t._2)

  "A field path parser" should "accept . and [] access only" in {
    parsePath(".foo") shouldEqual Result.Ok(path("foo" -> span(1, 4))(span(0, 4)))
    parsePath(".['foo']") shouldEqual Result.Ok(
      path("foo" -> span(2, 7))(span(0, 8)))
    parsePath(".foo['foo']") shouldEqual Result.Ok(
      path("foo" -> span(1, 4), "foo" -> span(5, 10))(span(0, 11)))
    parsePath(""".foo["foo"]""") shouldEqual Result.Ok(
      path("foo" -> span(1, 4), "foo" -> span(5, 10))(span(0, 11)))
    parsePath(".foo[0]") shouldEqual Result.Ok(
      path("foo" -> span(1, 4), 0 -> span(5, 6))(span(0, 7)))

    // interpolation not allowed
    parsePath(""".foo["fo#{o}"]""") shouldEqual Result.Err(
      ParseError("Invalid field path", span(4, 14)))

    // apply() not allowed
    parsePath(".foo()") shouldEqual Result.Err(
      ParseError("Invalid field path", span(0, 6)))

    // projection not allowed
    parsePath(".foo { bar }") shouldEqual Result.Err(
      ParseError("Invalid field path", span(0, 12)))

    // ?. not allowed
    parsePath(".foo?.bar") shouldEqual Result.Err(
      ParseError("Invalid field path", span(4, 9)))
    parsePath(".foo?.['bar']") shouldEqual Result.Err(
      ParseError("Invalid field path", span(4, 13)))

    // op exprs aren't allowed
    parsePath("1 + 2") shouldEqual Result.Err(
      ParseError("Invalid field path", span(0, 5)))

    // bare identifiers aren't allowed. (This will work in ext/model because it adds
    // a preceding dot.)
    parsePath("foo") shouldEqual
      Result.Err(ParseError("Invalid field path", span(0, 3)))
  }

}
