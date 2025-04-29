package fql.test.parser

import fastparse._
import fql.ast._
import fql.parser._
import fql.test.Spec

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
class IdentifiersSpec extends Spec {
  val parser = new Parser(Src.Null)
  import parser._

  def yeet[_: P] = KW("yeet")

  "A keyword parser" should "accept its keyword" in {
    parse("yeet", yeet(_)) shouldEqual Parsed.Success((), 4)
  }

  it should "reject with extra characters" in {
    parse("yeety", yeet(_)) should matchPattern { case Parsed.Failure(_, _, _) => }
  }

  "An ID parser" should "accept identifiers" in {
    def id[_: P] = P(ident.!)

    parse("foo", id(_)) shouldEqual Parsed.Success("foo", 3)
    parse("Foo", id(_)) shouldEqual Parsed.Success("Foo", 3)
    parse("foo'", id(_)) shouldEqual Parsed.Success("foo'", 4)
    parse("f0oo", id(_)) shouldEqual Parsed.Success("f0oo", 4)
    parse("_foo", id(_)) shouldEqual Parsed.Success("_foo", 4)
  }

  it should "reject invalid with non-init characters" in {
    val Parsed.Failure(_, _, _) = parse("0foo", ident(_))
    val Parsed.Failure(_, _, _) = parse("'foo", ident(_))
  }
}
