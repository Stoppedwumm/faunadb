package fql.test.parser

import fastparse._
import fql.ast._
import fql.parser._
import fql.test.Spec

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
class WhitespaceSpec extends Spec {
  val parser = new Parser(Src.Null)

  def nl[_: P] = P(parser.nl ~~ End)
  def ws[_: P] = P(parser.ws ~~ End)

  "The nl (multiline whitespace) parser" should "accept C++ style comments" in {
    parse(
      """|// foo
         |// bar""".stripMargin,
      nl(_)) shouldEqual Parsed.Success((), 13)
  }

  it should "accept C style comments" in {
    parse(
      """|/* foo */
         |/* bar */""".stripMargin,
      nl(_)) shouldEqual Parsed.Success((), 19)
  }

  it should "accept nested C style comments" in {
    parse(
      """|/* foo /* nested */ */
         |/* bar */""".stripMargin,
      nl(_)) shouldEqual Parsed.Success((), 32)
  }

  it should "disallow control characters in comments" in {
    val nul = "\u0000"
    parse(
      s"""|/* $nul foo */
          |/* bar */""".stripMargin,
      nl(_)) should matchPattern { case _: Parsed.Failure => }
  }

  "The ws (single line whitespace) parser" should "accept C style comments" in {
    parse("""/* foo */""", ws(_)) shouldEqual Parsed.Success((), 9)
  }

  it should "accept nested C style comments" in {
    parse("""/* foo /* nested */ */""", ws(_)) shouldEqual Parsed.Success((), 22)
  }

  it should "disallow control characters in comments" in {
    parse("/* \u0000 foo */", ws(_)) should matchPattern { case _: Parsed.Failure =>
    }
  }
}
