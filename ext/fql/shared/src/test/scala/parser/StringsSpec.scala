package fql.test.parser

import fastparse._
import fql.ast._
import fql.parser._
import fql.test.Spec
import fql.Result

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
class StringsSpec extends Spec {
  val parser = new Parser(Src.Query(""))
  import parser._

  "A string parser" should "accept printable ascii" in {
    val str = """Aa;/f!@'%^!&@*"""
    parse(s""""$str"""", doublestring(_)) shouldEqual Parsed.Success(str, 16)
  }

  it should "accept escapes" in {
    parse(""""\\"""", doublestring(_)) shouldEqual Parsed.Success("\\", 4)
    parse(""""\#"""", doublestring(_)) shouldEqual Parsed.Success("#", 4)
    parse(""""\""""", doublestring(_)) shouldEqual Parsed.Success("\"", 4)
    parse(""""\n"""", doublestring(_)) shouldEqual Parsed.Success("\n", 4)
    parse("\"\\xff\"", doublestring(_)) shouldEqual Parsed.Success("\u00ff", 6)
    parse("\"\\u00ff\"", doublestring(_)) shouldEqual Parsed.Success("\u00ff", 8)
    parse("\"\\u{0000ff}\"", doublestring(_)) shouldEqual Parsed.Success(
      "\u00ff",
      12)
  }

  it should "accept long escapes" in {
    parse("\"\\u{101234}\"", doublestring(_)) shouldEqual Parsed.Success(
      "\udbc4\ude34",
      12)
  }

  it should "disallow surrogate escapes" in {
    Parser.expr("\"\\udc00\"") shouldEqual Result.Err(
      List(Failure.InvalidEscape(span(1, 7), "surrogates are disallowed")))
    Parser.expr("\"\\u{11ffff}\"") shouldEqual Result.Err(
      List(Failure.InvalidEscape(span(1, 11), "outside of valid unicode range")))
  }

  it should "accept weird unicode characters" in {
    parse("\"à\"", doublestring(_)) shouldEqual Parsed.Success("à", 3)
    parse("\'à\'", singlestring(_)) shouldEqual Parsed.Success("à", 3)
  }

  it should "disallow control characters" in {
    parse("\"\u0000\"", doublestring(_)) should matchPattern {
      case _: Parsed.Failure =>
    }
    parse("\'\u0000\'", singlestring(_)) should matchPattern {
      case _: Parsed.Failure =>
    }
  }

  it should "accept non-interpolation hashes" in {
    parse("\"foo#bar\"", doublestring(_)) shouldEqual Parsed.Success("foo#bar", 9)
  }

  def longstring[_: P] = istring(number)

  "An interpolated string parser" should "accept printable ascii" in {
    val str = """Aa;/f!@'%^!&@*"""
    parse(s""""$str"""", longstring(_)) shouldEqual Parsed.Success(
      List(Left(str)),
      16)
  }

  it should "accept interpolations" in {
    parse("\"foo#{123}bar\"", longstring(_)) shouldEqual Parsed.Success(
      List(Left("foo"), Right(Literal.Int(123)), Left("bar")),
      14)
  }

  it should "accept non-interpolation hashes" in {
    parse("\"foo#bar\"", longstring(_)) shouldEqual Parsed.Success(
      List(Left("foo"), Left("#"), Left("bar")),
      9)
  }

  it should "accept escaped hashes which precede left-curly" in {
    parse("\"foo\\#{bar\"", longstring(_)) shouldEqual Parsed.Success(
      List(Left("foo#{bar")),
      11)
  }

  it should "allow little 2" in {
    parse("\"₂\"", longstring(_)) shouldBe Parsed.Success(List(Left("₂")), 3)
  }

  def hsinglestring[_: P] = heredocistring(singlestring)

  "A multiline interpolated string parser" should "accept multiline strings" in {
    parse(
      """|<<+END
         |a multiline
         |string
         |END""".stripMargin,
      hsinglestring(_)) shouldEqual Parsed.Success(
      List(Left("a multiline"), Left("\n"), Left("string"), Left("\n")),
      29
    )
  }

  it should "accept indentation + whitespaces" in {
    parse(
      s"""|<<+END
          |a\tmultiline
          |   \tstring
          |    END""".stripMargin,
      hsinglestring(_)) shouldEqual Parsed.Success(
      List(Left("a\tmultiline"), Left("\n"), Left("   \tstring"), Left("\n")),
      37
    )
  }

  it should "accepts interpolation" in {
    parse(
      """|<<+END
         |a multiline
         |#{'str'}
         |string
         |END""".stripMargin,
      hsinglestring(_)) shouldEqual Parsed.Success(
      List(
        Left("a multiline"),
        Left("\n"),
        Right("str"),
        Left("\n"),
        Left("string"),
        Left("\n")),
      38
    )
  }

  it should "un-indent accepts whitespaces between tokens" in {
    parse(
      s"""|<<+  \t  \t END  \t  \t
          |multiline
          |  \t  END  \t  \t""".stripMargin,
      hsinglestring(_)) shouldEqual Parsed.Success(
      List(Left("multiline"), Left("\n")),
      38
    )
  }

  it should "un-indent to the smallest indented line" in {
    parse(
      """|<<+END
         |  a multiline
         |    #{'str'}
         |      string
         |END""".stripMargin,
      hsinglestring(_)) shouldEqual Parsed.Success(
      List(
        Left("a multiline"),
        Left("\n"),
        Left("  "),
        Right("str"),
        Left("\n"),
        Left("    string"),
        Left("\n")),
      50
    )
  }

  it should "disallow control characters" in {
    parse("<<+END\n \u0000\n END", hsinglestring(_)) should matchPattern {
      case _: Parsed.Failure =>
    }
  }

  "A multiline non-interpolated string parser" should "accept multiline strings" in {
    parse(
      """|<<-END
         |a multiline
         |string
         |END""".stripMargin,
      heredocstring(_)) shouldEqual Parsed.Success(
      "a multiline\nstring\n",
      29
    )
  }

  it should "accept indentation + whitespaces" in {
    parse(
      s"""|<<-END
          |a\tmultiline
          |   \tstring
          |    END""".stripMargin,
      heredocstring(_)) shouldEqual Parsed.Success(
      "a\tmultiline\n   \tstring\n",
      37
    )
  }

  it should "un-indent accepts whitespaces between tokens" in {
    parse(
      s"""|<<-  \t  \t END  \t  \t
          |multiline
          |  \t  END  \t  \t""".stripMargin,
      heredocstring(_)) shouldEqual Parsed.Success(
      "multiline\n",
      38
    )
  }

  it should "un-indent to the smallest indented line" in {
    parse(
      """|<<-END
         |  a multiline
         |    string
         |END""".stripMargin,
      heredocstring(_)) shouldEqual Parsed.Success(
      "a multiline\n  string\n",
      35
    )
  }

  it should "disallow control characters" in {
    parse("<<-END\n \u0000\n END", heredocstring(_)) should matchPattern {
      case _: Parsed.Failure =>
    }
  }
}
