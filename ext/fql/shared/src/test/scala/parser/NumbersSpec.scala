package fql.test.parser

import fastparse._
import fql.ast._
import fql.parser._
import fql.test.Spec

class NumbersSpec extends Spec {
  val parser = new Parser(Src.Null)
  import parser._
  "A number parser" should "accept integers" in {
    parse("123", number(_)) shouldEqual Parsed.Success(Literal.Int(123), 3)
    parse("1_2_3", number(_)) shouldEqual Parsed.Success(Literal.Int(123), 5)
    parse("-123", number(_)) shouldEqual Parsed.Success(Literal.Int(-123), 4)
  }

  it should "accept floats" in {
    val Parsed.Failure(_, _, _) = parse(".1", number(_))
    parse("1.", number(_)) shouldEqual Parsed.Success(Literal.Int(1), 1)
    parse("1e1", number(_)) shouldEqual Parsed.Success(Literal.Float("1e1"), 3)
    parse("1e-1", number(_)) shouldEqual Parsed.Success(Literal.Float("1e-1"), 4)
    val Parsed.Failure(_, _, _) = parse("-.1", number(_))
    parse("-1.", number(_)) shouldEqual Parsed.Success(Literal.Int(-1), 2)
    parse("-1e1", number(_)) shouldEqual Parsed.Success(Literal.Float("-1e1"), 4)
    parse("-1e-1", number(_)) shouldEqual Parsed.Success(Literal.Float("-1e-1"), 5)
  }

  it should "accept hex integers" in {
    parse("0xff", number(_)) shouldEqual Parsed.Success(Literal.Int(255), 4)
    parse("-0xff", number(_)) shouldEqual Parsed.Success(Literal.Int(-255), 5)
  }
}
