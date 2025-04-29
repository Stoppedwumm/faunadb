package fql.test.parser

import fql.ast._
import fql.parser._
import fql.test.Spec
import fql.Result

class PatExprParserSpec extends Spec {
  def span(s: Int, e: Int) = Span(s, e, Src.Query(""))

  "A PatExpr parser" should "accept holes" in {
    import PatExpr.{ Hole, Lit, Object, Type }
    import TypeExpr.Id
    Parser.patExpr("_") shouldEqual Result.Ok(Hole(None, span(0, 1)))
    Parser.patExpr("_: int") shouldEqual Result.Ok(
      Hole(Some(Type(Id("int", span(3, 6)), span(3, 6))), span(0, 6)))
    Parser.patExpr("_ @ true") shouldEqual Result.Ok(
      Hole(Some(Lit(Literal.True, span(4, 8))), span(0, 8)))
    Parser.patExpr("_ @ {}") shouldEqual Result.Ok(
      Hole(Some(Object(List(), span(4, 6))), span(0, 6)))
  }

  it should "accept bindings" in {
    import PatExpr.{ Bind, Lit, Object, Type }
    import TypeExpr.Id
    Parser.patExpr("a") shouldEqual Result.Ok(
      Bind(Name("a", span(0, 1)), None, span(0, 1)))
    Parser.patExpr("a: int") shouldEqual Result.Ok(
      Bind(
        Name("a", span(0, 1)),
        Some(Type(Id("int", span(3, 6)), span(3, 6))),
        span(0, 6)))
    Parser.patExpr("a @ true") shouldEqual Result.Ok(
      Bind(Name("a", span(0, 1)), Some(Lit(Literal.True, span(4, 8))), span(0, 8)))
    Parser.patExpr("a @ {}") shouldEqual Result.Ok(
      Bind(Name("a", span(0, 1)), Some(Object(List(), span(4, 6))), span(0, 6)))
  }

  it should "accept objects" in {
    import PatExpr.{ Bind, Object, Type }
    import TypeExpr.Id
    Parser.patExpr("{}") shouldEqual Result.Ok(Object(Nil, span(0, 2)))
    Parser.patExpr("{a, b}") shouldEqual Result.Ok(
      Object(
        List(
          (Name("a", span(1, 2)), Bind(Name("a", span(1, 2)), None, span(1, 2))),
          (Name("b", span(4, 5)), Bind(Name("b", span(4, 5)), None, span(4, 5)))),
        span(0, 6)))
    Parser.patExpr("{\na\n,\nb\n}") shouldEqual Result.Ok(
      Object(
        List(
          (Name("a", span(2, 3)), Bind(Name("a", span(2, 3)), None, span(2, 3))),
          (Name("b", span(6, 7)), Bind(Name("b", span(6, 7)), None, span(6, 7)))),
        span(0, 9)))
    Parser.patExpr("{a: x, b: y}") shouldEqual Result.Ok(Object(
      List(
        (Name("a", span(1, 2)), Bind(Name("x", span(4, 5)), None, span(4, 5))),
        (Name("b", span(7, 8)), Bind(Name("y", span(10, 11)), None, span(10, 11)))),
      span(0, 12)))
    Parser.patExpr("{\na :\n{\n}\n}") shouldEqual Result.Ok(
      Object(List((Name("a", span(2, 3)), Object(List(), span(6, 9)))), span(0, 11)))
    Parser.patExpr("{\"a\": x, \"b\": y}") shouldEqual Result.Ok(Object(
      List(
        (Name("a", span(1, 4)), Bind(Name("x", span(6, 7)), None, span(6, 7))),
        (Name("b", span(9, 12)), Bind(Name("y", span(14, 15)), None, span(14, 15)))),
      span(0, 16)))
    Parser.patExpr("{\'a\': x, \'b\': y}") shouldEqual Result.Ok(Object(
      List(
        (Name("a", span(1, 4)), Bind(Name("x", span(6, 7)), None, span(6, 7))),
        (Name("b", span(9, 12)), Bind(Name("y", span(14, 15)), None, span(14, 15)))),
      span(0, 16)))
    // FIXME this is terrible
    Parser.patExpr("{a: x: int}") shouldEqual Result.Ok(
      Object(
        List(
          (
            Name("a", span(1, 2)),
            Bind(
              Name("x", span(4, 5)),
              Some(Type(Id("int", span(7, 10)), span(7, 10))),
              span(4, 10)))),
        span(0, 11)))
    // FIXME this is terrible
    Parser.patExpr("{a: x @ {}}") shouldEqual Result.Ok(
      Object(
        List(
          (
            Name("a", span(1, 2)),
            Bind(
              Name("x", span(4, 5)),
              Some(Object(List(), span(8, 10))),
              span(4, 10)))),
        span(0, 11)))
  }

  it should "accept arrays/tuples" in {
    import PatExpr.{ Array, Hole, Lit, Type }
    import TypeExpr.Id

    Parser.patExpr("[]") shouldEqual Result.Ok(Array(Nil, None, span(0, 2)))
    Parser.patExpr("[1]") shouldEqual Result.Ok(
      Array(List(Lit(Literal.Int(1), span(1, 2))), None, span(0, 3)))
    Parser.patExpr("[1, 2]") shouldEqual Result.Ok(
      Array(
        List(Lit(Literal.Int(1), span(1, 2)), Lit(Literal.Int(2), span(4, 5))),
        None,
        span(0, 6)))
    Parser.patExpr("[_: int]") shouldEqual Result.Ok(
      Array(
        List(Hole(Some(Type(Id("int", span(4, 7)), span(4, 7))), span(1, 7))),
        None,
        span(0, 8)))
    Parser.patExpr("[_ @ \"a\"]") shouldEqual Result.Ok(
      Array(
        List(Hole(Some(Lit(Literal.Str("a"), span(5, 8))), span(1, 8))),
        None,
        span(0, 9)))
    Parser.patExpr("[1, ...2]") shouldEqual Result.Ok(
      Array(
        List(Lit(Literal.Int(1), span(1, 2))),
        Some(Lit(Literal.Int(2), span(7, 8))),
        span(0, 9)))
    Parser.patExpr("[...2]") shouldEqual Result.Ok(
      Array(Nil, Some(Lit(Literal.Int(2), span(4, 5))), span(0, 6)))
  }

  it should "accept paren-based patterns" in {
    import PatExpr.{ Array, Hole, Lit, Type }
    import TypeExpr.Id

    Parser.patExpr("()") shouldEqual Result.Ok(Array(Nil, None, span(0, 2)))
    Parser.patExpr("(1)") shouldEqual Result.Ok(Lit(Literal.Int(1), span(1, 2)))
    Parser.patExpr("(1, 2)") shouldEqual Result.Ok(
      Array(
        List(Lit(Literal.Int(1), span(1, 2)), Lit(Literal.Int(2), span(4, 5))),
        None,
        span(0, 6)))
    Parser.patExpr("(_: int)") shouldEqual Result.Ok(
      Hole(Some(Type(Id("int", span(4, 7)), span(4, 7))), span(1, 7)))
    Parser.patExpr("(_ @ \"a\")") shouldEqual Result.Ok(
      Hole(Some(Lit(Literal.Str("a"), span(5, 8))), span(1, 8)))
  }
}
