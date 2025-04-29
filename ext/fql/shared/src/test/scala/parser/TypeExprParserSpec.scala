package fql.test.parser

import fql.{ Result, TextUtil }
import fql.ast._
import fql.error._
import fql.parser._
import fql.test.Spec
import org.scalactic.source.Position

class TypeExprParserSpec extends Spec {
  def span(s: Int, e: Int) = Span(s, e, Src.Query(""))

  def parseTExprErr(src: String, expected: String)(implicit pos: Position) = {
    val parsed = Parser.typeExpr(src, Src.Query(src))
    val actual = parsed.errOrElse(fail("parsing passed"))
    val actualError = actual
      .map(_.renderWithSource(Map.empty))
      .mkString("\n\n")

    if (actualError != expected) {
      fail(s"errors did not match:\n${TextUtil.diff(actualError, expected)}")
    }
  }

  "A TypeExpr parser" should "accept keywords" in {
    import TypeExpr.{ Any, Never, Singleton, Id }

    Parser.typeExpr("Any") shouldEqual Result.Ok(Any(span(0, 3)))
    Parser.typeExpr("Never") shouldEqual Result.Ok(Never(span(0, 5)))
    Parser.typeExpr("true") shouldEqual Result.Ok(
      Singleton(Literal.True, span(0, 4)))
    Parser.typeExpr("false") shouldEqual Result.Ok(
      Singleton(Literal.False, span(0, 5)))
    Parser.typeExpr("Null") shouldEqual Result.Ok(Id("Null", span(0, 4)))
  }

  it should "accept singletons" in {
    import TypeExpr.Singleton
    import Literal.{ Int, Str }

    Parser.typeExpr("\"foo\"") shouldEqual Result.Ok(
      Singleton(Str("foo"), span(0, 5)))
    Parser.typeExpr("1") shouldEqual Result.Ok(Singleton(Int("1"), span(0, 1)))
  }

  it should "accept identifiers" in {
    import TypeExpr.Id
    Parser.typeExpr("boolean") shouldEqual Result.Ok(Id("boolean", span(0, 7)))
    Parser.typeExpr("int") shouldEqual Result.Ok(Id("int", span(0, 3)))
  }

  it should "accept pathed identifiers" in {
    import TypeExpr.Id
    Parser.typeExpr("Foo.Bar.Baz") shouldEqual Result.Ok(
      Id("Foo.Bar.Baz", span(0, 11)))

    Parser.typeExpr("Foo..Baz") shouldEqual Result.Err(
      ParseError("Expected `<` or end-of-input", span(3, 4)))
  }

  it should "accept type construction" in {
    import TypeExpr.{ Cons, Id }
    Parser.typeExpr("Array<boolean>") shouldEqual Result.Ok(
      Cons(Name("Array", span(0, 5)), List(Id("boolean", span(6, 13))), span(0, 14)))
  }

  it should "accept objects" in {
    import TypeExpr.{ Object, Id, Singleton }

    Parser.typeExpr("{}") shouldEqual Result.Ok(Object(Nil, None, span(0, 2)))
    Parser.typeExpr("{a:int,b:\"foo\"}") shouldEqual Result.Ok(
      Object(
        List(
          (Name("a", span(1, 2)), Id("int", span(3, 6))),
          (Name("b", span(7, 8)), Singleton(Literal.Str("foo"), span(9, 14)))),
        None,
        span(0, 15)))
    Parser.typeExpr("{a:int,b:\"foo\",*:boolean}") shouldEqual Result.Ok(
      Object(
        List(
          (Name("a", span(1, 2)), Id("int", span(3, 6))),
          (Name("b", span(7, 8)), Singleton(Literal.Str("foo"), span(9, 14)))),
        Some(Id("boolean", span(17, 24))),
        span(0, 25)
      ))

    // Trailing comma is allowed
    Parser.typeExpr("{a:int,*:boolean,}") shouldEqual Result.Ok(
      Object(
        List((Name("a", span(1, 2)), Id("int", span(3, 6)))),
        Some(Id("boolean", span(9, 16))),
        span(0, 18)
      ))

    // Wildcard must be last
    Parser.typeExpr("{a:int,*:boolean,b:3}") shouldEqual Result.Err(
      ParseError(
        "Wildcard must be the last element in an object",
        span(17, 20),
        hints = Seq(Hint("Wildcard defined here", span(9, 16)))))
  }

  it should "accept interfaces" in {
    import TypeExpr.{ Interface, Id, Singleton }

    Parser.typeExpr("{a:int, b:\"foo\", ...}") shouldEqual Result.Ok(
      Interface(
        List(
          (Name("a", span(1, 2)), Id("int", span(3, 6))),
          (Name("b", span(8, 9)), Singleton(Literal.Str("foo"), span(10, 15)))),
        span(0, 21)))

    // Trailing comma is allowed
    Parser.typeExpr("{a:int, ..., }") shouldEqual Result.Ok(
      Interface(
        List((Name("a", span(1, 2)), Id("int", span(3, 6)))),
        span(0, 14)
      ))

    // `...` must be last
    Parser.typeExpr("{a:int, ..., b:3}") shouldEqual Result.Err(
      ParseError(
        "Interface must be the last element in an object",
        span(13, 16),
        hints = Seq(Hint("Interface defined here", span(8, 11)))))
  }

  it should "accept tuples" in {
    import TypeExpr.{ Tuple, Id, Singleton }

    // paren tuples
    Parser.typeExpr("()") shouldEqual Result.Ok(Tuple(Nil, span(0, 2)))
    Parser.typeExpr("(a)") shouldEqual Result.Ok(Id("a", span(1, 2)))

    Parser.typeExpr("(a,b,\"foo\",2)") shouldEqual Result.Ok(
      Tuple(
        List(
          Id("a", span(1, 2)),
          Id("b", span(3, 4)),
          Singleton(Literal.Str("foo"), span(5, 10)),
          Singleton(Literal.Int(2), span(11, 12))),
        span(0, 13)))
    Parser.typeExpr("( a , b , \"foo\" , 2 ) ") shouldEqual Result.Ok(
      Tuple(
        List(
          Id("a", span(2, 3)),
          Id("b", span(6, 7)),
          Singleton(Literal.Str("foo"), span(10, 15)),
          Singleton(Literal.Int(2), span(18, 19))),
        span(0, 21)))
    Parser.typeExpr("(\na\n,\nb\n,\n\"foo\"\n,\n2\n)\n") shouldEqual Result.Ok(
      Tuple(
        List(
          Id("a", span(2, 3)),
          Id("b", span(6, 7)),
          Singleton(Literal.Str("foo"), span(10, 15)),
          Singleton(Literal.Int(2), span(18, 19))),
        span(0, 21)))

    // array tuples
    Parser.typeExpr("[]") shouldEqual Result.Ok(Tuple(Nil, span(0, 2)))
    Parser.typeExpr("[a]") shouldEqual Result.Ok(
      Tuple(List(Id("a", span(1, 2))), span(0, 3)))

    Parser.typeExpr("[a,b,\"foo\",2]") shouldEqual Result.Ok(
      Tuple(
        List(
          Id("a", span(1, 2)),
          Id("b", span(3, 4)),
          Singleton(Literal.Str("foo"), span(5, 10)),
          Singleton(Literal.Int(2), span(11, 12))),
        span(0, 13)))
    Parser.typeExpr("[ a , b , \"foo\" , 2 ] ") shouldEqual Result.Ok(
      Tuple(
        List(
          Id("a", span(2, 3)),
          Id("b", span(6, 7)),
          Singleton(Literal.Str("foo"), span(10, 15)),
          Singleton(Literal.Int(2), span(18, 19))),
        span(0, 21)))
    Parser.typeExpr("[\na\n,\nb\n,\n\"foo\"\n,\n2\n]\n") shouldEqual Result.Ok(
      Tuple(
        List(
          Id("a", span(2, 3)),
          Id("b", span(6, 7)),
          Singleton(Literal.Str("foo"), span(10, 15)),
          Singleton(Literal.Int(2), span(18, 19))),
        span(0, 21)))
  }

  it should "accept lambdas" in {
    import TypeExpr.{ Id, Lambda, Union }

    Parser.typeExpr("() => int") shouldEqual Result.Ok(
      Lambda(Seq.empty, None, Id("int", span(6, 9)), span(0, 9)))

    Parser.typeExpr("int => int") shouldEqual Result.Ok(
      Lambda(
        Seq(None -> Id("int", span(0, 3))),
        None,
        Id("int", span(7, 10)),
        span(0, 10)))

    Parser.typeExpr("int | str => int") shouldEqual Result.Ok(
      Lambda(
        Seq(
          None -> Union(
            Seq(Id("int", span(0, 3)), Id("str", span(6, 9))),
            span(0, 9))),
        None,
        Id("int", span(13, 16)),
        span(0, 16)))

    Parser.typeExpr("(a, b) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(None -> Id("a", span(1, 2)), None -> Id("b", span(4, 5))),
        None,
        Id("int", span(10, 13)),
        span(0, 13)))

    Parser.typeExpr("(a, b, ...c) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(None -> Id("a", span(1, 2)), None -> Id("b", span(4, 5))),
        Some(None -> Id("c", span(10, 11))),
        Id("int", span(16, 19)),
        span(0, 19)))

    Parser.typeExpr("(a, ...b)") shouldEqual Result.Err(
      List(Failure.InvalidVariadic(span(7, 8))))
    Parser.typeExpr("(a, ...b, c)") shouldEqual Result.Err(
      List(Failure.InvalidVariadic(span(7, 8))))
    Parser.typeExpr("(a, ...b, c) => int") shouldEqual Result.Err(
      List(Failure.InvalidVariadic(span(7, 8))))

    // With named parameters.
    Parser.typeExpr("(a: int) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(Some(Name("a", span(1, 2))) -> Id("int", span(4, 7))),
        None,
        Id("int", span(12, 15)),
        span(0, 15)))

    Parser.typeExpr("(a: (int | str)) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(
          Some(Name("a", span(1, 2))) -> Union(
            Seq(Id("int", span(5, 8)), Id("str", span(11, 14))),
            span(5, 14))),
        None,
        Id("int", span(20, 23)),
        span(0, 23)))

    Parser.typeExpr("(one: a, two: b) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(
          Some(Name("one", span(1, 4))) -> Id("a", span(6, 7)),
          Some(Name("two", span(9, 12))) -> Id("b", span(14, 15))),
        None,
        Id("int", span(20, 23)),
        span(0, 23)))

    Parser.typeExpr("(one: a, two: b, rest: ...c) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(
          Some(Name("one", span(1, 4))) -> Id("a", span(6, 7)),
          Some(Name("two", span(9, 12))) -> Id("b", span(14, 15))),
        Some(Some(Name("rest", span(17, 21))) -> Id("c", span(26, 27))),
        Id("int", span(32, 35)),
        span(0, 35)
      ))

    // Requires a tuple.
    // TODO: Engineer a better error, or accept names with "bare" arguments.
    Parser.typeExpr("a: int => int").isErr shouldEqual true

    // Doesn't work with & or |
    Parser.typeExpr("(a: int) & string") shouldEqual Result.Err(
      Failure.InvalidNamedType(span(1, 2)))
    Parser.typeExpr("(a: int) | string") shouldEqual Result.Err(
      Failure.InvalidNamedType(span(1, 2)))

    // Naming only some args is allowed.
    Parser.typeExpr("(one: a, two: b, ...c) => int") shouldEqual Result.Ok(
      Lambda(
        Seq(
          Some(Name("one", span(1, 4))) -> Id("a", span(6, 7)),
          Some(Name("two", span(9, 12))) -> Id("b", span(14, 15))),
        Some(None -> Id("c", span(20, 21))),
        Id("int", span(26, 29)),
        span(0, 29)
      ))
  }

  it should "parse lambda arrows as right-associative" in {
    import TypeExpr.{ Lambda, Id }

    Parser.typeExpr("A => B => C") shouldEqual Result.Ok(
      Lambda(
        Seq(None -> Id("A", span(0, 1))),
        None,
        Lambda(
          Seq(None -> Id("B", span(5, 6))),
          None,
          Id("C", span(10, 11)),
          span(5, 11)),
        span(0, 11)))

    Parser.typeExpr("(A => B) => C") shouldEqual Result.Ok(
      Lambda(
        Seq(
          None ->
            Lambda(
              Seq(None -> Id("A", span(1, 2))),
              None,
              Id("B", span(6, 7)),
              span(1, 7))
        ),
        None,
        Id("C", span(12, 13)),
        span(1, 13)))

    Parser.typeExpr("A => (B => C)") shouldEqual Result.Ok(
      Lambda(
        Seq(None -> Id("A", span(0, 1))),
        None,
        Lambda(
          Seq(None -> Id("B", span(6, 7))),
          None,
          Id("C", span(11, 12)),
          span(6, 12)),
        span(0, 12)))
  }

  it should "handle invalid exprs nicely" in {
    parseTExprErr(
      "3 | ##",
      """|error: Expected type
         |at *query*:1:5
         |  |
         |1 | 3 | ##
         |  |     ^
         |  |""".stripMargin
    )

    parseTExprErr(
      "{ a: 3, b: ## }",
      """|error: Expected type
         |at *query*:1:12
         |  |
         |1 | { a: 3, b: ## }
         |  |            ^
         |  |""".stripMargin
    )

    parseTExprErr(
      "{ a: 3, ## }",
      """|error: Expected `}`
         |at *query*:1:9
         |  |
         |1 | { a: 3, ## }
         |  |         ^
         |  |""".stripMargin
    )
  }
}
