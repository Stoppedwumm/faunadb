package fql.test.parser

import fql.ast._
import fql.ast.Expr.{ MethodChain, OperatorCall }
import fql.error.Hint
import fql.error.ParseError
import fql.parser._
import fql.test.Spec
import fql.Result

class ExprParserSpec extends Spec {
  def span(s: Int, e: Int) = Span(s, e, Src.Query(""))

  def parseErr(query: String) = {
    Parser.expr(query, src = Src.Query(query)) match {
      case Result.Ok(ast)  => fail(s"expected an error, got tree: $ast")
      case Result.Err(err) => err.map(_.renderWithSource(Map.empty)).mkString("\n")
    }
  }

  "An Expr parser" should "respect the source offset" in {
    import Expr.Lit
    import Literal.Null

    Parser.expr("null") shouldEqual Result.Ok(Lit(Null, span(0, 4)))
    Parser.expr("null", srcOffset = 10) shouldEqual Result.Ok(
      Lit(Null, span(10, 14)))
  }

  it should "accept literals, consuming trailing whitespace" in {
    import Expr.Lit
    import Literal._

    Parser.expr("null  ") shouldEqual Result.Ok(Lit(Null, span(0, 4)))
    Parser.expr("true  ") shouldEqual Result.Ok(Lit(True, span(0, 4)))
    Parser.expr("false ") shouldEqual Result.Ok(Lit(False, span(0, 5)))
    Parser.expr("123   ") shouldEqual Result.Ok(Lit(Int("123"), span(0, 3)))
    Parser.expr("0.123 ") shouldEqual Result.Ok(Lit(Float("0.123"), span(0, 5)))
    Parser.expr("'foo' ") shouldEqual Result.Ok(Lit(Str("foo"), span(0, 5)))
  }

  it should "accept string interpolations/templates, consuming trailing whitespace" in {
    import Expr.{ Lit, StrTemplate }
    import Literal.{ Int, Str }

    Parser.expr("''") shouldEqual Result.Ok(Lit(Str(""), span(0, 2)))

    Parser.expr("\"foo\" ") shouldEqual Result.Ok(Lit(Str("foo"), span(0, 5)))

    Parser.expr("\"foo#{123}bar\" ") shouldEqual Result.Ok(
      StrTemplate(
        List(Left("foo"), Right(Lit(Int("123"), span(6, 9))), Left("bar")),
        span(0, 14)))

    Parser.expr("\"foo#{123 }bar\" ") shouldEqual Result.Ok(
      StrTemplate(
        List(Left("foo"), Right(Lit(Int("123"), span(6, 9))), Left("bar")),
        span(0, 15)))

    Parser.expr("\"foo#{ 123}bar\" ") shouldEqual Result.Ok(
      StrTemplate(
        List(Left("foo"), Right(Lit(Int("123"), span(7, 10))), Left("bar")),
        span(0, 15)))

    Parser.expr("\"#{123}\" ") shouldEqual Result.Ok(
      StrTemplate(List(Right(Lit(Int("123"), span(3, 6)))), span(0, 8)))
  }

  it should "accept multiline strings" in {
    import Expr.StrTemplate

    Parser.expr("""|<<+END
         |a multiline
         |string
         |END""".stripMargin) shouldEqual Result.Ok(
      StrTemplate(
        List(Left("a multiline"), Left("\n"), Left("string"), Left("\n")),
        span(0, 29)))

    Parser.expr("""|<<-END
                   |a multiline
                   |string
                   |END""".stripMargin) shouldEqual Result.Ok(
      Expr.Lit(Literal.Str("a multiline\nstring\n"), span(0, 29)))
  }

  it should "accept multiline strings interpolations" in {
    import Expr.{ Lit, StrTemplate }
    import Literal.{ Int, Str }

    Parser.expr("""|<<+END
         |a multiline
         |#{10} #{'str'}
         |string
         |END""".stripMargin) shouldEqual Result.Ok(
      StrTemplate(
        List(
          Left("a multiline"),
          Left("\n"),
          Right(Lit(Int(10), span(21, 23))),
          Left(" "),
          Right(Lit(Str("str"), span(27, 32))),
          Left("\n"),
          Left("string"),
          Left("\n")
        ),
        span(0, 44)
      ))
  }

  it should "accept token-like in the middle of multiline strings" in {
    import Expr.StrTemplate

    Parser.expr("""|<<+END
                   |ENDOR
                   |APPEND
                   |END""".stripMargin) shouldEqual Result.Ok(
      StrTemplate(
        List(Left("ENDOR"), Left("\n"), Left("APPEND"), Left("\n")),
        span(0, 23)
      ))

    Parser.expr("""|<<-END
                   |ENDOR
                   |APPEND
                   |END""".stripMargin) shouldEqual Result.Ok(
      Expr.Lit(Literal.Str("ENDOR\nAPPEND\n"), span(0, 23)))
  }

  it should "accept empty multiline string" in {
    Parser.expr("""|<<+END
       |END""".stripMargin) shouldEqual Result.Ok(
      Expr.Lit(Literal.Str(""), span(0, 10)))

    Parser.expr("""|<<-END
                   |END""".stripMargin) shouldEqual Result.Ok(
      Expr.Lit(
        Literal.Str(""),
        span(0, 10)
      ))
  }

  it should "ignore whitespaces after multiline end token" in {
    import Expr.StrTemplate

    Parser.expr(s"""|<<+END
          |a string
          |END
          | \t
          | \t
          | \t""".stripMargin) shouldEqual Result.Ok(
      StrTemplate(
        List(Left("a string"), Left("\n")),
        span(0, 19)
      ))

    Parser.expr(s"""|<<-END
                    |a string
                    |END
                    | \t
                    | \t
                    | \t""".stripMargin) shouldEqual Result.Ok(
      Expr.Lit(
        Literal.Str("a string\n"),
        span(0, 19)
      ))
  }

  it should "ignore interpolation tokens" in {
    Parser.expr(s"""|<<-END
                    |a string #{str}
                    |END""".stripMargin) shouldEqual Result.Ok(
      Expr.Lit(
        Literal.Str("a string #{str}\n"),
        span(0, 26)
      ))
  }

  it should "accept arrays" in {
    import Expr.Array
    import Expr.Lit
    import Literal.Int

    Parser.expr("[1,2,3]") shouldEqual Result.Ok(
      Array(
        List(
          Lit(Int("1"), span(1, 2)),
          Lit(Int("2"), span(3, 4)),
          Lit(Int("3"), span(5, 6))
        ),
        span(0, 7)))

    Parser.expr("\n [ \n 1 \n , \n 2 \n , \n 3 \n ] \n") shouldEqual Result.Ok(
      Array(
        List(
          Lit(Int("1"), span(6, 7)),
          Lit(Int("2"), span(14, 15)),
          Lit(Int("3"), span(22, 23))
        ),
        span(2, 27)))
  }

  it should "accept objects" in {
    import Expr.{ Lit, Object }
    import Literal.Int

    Parser.expr("{a:1,b:2,c:3}") shouldEqual Result.Ok(
      Object(
        List(
          Name("a", span(1, 2)) -> Lit(Int("1"), span(3, 4)),
          Name("b", span(5, 6)) -> Lit(Int("2"), span(7, 8)),
          Name("c", span(9, 10)) -> Lit(Int("3"), span(11, 12))
        ),
        span(0, 13)
      ))
    Parser.expr("{'a':1}") shouldEqual Result.Ok(
      Object(List(Name("a", span(1, 4)) -> Lit(Int("1"), span(5, 6))), span(0, 7)))
    Parser.expr("{}") shouldEqual Result.Ok(Object(Nil, span(0, 2)))

    Parser.expr("\n{\na :\n1\n}\n") shouldEqual Result.Ok(
      Object(List(Name("a", span(3, 4)) -> Lit(Int("1"), span(7, 8))), span(1, 10)))

    Parser.expr("{\"\":0}") shouldEqual Result.Ok(
      Object(List(Name("", span(1, 3)) -> Lit(Int("0"), span(4, 5))), span(0, 6)))
  }

  it should "accept blocks" in {
    import Expr.{ Block }
    import Literal.Str

    Parser.expr("{'a'}") shouldEqual Result.Ok(
      Block(Seq(Expr.Stmt.Expr(Expr.Lit(Str("a"), span(1, 4)))), span(0, 5)))

    // This tests to make sure that istring0 can backtrack for object keys
    Parser.expr("{\"a\"}") shouldEqual Result.Ok(
      Block(Seq(Expr.Stmt.Expr(Expr.Lit(Str("a"), span(1, 4)))), span(0, 5)))

    Parser.expr("{'a';'b'}") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(Expr.Lit(Str("a"), span(1, 4))),
          Expr.Stmt.Expr(Expr.Lit(Str("b"), span(5, 8)))),
        span(0, 9)))

    Parser.expr("\n{\n'a'\n}\n") shouldEqual Result.Ok(
      Block(Seq(Expr.Stmt.Expr(Expr.Lit(Str("a"), span(3, 6)))), span(1, 8)))

    Parser.expr("\n{\n'a'\n'b'\n}\n") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(Expr.Lit(Str("a"), span(3, 6))),
          Expr.Stmt.Expr(Expr.Lit(Str("b"), span(7, 10)))),
        span(1, 12)))

    Parser.expr("\n{//foo\n'a'//bar\n'b'//baz\n}\n") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(Expr.Lit(Str("a"), span(8, 11))),
          Expr.Stmt.Expr(Expr.Lit(Str("b"), span(17, 20)))),
        span(1, 27)))

    Parser.expr("\n{/*foo*/\n'a'/*bar*/\n'b'/*baz*/\n}\n") shouldEqual Result
      .Ok(
        Block(
          Seq(
            Expr.Stmt.Expr(Expr.Lit(Str("a"), span(10, 13))),
            Expr.Stmt.Expr(Expr.Lit(Str("b"), span(21, 24)))),
          span(1, 33)))
  }

  it should "accept blocks with final semicolon" in {
    import Expr.{ Block }
    import Literal.Str

    Parser.expr("{'a';}") shouldEqual Result.Ok(
      Block(Seq(Expr.Stmt.Expr(Expr.Lit(Str("a"), span(1, 4)))), span(0, 6)))

    Parser.expr("{'a';'b';}") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(Expr.Lit(Str("a"), span(1, 4))),
          Expr.Stmt.Expr(Expr.Lit(Str("b"), span(5, 8)))),
        span(0, 10)))

    Parser.expr("\n{\n'a';\n}\n") shouldEqual Result.Ok(
      Block(Seq(Expr.Stmt.Expr(Expr.Lit(Str("a"), span(3, 6)))), span(1, 9)))

    Parser.expr("\n{\n'a';\n'b';\n}\n") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(Expr.Lit(Str("a"), span(3, 6))),
          Expr.Stmt.Expr(Expr.Lit(Str("b"), span(8, 11)))),
        span(1, 14)))

    Parser.expr("\n{//foo\n'a';//bar\n'b';//baz\n}\n") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(Expr.Lit(Str("a"), span(8, 11))),
          Expr.Stmt.Expr(Expr.Lit(Str("b"), span(18, 21)))),
        span(1, 29)))

    Parser.expr("\n{/*foo*/\n'a';/*bar*/\n'b';/*baz*/\n}\n") shouldEqual Result
      .Ok(
        Block(
          Seq(
            Expr.Stmt.Expr(Expr.Lit(Str("a"), span(10, 13))),
            Expr.Stmt.Expr(Expr.Lit(Str("b"), span(22, 25)))),
          span(1, 35)))
  }

  it should "reject objects with duplicate keys" in {
    def err(spans: List[(Int, Int, Int, Int)]) =
      Result.Err(spans.map { case (f1, f2, h1, h2) =>
        Failure.DuplicateObjectKey(span(f1, f2), span(h1, h2))
      })

    Parser.expr("{ a: 2, a: 3 }") shouldEqual err(List((2, 3, 8, 9)))
    Parser.expr("{ a: 2, b: 3, a: 4 }") shouldEqual err(List((2, 3, 14, 15)))
    Parser.expr("{ a: 2, a: 3, a: 4 }") shouldEqual err(
      List((2, 3, 8, 9), (2, 3, 14, 15)))
  }

  it should "reject objects with interpolated string keys" in {
    def err(a1: Int, a2: Int, b1: Int, b2: Int, c1: Int, c2: Int) =
      Result.Err(
        ParseError(
          "Object keys do not support interpolation",
          span(a1, a2),
          hints = Seq(
            Hint(
              "Add a backslash to ignore the interpolation",
              span(b1, b2),
              Some("\\")),
            Hint("Use Object.fromEntries if you need a dynamic key", span(c1, c2))
          )
        ))

    Parser.expr("{ \"#{3}\": 2 }") shouldEqual err(5, 6, 3, 3, 2, 8)
  }

  it should "reject blocks that end with let statement" in {
    def err(f1: Int, f2: Int, h1: Int, h2: Int) =
      Result.Err(List(Failure.MissingReturn(span(f1, f2), span(h1, h2))))

    Parser.expr("{ x; let y = \"foo\" }") shouldEqual err(19, 20, 5, 18)
    Parser.expr("{ x; let y = \"foo\"\n}") shouldEqual err(19, 20, 5, 18)
    Parser.expr("{\n  x\n  let x = 'foo' \n}") shouldEqual err(23, 24, 8, 21)
  }

  it should "reject short lambdas outside of calls" in {
    def err(f1: Int, f2: Int) =
      Result.Err(List(Failure.InvalidShortLambda(span(f1, f2))))

    Parser.expr(".foo") shouldEqual err(0, 4)
    Parser.expr("{ let a = .foo; 3 }") shouldEqual err(10, 14)
    // TODO: The span could be better here.
    Parser.expr("2 + .foo") shouldEqual err(0, 8)
    Parser.expr("(.foo + .bar) / 3") shouldEqual err(0, 17)
  }

  it should "reject lambdas with duplicate arguments" in {
    def err(spans: List[(Int, Int, Int, Int)]) =
      Result.Err(spans.map { case (f1, f2, h1, h2) =>
        Failure.DuplicateLambdaArgument(span(f1, f2), span(h1, h2))
      })

    Parser.expr("(x, x) => x") shouldEqual err(List((1, 2, 4, 5)))
    Parser.expr("(x, foo, x) => x") shouldEqual err(List((1, 2, 9, 10)))
    Parser.expr("(x, x, x) => x") shouldEqual err(List((1, 2, 4, 5), (1, 2, 7, 8)))
  }

  it should "reject keywords as idents" in {
    def err(str: String, start: Int, end: Int) =
      Result.Err(
        List(Failure.InvalidIdent(Name(str, Span(start, end, Src.Query(""))))))

    Parser.query("let let = 3") shouldEqual err("let", 4, 7)
    Parser.query("let true = 3") shouldEqual err("true", 4, 8)
    Parser.query("let FQL = 3") shouldEqual err("FQL", 4, 7)

    // FIXME: This produces an 'unexpected end of block' error
    Parser.query("let.log") should matchPattern { case Result.Err(_) => }
    Parser.query("true.log") should matchPattern { case Result.Ok(_) => }
    Parser.query("FQL.log") should matchPattern { case Result.Ok(_) => }
  }

  it should "accept keywords as field access" in {
    import Expr.{ Block, Object, MethodChain, Lit }
    Parser.query("{ 'if': 123 }.if") shouldEqual Result.Ok(
      Block(
        Seq(
          Expr.Stmt.Expr(MethodChain(
            Object(
              List((Name("if", span(2, 6)), Lit(Literal.Int(123), span(8, 11)))),
              span(0, 13)),
            List(MethodChain
              .Select(span(13, 14), Name("if", span(14, 16)), false)),
            span(0, 16)
          ))
        ),
        span(0, 16)
      ))
  }

  it should "allow block with semicolon after final statement" in {
    import Expr.{ Block, Lit, Id }
    import Literal.Str

    def res(ret: Expr, b1: Int, b2: Int) =
      Result.Ok(Block(Seq(Expr.Stmt.Expr(ret)), span(b1, b2)))
    Parser.expr("{ 'foo'; }") shouldEqual res(Lit(Str("foo"), span(2, 7)), 0, 10)
    Parser.expr("{ x; }") shouldEqual res(Id("x", span(2, 3)), 0, 6)
  }

  it should "handle unexpected close \"}\"" in {
    def err(b1: Int, b2: Int) =
      Result.Err(List(Failure.Unexpected.EndOfQuery(span(b1, b2))))
    Parser.query("}") shouldEqual err(0, 1)
  }

  it should "accept tuples" in {
    import Expr.{ Id, Tuple, Lit }
    import Literal.Int

    Parser.expr("(1)") shouldEqual Result.Ok(
      Tuple(List(Lit(Int("1"), span(1, 2))), span(0, 3)))
    Parser.expr("(1,2)") shouldEqual Result.Ok(
      Tuple(List(Lit(Int("1"), span(1, 2)), Lit(Int("2"), span(3, 4))), span(0, 5)))
    Parser.expr("(a)") shouldEqual Result.Ok(
      Tuple(List(Id("a", span(1, 2))), span(0, 3)))
    Parser.expr("(a,b)") shouldEqual Result.Ok(
      Tuple(List(Id("a", span(1, 2)), Id("b", span(3, 4))), span(0, 5)))
  }

  it should "accept ids" in {
    import Expr.Id
    Parser.expr("foo") shouldEqual Result.Ok(Id("foo", span(0, 3)))
  }

  it should "accept lambdas" in {
    import Expr.{ LongLambda, Lit }
    Parser.expr("(a)=>null") shouldEqual Result.Ok(
      LongLambda(
        List(Some(Name("a", span(1, 2)))),
        None,
        Lit(Literal.Null, span(5, 9)),
        span(0, 9)))
    Parser.expr("(a,b)=>null") shouldEqual Result.Ok(
      LongLambda(
        List(Some(Name("a", span(1, 2))), Some(Name("b", span(3, 4)))),
        None,
        Lit(Literal.Null, span(7, 11)),
        span(0, 11)))
    Parser.expr("a=>null") shouldEqual Result.Ok(
      LongLambda(
        List(Some(Name("a", span(0, 1)))),
        None,
        Lit(Literal.Null, span(3, 7)),
        span(0, 7)))
    Parser.expr("(a,_,b,_)=>null") shouldEqual Result.Ok(
      LongLambda(
        List(Some(Name("a", span(1, 2))), None, Some(Name("b", span(5, 6))), None),
        None,
        Lit(Literal.Null, span(11, 15)),
        span(0, 15)))
  }

  it should "accept variadic lambdas" in {
    import Expr.{ LongLambda, Lit }
    Parser.expr("(...a)=>null") shouldEqual Result.Ok(
      LongLambda(
        List(),
        Some(Some(Name("a", span(4, 5)))),
        Lit(Literal.Null, span(8, 12)),
        span(0, 12)))

    Parser.expr("(a, ...b)=>null") shouldEqual Result.Ok(
      LongLambda(
        List(Some(Name("a", span(1, 2)))),
        Some(Some(Name("b", span(7, 8)))),
        Lit(Literal.Null, span(11, 15)),
        span(0, 15)))

    Parser.expr("(_, ..._)=>null") shouldEqual Result.Ok(
      LongLambda(
        List(None),
        Some(None),
        Lit(Literal.Null, span(11, 15)),
        span(0, 15)))
  }

  it should "reject invalid lambdas" in {
    Parser.expr("('a') => null") should matchPattern { case Result.Err(_) => }
    Parser.expr("a, a => null") should matchPattern { case Result.Err(_) => }
  }

  it should "reject invalid variadic lambdas" in {
    Parser.expr("(...a + 1)=>null") should matchPattern { case Result.Err(_) => }

    Parser.expr("(a + 1, ...b)=>null") should matchPattern { case Result.Err(_) => }

    Parser.expr("(...a, b)=>null") should matchPattern { case Result.Err(_) => }

    Parser.expr("(..._, ..._)=>null") should matchPattern { case Result.Err(_) => }

    Parser.expr("(a, ...b, c)=>null") should matchPattern { case Result.Err(_) => }
  }

  it should "parse underscore args correctly" in {
    import Expr.{ LongLambda, Lit }

    Parser.expr("_ => 3") shouldEqual Result.Ok(
      LongLambda(List(None), None, Lit(Literal.Int(3), span(5, 6)), span(0, 6)))

    Parser.expr("(_) => 3") shouldEqual Result.Ok(
      LongLambda(List(None), None, Lit(Literal.Int(3), span(7, 8)), span(0, 8)))
  }

  it should "accept if statements" in {
    import Expr.{ Lit }
    import Literal.{ True, Int }

    Parser.expr("if (true) 3") shouldEqual Result.Ok(
      Expr.If(Lit(True, span(4, 8)), Lit(Int("3"), span(10, 11)), span(0, 11)))
    Parser.expr("if (true) 3 else 4") shouldEqual Result.Ok(
      Expr.IfElse(
        Lit(True, span(4, 8)),
        Lit(Int("3"), span(10, 11)),
        Lit(Int("4"), span(17, 18)),
        span(0, 18)))
  }

  it should "accept newlines before else" in {
    import Expr.{ Lit }
    import Literal.{ True, Int }

    Parser.expr("if (true) 3\nelse 4") shouldEqual Result.Ok(
      Expr.IfElse(
        Lit(True, span(4, 8)),
        Lit(Int("3"), span(10, 11)),
        Lit(Int("4"), span(17, 18)),
        span(0, 18)))

    Parser.expr("{ if (true) 3\n\nelse 4 }") shouldEqual Result.Ok(
      Expr.Block(
        Seq(
          Expr.Stmt.Expr(
            Expr.IfElse(
              Lit(True, span(6, 10)),
              Lit(Int("3"), span(12, 13)),
              Lit(Int("4"), span(20, 21)),
              span(2, 21)))),
        span(0, 23)))

    Parser.expr("{ let a = if (true) 3; a }") shouldEqual Result.Ok(
      Expr.Block(
        Seq(
          Expr.Stmt.Let(
            Name("a", span(6, 7)),
            None,
            Expr.If(
              Lit(True, span(14, 18)),
              Lit(Int("3"), span(20, 21)),
              span(10, 21)),
            span(2, 21)),
          Expr.Stmt.Expr(Expr.Id("a", span(23, 24)))
        ),
        span(0, 26)
      ))

    Parser.expr("{ let a = if (true) 3\na }") shouldEqual Result.Ok(
      Expr.Block(
        Seq(
          Expr.Stmt.Let(
            Name("a", span(6, 7)),
            None,
            Expr.If(
              Lit(True, span(14, 18)),
              Lit(Int("3"), span(20, 21)),
              span(10, 21)),
            span(2, 21)),
          Expr.Stmt.Expr(Expr.Id("a", span(22, 23)))
        ),
        span(0, 25)
      ))
  }

  def term0s(implicit i: Int = 0) = {
    import Expr.{ Id, Lit, Tuple, Array, Block, Object }
    import Literal.{ Float, Int }
    Seq(
      "a" -> Id("a", span(i, i + 1)),
      "1" -> Lit(Int("1"), span(i, i + 1)),
      "1e1" -> Lit(Float("1e1"), span(i, i + 3)),
      "1e-1" -> Lit(Float("1e-1"), span(i, i + 4)),
      "1e+1" -> Lit(Float("1e+1"), span(i, i + 4)),
      // -/+ bind higher than field access/apply on numbers.
      "-1" -> Lit(Int("-1"), span(i, i + 2)),
      "- 1" -> Lit(Int("-1"), span(i, i + 3)),
      "-1e1" -> Lit(Float("-1e1"), span(i, i + 4)),
      "-1e-1" -> Lit(Float("-1e-1"), span(i, i + 5)),
      "-1e+1" -> Lit(Float("-1e+1"), span(i, i + 5)),
      "+1" -> Lit(Int("1"), span(i, i + 2)),
      "+ 1" -> Lit(Int("1"), span(i, i + 3)),
      "+1e1" -> Lit(Float("1e1"), span(i, i + 4)),
      "+1e-1" -> Lit(Float("1e-1"), span(i, i + 5)),
      "+1e+1" -> Lit(Float("1e+1"), span(i, i + 5)),
      "(a,a)" -> Tuple(
        List(Id("a", span(i + 1, i + 2)), Id("a", span(i + 3, i + 4))),
        span(i, i + 5)),
      "(1,1)" -> Tuple(
        List(Lit(Int("1"), span(i + 1, i + 2)), Lit(Int("1"), span(i + 3, i + 4))),
        span(i, i + 5)),
      "[a]" -> Array(List(Id("a", span(i + 1, i + 2))), span(i, i + 3)),
      "{a}" -> Block(
        Seq(Expr.Stmt.Expr(Id("a", span(i + 1, i + 2)))),
        span(i, i + 3)),
      "{a:1}" -> Object(
        List((Name("a", span(i + 1, i + 2)), Lit(Int("1"), span(i + 3, i + 4)))),
        span(i, i + 5))
    )
  }

  it should "accept term0s" in {
    for ((term, ast) <- term0s) {
      Parser.expr(term) shouldEqual Result.Ok(ast)
    }
  }

  def sel(term: String, ast: Expr) = {
    val Span(i1, i2, _) = ast.span
    ast match {
      // in the scenario where the ast is already a method chain, we want to append
      // to that chain
      // as opposed to wrapping the method chain in another method chain
      case mc: MethodChain =>
        Seq(
          s"$term.foo" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Select(
                span(i2, i2 + 1),
                Name("foo", span(i2 + 1, i2 + 4)),
                optional = false)
            ),
            span = span(i1, i2 + 4)
          ),
          s"$term._foo" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Select(
                span(i2, i2 + 1),
                Name("_foo", span(i2 + 1, i2 + 5)),
                optional = false)
            ),
            span = span(i1, i2 + 5)
          ),
          s"$term?.foo" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Select(
                span(i2, i2 + 2),
                Name("foo", span(i2 + 2, i2 + 5)),
                optional = true)
            ),
            span = span(i1, i2 + 5)
          )
        )
      case _ =>
        Seq(
          s"$term.foo" -> MethodChain(
            ast,
            Seq(
              MethodChain.Select(
                span(i2, i2 + 1),
                Name("foo", span(i2 + 1, i2 + 4)),
                false)
            ),
            span(i1, i2 + 4)
          ),
          s"$term._foo" -> MethodChain(
            ast,
            Seq(
              MethodChain.Select(
                span(i2, i2 + 1),
                Name("_foo", span(i2 + 1, i2 + 5)),
                false)
            ),
            span(i1, i2 + 5)
          ),
          s"$term?.foo" -> MethodChain(
            ast,
            Seq(
              MethodChain.Select(
                span(i2, i2 + 2),
                Name("foo", span(i2 + 2, i2 + 5)),
                optional = true)
            ),
            span = span(i1, i2 + 5)
          )
        )
    }
  }

  def app(term: String, ast: Expr) = {
    import Expr.Lit
    import Literal.Int
    val Span(i1, i2, _) = ast.span
    ast match {
      // in the scenario where the ast is already a method chain, we want to append
      // to that chain
      // as opposed to wrapping the method chain in another method chain
      case mc: MethodChain =>
        Seq(
          s"$term()" -> mc.copy(
            chain =
              mc.chain.appended(MethodChain.Apply(List(), None, span(i2, i2 + 2))),
            span = span(i1, i2 + 2)),
          s"$term?.()" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain
                .Apply(List(), Some(span(i2, i2 + 2)), span(i2 + 2, i2 + 4))),
            span = span(i1, i2 + 4)),
          s"$term(1)" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Apply(
                List(Lit(Int("1"), span(i2 + 1, i2 + 2))),
                None,
                span(i2, i2 + 3)
              )
            ),
            span = span(i1, i2 + 3)),
          s"$term(1,2)" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Apply(
                List(
                  Lit(Int("1"), span(i2 + 1, i2 + 2)),
                  Lit(Int("2"), span(i2 + 3, i2 + 4))
                ),
                None,
                span(i2, i2 + 5)
              )),
            span = span(i1, i2 + 5)
          )
        )
      case _ =>
        Seq(
          s"$term()" -> MethodChain(
            ast,
            Seq(MethodChain.Apply(List(), None, span(i2, i2 + 2))),
            span(i1, i2 + 2)),
          s"$term?.()" -> MethodChain(
            ast,
            Seq(
              MethodChain
                .Apply(List(), Some(span(i2, i2 + 2)), span(i2 + 2, i2 + 4))),
            span(i1, i2 + 4)),
          s"$term(1)" -> MethodChain(
            ast,
            Seq(
              MethodChain.Apply(
                List(Lit(Int("1"), span(i2 + 1, i2 + 2))),
                None,
                span(i2, i2 + 3)
              )
            ),
            span(i1, i2 + 3)),
          s"$term(1,2)" -> MethodChain(
            ast,
            Seq(
              MethodChain.Apply(
                List(
                  Lit(Int("1"), span(i2 + 1, i2 + 2)),
                  Lit(Int("2"), span(i2 + 3, i2 + 4))
                ),
                None,
                span(i2, i2 + 5)
              )),
            span(i1, i2 + 5)
          )
        )

    }
  }

  def call(term: String, ast: Expr) = {
    import Expr.Lit
    import Literal.Int
    val Span(i1, i2, _) = ast.span
    Seq(
      s"$term.foo()" -> MethodChain(
        ast,
        Seq(
          MethodChain.MethodCall(
            span(i2, i2 + 1),
            Name("foo", span(i2 + 1, i2 + 4)),
            List(),
            selectOptional = false,
            applyOptional = None,
            span(i2 + 4, i2 + 6))
        ),
        span(i1, i2 + 6)
      ),
      s"$term?.foo()" -> MethodChain(
        ast,
        Seq(
          MethodChain.MethodCall(
            span(i2, i2 + 2),
            Name("foo", span(i2 + 2, i2 + 5)),
            List(),
            selectOptional = true,
            applyOptional = None,
            span(i2 + 5, i2 + 7))
        ),
        span(i1, i2 + 7)
      ),
      s"$term.foo?.()" -> MethodChain(
        ast,
        Seq(
          MethodChain.MethodCall(
            span(i2, i2 + 1),
            Name("foo", span(i2 + 1, i2 + 4)),
            List(),
            selectOptional = false,
            applyOptional = Some(span(i2 + 4, i2 + 6)),
            span(i2 + 6, i2 + 8))
        ),
        span(i1, i2 + 8)
      ),
      s"$term.foo(1)" -> MethodChain(
        ast,
        Seq(
          MethodChain.MethodCall(
            span(i2, i2 + 1),
            Name("foo", span(i2 + 1, i2 + 4)),
            List(Lit(Int("1"), span(i2 + 5, i2 + 6))),
            selectOptional = false,
            applyOptional = None,
            span(i2 + 4, i2 + 7))),
        span(i1, i2 + 7)
      ),
      s"$term.foo(1,2)" -> MethodChain(
        ast,
        Seq(
          MethodChain.MethodCall(
            span(i2, i2 + 1),
            Name("foo", span(i2 + 1, i2 + 4)),
            List(
              Lit(Int("1"), span(i2 + 5, i2 + 6)),
              Lit(Int("2"), span(i2 + 7, i2 + 8))),
            selectOptional = false,
            applyOptional = None,
            span(i2 + 4, i2 + 9)
          )),
        span(i1, i2 + 9)
      )
    )
  }

  def arr(term: String, ast: Expr) = {
    import Expr.{ Lit }
    import Literal.Int
    val Span(i1, i2, _) = ast.span
    ast match {
      // in the scenario where the ast is already a method chain, we want to append
      // to that chain
      // as opposed to wrapping the method chain in another method chain
      case mc: MethodChain =>
        Seq(
          s"$term[]" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain
                .Access(List(), optional = None, span(i2, i2 + 2))),
            span = span(i1, i2 + 2)
          ),
          s"$term?.[]" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain
                .Access(
                  List(),
                  optional = Some(span(i2, i2 + 2)),
                  span(i2 + 2, i2 + 4))),
            span = span(i1, i2 + 4)
          ),
          s"$term[1]" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Access(
                List(Lit(Int("1"), span(i2 + 1, i2 + 2))),
                optional = None,
                span(i2, i2 + 3))),
            span = span(i1, i2 + 3)
          ),
          s"$term[1,2]" -> mc.copy(
            chain = mc.chain.appended(
              MethodChain.Access(
                List(
                  Lit(Int("1"), span(i2 + 1, i2 + 2)),
                  Lit(Int("2"), span(i2 + 3, i2 + 4))),
                optional = None,
                span(i2, i2 + 5))),
            span = span(i1, i2 + 5)
          )
        )
      case _ =>
        Seq(
          s"$term[]" -> MethodChain(
            ast,
            Seq(
              MethodChain
                .Access(List(), optional = None, span(i2, i2 + 2))),
            span(i1, i2 + 2)
          ),
          s"$term?.[]" -> MethodChain(
            ast,
            Seq(
              MethodChain
                .Access(
                  List(),
                  optional = Some(span(i2, i2 + 2)),
                  span(i2 + 2, i2 + 4))),
            span(i1, i2 + 4)
          ),
          s"$term[1]" -> MethodChain(
            ast,
            Seq(
              MethodChain.Access(
                List(Lit(Int("1"), span(i2 + 1, i2 + 2))),
                optional = None,
                span(i2, i2 + 3))),
            span(i1, i2 + 3)
          ),
          s"$term[1,2]" -> MethodChain(
            ast,
            Seq(
              MethodChain.Access(
                List(
                  Lit(Int("1"), span(i2 + 1, i2 + 2)),
                  Lit(Int("2"), span(i2 + 3, i2 + 4))),
                optional = None,
                span(i2, i2 + 5))),
            span(i1, i2 + 5)
          )
        )
    }
  }

  def bang(term: String, ast: Expr) = {
    val bangExpr = ast match {
      // in the scenario where the ast is already a method chain, we want to append
      // to that chain
      // as opposed to wrapping the method chain in another method chain
      case mc: MethodChain =>
        mc.copy(
          chain = mc.chain.appended(
            MethodChain.Bang(span(ast.span.end, ast.span.end + 1))
          ),
          span = span(ast.span.start, ast.span.end + 1)
        )
      case _ =>
        MethodChain(
          ast,
          Seq(
            MethodChain.Bang(span(ast.span.end, ast.span.end + 1))
          ),
          span(ast.span.start, ast.span.end + 1)
        )
    }

    Seq(
      s"$term!" -> bangExpr
    )
  }

  def term1s(implicit i: Int = 0) = {
    term0s ++
      term0s.flatMap((sel _).tupled) ++
      term0s.flatMap((app _).tupled) ++
      term0s.flatMap((arr _).tupled) ++
      term0s.flatMap((call _).tupled) ++
      term0s.flatMap((app _).tupled(_).flatMap((sel _).tupled)) ++
      term0s.flatMap((sel _).tupled(_).flatMap((arr _).tupled)) ++
      term0s.flatMap((arr _).tupled(_).flatMap((sel _).tupled)) ++
      term0s.flatMap((app _).tupled(_).flatMap((arr _).tupled)) ++
      term0s.flatMap((arr _).tupled(_).flatMap((app _).tupled))
  }

  it should "accept term1s" in {
    for ((term, ast) <- term1s) {
      Parser.expr(term) shouldEqual Result.Ok(ast)
    }
  }

  it should "accept field access syntax" in {
    for {
      (term, ast)      <- term1s
      (selstr, selast) <- sel(term, ast)
    } {
      Parser.expr(selstr) shouldEqual Result.Ok(selast)
    }
  }

  it should "accept array access syntax" in {
    for {
      (term, ast)      <- term1s
      (arrstr, arrast) <- arr(term, ast)
    } {
      Parser.expr(arrstr) shouldEqual Result.Ok(arrast)
    }
  }

  it should "accept apply syntax" in {
    for {
      (term, ast) <- term1s
      if !ast.isInstanceOf[MethodChain] || (!ast
        .asInstanceOf[MethodChain]
        .chain
        .lastOption
        .exists(_.isInstanceOf[MethodChain.Select]))
      (appstr, appast) <- app(term, ast)
    } {
      Parser.expr(appstr) shouldEqual Result.Ok(appast)
    }

    for {
      (term, ast) <- term1s
      if ast.isInstanceOf[MethodChain] && (ast
        .asInstanceOf[MethodChain]
        .chain
        .lastOption
        .exists(_.isInstanceOf[MethodChain.Select]))
      (appstr, appast) <- app(term, ast)
    } {
      val Expr.MethodChain(_, chain, _) = appast
      val selApp = chain.takeRight(2)
      val select = selApp.head.asInstanceOf[MethodChain.Select]
      val apply = selApp.last.asInstanceOf[MethodChain.Apply]
      val methodCall = MethodChain.MethodCall(
        select.dotSpan,
        select.field,
        apply.args,
        selectOptional = select.optional,
        applyOptional = apply.optional,
        apply.applySpan
      )
      val appAst0 = appast.copy(
        chain = appast.chain.dropRight(2).appended(methodCall)
      )
      if (Parser.expr(appstr) != Result.Ok(appAst0)) {
        println(appstr)
      }
      Parser.expr(appstr) shouldEqual Result.Ok(appAst0)
    }
  }

  it should "combine bang operator with other operators" in {
    import Expr.{ Id, Lit, MethodChain }

    val foo = Id("foo", span(0, 3))

    Parser.expr("foo!") shouldEqual Result.Ok(
      MethodChain(foo, Seq(MethodChain.Bang(span(3, 4))), span(0, 4)))

    Parser.expr("foo()!") shouldEqual Result.Ok(
      MethodChain(
        foo,
        Seq(
          MethodChain.Apply(Seq(), None, span(3, 5)),
          MethodChain.Bang(span(5, 6))
        ),
        span(0, 6)))

    Parser.expr("foo!()!") shouldEqual Result.Ok(
      MethodChain(
        foo,
        Seq(
          MethodChain.Bang(span(3, 4)),
          MethodChain.Apply(Seq.empty, None, span(4, 6)),
          MethodChain.Bang(span(6, 7))
        ),
        span(0, 7)
      )
    )

    Parser.expr("foo[0]!") shouldEqual Result.Ok(
      MethodChain(
        foo,
        Seq(
          MethodChain.Access(
            Seq(Lit(Literal.Int(0), span(4, 5))),
            optional = None,
            span(3, 6)
          ),
          MethodChain.Bang(span(6, 7))
        ),
        span(0, 7)
      )
    )

    Parser.expr("foo![0]!") shouldEqual Result.Ok(
      MethodChain(
        foo,
        Seq(
          MethodChain.Bang(span(3, 4)),
          MethodChain.Access(
            Seq(Lit(Literal.Int(0), span(5, 6))),
            optional = None,
            span(4, 7)
          ),
          MethodChain.Bang(span(7, 8))
        ),
        span(0, 8)
      )
    )
  }

  it should "chain multiple bang operators" in {
    import Expr.{ Id }

    val bangFoo = MethodChain(
      Id("foo", span(0, 3)),
      Seq(
        MethodChain.Bang(span(3, 4))
      ),
      span(0, 4)
    )
    Parser.expr("foo!") shouldEqual Result.Ok(bangFoo)

    val bangBar = bangFoo.copy(
      chain = bangFoo.chain.concat(
        Seq(
          MethodChain.Select(
            span(4, 5),
            Name("bar", span(5, 8)),
            false
          ),
          MethodChain.Bang(span(8, 9))
        )
      ),
      span = span(0, 9)
    )
    Parser.expr("foo!.bar!") shouldEqual Result.Ok(bangBar)

    val bangBaz = bangBar.copy(
      chain = bangBar.chain.concat(
        Seq(
          MethodChain.Select(
            span(9, 10),
            Name("baz", span(10, 13)),
            false
          ),
          MethodChain.Bang(span(13, 14))
        )
      ),
      span = span(0, 14)
    )
    Parser.expr("foo!.bar!.baz!") shouldEqual Result.Ok(bangBaz)
  }

  it should "accept bang syntax after term" in {
    for {
      (term, ast)        <- term1s
      (bangstr, bangast) <- bang(term, ast)
    } {
      Parser.expr(bangstr) shouldEqual Result.Ok(bangast)
    }
  }

  it should "accept bang syntax after field select" in {
    for {
      (term, ast)        <- term1s
      (selstr, selast)   <- sel(term, ast)
      (bangstr, bangast) <- bang(selstr, selast)
    } {
      Parser.expr(bangstr) shouldEqual Result.Ok(bangast)
    }
  }

  it should "accept bang syntax after array access" in {
    for {
      (term, ast)        <- term1s
      (arrstr, arrast)   <- arr(term, ast)
      (bangstr, bangast) <- bang(arrstr, arrast)
    } {
      Parser.expr(bangstr) shouldEqual Result.Ok(bangast)
    }
  }

  it should "accept bang syntax after apply" in {
    for {
      (term, ast) <- term1s
      if !ast.isInstanceOf[Expr.MethodChain] || (!ast
        .asInstanceOf[Expr.MethodChain]
        .chain
        .lastOption
        .exists(_.isInstanceOf[MethodChain.Select]))
      (appstr, appast)   <- app(term, ast)
      (bangstr, bangast) <- bang(appstr, appast)
    } {
      Parser.expr(bangstr) shouldEqual Result.Ok(bangast)
    }

    for {
      (term, ast) <- term1s
      if ast.isInstanceOf[Expr.MethodChain] && ast
        .asInstanceOf[Expr.MethodChain]
        .chain
        .lastOption
        .exists(_.isInstanceOf[MethodChain.Select])
      (appstr, appast)   <- app(term, ast)
      (bangstr, bangast) <- bang(appstr, appast)
    } {
      val Expr.MethodChain(_, chain, _) = bangast
      val selAppBang = chain.takeRight(3)
      val select = selAppBang.head.asInstanceOf[MethodChain.Select]
      val apply = selAppBang(1).asInstanceOf[MethodChain.Apply]
      val bang = selAppBang.last.asInstanceOf[MethodChain.Bang]
      val methodCall = MethodChain.MethodCall(
        select.dotSpan,
        select.field,
        apply.args,
        selectOptional = select.optional,
        applyOptional = apply.optional,
        apply.applySpan
      )

      val bangast0 = bangast.copy(
        chain = bangast.chain.dropRight(3).appended(methodCall).appended(bang)
      )

      Parser.expr(bangstr) shouldEqual Result.Ok(bangast0)
    }
  }

  def unop(term: String, ast: Expr): Seq[(String, Expr)] = {
    val ops = Seq("-", "!", "~")
    for (op <- ops) yield {
      val Span(i1, i2, _) = ast.span
      s"$op$term" -> OperatorCall(
        ast,
        Name(op, span(i1 - 1, i1)),
        None,
        span(i1 - 1, i2))
    }
  }

  def terms(implicit i: Int = 0) = term1s ++
    term0s(i + 1)
      .flatMap((unop _).tupled)
      .filterNot(
        Set(
          "-1",
          "-1e1",
          "-1e-1",
          "-1e+1",
          "+1",
          "+1e1",
          "+1e-1",
          "+1e+1") contains _._1)

  it should "accept terms" in {
    for ((term, ast) <- terms) {
      Parser.expr(term) shouldEqual Result.Ok(ast)
    }
  }

  it should "accept unary ops" in {
    for {
      (term, ast)    <- term1s(1)
      (opstr, opast) <- unop(term, ast)
      if !(Seq("-1", "+1").exists(opstr startsWith _))
    } {
      Parser.expr(opstr) shouldEqual Result.Ok(opast)
    }
  }

  it should "accept binary ops" in {
    for {
      (term1, ast1) <- terms
      (term2, ast2) <- term0s(ast1.span.end + 1)
    } {
      val Result.Ok(_) = Parser.expr(s"$term1 isa $term2")

      val Result.Ok(_) = Parser.expr(s"$term1??$term2")

      val Result.Ok(_) = Parser.expr(s"$term1||$term2")
      val Result.Ok(_) = Parser.expr(s"$term1&&$term2")

      val Result.Ok(_) = Parser.expr(s"$term1==$term2")
      val Result.Ok(_) = Parser.expr(s"$term1!=$term2")
      val Result.Ok(_) = Parser.expr(s"$term1=~$term2")
      val Result.Ok(_) = Parser.expr(s"$term1!~$term2")
      val Result.Ok(_) = Parser.expr(s"$term1>$term2")
      val Result.Ok(_) = Parser.expr(s"$term1<$term2")
      val Result.Ok(_) = Parser.expr(s"$term1>=$term2")
      val Result.Ok(_) = Parser.expr(s"$term1<=$term2")

      val Result.Ok(_) = Parser.expr(s"$term1|$term2")
      val Result.Ok(_) = Parser.expr(s"$term1^$term2")
      val Result.Ok(_) = Parser.expr(s"$term1&$term2")

      val Result.Ok(_) = Parser.expr(s"$term1+$term2")
      val Result.Ok(_) = Parser.expr(s"$term1-$term2")
      val Result.Ok(_) = Parser.expr(s"$term1*$term2")
      val Result.Ok(_) = Parser.expr(s"$term1/$term2")
      val Result.Ok(_) = Parser.expr(s"$term1%$term2")
    }
  }

  it should "correctly handle chains in short lambdas" in {
    Parser.expr("(.foo.bar)") shouldEqual Result.Ok(
      Expr.Tuple(
        Seq(
          Expr.ShortLambda(
            MethodChain(
              Expr.This(span(1, 1)),
              Seq(
                MethodChain.Select(
                  span(1, 2),
                  Name("foo", span(2, 5)),
                  false
                ),
                MethodChain.Select(
                  span(5, 6),
                  Name("bar", span(6, 9)),
                  false
                )
              ),
              span(1, 9)
            )
          )),
        span(0, 10)
      ))
  }
  it should "handle bare method calls in short lambdas" in {
    Parser.expr("(.foo())") shouldEqual Result.Ok(
      Expr.Tuple(
        Seq(
          Expr.ShortLambda(
            MethodChain(
              Expr.This(span(1, 1)),
              Seq(
                MethodChain.MethodCall(
                  span(1, 2),
                  Name("foo", span(2, 5)),
                  List.empty,
                  selectOptional = false,
                  applyOptional = None,
                  span(5, 7)
                )
              ),
              span(1, 7)
            )
          )
        ),
        span(0, 8)
      ))
  }
  it should "accept bare selects" in {
    import Expr.{ Id }
    Parser.expr("(.foo)") shouldEqual Result.Ok(
      Expr.Tuple(
        Seq(
          Expr.ShortLambda(
            Expr.MethodChain(
              Id(Expr.This.name, span(1, 1)),
              Seq(
                MethodChain.Select(
                  span(1, 2),
                  Name("foo", span(2, 5)),
                  false
                )
              ),
              span(1, 5)
            )
          )
        ),
        span(0, 6)
      )
    )

    Parser.expr("(.foo + (.bar))") shouldEqual Result.Ok(
      Expr.Tuple(
        List(
          Expr.ShortLambda(
            Expr.OperatorCall(
              Expr.MethodChain(
                Expr.This(span(1, 1)),
                Seq(
                  MethodChain.Select(
                    span(1, 2),
                    Name("foo", span(2, 5)),
                    false
                  )
                ),
                span(1, 5)
              ),
              Name("+", span(6, 7)),
              Some(
                Expr.Tuple(
                  Seq(
                    Expr.MethodChain(
                      Expr.This(span(9, 9)),
                      Seq(
                        MethodChain.Select(
                          span(9, 10),
                          Name("bar", span(10, 13)),
                          false
                        )
                      ),
                      span(9, 13)
                    )
                  ),
                  span(8, 14)
                )
              ),
              span(8, 14)
            )
          )
        ),
        span(0, 15)
      )
    )

    Parser.expr("((.foo + .bar) / (.baz))") shouldEqual Result.Ok(
      Expr.Tuple(
        Seq(
          Expr.ShortLambda(
            OperatorCall(
              Expr.Tuple(
                Seq(
                  Expr.OperatorCall(
                    Expr.MethodChain(
                      Expr.This(span(2, 2)),
                      Seq(
                        MethodChain.Select(
                          span(2, 3),
                          Name("foo", span(3, 6)),
                          false
                        )
                      ),
                      span(2, 6)
                    ),
                    Name("+", span(7, 8)),
                    Some(
                      Expr.MethodChain(
                        Expr.This(span(9, 9)),
                        Seq(
                          MethodChain.Select(
                            span(9, 10),
                            Name("bar", span(10, 13)),
                            false
                          )
                        ),
                        span(9, 13)
                      )
                    ),
                    span(9, 13)
                  )
                ),
                span(1, 14)
              ),
              Name("/", span(15, 16)),
              Some(
                Expr.Tuple(
                  List(
                    Expr.MethodChain(
                      Expr.This(span(18, 18)),
                      Seq(
                        MethodChain.Select(
                          span(18, 19),
                          Name("baz", span(19, 22)),
                          false
                        )
                      ),
                      span(18, 22)
                    )
                  ),
                  span(17, 23)
                )
              ),
              span(17, 23)
            )
          )
        ),
        span(0, 24)
      )
    )

    Parser.expr("(.foo)(x)") shouldEqual Result.Ok(
      MethodChain(
        Expr.Tuple(
          List(
            Expr.ShortLambda(
              MethodChain(
                Expr.This(span(1, 1)),
                Seq(
                  MethodChain.Select(
                    span(1, 2),
                    Name("foo", span(2, 5)),
                    false
                  )
                ),
                span(1, 5)
              )
            )
          ),
          span(0, 6)
        ),
        Seq(
          MethodChain.Apply(
            List(Id("x", span(7, 8))),
            None,
            span(6, 9)
          )
        ),
        span(0, 9)
      )
    )
  }

  it should "accept bare access" in {
    import Expr.{ Id, Lit }
    Parser.expr("(.['foo'])") shouldEqual Result.Ok(
      Expr.Tuple(
        Seq(
          Expr.ShortLambda(
            Expr.MethodChain(
              Id(Expr.This.name, span(1, 1)),
              Seq(
                MethodChain.Access(
                  Seq(Lit(Literal.Str("foo"), span(3, 8))),
                  None,
                  span(2, 9)
                )
              ),
              span(1, 9)
            )
          )
        ),
        span(0, 10)
      )
    )
  }
  it should "accept bare apply" in {
    import Expr.{ Id }
    Parser.expr("(.())") shouldEqual Result.Ok(
      Expr.Tuple(
        Seq(
          Expr.ShortLambda(
            Expr.MethodChain(
              Id(Expr.This.name, span(1, 1)),
              Seq(
                MethodChain.Apply(
                  Seq(),
                  None,
                  span(2, 4)
                )
              ),
              span(1, 4)
            )
          )
        ),
        span(0, 5)
      )
    )
  }

  it should "accept short lambdas in parens after a &&" in {
    Parser.expr("(.foo && .bar)") shouldEqual Result.Ok(
      Expr.Tuple(
        List(
          Expr.ShortLambda(
            OperatorCall(
              MethodChain(
                Expr.This(span(1, 1)),
                Seq(
                  MethodChain.Select(
                    span(1, 2),
                    Name("foo", span(2, 5)),
                    false
                  )
                ),
                span(1, 5)
              ),
              Name("&&", span(6, 8)),
              Some(
                MethodChain(
                  Expr.Id(Expr.This.name, span(9, 9)),
                  Seq(
                    MethodChain.Select(
                      span(9, 10),
                      Name("bar", span(10, 13)),
                      false
                    )
                  ),
                  span(9, 13)
                )
              ),
              span(9, 13)
            )
          )
        ),
        span(0, 14)
      )
    )
  }

  it should "include unary ops in short lambdas" in {
    Parser.expr("(!.foo)") shouldEqual Result.Ok(
      Expr.Tuple(
        List(
          Expr.ShortLambda(
            OperatorCall(
              MethodChain(
                Expr.This(span(2, 2)),
                Seq(
                  MethodChain.Select(
                    span(2, 3),
                    Name("foo", span(3, 6)),
                    false
                  )
                ),
                span(2, 6)
              ),
              Name("!", span(1, 2)),
              None,
              span(1, 6)
            )
          )
        ),
        span(0, 7)
      )
    )
  }

  it should "accept newlines before boolean and bitwise ops" in {
    for {
      (term1, ast1) <- terms
      (term2, ast2) <- term0s(ast1.span.end + 1)
    } {
      val Result.Ok(_) = Parser.expr(s"$term1\n||$term2")
      val Result.Ok(_) = Parser.expr(s"$term1\n&&$term2")

      val Result.Ok(_) = Parser.expr(s"$term1\n|$term2")
      val Result.Ok(_) = Parser.expr(s"$term1\n^$term2")
      val Result.Ok(_) = Parser.expr(s"$term1\n&$term2")
    }
  }

  it should "reject newlines before comparison and arthmetic ops" in {
    for {
      (term1, ast1) <- terms
      (term2, ast2) <- term0s(ast1.span.end + 1)
    } {
      val Result.Err(_) = Parser.expr(s"$term1\n==$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n!=$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n=~$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n!~$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n>$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n<$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n>=$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n<=$term2")

      val Result.Err(_) = Parser.expr(s"$term1\n+$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n-$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n*$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n/$term2")
      val Result.Err(_) = Parser.expr(s"$term1\n%$term2")
    }
  }

  it should "respect op associativity" in {
    val ops = ExprParser.ExprOpPrecedences.keys
    for {
      op <- ops
      (term1, e1) = terms.head
      (term2, e2) = term0s(e1.span.end + op.size + 2).head
      (term3, e3) = term0s(e2.span.end + op.size + 2).head
      // Sadly, spaces required for `isa`.
      expr = s"$term1 $op $term2 $op $term3"
    } {
      // right associative
      if (ExprParser.ExprRightAcc(op)) {
        Parser.expr(expr) shouldEqual Result.Ok(
          OperatorCall(
            e1,
            Name(op, span(e1.span.end + 1, e1.span.end + op.size + 1)),
            Some(
              OperatorCall(
                e2,
                Name(op, span(e2.span.end + 1, e2.span.end + op.size + 1)),
                Some(e3),
                e3.span)),
            span(e2.span.start, e3.span.end)
          ))
      } else {
        Parser.expr(expr) shouldEqual Result.Ok(
          OperatorCall(
            OperatorCall(
              e1,
              Name(op, span(e1.span.end + 1, e1.span.end + op.size + 1)),
              Some(e2),
              e2.span),
            Name(op, span(e2.span.end + 1, e2.span.end + op.size + 1)),
            Some(e3),
            e3.span
          ))
      }
    }
  }

  it should "respect op precedence" in {
    for {
      (op1, p1) <- ExprParser.ExprOpPrecedences
      (op2, p2) <- ExprParser.ExprOpPrecedences
      if (op1 != op2)
      (term1, e1) = terms.head
      (term2, e2) = term0s(e1.span.end + op1.size + 2).head
      (term3, e3) = term0s(e2.span.end + op2.size + 2).head
      // Sadly, spaces required for `isa`.
      expr = s"$term1 $op1 $term2 $op2 $term3"
    } {
      if (p1 >= p2) {
        // high to low precedence
        Parser.expr(expr) shouldEqual Result.Ok(
          OperatorCall(
            OperatorCall(
              e1,
              Name(op1, span(e1.span.end + 1, e1.span.end + op1.size + 1)),
              Some(e2),
              e2.span),
            Name(op2, span(e2.span.end + 1, e2.span.end + op2.size + 1)),
            Some(e3),
            e3.span
          ))
      } else {
        Parser.expr(expr) shouldEqual Result.Ok(
          OperatorCall(
            e1,
            Name(op1, span(e1.span.end + 1, e1.span.end + op1.size + 1)),
            Some(
              OperatorCall(
                e2,
                Name(op2, span(e2.span.end + 1, e2.span.end + op2.size + 1)),
                Some(e3),
                e3.span)),
            span(e2.span.start, e3.span.end)
          ))
      }
    }
  }

  it should "not stack overflow" in {
    val q = (0 to 10000).mkString("+")
    Parser.expr(q) should matchPattern { case Result.Ok(_) =>
    }
  }

  it should "disallow reserved words as identifiers" in {
    parseErr("{ let join = 3; join }") shouldBe (
      """|error: Invalid identifier
         |at *query*:1:7
         |  |
         |1 | { let join = 3; join }
         |  |       ^^^^ `join` is a keyword
         |  |""".stripMargin
    )

    parseErr("{ let of = 3; of }") shouldBe (
      """|error: Invalid identifier
         |at *query*:1:7
         |  |
         |1 | { let of = 3; of }
         |  |       ^^ `of` is a keyword
         |  |""".stripMargin
    )

    parseErr("{ let on = 3; on }") shouldBe (
      """|error: Invalid identifier
         |at *query*:1:7
         |  |
         |1 | { let on = 3; on }
         |  |       ^^ `on` is a keyword
         |  |""".stripMargin
    )
  }
}
