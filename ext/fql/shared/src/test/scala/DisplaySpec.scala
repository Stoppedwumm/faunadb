package fql.test

import fql.ast.display._
import fql.parser.Parser
import fql.Result
import org.scalactic.source.Position

class DisplaySpec extends Spec {
  def checkTExpr(src1: String)(implicit pos: Position): Unit = {
    val e1 = Parser.typeExpr(src1).toOption.get
    val src2 = e1.display
    Parser.typeExpr(src2) match {
      case Result.Err(err) => fail(s"display result `$src2` failed to parse $err")
      case Result.Ok(e2)   => e1 shouldEqual e2
    }
    ()
  }

  def checkExpr(src1: String)(implicit pos: Position): Unit = checkExpr(src1, src1)
  def checkExpr(src1: String, src2: String)(implicit pos: Position): Unit = {
    val e0 = Parser.expr(src1)
    if (e0.isErr) { println(e0) }
    val e1 = e0.toOption.getOrElse(fail("couldn't parse src1"))
    val src2actual = e1.display
    src2actual shouldEqual src2
    ()
  }

  "Display" should "round trip TypeExprs" in {
    checkTExpr("a => a")
    checkTExpr("(foo: a) => a")
    checkTExpr("(a, b) => c")
    checkTExpr("(a, ...b) => c")
    checkTExpr("(a, ..._) => c")
    checkTExpr("(foo: a, bar: b) => c")
    checkTExpr("(a, b, c)")
    checkTExpr("{ a: a, b: b, c: c }")

    // invalid idents
    checkTExpr("""{ "-foo-": a } => a""")
    checkTExpr("""{ "+": a } => a""")
  }

  it should "surround complex nullables with parentheses" in {
    checkTExpr("((foo: a??, bar: (a | b?)?) => (a? & b)?)?")
  }

  it should "round trip Exprs" in {
    checkExpr("""|{
                 |  let a = 1
                 |  a
                 |}""".stripMargin)
    checkExpr("1 + 2 + 3")
    checkExpr("if (foo) 1 else true")
    checkExpr("if (foo) 1")
    checkExpr("""|{
                 |  a: 0,
                 |  b: 1
                 |}""".stripMargin)
    // No trailing newline after object literal.
    checkExpr("""|foo({
                 |  a: 0,
                 |  b: 1
                 |})""".stripMargin)

    checkExpr("(a, b) => a + b")
    checkExpr("(i, ...b) => b.at(i)")
    checkExpr("(_, ..._) => 0")

    // No ""'s around at.
    checkExpr("[0, 1, 2].at")
    checkExpr("[0, 1, 2].at(0)")

    // Include ,'s in projection.
    checkExpr("""|Foo.all() {
                 |  a: .a,
                 |  b: .b
                 |}
                 |""".stripMargin)
  }

  it should "round trip strings" in {
    // Display characters.
    checkExpr(""" "hi" """.trim)
    checkExpr("'hi'", "\"hi\"")

    checkExpr(""" "\\" """.trim)
    checkExpr(""" "'" """.trim)
    checkExpr(""" "`" """.trim)

    // Escaped characters.
    checkExpr(""" "\0" """.trim)
    checkExpr(""" "\n" """.trim)
    checkExpr(""" "\r" """.trim)
    checkExpr(""" "\v" """.trim)
    checkExpr(""" "\t" """.trim)
    checkExpr(""" "\b" """.trim)
    checkExpr(""" "\f" """.trim)
    checkExpr(""" "\#" """.trim)

    // \0 and \b cannot be used in a string literal.
    checkExpr(s""" "${"\n"}" """.trim, """ "\n" """.trim)
    checkExpr(s""" "${"\r"}" """.trim, """ "\r" """.trim)
    checkExpr(s""" "${"\u000b"}" """.trim, """ "\v" """.trim)
    checkExpr(s""" "${"\t"}" """.trim, """ "\t" """.trim)
    checkExpr(s""" "${"\f"}" """.trim, """ "\f" """.trim)
    checkExpr(s""" "\\#" """.trim, """ "\#" """.trim)

    // The other guys.
    checkExpr(s""" "\\u200b" """.trim, s""" "\\u200b" """.trim)
    checkExpr(s""" "\\u{10ffff}" """.trim, s""" "\\u{10ffff}" """.trim)
  }

  it should "handle precedence correctly" in {
    checkExpr("2 * 3 + 5")
    checkExpr("2 * (3 + 5)")
    checkExpr("3 + 5 * 2")
    checkExpr("(3 + 5) * 2")
    checkExpr("(3 + 5)!")
    checkExpr("3 + 5!")
    checkExpr("(3 + 5).foo")
    checkExpr("3 + 5.foo")
  }

  it should "render unary ops correctly" in {
    checkExpr("!true")
    checkExpr("-(3)")
    checkExpr("~1234")
  }
}
