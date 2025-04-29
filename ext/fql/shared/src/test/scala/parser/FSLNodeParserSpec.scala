package fql.test.parser

import fql.{ Result, TextUtil }
import fql.ast._
import fql.parser._
import fql.test.Spec
import org.scalactic.source.Position

class FSLNodeParserSpec extends Spec {
  def span(s: Int, e: Int) = Span(s, e, Src.FSL(""))

  def raw(src: String)(implicit pos: Position) =
    Parser.fslNodes(src) match {
      case Result.Ok(v) => v
      case Result.Err(errs) =>
        errs foreach { err =>
          println(err.renderWithSource(Map(Src.Id("*fsl*") -> src)))
        }
        fail("failed to parse schema item")
    }

  def rawErr(src: String, expected: String)(implicit pos: Position) =
    Parser.fslNodes(src) match {
      case Result.Ok(v) =>
        fail(s"should not have passed. got value: $v")
      case Result.Err(errs) =>
        val actual = errs
          .map(_.renderWithSource(Map(Src.Id("*fsl*") -> src)))
          .mkString("\n\n")
        if (actual != expected) {
          val sb = new StringBuilder
          sb.append("Unexpected error (- is test, + is output):\n")
          TextUtil.printDiff(actual, expected, sb)
          fail(sb.result())
        }
    }

  def schemaErr(src: String, expected: String)(implicit pos: Position) =
    Parser.schemaItems(src) match {
      case Result.Ok(v) =>
        fail(s"should not have passed. got value: $v")
      case Result.Err(errs) =>
        val actual = errs
          .map(_.renderWithSource(Map(Src.Id("*fsl*") -> src)))
          .mkString("\n\n")
        if (actual != expected) {
          val sb = new StringBuilder
          sb.append("Unexpected error (- is test, + is output):\n")
          TextUtil.printDiff(actual, expected, sb)
          fail(sb.result())
        }
    }

  it should "error collections nicely" in {
    schemaErr(
      "foo",
      """|error: Invalid schema item `foo`
         |at *fsl*:1:1
         |  |
         |1 | foo
         |  | ^^^
         |  |""".stripMargin
    )

    schemaErr(
      "foo bar",
      """|error: Invalid schema item `foo`
         |at *fsl*:1:1
         |  |
         |1 | foo bar
         |  | ^^^
         |  |""".stripMargin
    )

    schemaErr(
      "collection 3",
      """|error: Missing required name
         |at *fsl*:1:1
         |  |
         |1 | collection 3
         |  | ^^^^^^^^^^
         |  |""".stripMargin
    )

    schemaErr(
      "collection Foo 123",
      """|error: Expected a block
         |at *fsl*:1:16
         |  |
         |1 | collection Foo 123
         |  |                ^^^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  foo bar
         |}""".stripMargin,
      """|error: Invalid field or member `foo`
         |at *fsl*:2:3
         |  |
         |2 |   foo bar
         |  |   ^^^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  index
         |}""".stripMargin,
      """|error: Missing required name
         |at *fsl*:2:3
         |  |
         |2 |   index
         |  |   ^^^^^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  index bar
         |}""".stripMargin,
      """|error: Missing required value
         |at *fsl*:2:3
         |  |
         |2 |   index bar
         |  |   ^^^^^^^^^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  index bar 3
         |}""".stripMargin,
      """|error: Expected a block
         |at *fsl*:2:13
         |  |
         |2 |   index bar 3
         |  |             ^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  index bar {
         |    foo
         |  }
         |}""".stripMargin,
      """|error: Invalid member `foo`
         |at *fsl*:3:5
         |  |
         |3 |     foo
         |  |     ^^^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  index bar {
         |    terms 3
         |  }
         |}""".stripMargin,
      """|error: Expected an array of terms
         |at *fsl*:3:11
         |  |
         |3 |     terms 3
         |  |           ^
         |  |""".stripMargin
    )

    schemaErr(
      """|collection Foo {
         |  index bar {
         |    terms [.a]
         |    terms [.b]
         |  }
         |}""".stripMargin,
      """|error: Duplicate terms definition
         |at *fsl*:4:5
         |  |
         |4 |     terms [.b]
         |  |     ^^^^^^^^^^
         |  |
         |hint: Originally defined here
         |at *fsl*:3:5
         |  |
         |3 |     terms [.a]
         |  |     ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should ("disallow duplicates in split migrations") in {
    schemaErr(
      """|collection Foo {
         |  migrations {
         |    split .x -> .y, .z, .y
         |  }
         |}""".stripMargin,
      """|error: Expected list of fields with no duplicates
         |at *fsl*:3:17
         |  |
         |3 |     split .x -> .y, .z, .y
         |  |                 ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "A raw parser" should "parse raw items" in {
    raw("collection Foo {}") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("collection", span(0, 10)),
        Some(Name("Foo", span(11, 14))),
        Some(FSL.Block(span(15, 16), List(), span(16, 17))),
        span(0, 17)))

    raw("foo bar {}") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("foo", span(0, 3)),
        None,
        Some(FSL.Invalid(span(4, 10))),
        span(0, 10)))
  }

  it should "parse annotations" in {
    raw("@alias(foo) @role(bar) baz {}") shouldBe Seq(
      FSL.Node(
        Seq(
          FSL.Annotation(
            Name("alias", span(1, 6)),
            Name("foo", span(7, 10)),
            span(0, 11)),
          FSL.Annotation(
            Name("role", span(13, 17)),
            Name("bar", span(18, 21)),
            span(12, 22))
        ),
        Name("baz", span(23, 26)),
        None,
        Some(FSL.Invalid(span(27, 29))),
        span(0, 29)
      ))
  }

  it should "parse paths" in {
    raw("terms [.x, .y, .[3].foo]") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("terms", span(0, 5)),
        None,
        Some(
          FSL
            .Paths(
              span(6, 7),
              Seq(
                Expr.ShortLambda(
                  Expr.MethodChain(
                    Expr.This(span(7, 7)),
                    Seq(Expr.MethodChain
                      .Select(span(7, 8), Name("x", span(8, 9)), false)),
                    span(7, 9))),
                Expr.ShortLambda(
                  Expr.MethodChain(
                    Expr.This(span(11, 11)),
                    Seq(Expr.MethodChain
                      .Select(span(11, 12), Name("y", span(12, 13)), false)),
                    span(11, 13))),
                Expr.ShortLambda(Expr.MethodChain(
                  Expr.This(span(15, 15)),
                  Seq(
                    Expr.MethodChain
                      .Access(
                        Seq(Expr.Lit(Literal.Int(3), span(17, 18))),
                        None,
                        span(16, 19)),
                    Expr.MethodChain
                      .Select(span(19, 20), Name("foo", span(20, 23)), false)
                  ),
                  span(15, 23)
                ))
              ),
              span(23, 24)
            )),
        span(0, 24)
      ))
  }

  it should "parse functions" in {
    raw("function foo(x, y: Int) { 0 }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("foo", span(9, 12))),
        Some(FSL.Function(
          span(12, 13),
          Seq(
            FSL.Arg(Name("x", span(13, 14)), None),
            FSL.Arg(
              Name("y", span(16, 17)),
              Some(FSL.ColonType(span(17, 18), TypeExpr.Id("Int", span(19, 22)))))
          ),
          None,
          span(22, 23),
          None,
          Expr.Block(
            Seq(Expr.Stmt.Expr(Expr.Lit(Literal.Int(0), span(26, 27)))),
            span(24, 29))
        )),
        span(0, 29)
      ))
  }

  it should "parse variadic functions" in {
    raw("function foo(x, ...y) { 0 }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("foo", span(9, 12))),
        Some(FSL.Function(
          span(12, 13),
          List(FSL.Arg(Name("x", span(13, 14)), None)),
          Some(FSL.VarArg(span(16, 19), Name("y", span(19, 20)), None)),
          span(20, 21),
          None,
          Expr.Block(
            List(Expr.Stmt.Expr(Expr.Lit(Literal.Int(0), span(24, 25)))),
            span(22, 27))
        )),
        span(0, 27),
        None
      ))
  }

  it should "allow whitespace before arg list" in {
    raw("function foo () { 0 }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("foo", span(9, 12))),
        Some(
          FSL.Function(
            span(13, 14),
            Seq.empty,
            None,
            span(14, 15),
            None,
            Expr.Block(
              Seq(Expr.Stmt.Expr(Expr.Lit(Literal.Int(0), span(18, 19)))),
              span(16, 21))
          )),
        span(0, 21)
      ))
  }

  it should "parse lits" in {
    raw("history_days bar 3") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("history_days", span(0, 12)),
        Some(Name("bar", span(13, 16))),
        Some(FSL.Lit(Expr.Lit(Literal.Int(3), span(17, 18)))),
        span(0, 18)
      ))

    raw("ttl_days bar 2 + 3") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("ttl_days", span(0, 8)),
        Some(Name("bar", span(9, 12))),
        Some(FSL.Lit(Expr.OperatorCall(
          Expr.Lit(Literal.Int(2), span(13, 14)),
          Name("+", span(15, 16)),
          Some(Expr.Lit(Literal.Int(3), span(17, 18))),
          // TODO: this span should probably cover the whole operator call
          span(17, 18)
        ))),
        span(0, 18)
      ))
  }

  it should "parse access providers" in {
    raw("access provider foo {}") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("access provider", span(0, 15)),
        Some(Name("foo", span(16, 19))),
        Some(FSL.Block(span(20, 21), Seq.empty, span(21, 22))),
        span(0, 22)))

    raw("access provider foo { role foo }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("access provider", span(0, 15)),
        Some(Name("foo", span(16, 19))),
        Some(
          FSL.Block(
            span(20, 21),
            Seq(
              FSL.Node(
                Seq.empty,
                Name("role", span(22, 26)),
                Some(Name("foo", span(27, 30))),
                None,
                span(22, 30)
              )),
            span(31, 32))),
        span(0, 32)
      ))
  }

  it should "parse predicate and allow newline" in {
    raw("predicate\n(x => x)") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("predicate", span(0, 9)),
        None,
        Some(
          FSL.Lit(
            Expr.Tuple(
              Seq(
                Expr.LongLambda(
                  List(Some(Name("x", span(11, 12)))),
                  None,
                  Expr.Id("x", span(16, 17)),
                  span(11, 17)
                )),
              span(10, 18)))),
        span(0, 18)
      )
    )
  }

  it should "parse computed fields" in {
    raw("compute foo = x => x + 3") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("field", span(0, 8)),
        Some(Name("foo", span(8, 11))),
        Some(
          FSL.Field(
            FSL.FieldKind.Compute,
            None,
            Some(
              FSL.Lit(
                Expr.LongLambda(
                  List(Some(Name("x", span(14, 15)))),
                  None,
                  Expr.OperatorCall(
                    Expr.Id("x", span(19, 20)),
                    Name("+", span(21, 22)),
                    Some(Expr.Lit(Literal.Int(3), span(23, 24))),
                    span(23, 24)),
                  span(14, 24)
                )
              ))
          )),
        span(0, 24)
      )
    )
  }

  it should "parse migration blocks" in {
    raw(
      """|collection Foo {
         |  migrations {
         |    backfill .foo = 3
         |
         |    split .a -> .B, .C, .X
         |    drop .X
         |
         |    move .foo -> .bar
         |
         |    add .T
         |  }
         |}""".stripMargin
    ) shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("collection", span(0, 10)),
        Some(Name("Foo", span(11, 14))),
        Some(
          FSL
            .Block(
              span(15, 16),
              List(
                FSL
                  .Node(
                    Seq.empty,
                    Name("migrations", span(19, 29)),
                    None,
                    Some(
                      FSL
                        .Block(
                          span(30, 31),
                          Seq(
                            FSL.Node(
                              Seq.empty,
                              Name("backfill", span(36, 44)),
                              None,
                              Some(
                                FSL.Migration.Backfill(
                                  Path(
                                    List(PathElem.Field("foo", span(46, 49))),
                                    span(45, 49)),
                                  FSL.Lit(Expr.Lit(Literal.Int(3), span(52, 53))),
                                  span(36, 53)
                                )),
                              span(36, 53)
                            ),
                            FSL
                              .Node(
                                Seq.empty,
                                Name("split", span(59, 64)),
                                None,
                                Some(FSL.Migration
                                  .Split(
                                    Path(
                                      List(PathElem.Field("a", span(66, 67))),
                                      span(65, 67)),
                                    Seq(
                                      Path(
                                        List(PathElem.Field("B", span(72, 73))),
                                        span(71, 73)),
                                      Path(
                                        List(PathElem.Field("C", span(76, 77))),
                                        span(75, 77)),
                                      Path(
                                        List(PathElem.Field("X", span(80, 81))),
                                        span(79, 81))
                                    ),
                                    span(59, 81)
                                  )),
                                span(59, 81)
                              ),
                            FSL.Node(
                              Seq.empty,
                              Name("drop", span(86, 90)),
                              None,
                              Some(FSL.Migration.Drop(
                                Path(
                                  List(PathElem.Field("X", span(92, 93))),
                                  span(91, 93)),
                                span(86, 93))),
                              span(86, 93)
                            ),
                            FSL.Node(
                              Seq.empty,
                              Name("move", span(99, 103)),
                              None,
                              Some(
                                FSL.Migration
                                  .Move(
                                    Path(
                                      List(PathElem.Field("foo", span(105, 108))),
                                      span(104, 108)),
                                    Path(
                                      List(PathElem.Field("bar", span(113, 116))),
                                      span(112, 116)),
                                    span(99, 116))),
                              span(99, 116),
                              None
                            ),
                            FSL.Node(
                              Seq.empty,
                              Name("add", span(122, 125)),
                              None,
                              Some(FSL.Migration.Add(
                                Path(
                                  List(PathElem.Field("T", span(127, 128))),
                                  span(126, 128)),
                                span(122, 128))),
                              span(122, 128)
                            )
                          ),
                          span(131, 132)
                        )),
                    span(19, 132)
                  )),
              span(133, 134)
            )),
        span(0, 134)
      ))
  }

  it should "parse semicolons like crazy" in {
    raw(
      """|;
         |collection Foo {
         |  ;
         |  ;
         |  history_days 3 ;;
         |  ;
         |  index byFoo {} ;; /* comment */
         |} ;;
         |
         |collection Bar { ; };
         |""".stripMargin
    ) shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("collection", span(2, 12)),
        Some(Name("Foo", span(13, 16))),
        Some(
          FSL.Block(
            span(17, 18),
            List(
              FSL.Node(
                Seq.empty,
                Name("history_days", span(29, 41)),
                None,
                Some(FSL.Lit(Expr.Lit(Literal.Int(3), span(42, 43)))),
                // This span includes the trailing semicolons, but not the semicolon
                // on the next line.
                span(29, 46)
              ),
              FSL.Node(
                Seq.empty,
                Name("index", span(53, 58)),
                Some(Name("byFoo", span(59, 64))),
                Some(FSL.Block(span(65, 66), Seq.empty, span(66, 67))),
                // This span includes the trailing semicolons, but not the trailing
                // comment.
                span(53, 70)
              )
            ),
            span(85, 86)
          )),
        span(2, 89) // This span includes the trailing semicolons.
      ),
      FSL.Node(
        Seq.empty,
        Name("collection", span(91, 101)),
        Some(Name("Bar", span(102, 105))),
        Some(
          FSL.Block(
            span(106, 107),
            Seq.empty,
            span(110, 111)
          )),
        span(91, 112) // This span includes a trailing semicolon.
      )
    )
  }

  it should "rewinds semicolons correctly" in {
    raw(
      "collection Foo { index; foo }"
    ) shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("collection", span(0, 10)),
        Some(Name("Foo", span(11, 14))),
        Some(
          FSL.Block(
            span(15, 16),
            List(
              FSL.Node(
                Seq.empty,
                Name("index", span(17, 22)),
                None,
                None,
                span(17, 23)
              ),
              FSL.Node(
                Seq.empty,
                Name("foo", span(24, 27)),
                None,
                Some(FSL.Invalid(span(28, 28))),
                span(24, 28)
              )
            ),
            span(28, 29)
          )),
        span(0, 29)
      )
    )
  }

  it should "error nicely" in {
    // these all pass, because of Raw.Invalid. The second phase parser will handle
    // it.
    raw("foo")
    raw("foo bar")
    raw("foo bar baz")
    raw("foo bar baz baz")
    raw("foo bar baz {}")
    raw("foo bar baz 3")
  }

  it should "parse invalid chars" in {
    // this is testing brace matching. The brace stack should find the matching `{}`,
    // and then stop iterating when it finds the closing `}`.
    raw("collection foo { bar {} }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("collection", span(0, 10)),
        Some(Name("foo", span(11, 14))),
        Some(
          FSL.Block(
            span(15, 16),
            Seq(
              FSL.Node(
                Seq.empty,
                Name("bar", span(17, 20)),
                None,
                Some(FSL.Invalid(span(21, 24))),
                span(17, 24)
              )
            ),
            span(24, 25)
          )),
        span(0, 25)
      ))

    raw("collection foo { bar {}; }")
    raw("collection foo {\nbar {}\n}")
    raw("collection foo {\nbar {}\nfoo{} }")
    raw("collection foo { foo() }")
    raw("collection foo { foo) }")
    raw("collection foo { foo( }")
    raw("collection foo { foo((( }")
    raw("collection foo { foo(]] }")
    raw("collection foo { foo(])) }")
    raw("collection foo { foo(\n) }")

    raw("collection foo { foo(\n) '}' }")
    raw("collection foo { foo(\n) \"}\" }")
    raw("collection foo { foo(\n) \"foo}\" }")

    // and at this point we can't really expect the parser to figure these out
    rawErr(
      "collection foo { foo(]} }",
      """|error: Expected end-of-input
         |at *fsl*:1:25
         |  |
         |1 | collection foo { foo(]} }
         |  |                         ^
         |  |""".stripMargin
    )
  }

  it should "parse comments in invalid chars" in {
    raw("collection foo { foo(\n) /* } */ }")
    raw("collection foo { foo(\n) // }\n }")
    raw("collection foo { foo(\n) /* /* */ } */ }")
  }

  it should "parse invalid function bodies" in {
    raw("function bar() { 2... }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("bar", span(9, 12))),
        Some(FSL.Invalid(span(12, 23))),
        span(0, 23)
      ))

    raw("function bar() { if (3..) 4 }") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("bar", span(9, 12))),
        Some(FSL.Invalid(span(12, 29))),
        span(0, 29)
      ))
  }

  it should "error paths nicely" in {
    rawErr(
      "terms: [.bar]",
      """|error: Expected `,` or `]`
         |at *fsl*:1:9
         |  |
         |1 | terms: [.bar]
         |  |         ^
         |  |""".stripMargin
    )

    rawErr(
      "terms [.]",
      """|error: Expected `]` or expression
         |at *fsl*:1:8
         |  |
         |1 | terms [.]
         |  |        ^
         |  |""".stripMargin
    )
  }

  it should "error functions nicely" in {
    rawErr(
      "function ()",
      """|error: Expected name
         |at *fsl*:1:10
         |  |
         |1 | function ()
         |  |          ^
         |  |""".stripMargin
    )

    // while correct, this is a bit verbose, especially with all the commas.
    rawErr(
      "function foo(bar baz)",
      """|error: Expected `)`, `,`, or a type annotation
         |at *fsl*:1:18
         |  |
         |1 | function foo(bar baz)
         |  |                  ^
         |  |""".stripMargin
    )

    raw("function foo(): bar baz") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("foo", span(9, 12))),
        Some(FSL.Invalid(span(12, 23))),
        span(0, 23)
      ))

    raw("function foo() bar") shouldBe Seq(
      FSL.Node(
        Seq.empty,
        Name("function", span(0, 8)),
        Some(Name("foo", span(9, 12))),
        Some(FSL.Invalid(span(12, 18))),
        span(0, 18)
      ))
  }

  it should "error for access providers nicely" in {
    rawErr(
      "access foo",
      """|error: Expected provider
         |at *fsl*:1:8
         |  |
         |1 | access foo
         |  |        ^
         |  |""".stripMargin
    )

    rawErr(
      "access provider foo",
      """|error: Expected expression
         |at *fsl*:1:20
         |  |
         |1 | access provider foo
         |  |                    ^
         |  |""".stripMargin
    )
  }

  it should "parse doc comments associated with each top level node" in {
    val fsl =
      """|// Foo
         |collection Foo {}
         |
         |// Free comment
         |
         |collection Bar {}
         |
         |// Baz
         |// Fizz
         |// Buzz
         |function baz() { 42 }
         |""".stripMargin

    val parsed = raw(fsl)
    parsed(0).name.value.str shouldBe "Foo"
    parsed(0).docComment.value.extract(fsl) shouldBe "// Foo\n"
    parsed(1).name.value.str shouldBe "Bar"
    parsed(1).docComment shouldBe empty
    parsed(2).name.value.str shouldBe "baz"
    parsed(2).docComment.value.extract(fsl) shouldBe "// Baz\n// Fizz\n// Buzz\n"
  }

  it should "error for migrations" in {
    rawErr(
      "backfill .foo",
      """|error: Expected `=`
         |at *fsl*:1:14
         |  |
         |1 | backfill .foo
         |  |              ^
         |  |""".stripMargin
    )

    rawErr(
      "backfill .foo =",
      """|error: Expected expression
         |at *fsl*:1:16
         |  |
         |1 | backfill .foo =
         |  |                ^
         |  |""".stripMargin
    )

    rawErr(
      "drop",
      """|error: Expected expression
         |at *fsl*:1:5
         |  |
         |1 | drop
         |  |     ^
         |  |""".stripMargin
    )

    rawErr(
      "split",
      """|error: Expected expression
         |at *fsl*:1:6
         |  |
         |1 | split
         |  |      ^
         |  |""".stripMargin
    )

    rawErr(
      "split .foo",
      """|error: Expected `->`
         |at *fsl*:1:11
         |  |
         |1 | split .foo
         |  |           ^
         |  |""".stripMargin
    )

    rawErr(
      "move",
      """|error: Expected expression
         |at *fsl*:1:5
         |  |
         |1 | move
         |  |     ^
         |  |""".stripMargin
    )
    rawErr(
      "move .foo",
      """|error: Expected `->`
         |at *fsl*:1:10
         |  |
         |1 | move .foo
         |  |          ^
         |  |""".stripMargin
    )
    rawErr(
      "move .foo ->",
      """|error: Expected expression
         |at *fsl*:1:13
         |  |
         |1 | move .foo ->
         |  |             ^
         |  |""".stripMargin
    )
  }

  it should "parse newlines in signatures" in {
    raw(
      """|function foo(
         |  x: Int
         |) {
         |  x
         |}
         |""".stripMargin
    )
  }

  it should "parse commas in signatures correctly" in {
    rawErr(
      """|function foo(
         |  x: Int,
         |  ...y: Int,
         |  z: Int
         |) {
         |  x
         |}
         |""".stripMargin,
      """|error: Variadic argument must be the last argument.
         |at *fsl*:3:3
         |  |
         |3 |   ...y: Int,
         |  |   ^^^^^^^^^
         |  |""".stripMargin
    )

    rawErr(
      """|function foo(
         |  x: Int,
         |  z: Int
         |  ...y: Int
         |) {
         |  x
         |}
         |""".stripMargin,
      """|error: Expected `)`
         |at *fsl*:4:3
         |  |
         |4 |   ...y: Int
         |  |   ^
         |  |""".stripMargin
    )
  }

  it should "disallow multiple commas in type exprs" in {
    rawErr(
      """|collection Foo {
         |  nested: {
         |    foo: Int,,
         |    bar: String
         |  }
         |}
         |""".stripMargin,
      """|error: Expected `}`
         |at *fsl*:3:14
         |  |
         |3 |     foo: Int,,
         |  |              ^
         |  |""".stripMargin
    )
  }
}
