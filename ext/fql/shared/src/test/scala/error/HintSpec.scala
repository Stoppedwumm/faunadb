package fql.test

import fql.ast.{ Span, Src }
import fql.error.Hint

class HintSpec extends Spec {
  // test both inline and id-based sources
  def render(hint: Hint, expected: String, source: String = "hello world") = {
    val hint0 = hint.copy(span = hint.span.copy(src = Src.Query(source)))
    val rendered0 = hint0.renderWithSource(Map.empty, None)
    rendered0 shouldEqual expected

    val hint1 = hint.copy(span = hint.span.copy(src = Src.Id("*query*")))
    val rendered1 = hint1.renderWithSource(Map(Src.Id("*query*") -> source))
    rendered1 shouldEqual expected
  }

  it should "render no suggestion hints" in {
    render(
      Hint("this is a hint", Span(1, 3, Src.Query(""))),
      """|hint: this is a hint
         |at *query*:1:2
         |  |
         |1 | hello world
         |  |  ^^
         |  |""".stripMargin
    )
  }

  it should "render spans at EOF correctly" in {
    render(
      Hint("this is at the EOF", Span(11, 12, Src.Query(""))),
      """|hint: this is at the EOF
         |at *query*:1:12
         |  |
         |1 | hello world
         |  |            ^
         |  |""".stripMargin
    )
  }

  it should "render spans at end of multiline source correctly" in {
    render(
      Hint("this is near the EOF", Span(9, 11, Src.Query(""))),
      """|hint: this is near the EOF
         |at *query*:2:4
         |  |
         |2 | world
         |  |    ^^
         |  |""".stripMargin,
      "hello\nworld"
    )
  }

  it should "strip trailing whitespace" in {
    render(
      Hint("this is at the EOF", Span(11, 12, Src.Query(""))),
      """|hint: this is at the EOF
         |at *query*:1:4
         |  |
         |1 | foo
         |  |    ^
         |  |""".stripMargin,
      "foo\n   "
    )
  }

  it should "render something with empty query" in {
    render(
      Hint("this is a hint", Span(10, 11, Src.Query(""))),
      """|hint: this is a hint
         |at *query*:1:1
         |  |
         |1 |
         |  | ^
         |  |""".stripMargin,
      ""
    )
  }

  it should "render remove suggestions correctly" in {
    render(
      Hint("this is a hint", Span(1, 3, Src.Query("")), Some("")),
      """|hint: this is a hint
         |at *query*:1:2
         |  |
         |1 | hello world
         |  |  --
         |  |""".stripMargin
    )
  }

  it should "render add suggestions correctly" in {
    // Span must be empty, which means its an add
    render(
      Hint("this is a hint", Span(5, 5, Src.Query("")), Some(" 123")),
      """|hint: this is a hint
         |at *query*:1:6
         |  |
         |1 | hello 123 world
         |  |      ++++
         |  |""".stripMargin
    )
  }

  it should "render change suggestions correctly" in {
    // Span underlines `world`, so we replace that with `foo`
    render(
      Hint("this is a hint", Span(6, 11, Src.Query("")), Some("foo")),
      """|hint: this is a hint
         |at *query*:1:7
         |  |
         |1 | hello foo
         |  |       ~~~
         |  |""".stripMargin
    )
  }

  it should "render null spans" in {
    // render() will assign a valid span, so we test these manually
    val rendered0 = Hint("this is a hint", Span.Null).renderWithSource(
      Map(Src.Id("*query*") -> "foo"))
    rendered0 shouldEqual """|hint: this is a hint
                             |at <no source>""".stripMargin

    // Makes sure that suggestions won't break when there is a null span
    val rendered1 = Hint("this is a hint", Span.Null, Some("1234")).renderWithSource(
      Map(Src.Id("*query*") -> "foo"))
    rendered1 shouldEqual """|hint: this is a hint
                             |at <no source>""".stripMargin
  }
}
