package fql.test

import fql.ast._
import fql.parser.TemplateSigil

class SpanSpec extends Spec {
  "replaceWith(..., \"\")" should "handle inline w/ spaces" in {
    val src = "foo bar baz"
    val span = Span(4, 7, Src.Null)

    span.extract(src) shouldEqual "bar"
    span.replaceWith(src, "") shouldEqual "foo baz"
  }

  it should "handle inline no spaces" in {
    val src = "foobarbaz"
    val span = Span(3, 6, Src.Null)

    span.extract(src) shouldEqual "bar"
    span.replaceWith(src, "") shouldEqual "foobaz"
  }

  it should "handle preceding newlines" in {
    val src1 = """|foo
                  |bar baz""".stripMargin
    val span1 = Span(4, 7, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "") shouldEqual "foo\nbaz"

    val src2 = """|foo
                  |
                  |bar baz""".stripMargin
    val span2 = Span(5, 8, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "") shouldEqual "foo\n\nbaz"
  }

  it should "handle trailing newlines" in {
    val src1 = """|foo bar
                  |baz""".stripMargin
    val span1 = Span(4, 7, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "") shouldEqual "foo\nbaz"

    val src2 = """|foo bar
                  |
                  |baz""".stripMargin
    val span2 = Span(4, 7, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "") shouldEqual "foo\n\nbaz"
  }

  it should "handle indents" in {
    val src1 = """|foo
                  |  bar
                  |baz""".stripMargin
    val span1 = Span(6, 9, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "") shouldEqual "foo\nbaz"

    val src2 = """|foo
                  |bar
                  |  baz""".stripMargin
    val span2 = Span(4, 7, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "") shouldEqual "foo\n  baz"
  }

  "replaceWith(..., \"word\")" should "handle inline w/ spaces" in {
    val src = "foo bar baz"
    val span = Span(4, 7, Src.Null)

    span.extract(src) shouldEqual "bar"
    span.replaceWith(src, "word") shouldEqual "foo word baz"
  }

  it should "handle inline no spaces" in {
    val src = "foobarbaz"
    val span = Span(3, 6, Src.Null)

    span.extract(src) shouldEqual "bar"
    span.replaceWith(src, "word") shouldEqual "foowordbaz"
  }

  it should "handle preceding newlines" in {
    val src1 = """|foo
                  |bar baz""".stripMargin
    val span1 = Span(4, 7, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "word") shouldEqual "foo\nword baz"

    val src2 = """|foo
                  |
                  |bar baz""".stripMargin
    val span2 = Span(5, 8, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "word") shouldEqual "foo\n\nword baz"
  }

  it should "handle trailing newlines" in {
    val src1 = """|foo bar
                  |baz""".stripMargin
    val span1 = Span(4, 7, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "word") shouldEqual "foo word\nbaz"

    val src2 = """|foo bar
                  |
                  |baz""".stripMargin
    val span2 = Span(4, 7, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "word") shouldEqual "foo word\n\nbaz"
  }

  it should "handle indents" in {
    val src1 = """|foo
                  |  bar
                  |baz""".stripMargin
    val span1 = Span(6, 9, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "word") shouldEqual "foo\n  word\nbaz"

    val src2 = """|foo
                  |bar
                  |  baz""".stripMargin
    val span2 = Span(4, 7, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "word") shouldEqual "foo\nword\n  baz"
  }
  "replaceWith(..., \"line1\\nline2\")" should "handle inline w/ spaces" in {
    val src = "foo bar baz"
    val span = Span(4, 7, Src.Null)

    span.extract(src) shouldEqual "bar"
    span.replaceWith(src, "line1\nline2") shouldEqual "foo line1\nline2 baz"
  }

  it should "handle inline no spaces" in {
    val src = "foobarbaz"
    val span = Span(3, 6, Src.Null)

    span.extract(src) shouldEqual "bar"
    span.replaceWith(src, "line1\nline2") shouldEqual "fooline1\nline2baz"
  }

  it should "handle preceding newlines" in {
    val src1 = """|foo
                  |bar baz""".stripMargin
    val span1 = Span(4, 7, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "line1\nline2") shouldEqual "foo\nline1\nline2 baz"

    val src2 = """|foo
                  |
                  |bar baz""".stripMargin
    val span2 = Span(5, 8, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "line1\nline2") shouldEqual "foo\n\nline1\nline2 baz"
  }

  it should "handle trailing newlines" in {
    val src1 = """|foo bar
                  |baz""".stripMargin
    val span1 = Span(4, 7, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "line1\nline2") shouldEqual "foo line1\nline2\nbaz"

    val src2 = """|foo bar
                  |
                  |baz""".stripMargin
    val span2 = Span(4, 7, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "line1\nline2") shouldEqual "foo line1\nline2\n\nbaz"
  }

  it should "handle indents" in {
    val src1 = """|foo
                  |  bar
                  |baz""".stripMargin
    val span1 = Span(6, 9, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceWith(src1, "line1\nline2") shouldEqual "foo\n  line1\n  line2\nbaz"

    val src2 = """|foo
                  |bar
                  |  baz""".stripMargin
    val span2 = Span(4, 7, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceWith(src2, "line1\nline2") shouldEqual "foo\nline1\nline2\n  baz"
  }

  "replaceLineWith(..., \"\")" should "handle semicolons" in {
    val src1 = "foo; bar; baz"
    val span1 = Span(5, 8, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceLineWith(src1, "") shouldEqual "foo; baz"

    val src2 = "foo; bar  ; baz"
    val span2 = Span(5, 8, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceLineWith(src2, "") shouldEqual "foo; baz"
  }

  "replaceLineWith(..., \"line;\")" should "handle semicolons" in {
    val src1 = "foo; bar; baz"
    val span1 = Span(5, 8, Src.Null)

    span1.extract(src1) shouldEqual "bar"
    span1.replaceLineWith(src1, "line;") shouldEqual "foo; line; baz"

    val src2 = "foo; bar  ; baz"
    val span2 = Span(5, 8, Src.Null)

    span2.extract(src2) shouldEqual "bar"
    span2.replaceLineWith(src2, "line;") shouldEqual "foo; line; baz"
  }

  "annotate" should "replace null with left arrows" in {
    val src = "foo(" + TemplateSigil.Value + ")"

    Span(3, 12, Src.Null).annotate(src) shouldBe (
      """|  |
         |1 | foo(<value>)
         |  |    ^^^^^^^^^
         |  |""".stripMargin
    )
  }

  it should "replace null with left arrows for multiline spans" in {
    val src = "foo(\n  " + TemplateSigil.Value + "\n)"

    Span(3, 16, Src.Null).annotate(src) shouldBe (
      """|  |
         |1 |   foo(
         |  |  ____^
         |2 | |   <value>
         |3 | | )
         |  | |_^
         |  |""".stripMargin
    )
  }
}
