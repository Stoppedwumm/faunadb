package fql.test.schema

import fql.ast._
import fql.color.ColorKind
import fql.schema.DiffRender
import fql.TextUtil
import org.scalactic.source.Position

class SchemaDiffTextSpec extends SchemaDiffHelperSpec {
  "SchemaDiffText" should "hunk diffs correctly" in {
    diff(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |  j: String
         |  k: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  foo: String
         |  f: String
         |  bar: String
         |  h: String
         |  i: String
         |  j: String
         |  k: String
         |}
         |""".stripMargin
    )(
      """|main.fsl
         |@ line 3 to 11
         |    b: String
         |    c: String
         |    d: String
         |-   e: String
         |+   foo: String
         |    f: String
         |-   g: String
         |+   bar: String
         |    h: String
         |    i: String
         |    j: String
         |""".stripMargin
    )
  }

  it should "work for hunks near the start" in {
    diff(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  e: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: String
         |  foo: String
         |  c: String
         |  d: String
         |  e: String
         |}
         |""".stripMargin
    )(
      """|main.fsl
         |@ line 1 to 6
         |  collection Foo {
         |    a: String
         |-   b: String
         |+   foo: String
         |    c: String
         |    d: String
         |    e: String
         |""".stripMargin
    )
  }

  "SchemaDiffText" should "work for hunks near the end" in {
    diff(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  e: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  foo: String
         |  e: String
         |}
         |""".stripMargin
    )(
      """|main.fsl
         |@ line 2 to 7
         |    a: String
         |    b: String
         |    c: String
         |-   d: String
         |+   foo: String
         |    e: String
         |  }
         |""".stripMargin
    )
  }

  it should "work for multiple hunks near the end" in {
    diff(
      """|collection Foo {
         |  a: String
         |  foo: String
         |  c: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |  bar: String
         |  k: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |  j: String
         |  k: String
         |}
         |""".stripMargin
    )(
      """|main.fsl
         |@ line 1 to 6
         |  collection Foo {
         |    a: String
         |-   foo: String
         |+   b: String
         |    c: String
         |    d: String
         |    e: String
         |@ line 8 to 13
         |    g: String
         |    h: String
         |    i: String
         |-   bar: String
         |+   j: String
         |    k: String
         |  }
         |""".stripMargin
    )
  }

  it should "work when appending lines" in {
    diff(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |  j: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: String
         |  b: String
         |  b2: String
         |  c: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |  foo: String
         |}
         |""".stripMargin
    )(
      // NB: These line numbers are in terms of the `after` file.
      """|main.fsl
         |@ line 1 to 7
         |  collection Foo {
         |    a: String
         |    b: String
         |+   b2: String
         |    c: String
         |    d: String
         |    e: String
         |@ line 9 to 13
         |    g: String
         |    h: String
         |    i: String
         |-   j: String
         |+   foo: String
         |  }
         |""".stripMargin
    )
  }

  it should "work when removing lines" in {
    diff(
      """|collection Foo {
         |  a: String
         |  b: String
         |  c: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  a: String
         |  b: String
         |  d: String
         |  e: String
         |  f: String
         |  g: String
         |  h: String
         |  i: String
         |}
         |""".stripMargin
    )(
      // NB: These line numbers are in terms of the `after` file.
      """|main.fsl
         |@ line 1 to 6
         |  collection Foo {
         |    a: String
         |    b: String
         |-   c: String
         |    d: String
         |    e: String
         |    f: String
         |""".stripMargin
    )
  }

  def diff(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl")
  )(
    expectedDiff: String
  )(implicit pos: Position) = {
    val diffText =
      DiffRender.renderTextual(
        Map(srcBefore -> before),
        Map(srcAfter -> after),
        ColorKind.None
      )

    if (diffText != expectedDiff) {
      withClue(TextUtil.diff(diffText, expectedDiff)) {
        fail("generated and expected diffs do not match")
      }
    }
  }
}
