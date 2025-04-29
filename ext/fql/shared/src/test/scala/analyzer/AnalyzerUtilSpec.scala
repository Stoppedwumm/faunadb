package fql.test

import fql.analyzer.QueryContext

class AnalyzerUtilSpec extends AnalyzerSpec {
  val ctx = QueryContext(globals, shapes)

  def ident(srcWithCursor: String) = {
    val cursor = srcWithCursor.indexOf("|")
    val src = srcWithCursor.slice(0, cursor) + srcWithCursor.slice(
      cursor + 1,
      srcWithCursor.length)

    ctx.update(src)
    ctx.findIdent(cursor).map(_.span)
  }

  def span(start: Int, end: Int) = ctx.span(start, end)

  // this test validates the actual spans and such. its much more complete, but much
  // more verbose, so there will only be a couple of these.
  "A context" should "find ident under the cursor" in {
    ident("|foo") shouldBe Some(span(0, 3))
    ident("f|oo") shouldBe Some(span(0, 3))
    ident("fo|o") shouldBe Some(span(0, 3))
    ident("foo|") shouldBe Some(span(0, 3))
  }

  it should "handle whitespace" in {
    ident("| foo") shouldBe None
    ident(" |foo") shouldBe Some(span(1, 4))
    ident(" f|oo") shouldBe Some(span(1, 4))
    ident(" fo|o") shouldBe Some(span(1, 4))
    ident(" foo|") shouldBe Some(span(1, 4))

    ident("|foo ") shouldBe Some(span(0, 3))
    ident("f|oo ") shouldBe Some(span(0, 3))
    ident("fo|o ") shouldBe Some(span(0, 3))
    ident("foo| ") shouldBe Some(span(0, 3))
    ident("foo |") shouldBe None
  }

  it should "handle weird edge cases with ident continue" in {
    ident("00|0foo ") shouldBe None
    ident("000|foo ") shouldBe Some(span(3, 6))
    ident("000f|oo ") shouldBe Some(span(3, 6))
    ident("000fo|o ") shouldBe Some(span(3, 6))
    ident("000foo| ") shouldBe Some(span(3, 6))
    ident("000foo |") shouldBe None

    ident("| foo000 ") shouldBe None
    ident(" |foo000 ") shouldBe Some(span(1, 7))
    ident(" f|oo000 ") shouldBe Some(span(1, 7))
    ident(" fo|o000 ") shouldBe Some(span(1, 7))
    ident(" foo|000 ") shouldBe Some(span(1, 7))
    ident(" foo0|00 ") shouldBe Some(span(1, 7))
  }
}
