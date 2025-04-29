package fql.parser

import fastparse._

// fastparse macro expansion introduces this unused `charIn` var in many places.
// ignore resulting warnings.
@annotation.nowarn("msg=pattern var charIn*")
// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait Whitespace { parser: Parser =>
  def lineComment[_: P] = P("//" ~~ (!CharIn("\n") ~~ anychar).repX ~~ "\n".?)
  def blockComment[_: P]: P[Unit] = P(
    "/*" ~~ ((!"/*" ~~ !"*/" ~~ anychar) | blockComment).repX ~~ "*/")

  def ws0[_: P] = P(CharsWhileIn(" \t"))
  def nl0[_: P] = P(CharsWhileIn("\n\r"))

  def ws[_: P] = P((ws0 | blockComment).repX)
  def nl[_: P] = P((nl0 | ws0 | lineComment | blockComment).repX)

  def blocksep[_: P] = P(ws ~~ ((CharsWhileIn(";\n\r") | lineComment) ~~ ws).repX(1))
  def objectsep[_: P] = P(ws ~~ (CharIn(";,\n\r") | lineComment) ~~ nl)

  // This is a bit of nonsense, but effectively:
  // - Parse any sequence of whitespace, comments, or semicolons.
  // - Once we hit something that doesn't match (presmuably the end of a line),
  //   then stop parsing.
  // - Return `Some(index)` if there was a semicolon in there, and `None` if there
  //   was no semicolon.
  //
  // This is used in a positive lookahead to include trailing semicolons in the
  // span of an item.
  def optsemi[_: P]: P[Option[Int]] = P(
    ((ws0 | blockComment).map(_ => None) |
      (CharIn(";") ~~ Index).map(i => Some(i))).repX())
    .map { items =>
      // Return the end of the last semicolon, if there is one.
      items.findLast(_.isDefined).flatten
    }
    .opaque("")

  // This is a copy of the `&` (positive lookahead) operator, but I actually return
  // the parsed thing instead of `Unit`.
  //
  // Also I removed the error handling, because lookaheads should not produce errors.
  def lookahead[T](parse: => P[T])(implicit ctx: P[_]): P[T] = {
    val startPos = ctx.index
    val startCut = ctx.cut
    val oldNoCut = ctx.noDropBuffer
    ctx.noDropBuffer = true
    parse
    ctx.noDropBuffer = oldNoCut

    val res = if (ctx.isSuccess) {
      // Make a fresh success at the start position, which is what rewinds before
      // `parse`.
      ctx.freshSuccess(ctx.successValue, startPos).asInstanceOf[P[T]]
    } else {
      ctx.asInstanceOf[P[T]]
    }
    res.cut = startCut
    res
  }
}
