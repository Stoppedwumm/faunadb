package fql.parser

import fastparse._
import fql.ast.{ Span, Src }

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait Spans { parser: Parser =>
  final def span(s: Int, e: Int) =
    if (src == Src.Null) {
      Span(0, 0, src)
    } else {
      Span(s + srcOffset, e + srcOffset, src)
    }

  // In order to ensure the generated span is correct, it is important for the
  // inner parser does not consume leading or trailing whitespace.
  @inline
  final def spanned[I, T, _: P](inner: => P[I])(f: (I, Span) => T): P[T] =
    P(Index ~~ inner ~~ Index).map { case (s, i, e) => f(i, span(s, e)) }

  final def spannedUnit[T, _: P](inner: => P[Unit]): P[Span] =
    P(Index ~~ inner ~~ Index).map { case (s, e) => span(s, e) }
}
