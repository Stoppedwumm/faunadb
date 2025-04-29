package fql.parser

import fastparse._
import fql.error.{ DiagnosticEmitter, ParseError }

// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
trait Errors extends DiagnosticEmitter[ParseError] { parser: Parser =>
  var errs = List.empty[ParseError]

  def fail[_: P](e: ParseError): P[Nothing] = {
    errs = errs :+ e
    Fail
  }

  def continue[A, _: P](e: ParseError, fallback: A) = {
    emit(e)
    Pass(fallback)
  }

  def emit(e: ParseError) = errs = errs :+ e
}
