package fql.test.error

import fql.ast.Src
import fql.error.ParseError
import fql.test.Spec
import fql.Result
import org.scalactic.source.Position

abstract class ErrorSpec extends Spec {

  def parse(query: String): Result[_]

  val EOF = "\u0000"

  /** The first argument is the query. The second argument is the string to
    * search for, of which the first character will be underlined by the error.
    * The third argument is the error message.
    *
    * The second and their arguments can be ommited, and that will cause the
    * error message to just be printed to the console, for debugging.
    */
  def error(query: String, matching: String = "", expected: Object = "")(
    implicit pos: Position): Unit = {
    val index = if (matching == EOF) {
      query.length
    } else {
      query.indexOf(matching)
    }
    val matchLength = if (matching == "") 1 else matching.length
    if (index == -1) {
      fail(s"could not find `$matching` in query string")
    }
    error(query, index, index + matchLength, expected)(pos)
  }

  // Same thing as error, but takes a start/end index, because sometimes you don't
  // have a matching string.
  def error(query: String, start: Int, end: Int, expected: Object)(
    implicit pos: Position): Unit = {
    parse(query) match {
      case Result.Ok(_) => fail("should not work!")
      case Result.Err(List(err @ ParseError(msg, span, _, _))) =>
        val rendered = err.renderWithSource(Map(Src.Id("*query*") -> query))
        if (expected == "") {
          info(rendered)
        } else {
          if (span.start != start) {
            info(rendered)
            fail(s"span start should be $start, got ${span.start}")
          }
          if (span.end != end) {
            info(rendered)
            fail(s"span end should be $end, got ${span.end}")
          }
          if (msg != expected) {
            info("ACTUAL ERROR:")
            info(rendered)
            info("EXPECTED MESSAGE:")
            info(expected.toString)
            fail("message was not equal to expected message.")
          }
        }
      case Result.Err(errors) =>
        info(s"query: $query")
        val rendered = errors
          .map { _.renderWithSource(Map(Src.Id("*query*") -> query)) }
          .mkString("\n\n")
        info(rendered)
        fail("too many errors")
    }
  }

  def error(query: String, expected: Seq[String]): Unit = {
    parse(query) match {
      case Result.Ok(_) => fail("should not work!")
      case Result.Err(errors) =>
        val errorMsgs = errors.map { _.message }
        if (errorMsgs != expected) {
          info("ACTUAL MESSAGES:")
          info(errorMsgs.toString)
          info("EXPECTED MESSAGES:")
          info(expected.toString)
          fail("message was not equal to expected message.")
        }
    }
  }
}
