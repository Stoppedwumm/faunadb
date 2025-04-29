package fauna.model.test

import fauna.model.runtime.fql2.QueryCheckFailure
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import fql.error.Log
import fql.error.TypeError
import org.scalactic.source.Position

class FQL2LoggingSpec extends FQL2Spec {

  val auth = newAuth

  def span(start: Int, end: Int, src: Src = Src.Query("")) =
    Span(start, end, src)

  def checkLog(query: String, logs: Seq[Log])(implicit pos: Position) = {
    val out = eval(auth, query)
    val res = out.res.getOrElse(fail())

    res.value shouldBe Value.Null(Span.Null)
    res.typeStr shouldBe "Null"
    out.logs.check shouldBe Seq.empty
    out.logs.runtime shouldBe logs
  }

  def checkDbg(query: String, ty: String, logs: Seq[Log] = Seq.empty)(
    implicit pos: Position) = {
    val out = eval(auth, query)
    val res = out.res.getOrElse(fail())

    res.typeStr shouldBe ty
    out.logs.check shouldBe Seq.empty
    out.logs.runtime shouldBe logs
  }

  "log() works" in {
    checkLog("log('hi')", Seq(Log("hi", span(3, 9), long = false)))
    checkLog("log(3, 4)", Seq(Log("3 4", span(3, 9), long = false)))

    checkLog(
      "log(3); log(4)",
      Seq(Log("3", span(3, 6), long = false), Log("4", span(11, 14), long = false)))

    checkLog("log()", Seq(Log("", span(3, 5), long = false)))
  }

  "dbg() works" in {
    checkDbg("dbg('hi')", "\"hi\"", Seq(Log("\"hi\"", span(3, 9), long = true)))
    checkDbg("dbg(3)", "3", Seq(Log("3", span(3, 6), long = true)))

    checkDbg(
      "dbg(dbg(3))",
      "3",
      Seq(Log("3", span(7, 10), long = true), Log("3", span(3, 11), long = true)))

    evalErr(auth, "dbg(3, 4)") should matchPattern {
      case QueryCheckFailure(
            Seq(TypeError(
              "Function was called with too many arguments. Expected 1, received 2",
              _,
              Nil,
              Nil))) =>
    }
  }

  "log in the past" in {
    checkLog(
      "at (Time.now().subtract(1, 'day')) { log('hi') }",
      Seq(Log("hi", span(40, 46), long = false)))
  }
}
