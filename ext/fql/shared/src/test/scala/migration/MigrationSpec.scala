package fql.test

import fql.ast._
import fql.migration._
import fql.parser._
import fql.Result
import fql.TextUtil
import scala.concurrent.duration._

trait MigrationSpec extends TypeSpec {
  implicit val deadline = Int.MaxValue.seconds.fromNow

  def validate(from: String, to: String, expected: String)(
    implicit pos: org.scalactic.source.Position) = {
    val fromItem = Parser
      .schemaItems(from, Src.Inline("main.fsl", from)) match {
      case Result.Ok(v) => v.head.asInstanceOf[SchemaItem.Collection]
      case Result.Err(e) =>
        fail(e.map(_.renderWithSource(Map.empty)).mkString("\n"))
    }
    val toItem = Parser
      .schemaItems(to, Src.Inline("main.fsl", to)) match {
      case Result.Ok(v)  => v.head.asInstanceOf[SchemaItem.Collection]
      case Result.Err(e) => fail(e.map(_.renderWithSource(Map.empty)).mkString("\n"))
    }

    val result =
      MigrationValidator.validate(TestTyper(), fromItem, toItem) match {
        case Result.Ok(migrations) =>
          migrations.map(_.debug).mkString("\n")
        case Result.Err(errs) =>
          errs.map(_.renderWithSource(Map.empty)).mkString("\n")
      }

    if (result != expected) {
      println(result)
      withClue(TextUtil.diff(result, expected)) {
        fail("generated and expected diffs do not match")
      }
    }
  }
}
