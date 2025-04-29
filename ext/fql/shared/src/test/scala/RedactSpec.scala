package fql.test

import fql.ast.display._
import fql.ast.redact._
import fql.parser.Parser
import org.scalactic.source.Position

class RedactSpec extends Spec {
  def checkRedact(src1: String, src2: String)(implicit pos: Position): Unit = {
    val e1 = Parser.expr(src1).toOption.get
    val redacted = e1.redact.display
    redacted shouldBe src2
    ()
  }

  def checkRedactStrict(src1: String, src2: String)(implicit pos: Position): Unit = {
    val e1 = Parser.expr(src1).toOption.get
    val redacted = e1.redactStrict.display
    redacted shouldBe src2
    ()
  }

  "Redact" should "redact strings" in {
    checkRedact(
      "{ let a = 'hello' + 'bye' + 2; a }",
      """|{
         |  let a = <string with length 5> + <string with length 3> + 2
         |  a
         |}""".stripMargin
    )
    checkRedact(
      "{ User.where(.name == 'me') }",
      """|{
         |  User.where(.name == <string with length 2>)
         |}""".stripMargin)
  }

  it should "not redact floats" in {
    checkRedact(
      "{ let a = 3.0; a }",
      """|{
         |  let a = 3.0
         |  a
         |}""".stripMargin)
  }

  it should "not redact ints" in {
    checkRedact(
      "{ let a = 3; a }",
      """|{
         |  let a = 3
         |  a
         |}""".stripMargin)
  }

  "Redact strict" should "redact strings" in {
    checkRedactStrict(
      "{ let a = 'hello' + 'bye' + 2; a }",
      """|{
         |  let a = <string> + <string> + <integer>
         |  a
         |}""".stripMargin)
    checkRedactStrict(
      "{ User.where(.name == 'me') }",
      """|{
         |  User.where(.name == <string>)
         |}""".stripMargin)
  }

  it should "redact floats" in {
    checkRedactStrict(
      "{ let a = 3.0; a }",
      """|{
         |  let a = <float>
         |  a
         |}""".stripMargin)
  }

  it should "redact ints" in {
    checkRedactStrict(
      "{ let a = 3; a }",
      """|{
         |  let a = <integer>
         |  a
         |}""".stripMargin)
  }
}
