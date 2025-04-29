package fql.test

import fql.color.{ Color, ColorBuilder }

class AnsiSpec extends Spec {

  it should "work" in {
    val sb = new ColorBuilder.Ansi(new StringBuilder)
    sb.select(Color.Green)
    sb.bold()
    sb ++= "Hello"
    sb.result shouldBe "\u001b[1;32mHello"
  }

  it should "combine bold and color switching correctly" in {
    val sb = new ColorBuilder.Ansi(new StringBuilder)
    sb.bold()
    sb ++= "white"
    sb.select(Color.Green)
    sb ++= "green"
    sb.reset()
    sb ++= "reset"
    sb.result shouldBe "\u001b[1mwhite\u001b[32mgreen\u001b[0mreset"
  }
}
