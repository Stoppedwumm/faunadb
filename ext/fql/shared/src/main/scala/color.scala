package fql.color

/** A wrapper around a `StringBuilder`, that supports switching colors.
  *
  * This is currently implemented for ANSI escape codes, but could also be
  * extended to render to HTML or JSON.
  */
sealed trait ColorBuilder {

  /** Appends a string in the current color.
    */
  def append(v: String): Unit
  def append(v: Char): Unit
  def append(v: Int): Unit

  def ++=(v: String): Unit = append(v)

  /** Sets the current color.
    */
  def select(color: Color): Unit
  def bold(): Unit

  /** Resets the color to the default.
    */
  def reset(): Unit

  def result: String
}

sealed trait ColorKind {
  def createBuilder: ColorBuilder
}

object ColorKind {
  final object None extends ColorKind {
    def createBuilder = new ColorBuilder.None(new StringBuilder)
  }

  final object Ansi extends ColorKind {
    def createBuilder = new ColorBuilder.Ansi(new StringBuilder)
  }
}

sealed trait Color {
  def ansi: String
}

object Color {
  final object Green extends Color { def ansi = AnsiCode.ForegroundGreen }
  final object Red extends Color { def ansi = AnsiCode.ForegroundRed }
  final object Yellow extends Color { def ansi = AnsiCode.ForegroundYellow }
  final object Blue extends Color { def ansi = AnsiCode.ForegroundBlue }
  final object Cyan extends Color { def ansi = AnsiCode.ForegroundCyan }
  final object Default extends Color { def ansi = sys.error("unreachable") }
}

object ColorBuilder {
  final class None(sb: StringBuilder) extends ColorBuilder {
    def append(v: String): Unit = sb.append(v)
    def append(v: Char): Unit = sb.append(v)
    def append(v: Int): Unit = sb.append(v)

    def select(color: Color): Unit = ()
    def bold(): Unit = ()
    def reset(): Unit = ()
    def result = sb.toString()
  }

  // Represents the current ANSI color state (this includes the foreground color,
  // background color, bold, underline, etc.)
  //
  // Note that `None` means the default color, which is not nessisarily white!
  // Setting the foreground to white is not the same as setting it to the default, as
  // some terminals don't render white text by default.
  case class ColorState(var foreground: Color, var bold: Boolean) {
    def update(partial: PartialColorState) = {
      partial.foreground.foreach { this.foreground = _ }
      partial.bold.foreach { this.bold = _ }
    }

    def isChanged(partial: PartialColorState): Boolean = {
      partial.foreground.exists { _ != foreground } ||
      partial.bold.exists { _ != bold }
    }
  }

  // A ColorState, where only some fields are set.
  case class PartialColorState(
    var foreground: Option[Color],
    var bold: Option[Boolean]) {
    def isSet = foreground.isDefined || bold.isDefined
    def reset(): Unit = {
      foreground = None
      bold = None
    }
  }

  object PartialColorState {
    val Default = PartialColorState(None, None)
  }

  final class Ansi(sb: StringBuilder) extends ColorBuilder {
    val current = ColorState(Color.Default, false)
    val next: PartialColorState = PartialColorState.Default

    // This is a little state machine. We try our best to minimize the characters
    // emitted, so we collect all the color changes, and then only emit them when the
    // next character is added.
    private def applyNext(): Unit = {
      if (next.isSet && current.isChanged(next)) {
        val nextState = current.copy()
        nextState.update(next)

        sb ++= "\u001b["

        var isFirst = true
        def arg(v: String): Unit = {
          if (isFirst) {
            isFirst = false
          } else {
            sb ++= ";"
          }
          sb ++= v
        }

        nextState.foreground match {
          // The shortest way to reset the color is to also reset the bold state,
          // so we need this special case.
          case Color.Default if current.foreground != Color.Default =>
            arg(AnsiCode.Reset)

            if (nextState.bold) arg(AnsiCode.EnableBold)

          case foreground =>
            (current.bold, nextState.bold) match {
              case (false, true) => arg(AnsiCode.EnableBold)
              case (true, false) => arg(AnsiCode.DisableBold)
              case _             => ()
            }

            if (current.foreground != foreground) arg(foreground.ansi)
        }

        sb ++= "m"

        current.update(next)
        next.reset()
      }
    }

    def append(v: String): Unit = {
      applyNext()
      sb.append(v)
    }
    def append(v: Char): Unit = {
      applyNext()
      sb.append(v)
    }
    def append(v: Int): Unit = {
      applyNext()
      sb.append(v)
    }

    def select(color: Color): Unit = next.foreground = Some(color)
    def bold(): Unit = next.bold = Some(true)
    def reset(): Unit = {
      next.foreground = Some(Color.Default)
      next.bold = Some(false)
    }

    def result = sb.toString()
  }
}

// ANSI escape codes are formatted like:
// ```
// \u001b[<arg>;<arg>;<arg>m
// ```
//
// Where `arg` is one of the following escape codes. Any number of arguments may
// be added.
//
// Note that some codes require sequences. For example, the foreground color
// can be set to an RGB value by inserting a code like so (code 38 is special):
// ```
// \u001b[38;2;<r>;<g>;<b>m
// ```
object AnsiCode {
  val Reset = "0"

  val EnableBold = "1"
  val DisableBold = "22"

  val ForegroundBlack = "30"
  val ForegroundRed = "31"
  val ForegroundGreen = "32"
  val ForegroundYellow = "33"
  val ForegroundBlue = "34"
  val ForegroundMagenta = "35"
  val ForegroundCyan = "36"
  val ForegroundWhite = "37"
  val ForegroundDefault = "39"
}
