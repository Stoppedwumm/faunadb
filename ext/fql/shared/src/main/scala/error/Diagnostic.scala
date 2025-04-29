package fql.error

import fql.ast.{ Span, Src }
import fql.color.ColorBuilder
import fql.error.Hint.HintType

object Diagnostic {
  def writeSpan(sb: ColorBuilder, span: Span)(implicit ctx: RenderCtx): Unit = {
    ctx(span.src) match {
      case Some(query) =>
        val (line, col) = span.lineCol(query)
        writeSpan(sb, span.src, line, col)
      case None =>
        sb.append("<no source>")
    }
  }

  def writeSpan(sb: ColorBuilder, src: Src, line: Int, col: Int): Unit = {
    if (src != Src.Null) {
      sb.append(src.name)
      sb.append(":")
    }
    sb.append(line)
    sb.append(":")
    sb.append(col)
  }
}

/** A single warning or error message. The format renders like so:
  *
  * ```
  * <name>: <message>
  * at <span>
  *        |
  * <line> | <source code>
  *        |   ^^^^ <annotation>
  *        |
  * <hints rendered in the same way as above>
  * ```
  *
  * Hints are `Diagnostic`s as well, so they will render in the same way.
  */
sealed trait Diagnostic {
  def name: String
  def message: String
  def span: Span
  def annotation: Option[String] = None
  def hints: Seq[Hint] = Nil

  def allSrcs: Set[Src] = Set(span.src) ++ hints.map { _.span.src }

  def messageLine: String = {
    if (name == "") {
      return ""
    }
    val sb = new StringBuilder
    sb.append(name)
    sb.append(":")
    if (message != "") {
      sb.append(" ")
      sb.append(message)
    }
    sb.toString
  }

  /** Renders this Diagnostic with the given source.
    *
    * If the `parentLine` is set, then the `at *query*:<line>:<col>` line won't
    * be rendered if the line of this span is equal to the `parentLine`.
    *
    * This is for rendering hints, when we don't want that verbosity if the hint
    * is on the same line as the parent diagnostic.
    */
  final def renderWithSource(
    sourceCtx: Map[Src.Id, String],
    parent: Option[(Int, Src)] = None): String = {
    implicit val renderCtx = RenderCtx(sourceCtx)
    renderWithSource(parent)
  }

  protected def renderWithSource(parent: Option[(Int, Src)])(
    implicit ctx: RenderCtx): String = {
    val source = ctx(span.src)
    val sb = new StringBuilder
    renderBodyWithSource(sb, source, parent, '^')

    val line = source match {
      case Some(source) => source.lineNumberOf(span.start)
      case None         => 0
    }
    hints foreach { hint =>
      sb.append("\n")
      sb.append(hint.renderWithSource(Some((line, span.src))))
    }

    sb.toString
  }

  /** Renders the body of this diagnostic, using the given source. This will not
    * render any child hints of this Diagnostic, which is why it only takes the
    * `query` string, instead of a `Map[Src, String]`.
    *
    * Returns the line number of this diagnostic's span.
    */
  protected def renderBodyWithSource(
    sb: StringBuilder,
    query: Option[Source],
    parent: Option[(Int, Src)],
    underline: Char): Unit = {
    val msg = messageLine
    sb.append(msg)
    if (!msg.isEmpty) {
      sb.append("\n")
    }

    query match {
      case Some(query) =>
        val (annotated, line, col) =
          span.annotateWithLineColNumber(
            query,
            annotation getOrElse "",
            underline = underline)
        if (parent.isEmpty || parent.get._1 != line || parent.get._2 != span.src) {
          sb.append("at ")
          Diagnostic.writeSpan(new ColorBuilder.None(sb), span.src, line, col)
          sb.append("\n")
        }
        sb.append(annotated)
      case None =>
        sb.append("at <no source>")
    }
  }
}

trait Error extends Diagnostic {
  final def name = "error"
}

final case class Warning(
  message: String,
  span: Span,
  override val annotation: Option[String] = None,
  override val hints: Seq[Hint] = Seq.empty)
    extends Diagnostic {
  def name = "warning"
}

/** A log message, which can be rendered in a short or long form.
  */
final case class Log(message: String, span: Span, long: Boolean) extends Diagnostic {
  def name = "info"

  override def renderBodyWithSource(
    sb: StringBuilder,
    query: Option[Source],
    parent: Option[(Int, Src)],
    underline: Char): Unit = {
    if (long) {
      super.renderBodyWithSource(sb, query, parent, underline)
    } else {
      val line = query match {
        case Some(query) => query.lineNumberOf(span.start)
        case None        => 0
      }
      sb.append(name)
      sb.append(" at ")
      if (span.src == Src.Null) {
        sb.append("<no source>")
      } else {
        sb.append(span.src.name)
        sb.append(":")
        sb.append(line)
      }
      sb.append(": ")
      sb.append(message)
    }
  }
}

private sealed trait HintKind {
  def underline: Char
}

private object HintKind {
  object Simple extends HintKind {
    def underline = '^'
  }
  object Remove extends HintKind {
    def underline = '-'
  }
  object Add extends HintKind {
    def underline = '+'
  }
  object Change extends HintKind {
    def underline = '~'
  }
}

object Hint {
  sealed trait HintType
  object HintType {
    object General extends HintType
    object Performance extends HintType
    object Trace extends HintType
    object NamedTrace extends HintType
    object Cause extends HintType
  }

  def TraceWithName(span: Span) = Hint("", span, None, HintType.NamedTrace)
  def Trace(span: Span) = Hint("", span, None, HintType.Trace)
}

sealed case class Hint(
  message: String,
  span: Span,
  suggestion: Option[String] = None,
  hintType: HintType = HintType.General
) extends Diagnostic {

  // Hints never have children
  override def hints = Nil

  def name: String = hintType match {
    case HintType.General     => "hint"
    case HintType.Performance => "performance_hint"
    case HintType.Trace       => ""
    case HintType.NamedTrace  => "trace"
    case HintType.Cause       => "cause"
  }

  private def kind = suggestion match {
    case None                              => HintKind.Simple
    case Some("")                          => HintKind.Remove
    case Some(_) if span.start == span.end => HintKind.Add
    case Some(_)                           => HintKind.Change
  }

  override def renderWithSource(parent: Option[(Int, Src)])(
    implicit ctx: RenderCtx): String = {
    val source = ctx(span.src)
    val sb = new StringBuilder

    kind match {
      case HintKind.Simple | HintKind.Remove =>
        renderBodyWithSource(sb, source, parent, kind.underline)
      case HintKind.Add | HintKind.Change =>
        // Patch will start from span.start, then remove the number of
        // characters specified by `span.end - span.start`, then insert the text
        // of `suggestion.get` where those characters were.
        val modified =
          source.map {
            _.src.patch(span.start, suggestion.get, span.end - span.start)
          }
        val hint = this.copy(
          span = Span(span.start, span.start + suggestion.get.size, span.src))

        hint.renderBodyWithSource(
          sb,
          modified.map(Source(_)),
          parent,
          kind.underline)
    }

    sb.toString
  }
}
