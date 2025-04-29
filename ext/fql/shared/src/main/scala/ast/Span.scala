package fql.ast

import fql.error.Source
import fql.parser.TemplateSigil
import scala.collection.mutable.ArrayDeque

object Span {
  val Null = Span(0, 0, Src.Null)
  val DecodedSet = Span(0, 0, Src.DecodedSet)

  private[fql] val LeadingNLRegex = raw"\A(?:[\t ]*\n)+".r.unanchored
  private val TrailingNLRegex = raw"(?:\n[\t ]*)+\Z".r.unanchored
  private val LeadingSemiRegex = raw"\A[\t ]*;".r.unanchored
  private val TrailingSemiRegex = raw";[\t ]*\Z".r.unanchored
  private val TrailingIndentRegex = raw"[\t ]*\Z".r.unanchored
  private val NLWithWordRegex = raw"\n(?=[\t ]*\S)".r.unanchored
}

final case class Span(start: Int, end: Int, src: Src) extends Ordered[Span] {
  def compare(other: Span) =
    src.compare(other.src) match {
      case 0 =>
        java.lang.Integer.compare(start, other.start) match {
          case 0   => java.lang.Integer.compare(end, other.end)
          case cmp => cmp
        }
      case cmp => cmp
    }

  def contains(cursor: Int) = cursor >= start && cursor <= end

  def contains(inner: Span) =
    src == inner.src &&
      (inner.start >= start && inner.start <= end) &&
      (inner.end >= start && inner.end <= end)

  def overlaps(inner: Span) =
    src == inner.src &&
      ((inner.start >= start && inner.start <= end) ||
        (inner.end >= start && inner.end <= end))

  def getOrElse(other: Span) = if (this == Span.Null) other else this

  def length = end - start

  def extract(source: String) =
    source.substring(start, end)

  def from(start: Int) = this.copy(start = start)
  def to(end: Int) = this.copy(end = end)

  // TODO: Handle differing `src`?
  def from(other: Span): Span = from(other.start)
  def to(other: Span): Span = to(other.end)

  /** Replaces this span within `source` with `sub`, matching indent and preserving
    * line breaks/semicolons
    */
  def replaceLineWith(source: String, sub: String) =
    if (sub.isEmpty || Span.TrailingSemiRegex.matches(sub)) {
      val after = source.substring(end, source.length)
      val span0 = Span.LeadingSemiRegex.findFirstIn(after).fold(this) { m =>
        copy(end = end + m.length)
      }
      span0.replaceWith(source, sub)
    } else {
      replaceWith(source, sub)
    }

  /** Replaces this span within `source` with `sub`, matching indent.
    */
  def replaceWith(source: String, sub: String) = {
    val before = source.substring(0, start)
    val after = source.substring(end, source.length)

    if (sub.isEmpty) {
      if (!Span.LeadingNLRegex.matches(after)) {
        s"$before${after.stripLeading}"
      } else {
        s"${before.stripTrailing}$after"
      }
    } else if (!sub.contains("\n")) {
      s"$before$sub$after"
    } else {
      if (Span.TrailingNLRegex.matches(before)) {
        val indent = Span.TrailingIndentRegex.findFirstIn(before).getOrElse("")
        val sub0 = Span.NLWithWordRegex.replaceAllIn(sub, s"\n$indent")

        s"$before$sub0$after"
      } else {
        s"$before$sub$after"
      }
    }
  }

  def annotateWithLineColNumber(
    src: Source,
    message: String = "",
    context: Int = 0,
    underline: Char = '^'): (String, Int, Int) = {

    // Makes underlines at EOF underline the last line with visible characters.
    var trimmedSrc = src.src.stripTrailing()
    if (start > trimmedSrc.length) {
      val diff = start - trimmedSrc.length
      return copy(start = start - diff, end = end - diff).annotateWithLineColNumber(
        Source(trimmedSrc),
        message,
        context,
        underline)
    }

    // Empty query edge case
    if (trimmedSrc.isEmpty) {
      trimmedSrc = " "
    }

    val out = new ArrayDeque[(String, String, String)]

    var prefixwidth = 0
    var gutterwidth = 0
    var scanned = src.lineStart(start)
    var lno = src.lineIndexOf(start)
    var cno = -1

    val lines = trimmedSrc.linesWithSeparators.zipWithIndex.drop(lno)

    def append(prefix: String, gutter: String, line: String) = {
      prefixwidth = prefix.size max prefixwidth
      gutterwidth = gutter.size max gutterwidth
      out.append((prefix, gutter, line))
    }

    while (scanned <= start && lines.hasNext) {
      val (line, i) = lines.next()

      // only keep the required lines for preceding context
      if (out.size > context) {
        out.removeHead()
      }

      // If there isn't another line, then we need to highlight the last line.
      if (scanned + line.size <= start && lines.hasNext) {
        append("| ", "", line)
      } else {
        val indent = start - scanned
        lno = i + 1
        cno = indent + 1
        // Replaces any sigils with '<'. See
        // ext/model/src/main/scala/runtime/fql2/serialization/FQL2Query.scala
        append(
          s"$lno | ",
          "",
          line.replace(TemplateSigil.Nul, TemplateSigil.LeftAngle))

        // It will only be multiline if there is another line after this one.
        val multiline = indent + length > line.size && lines.hasNext

        if (!multiline) {
          // for single line hilights, repeat the underline character
          val b = new StringBuilder
          for (_ <- 1 to indent) { b += ' ' }
          for (_ <- 1 to length) { b += underline }
          if (message.size > 0) {
            b += ' '
            b ++= message
          }
          append("| ", "", b.result())
        } else {
          // for multiline hilights, we have to build a bracket.
          append("| ", " _", ("_" * indent) + underline)

          // track the remaining characters to underline
          var rem = length - (line.size - indent)
          while (rem > 0) {
            val (line, i) = lines.next()
            val rem0 = rem - line.size

            append(
              s"${i + 1} | ",
              "| ",
              line.replace(TemplateSigil.Nul, TemplateSigil.LeftAngle))

            // emit the bracket end if done.
            if (rem0 <= 0 || !lines.hasNext) {
              append("| ", "|_", ("_" * (rem - 1)) + "^")
              rem = 0
            } else {
              rem = rem0
            }
          }
        }
      }

      scanned += line.size
    }

    // append succeeding context
    lines.take(context).foreach(t => append("| ", "", t._1))

    out.prepend(("| ", "", ""))
    out.append(("| ", "", ""))

    val annotation = out.iterator
      .map { case (p, g, line) =>
        val prefix = (" " * (prefixwidth - p.size)) + p
        val gutter = (" " * (gutterwidth - g.size)) + g
        s"$prefix$gutter$line".stripTrailing()
      }
      .mkString("\n")

    (annotation, lno, cno)
  }

  def lineCol(src: Source): (Int, Int) = {
    // Makes underlines at EOF underline the last line with visible characters.
    var trimmedSrc = src.src.stripTrailing()
    if (start > trimmedSrc.length) {
      val diff = start - trimmedSrc.length
      return copy(start = start - diff, end = end - diff).lineCol(Source(trimmedSrc))
    }

    // Empty query edge case
    if (trimmedSrc.isEmpty) {
      trimmedSrc = " "
    }

    var scanned = src.lineStart(start)
    var lno = src.lineIndexOf(start)
    var cno = -1

    val lines = trimmedSrc.linesWithSeparators.zipWithIndex.drop(lno)

    while (scanned <= start && lines.hasNext) {
      val (line, i) = lines.next()

      // If there isn't another line, then we need to highlight the last line.
      if (scanned + line.size > start || lines.hasNext) {
        val indent = start - scanned
        lno = i + 1
        cno = indent + 1
      }

      scanned += line.size
    }

    (lno, cno)
  }

  def annotate(src: String, message: String = "", context: Int = 0): String = {
    annotateWithLineColNumber(Source(src), message, context)._1
  }

  override def toString =
    if (src == Src.Null) s"$start:$end" else s"$start:$end/${src.name}"
}
