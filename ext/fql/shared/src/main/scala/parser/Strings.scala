package fql.parser

import fastparse._
import fastparse.NoWhitespace._
import fql.ast.Span

/** Codepoints parsed and displayed by FQL and FSL fall into three categories:
  * 1. Escaped characters. This special class is parsed and displayed as escape
  *    sequences, and includes the usual suspects like line feeds, double quotes,
  *    and backslashes.
  * 2. Display characters. This class is parsed and displayed verbatim.
  * 3. The other guys. This class is parsed and displayed as special Unicode escape
  *    sequences of the form \uhhhh for 2-byte code points or \u{hhhhhh} for the rest
  *    of the code points.
  */
object Chars {
  // This map defines the class of escaped characters (the values) and the escape
  // symbol for each one (the keys).
  val Escaped = Map(
    "\\" -> "\\",
    "0" -> "\u0000",
    "'" -> "'",
    "\"" -> "\"",
    "`" -> "`",
    "n" -> "\n",
    "r" -> "\r",
    "v" -> "\u000b",
    "t" -> "\t",
    "b" -> "\b",
    "f" -> "\f",
    "#" -> "#")

  // A quasi-inverse map to Escaped, used in displaying escaped characters.
  val Unescaped = Map(
    '\\' -> '\\',
    '\u0000' -> '0',
    '"' -> '"',
    '\n' -> 'n',
    '\r' -> 'r',
    '\u000b' -> 'v',
    '\t' -> 't',
    '\b' -> 'b',
    '\f' -> 'f',
    '#' -> '#')

  // This predicate defines the class of displayed characters.
  def isDisplay(c: Char) = c match {
    // Disallow control characters.
    case c if c >= '\u0000' && c <= '\u0008' => false
    case c if c >= '\u000e' && c <= '\u001f' => false
    case '\u007f'                            => false
    case c if c >= '\ufff0' && c <= '\uffff' => false
    case '\ufeff'                            => false
    // These are control characters that can break highlighting. See
    // https://blog.rust-lang.org/2021/11/01/cve-2021-42574.html and
    // https://en.wikipedia.org/wiki/List_of_Unicode_characters#General_Punctuation
    case c if c >= '\u2000' && c <= '\u200f' => false
    case c if c >= '\u2028' && c <= '\u202f' => false
    case c if c >= '\u2060' && c <= '\u206f' => false
    case _                                   => true
  }
}

// fastparse macro expansion introduces this unused `charIn` var in many places.
// ignore resulting warnings.
@annotation.nowarn("msg=pattern var charIn*")
// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
/** Parser for strings and string iterpolation. */
trait Strings { parser: Parser =>
  import Chars._

  private def codepoint[_: P](str: String, span: Span): P[String] = {
    val codepoint = Integer.parseInt(str, 16)
    if (codepoint > 0x10ffff) {
      fail(Failure.InvalidEscape(span, "outside of valid unicode range"))
    } else if (codepoint > Char.MaxValue) {
      val sb = new java.lang.StringBuilder
      sb.appendCodePoint(codepoint)
      Pass(sb.toString)
    } else {
      val c = codepoint.toChar
      if (Character.isSurrogate(c)) {
        fail(Failure.InvalidEscape(span, "surrogates are disallowed"))
      } else {
        Pass(Character.toString(codepoint.toChar))
      }
    }
  }
  def hexescape[_: P] = P("x" ~~/ (hexdigit ~~ hexdigit).!)
  def unicodeescape[_: P] = P(
    "u" ~~/ ((hexdigit ~~/ hexdigit ~~/ hexdigit ~~/ hexdigit).! |
      P("{", "`{`") ~~/ (hexdigit.repX(min = 1, max = 6)).! ~~/ `}`))
  def escapecode[_: P] = P(CharIn("\\\\0'\"nrvtbf#"), "escape code")
  def escapeseq[_: P] = P(
    "\\" ~~/ (
      escapecode.!.map(Escaped) |
        (P(Index) ~~ (hexescape | unicodeescape) ~~ Index).flatMap {
          case (s, str, e) =>
            // -1 is to include the `\` in the span.
            codepoint(str, span(s - 1, e))
        }
    )
  )

  def anychar[_: P] = P(CharPred(isDisplay))
  // excludes \, ", ', `, #
  def stringchars[_: P](oq: => P[_]) = P(CharsWhile {
    case '\\' | '"' | '\'' | '`' | '#' => false
    case c                             => isDisplay(c)
  } | oq)

  def innerstring[_: P](oq: => P[_]) = P((stringchars(oq).! | escapeseq).repX(1))
  def string0[_: P](q: String, oq: => P[_]) = P(
    q ~~ innerstring(oq).?.map(_.getOrElse(Nil).mkString) ~~ q)
  def singlestring[_: P] = P(string0("'", CharsWhileIn("\"#`")))

  def doublestring[_: P] = P(string0("\"", CharsWhileIn("'#`")))

  def istringpart[T, _: P](inner: => P[T]): P[Either[String, (Int, T)]] = P(
    (Index ~~ "#{" ~~/ inner).map(Right(_)) ~~ `}` |
      "#".!.map(Left(_)) |
      innerstring(CharsWhileIn("'`")).map(s => Left(s.mkString))
  )

  def istring0[T, _: P](inner: => P[T]): P[Seq[Either[String, (Int, T)]]] =
    P(`"` ~~ istringpart(inner).repX ~~ `"`)
  def istring[T, _: P](inner: => P[T]): P[Seq[Either[String, T]]] =
    istring0(inner).map(_.map(_.map(_._2)))

  /** Heredoc non-interpolated string */
  def heredocstring[_: P]: P[String] = {
    def heredocdelim: P[String] =
      P(`<<-` ~~/ ws ~~ plainIdentStr ~~/ (ws ~~ nl0).repX(1))

    def heredocstringline: P[(String, String)] =
      (!nl0 ~~ anychar).repX(1).! ~~ nl0.repX(1).!

    def unIndent(strings: Seq[(String, String)]): String = {
      if (strings.isEmpty)
        return ""

      val index = strings.foldLeft(Int.MaxValue) {
        case (acc, (str, _)) =>
          val index = str indexWhere { ch => ch != ' ' && ch != '\t' }

          if (index != -1) {
            index min acc
          } else {
            str.length min acc
          }

        case _ => 0
      }

      val b = new StringBuilder

      strings foreach { case (str, nl) =>
        b ++= str.substring(index)
        b ++= nl
      }

      b.result()
    }

    P(heredocdelim flatMap { delim =>
      ((!(ws ~~ KW(delim)) ~~ heredocstringline).repX ~~ ws ~~ delim./) map {
        unIndent(_)
      }
    })
  }

  def heredocistringpart[T, _: P](inner: => P[T]): P[Either[String, T]] =
    P(
      "#{" ~~/ inner.map(Right(_)) ~~ `}` |
        "#".!.map(Left(_)) |
        (!("#" | nl0) ~~ anychar).repX(1).!.map(Left(_))
    )

  /** Heredoc interpolated string */
  def heredocistring[T, _: P](inner: => P[T]): P[Seq[Either[String, T]]] = {
    def heredocdelim =
      P(`<<+` ~~/ ws ~~ plainIdentStr ~~/ (ws ~~ nl0).repX(1))

    def heredocistringline(inner: => P[T]): P[(Seq[Either[String, T]], String)] =
      heredocistringpart(inner).repX ~~ nl0.repX(1).!

    P(heredocdelim flatMap { delim =>
      ((!(ws ~~ KW(delim)) ~~ heredocistringline(inner)).repX ~~ ws ~~ delim./)
        .map { parts => if (parts.nonEmpty) unIndent(parts) else Seq.empty }
    })
  }

  private def unIndent[T](linesParts: Seq[(Seq[Either[String, T]], String)]) = {
    val index = linesParts.foldLeft(Int.MaxValue) {
      case (acc, (Left(str) :: _, _)) =>
        val index = str indexWhere { ch => ch != ' ' && ch != '\t' }

        if (index != -1) {
          index min acc
        } else {
          str.length min acc
        }

      case _ => 0
    }

    val b = Seq.newBuilder[Either[String, T]]

    linesParts foreach {
      case (Left(str) :: tail, nl) =>
        b += Left(str.substring(index))
        b ++= tail
        b += Left(nl)

      case (parts, nl) =>
        b ++= parts
        b += Left(nl)
    }

    b.result()
  }
}
