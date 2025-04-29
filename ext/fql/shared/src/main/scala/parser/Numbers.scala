package fql.parser

import fastparse._
import fql.ast._

// fastparse macro expansion introduces this unused `charIn` var in many places.
// ignore resulting warnings.
@annotation.nowarn("msg=pattern var charIn*")
// FIXME: fastparse style uses top-level wildcards, however scala 2.12.8+
// warns about them. Need to fix this at some point.
@annotation.nowarn("msg=Top-level wildcard*")
/** Utility parsers. */
trait Numbers { parser: Parser =>
  private def toIntLit(str: String, radix: Int) =
    Literal.Int(BigInt(str.replace("_", ""), radix))
  private def toNumLit(in: (String, Option[String])) = in match {
    case (i, None) => toIntLit(i, 10)
    case (i, Some(fp)) =>
      Literal.Float(BigDecimal(i.replace("_", "") + fp.replace("_", "")))
  }
  private def negateLit(in: (Option[String], Literal)) = in match {
    case (Some("-"), Literal.Int(n))   => Literal.Int(-n)
    case (Some("-"), Literal.Float(n)) => Literal.Float(-n)
    case (_, n)                        => n
  }

  def nonzerodigit[_: P] = P(CharIn("1-9"))
  def digit[_: P] = P(CharIn("0-9"))
  def hexdigit[_: P] = P(CharIn("0-9a-fA-F")).opaque("hex digit")
  def digits[_: P] = P(CharsWhileIn("0-9_"))
  def exponent[_: P] = P(CharIn("eE") ~~/ CharIn("+\\-").!.? ~~ digit ~~ digits.?)
  def decimalinteger[_: P] = P("0" | nonzerodigit ~~ digits.?)
  def altinteger[_: P] = P(
    "0" ~~ (
      CharIn("xX") ~~/ CharsWhileIn("0-9a-fA-F_").!.map(toIntLit(_, 16)) |
        CharIn("oO") ~~/ CharsWhileIn("0-7_").!.map(toIntLit(_, 8)) |
        CharIn("bB") ~~/ CharsWhileIn("01_").!.map(toIntLit(_, 2))
    )
  )
  def unsignedInteger[_: P] = P(
    altinteger |
      decimalinteger.!.map(toIntLit(_, 10))
  )
  def integer[_: P] = P(CharIn("+\\-").!.? ~~ ws ~~ unsignedInteger).map(negateLit)

  def unsignedNumber[_: P] = P(
    altinteger |
      (decimalinteger.! ~~/ (
        (".".opaque("`.`") ~~ P(digit, "digit") ~~/ digits.? ~~ exponent.?).!.map(
          Some(_)) |
          exponent.!.map(Some(_)) |
          Pass.map(_ => None)
      )).map(toNumLit)
  )
  def number[_: P] = P(CharIn("+\\-").!.? ~~ ws ~~ unsignedNumber).map(negateLit)
}
