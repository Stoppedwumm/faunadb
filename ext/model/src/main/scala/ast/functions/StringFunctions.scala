package fauna.ast

import com.ibm.icu.lang.UCharacter
import com.ibm.icu.text.BreakIterator
import com.ibm.icu.text.Normalizer2
import fauna.atoms.APIVersion
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.Effect
import fauna.model.RenderContext
import fauna.repo.query.Query
import java.io.StringReader
import java.lang.Character.offsetByCodePoints
import java.time.{ LocalDateTime, ZoneOffset }
import java.time.temporal.Temporal
import java.util.{ IllegalFormatException, UnknownFormatConversionException }
import java.util.regex.Pattern
import org.apache.lucene.analysis.ngram.NGramTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.annotation.unused
import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

object ConcatFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    strings: List[String],
    separator: Option[String],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query(Right(StringL(strings.mkString(separator getOrElse ""))))
}

sealed trait NormalizerType
case object NFKCCaseFold extends NormalizerType
case object NFC extends NormalizerType
case object NFD extends NormalizerType
case object NFKC extends NormalizerType
case object NFKD extends NormalizerType

object CaseFoldFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    terms: List[ScalarL],
    normalizer: Option[NormalizerType],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {

    val n = normalizer match {
      case None               => Normalizer2.getNFKCCasefoldInstance()
      case Some(NFKCCaseFold) => Normalizer2.getNFKCCasefoldInstance()
      case Some(NFC)          => Normalizer2.getNFCInstance()
      case Some(NFD)          => Normalizer2.getNFDInstance()
      case Some(NFKC)         => Normalizer2.getNFKCInstance()
      case Some(NFKD)         => Normalizer2.getNFKDInstance()
    }

    val transformed = terms map {
      case StringL(str) => StringL(n.normalize(str))
      case term         => term
    }

    transformed match {
      case List(str) => Query(Right(str))
      case list      => Query(Right(ArrayL(list)))
    }
  }
}

object RegexEscapeFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[StringL]] =
    Query.value(Right(StringL(Pattern.quote(value))))
}

object StartsWithFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    search: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[BoolL]] =
    Query.value(Right(BoolL(value startsWith search)))
}

object EndsWithFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    search: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[BoolL]] =
    Query.value(Right(BoolL(value endsWith search)))
}

object ContainsStrFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    search: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[BoolL]] =
    Query.value(Right(BoolL(value contains search)))
}

object ContainsStrRegexFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    regex: Regex,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[BoolL]] =
    Query.value(Right(BoolL(regex.pattern.matcher(value).find)))
}

object FindStrFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    find: String,
    start: Option[Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {

    val offset = start match {
      case Some(s) if (s > 0 && s < value.length) =>
        offsetByCodePoints(value, 0, s.toInt)
      case _ => 0
    }

    val index = value.indexOf(find, offset)
    if (index > 0) {
      Query(Right(LongL(value.codePointCount(0, index))))
    } else {
      Query(Right(LongL(index)))
    }
  }
}

object FindStrRegexFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    regex: Regex,
    start: Option[Long],
    numResults: Option[Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    // The count is to protect the server against dumb users who might do
    //  a search which returns a million results, match paginate limit
    val count = (numResults.getOrElse(1024L) min 1024).toInt

    val (str, cpoff) = start match {
      case Some(s) if (s > 0 && s < value.length) =>
        val off = offsetByCodePoints(value, 0, s.toInt)
        (value.substring(off), s.toInt)
      case _ => (value, 0)
    }

    collectResults(str, regex.findAllIn(str), cpoff, count)
  }

  def collectResults(
    value: String,
    mi: MatchIterator,
    offset: Int,
    numResults: Int) = {
    var cnt = numResults
    val es = List.newBuilder[ObjectL]

    while (mi.hasNext && cnt > 0) {
      val data = mi.next()
      cnt -= 1
      val end = if (mi.start != mi.end) {
        mi.end - 1
      } else {
        mi.end
      }

      es += ObjectL(
        "start" -> LongL(offset + value.codePointCount(0, mi.start)),
        "end" -> LongL(offset + value.codePointCount(0, end)),
        "data" -> StringL(data))
    }
    Query(Right(ArrayL(es.result())))
  }
}

object LengthStringFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(Right(LongL(value.codePoints.count)))
  }
}

object LowerCaseFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(Right(StringL(UCharacter.toLowerCase(value))))
  }
}

object LTrimFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query.value(Right(StringL(value.stripLeading())))
}

object NGramFunction extends QFunction {
  val effect = Effect.Pure

  // XXX: these defaults are very small - 1 & 2.
  private[this] val DefaultMin = NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE
  private[this] val DefaultMax = NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE

  // we chose 1 here as a reasonable starting point based on the
  // experience of the elasticsearch team; as we gain our own
  // experience with ngram indexes, this value may change.
  private[this] val MaxDiff = 1

  def apply(
    terms: List[ScalarL],
    minimum: Option[Long],
    maximum: Option[Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {

    val es = List.newBuilder[EvalError]

    (minimum, maximum) match {
      case (Some(min), Some(max)) =>
        if (min > max || min <= 0) {
          es += BoundsError("min ngram size", "> 0 and <= max", pos)
        }

        if (max <= 0) {
          es += BoundsError("max ngram size", "> 0", pos)
        }

        if (max - min > MaxDiff) {
          es += BoundsError("max ngram size", s"<= min + $MaxDiff", pos)
        }
      case (Some(min), None) =>
        if (min > DefaultMax || min <= 0) {
          es += BoundsError("min ngram size", "> 0 and <= max", pos)
        }

        if (DefaultMax - min > MaxDiff) {
          es += BoundsError("min ngram size", s">= max - $MaxDiff", pos)
        }
      case (None, Some(max)) =>
        if (max <= 0 || max <= DefaultMin) {
          es += BoundsError("max ngram size", "> 0 and >= min", pos)
        }

        if (max - DefaultMin > MaxDiff) {
          es += BoundsError("max ngram size", s"<= min + $MaxDiff", pos)
        }

      case (None, None) => ()
    }

    val errs = es.result()

    if (errs.nonEmpty) {
      Query.value(Left(errs))
    } else {
      val ngrams = terms flatMap {
        case StringL(str) =>
          val b = Seq.newBuilder[StringL]
          val tokenizer =
            new NGramTokenizer(
              minimum map { _.toInt } getOrElse DefaultMin,
              maximum map { _.toInt } getOrElse DefaultMax)
          tokenizer.setReader(new StringReader(str))
          val attr = tokenizer.addAttribute(classOf[CharTermAttribute])

          try {
            tokenizer.reset()

            while (tokenizer.incrementToken) {
              b += StringL(attr.toString)
            }

            tokenizer.end()

            b.result()
          } finally {
            tokenizer.close()
          }

        case term => List(term)
      }

      Query.value(Right(ArrayL(ngrams)))
    }
  }
}

object RepeatStringFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    number: Option[Long],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    val n = (number.getOrElse(2L) min 1024).toInt
    Query(Right(StringL(value * n)))
  }
}

object ReplaceStringFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    find: String,
    replace: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {

    Query(Right(StringL(value.replace(find, replace))))
  }
}

object ReplaceStrRegexFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    regex: Regex,
    replace: String,
    firstOnly: Option[Boolean],
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {

    val matcher = regex.pattern.matcher(value)

    val rv = if (firstOnly.getOrElse(false)) {
      matcher.replaceFirst(replace)
    } else {
      matcher.replaceAll(replace)
    }

    Query(Right(StringL(rv)))
  }
}

object RTrimFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query.value(Right(StringL(value.stripTrailing())))
}

object SpaceFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    repeat: Long,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(
      Right(
        StringL(
          if (repeat > 0) {
            " " * (repeat min 1024).toInt
          } else {
            ""
          }
        )))
  }
}

/* The SubString function takes in codepoints and converts to character offsets to
 * preform a native substring function. */
object SubStringFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    start: Option[Long],
    length: Option[Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val codePointLength = value.codePoints.count.toInt

    // start position in codepoints
    val spos = (start: @unchecked) match {
      case Some(s) if (s >= 0) => s toInt
      case Some(s) if (s < 0)  => (codePointLength + s) toInt
      case None                => 0
    }

    val epos = (length: @unchecked) match {
      case Some(0)            => spos
      case Some(l) if (l > 0) => ((spos + l) min codePointLength).toInt
      case Some(l) if (l < 0) => -1
      case None               => codePointLength
    }

    Query {
      if (spos > codePointLength || spos < 0) {
        Left(
          List(BoundsError("start", "less than or equal to the string length", pos)))
      } else if (epos < 0 && epos < spos) {
        Left(List(BoundsError("length", "positive", pos)))
      } else {
        val startIndex = offsetByCodePoints(value, 0, spos)
        val endIndex = offsetByCodePoints(value, 0, epos)

        Right(StringL(value.substring(startIndex, endIndex)))
      }
    }
  }
}

object TitleCaseFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(
      Right(
        StringL(UCharacter.toTitleCase(value, BreakIterator.getWordInstance))
      )
    )
  }
}

object TrimFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    Query.value(Right(StringL(value.strip())))
}

object UpperCaseFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {
    Query(Right(StringL(UCharacter.toUpperCase(value))))
  }
}

object FormatFunction extends QFunction {
  override def effect: Effect = Effect.Pure

  sealed trait FormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]]
  }

  trait IndexedFormatString extends FormatString {
    val index: Int
    val format: String
  }

  case class RawStringFormat(string: String) extends FormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      Query(Right(string))
  }

  case class DateTimeFormat(index: Int, suffix: String, format: String)
      extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      value match {
        case DateL(dt) => printDate(dt, pos)
        case TimeL(ts) => printTime(ts, pos)
        case _ =>
          Query(
            Left(
              List(InvalidArgument(List(Type.Date, Type.Time), value.rtype, pos))))
      }

    private def printDate(dt: Temporal, pos: Position): Query[R[String]] =
      suffix match {
        case null                  => Query(Right(dt.toString)) // non-standard
        case "b" | "B" | "h"       => Query(Right(format.format(dt)))
        case "a" | "A"             => Query(Right(format.format(dt)))
        case "C" | "y" | "Y"       => Query(Right(format.format(dt)))
        case "d" | "e" | "m" | "j" => Query(Right(format.format(dt)))
        case "D" | "F"             => Query(Right(format.format(dt)))
        case _ => Query(Left(List(InvalidDateTimeSuffixError(format, pos))))
      }

    private def printTime(ts: Timestamp, pos: Position): Query[R[String]] =
      suffix match {
        case null => Query(Right(ts.toString)) // non-standard
        case "H" | "I" | "k" | "l" =>
          Query(Right(format.format(ts.toInstant.atOffset(ZoneOffset.UTC))))
        case "M" | "S" | "L" =>
          Query(Right(format.format(ts.toInstant.atOffset(ZoneOffset.UTC))))
        case "N" =>
          Query(
            Right("%09d".format(ts.toInstant.getNano))
          ) // simulate jdk11 precision on jdk8
        case "p" =>
          Query(Right(format.format(ts.toInstant.atOffset(ZoneOffset.UTC))))
        case "z" | "Z" =>
          Query(Right(format.format(ts.toInstant.atOffset(ZoneOffset.UTC))))
        case "s" | "Q" =>
          Query(Right(format.format(ts.toInstant.atOffset(ZoneOffset.UTC))))
        case "R" | "T" | "r" | "c" =>
          Query(Right(format.format(ts.toInstant.atOffset(ZoneOffset.UTC))))
        case _ =>
          printDate(LocalDateTime.from(ts.toInstant.atOffset(ZoneOffset.UTC)), pos)
      }
  }

  case class LongFormat(index: Int, format: String) extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      value match {
        case LongL(v) => Query(Right(format.format(v)))
        case _ =>
          Query(Left(List(InvalidArgument(List(Type.Integer), value.rtype, pos))))
      }
  }

  case class DoubleFormat(index: Int, format: String) extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      value match {
        case DoubleL(v) => Query(Right(format.format(v)))
        case _ =>
          Query(Left(List(InvalidArgument(List(Type.Number), value.rtype, pos))))
      }
  }

  case class StrFormat(index: Int, format: String) extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      ToStringFunction(value, ec, pos) flatMapT {
        case StringL(v) => Query(Right(format.format(v)))
        case v => Query(Left(List(InvalidArgument(List(Type.String), v.rtype, pos))))
      }
  }

  case class BoolFormat(index: Int, format: String) extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      value match {
        case BoolL(v) => Query(Right(format.format(v)))
        case NullL    => Query(Right(format.format(null)))
        case _        => Query(Right(format.format(value)))
      }
  }

  // %@
  case class DebugFormat(index: Int, format: String) extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      RenderContext.render(
        ec.auth,
        APIVersion.Default,
        Timestamp.Epoch,
        value,
        false) map { bytes =>
        Right(bytes.toUTF8String)
      }
  }

  // %^
  case class FixedDebugFormat(index: Int, format: String)
      extends IndexedFormatString {
    def print(value: Literal, ec: EvalContext, pos: Position): Query[R[String]] =
      RenderContext.render(ec.auth, ec.apiVers, Timestamp.Epoch, value, false) map {
        bytes =>
          Right(bytes.toUTF8String)
      }
  }

  private val formatSpecifier =
    "%(\\d+\\$)?(([-#+ 0,(\\<]*)?(\\d+)?(\\.\\d+)?([tT])?([a-zA-Z%@\\^])?)"

  private val fsPattern = Pattern.compile(formatSpecifier)

  private def parse(fmt: String, pos: Position): Query[R[Seq[FormatString]]] = {
    Query {
      val len = fmt.length
      val builder = Seq.newBuilder[FormatString]
      val m = fsPattern.matcher(fmt)

      var i = 0
      while (i < len) {
        if (m.find(i)) {
          if (m.start() != i) {
            builder += RawStringFormat(fmt.substring(i, m.start()))
          }

          val indexStr = m.group(1)
          val originalStr = m.group(2)
          val dt = m.group(6)
          val conversion = m.group(7)
          val formatStr = s"%$originalStr"

          val index =
            if (indexStr != null) indexStr.substring(0, indexStr.length - 1).toInt
            else 0

          val format = if (dt != null) {
            DateTimeFormat(index, conversion, formatStr)
          } else {
            conversion match {
              case "f" | "e" | "E" | "g" | "G" => DoubleFormat(index, formatStr)
              case "d" | "x" | "X" | "o"       => LongFormat(index, formatStr)
              case "s" | "S"                   => StrFormat(index, formatStr)
              case "b" | "B"                   => BoolFormat(index, formatStr)
              case "%"                         => RawStringFormat("%")
              case "n" => RawStringFormat(System.lineSeparator())
              case "@" => DebugFormat(index, formatStr)
              case "^" => FixedDebugFormat(index, formatStr)
              case _   => throw new UnknownFormatConversionException(m.group())
            }
          }

          builder += format

          i = m.end()
        } else {
          builder += RawStringFormat(fmt.substring(i, len))

          i = len
        }
      }

      Right(builder.result())
    } recover { case ex: UnknownFormatConversionException =>
      Left(List(InvalidConversionFormatError(ex.getConversion, pos)))
    }
  }

  private def format(
    parsed: Seq[FormatString],
    objects: List[Literal],
    ec: EvalContext,
    pos: Position): Query[R[List[String]]] = {
    var last = 0
    parsed map {
      case f: RawStringFormat =>
        f.print(NullL, ec, pos)

      case f: IndexedFormatString =>
        val printQ = if (f.index == 0) {
          val index = last
          last += 1

          if (index >= objects.size) {
            Query(
              Left(
                List(
                  BoundsError(
                    "format specifiers count",
                    "<= arguments size",
                    pos at "format"))))
          } else {
            f.print(objects(index), ec, pos at "values" at index)
          }
        } else {
          val index = f.index - 1

          if (index >= objects.size) {
            Query(Left(List(
              BoundsError("index", ">= 1 and <= arguments size", pos at "format"))))
          } else {
            f.print(objects(index), ec, pos at "values" at index)
          }
        }

        printQ recover { case _: IllegalFormatException =>
          Left(List(InvalidConversionFormatError(f.format, pos at "format")))
        }
    } sequenceT
  }

  def apply(
    fmt: String,
    objects: List[Literal],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    parse(fmt, pos at "format") flatMapT { parsed =>
      format(parsed, objects, ec, pos) mapT { strs => StringL(strs.mkString) }
    }
  }
}

abstract class SplitStrCommon {

  private def validateCount(count: Option[Long], pos: Position): R[Option[Int]] =
    count match {
      case Some(l) if l <= 0L =>
        Left(List(BoundsError("count", "count > 0", pos at "count")))
      case Some(l) if l > 1024L =>
        Left(List(BoundsError("count", "count <= 1024", pos at "count")))
      case _ =>
        Right(count.map { _.toInt })
    }

  def query(
    value: String,
    pattern: Pattern,
    count: Option[Long],
    pos: Position): Query[R[Literal]] =
    Query {
      validateCount(count, pos).map { limit =>
        ArrayL(
          pattern
            .split(
              value,
              limit.getOrElse(-1) // -1 means trailing empty Strings are kept
            )
            .iterator
            .map(StringL)
            .toList)
      }
    }
}

object SplitStrFunction extends SplitStrCommon with QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    token: String,
    count: Option[Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val pattern = Pattern.compile(Pattern.quote(token))
    query(value, pattern, count, pos)
  }
}

object SplitStrRegexFunction extends SplitStrCommon with QFunction {
  val effect = Effect.Pure

  def apply(
    value: String,
    pattern: Regex,
    count: Option[Long],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    query(value, pattern.pattern, count, pos)
}
