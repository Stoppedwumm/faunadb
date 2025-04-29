package fauna.model.runtime.fql2.stdlib

import com.ibm.icu.text.Normalizer2
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.ToString._
import fauna.repo.query.Query
import fauna.repo.values._
import fql.ast.Span
import java.lang.{ Double => JDouble, Long => JLong }
import java.util.regex.PatternSyntaxException
import scala.collection.immutable.ArraySeq
import scala.util.matching.Regex

object StringCompanion extends CompanionObject("String") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Str => true
    case _            => false
  }
}

object StringPrototype extends Prototype(TypeTag.Str, isPersistable = true) {
  import FieldTable.R

  // The maximum allowed length of a string.
  // It's set to the transaction size limit because longer strings
  // can't be written to the database.
  // See transaction_max_size_bytes in CoreConfig.scala.
  val MaxSize = 16 * 1024 * 1024

  private def maxExceeded(size: Int, stackTrace: FQLInterpreter.StackTrace) =
    QueryRuntimeFailure.ValueTooLarge(
      s"string size $size exceeds limit $MaxSize",
      stackTrace)

  defOp("+" -> tt.Str)("other" -> tt.Any) { (ctx, self, other) =>
    other.value.toDisplayString(ctx).flatMap { str =>
      val resLen = self.value.size + str.size
      if (resLen > MaxSize) {
        maxExceeded(resLen, ctx.stackTrace).toQuery
      } else {
        Value.Str(self.value.concat(str)).toQuery
      }
    }
  }

  defAccess(tt.Str)("index" -> tt.Int) { (ctx, self, idx, _) =>
    at(self, idx, ctx.stackTrace).map(R.Val(_)).toQuery
  }

  defMethod("at" -> tt.Str)("index" -> tt.Int) { (ctx, self, idx) =>
    at(self, idx, ctx.stackTrace).toQuery
  }

  defMethod("concat" -> tt.Str)("other" -> tt.Str) { (ctx, self, other) =>
    val resLen = self.value.size + other.value.size
    if (resLen > MaxSize) {
      maxExceeded(resLen, ctx.stackTrace).toQuery
    } else {
      Value.Str(self.value.concat(other.value)).toQuery
    }
  }

  defMethod("endsWith" -> tt.Boolean)("suffix" -> tt.Str) { (_, self, other) =>
    Value.Boolean(self.value.endsWith(other.value)).toQuery
  }

  defMethod("includes" -> tt.Boolean)("pattern" -> tt.Str) { (_, self, other) =>
    Value.Boolean(self.value.contains(other.value)).toQuery
  }
  defMethod("includesRegex" -> tt.Boolean)("regex" -> tt.Str) {
    (ctx, self, pattern) =>
      parseRegex(pattern, ctx.stackTrace).map { regex =>
        Value.Boolean(regex.findFirstMatchIn(self.value).isDefined)
      }.toQuery
  }

  defMethod("insert" -> tt.Str)("index" -> tt.Int, "other" -> tt.Str) {
    (ctx, self, index, str) =>
      codepointToIndexInclusive(self, index, ctx.stackTrace).flatMap { charIndex =>
        val left = self.value.slice(0, charIndex)
        val right = self.value.slice(charIndex, self.value.length)
        val resLen = left.size + str.value.size + right.size
        if (resLen > MaxSize) {
          maxExceeded(resLen, ctx.stackTrace).toResult
        } else {
          Value.Str(left + str.value + right).toResult
        }
      }.toQuery
  }

  defMethod("indexOf" -> tt.Optional(tt.Int))("pattern" -> tt.Str) {
    (ctx, self, other) =>
      self.value.indexOf(other.value) match {
        case charIndex if charIndex >= 0 =>
          indexToCodepoint(self, charIndex).toQuery
        case _ =>
          Value.Null
            .noSuchElement("no element found", ctx.stackTrace.currentStackFrame)
            .toQuery
      }
  }
  defMethod("indexOf" -> tt.Optional(tt.Int))(
    "pattern" -> tt.Str,
    "start" -> tt.Int) { (ctx, self, other, start) =>
    codepointToIndex(self, start, ctx.stackTrace).map { startCharIndex =>
      self.value.indexOf(other.value, startCharIndex) match {
        case charIndex if charIndex >= 0 => indexToCodepoint(self, charIndex)
        case _ =>
          Value.Null.noSuchElement(
            "no element found",
            ctx.stackTrace.currentStackFrame)
      }
    }.toQuery
  }

  defMethod("indexOfRegex" -> tt.Optional(tt.Int))("regex" -> tt.Str) {
    (ctx, self, pattern) =>
      parseRegex(pattern, ctx.stackTrace).map { regex =>
        regex.findFirstMatchIn(self.value) match {
          case Some(m) => indexToCodepoint(self, m.start)
          case _ =>
            Value.Null.noSuchElement(
              "no element found",
              ctx.stackTrace.currentStackFrame)
        }
      }.toQuery
  }
  defMethod("indexOfRegex" -> tt.Optional(tt.Int))(
    "regex" -> tt.Str,
    "start" -> tt.Int) { (ctx, self, pattern, start) =>
    codepointToIndex(self, start, ctx.stackTrace).flatMap { startCharIndex =>
      val slice = self.value.slice(startCharIndex, self.value.length)
      parseRegex(pattern, ctx.stackTrace).map { regex =>
        regex.findFirstMatchIn(slice) match {
          case Some(m) => indexToCodepoint(self, m.start + startCharIndex)
          case _ =>
            Value.Null.noSuchElement(
              "no element found",
              ctx.stackTrace.currentStackFrame)
        }
      }
    }.toQuery
  }

  defMethod("lastIndexOf" -> tt.Optional(tt.Int))("pattern" -> tt.Str) {
    (ctx, self, other) =>
      self.value.lastIndexOf(other.value) match {
        case charIndex if charIndex >= 0 =>
          indexToCodepoint(self, charIndex).toQuery
        case _ =>
          Value.Null
            .noSuchElement("no element found", ctx.stackTrace.currentStackFrame)
            .toQuery
      }
  }
  defMethod("lastIndexOf" -> tt.Optional(tt.Int))(
    "pattern" -> tt.Str,
    "end" -> tt.Int) { (ctx, self, other, end) =>
    codepointToIndex(self, end, ctx.stackTrace).map { endCharIndex =>
      self.value.lastIndexOf(other.value, endCharIndex) match {
        case charIndex if charIndex >= 0 => indexToCodepoint(self, charIndex)
        case _ =>
          Value.Null.noSuchElement(
            "no element found",
            ctx.stackTrace.currentStackFrame)
      }
    }.toQuery
  }

  defField("length" -> tt.Number) { (_, self) =>
    Query.value(Value.Int(codepointLength(self)))
  }

  defMethod("matches" -> tt.Array(tt.Str))("regex" -> tt.Str) {
    (ctx, self, pattern) =>
      matches0(self, pattern, ctx.stackTrace).map { arr =>
        Value.Array(arr.map { v => Value.Str(v._2) })
      }.toQuery
  }
  defMethod("matchIndexes" -> tt.Array(tt.Tuple(tt.Number, tt.Str)))(
    "regex" -> tt.Str) { (ctx, self, pattern) =>
    matches0(self, pattern, ctx.stackTrace).map { arr =>
      Value.Array(arr.map { case (index, v) =>
        Value.Array(indexToCodepoint(self, index), Value.Str(v))
      })
    }.toQuery
  }

  defMethod("replace" -> tt.Str)("pattern" -> tt.Str, "replacement" -> tt.Str) {
    (ctx, self, pattern, replacement) =>
      replace(
        self,
        pattern.value,
        replacement.value,
        Some(1),
        ctx.stackTrace).toQuery
  }
  defMethod("replace" -> tt.Str)(
    "pattern" -> tt.Str,
    "replacement" -> tt.Str,
    "amount" -> tt.Int) { (ctx, self, pattern, replacement, count) =>
    replace(
      self,
      pattern.value,
      replacement.value,
      Some(count.value),
      ctx.stackTrace).toQuery
  }
  defMethod("replaceRegex" -> tt.Str)("regex" -> tt.Str, "replacement" -> tt.Str) {
    (ctx, self, pattern, replacement) =>
      parseRegex(pattern, ctx.stackTrace).flatMap { regex =>
        replace(self, regex, replacement.value, Some(1), ctx.stackTrace)
      }.toQuery
  }
  defMethod("replaceRegex" -> tt.Str)(
    "regex" -> tt.Str,
    "replacement" -> tt.Str,
    "amount" -> tt.Int) { (ctx, self, pattern, replacement, count) =>
    parseRegex(pattern, ctx.stackTrace).flatMap { regex =>
      replace(self, regex, replacement.value, Some(count.value), ctx.stackTrace)
    }.toQuery
  }

  defMethod("replaceAll" -> tt.Str)("pattern" -> tt.Str, "replacement" -> tt.Str) {
    (ctx, self, pattern, replacement) =>
      replace(self, pattern.value, replacement.value, None, ctx.stackTrace).toQuery
  }
  defMethod("replaceAllRegex" -> tt.Str)(
    "pattern" -> tt.Str,
    "replacement" -> tt.Str) { (ctx, self, pattern, replacement) =>
    parseRegex(pattern, ctx.stackTrace).flatMap { regex =>
      replace(self, regex, replacement.value, None, ctx.stackTrace)
    }.toQuery
  }

  defMethod("startsWith" -> tt.Boolean)("prefix" -> tt.Str) { (_, self, other) =>
    Value.Boolean(self.value.startsWith(other.value)).toQuery
  }

  defMethod("slice" -> tt.Str)("start" -> tt.Int) { (ctx, self, from) =>
    codepointToIndex(self, from, ctx.stackTrace, checked = false).map {
      fromCharIndex =>
        Value.Str(self.value.slice(fromCharIndex, self.value.length))
    }.toQuery
  }
  defMethod("slice" -> tt.Str)("start" -> tt.Int, "end" -> tt.Int) {
    (ctx, self, from, until) =>
      // Slice is exclusive on the `end` argument, so this will handle surrogate
      // pairs correctly.
      codepointToIndex(self, from, ctx.stackTrace, checked = false).flatMap {
        startCharIndex =>
          codepointToIndex(self, until, ctx.stackTrace, checked = false).map {
            endCharIndex =>
              Value.Str(self.value.slice(startCharIndex, endCharIndex))
          }
      }.toQuery
  }

  defMethod("toLowerCase" -> tt.Str)() { (_, self) =>
    Value.Str(self.value.toLowerCase).toQuery
  }
  defMethod("toUpperCase" -> tt.Str)() { (_, self) =>
    Value.Str(self.value.toUpperCase).toQuery
  }
  defMethod("casefold" -> tt.Str)() { (_, self) =>
    val norm = Normalizer2.getNFKCCasefoldInstance()
    Value.Str(norm.normalize(self.value)).toQuery
  }
  defMethod("casefold" -> tt.Str)("format" -> tt.Str) { (ctx, self, format) =>
    Result.guardM {
      val norm = format match {
        case Value.Str("NFKCCaseFold") => Normalizer2.getNFKCCasefoldInstance()
        case Value.Str("NFC")          => Normalizer2.getNFCInstance()
        case Value.Str("NFD")          => Normalizer2.getNFDInstance()
        case Value.Str("NFKC")         => Normalizer2.getNFKCInstance()
        case Value.Str("NFKD")         => Normalizer2.getNFKDInstance()
        case _ =>
          Result.fail(
            QueryRuntimeFailure.InvalidArgument(
              "format",
              "expected a casefold format (one of NFKCCaseFold, NFC, NFD, NFKC, or NFKD)",
              ctx.stackTrace))
      }
      Value.Str(norm.normalize(self.value)).toQuery
    }
  }

  defMethod("parseNumber" -> tt.Optional(tt.Number))() { (ctx, self) =>
    parseNumber(self, ctx.stackTrace.currentStackFrame).toQuery
  }
  defMethod("parseDouble" -> tt.Optional(tt.Double))() { (ctx, self) =>
    parseDouble(self, ctx.stackTrace.currentStackFrame).toQuery
  }
  defMethod("parseLong" -> tt.Optional(tt.Long))() { (ctx, self) =>
    parseLong(self, 10, ctx.stackTrace.currentStackFrame).toQuery
  }
  defMethod("parseLong" -> tt.Optional(tt.Long))("radix" -> tt.Int) {
    (ctx, self, radix) =>
      checkRadix(radix.value, ctx.stackTrace).flatMap { _ =>
        Result.Ok(parseLong(self, radix.value, ctx.stackTrace.currentStackFrame))
      }.toQuery
  }
  defMethod("parseInt" -> tt.Optional(tt.Int))() { (ctx, self) =>
    parseInt(self, 10, ctx.stackTrace.currentStackFrame).toQuery
  }
  defMethod("parseInt" -> tt.Optional(tt.Int))("radix" -> tt.Int) {
    (ctx, self, radix) =>
      checkRadix(radix.value, ctx.stackTrace).flatMap { _ =>
        Result.Ok(parseInt(self, radix.value, ctx.stackTrace.currentStackFrame))
      }.toQuery
  }

  defMethod("split" -> tt.Array(tt.Str))("separator" -> tt.Str) {
    (_, self, separator) =>
      split(self, separator.value).toQuery
  }
  defMethod("splitRegex" -> tt.Array(tt.Str))("regex" -> tt.Str) {
    (ctx, self, separator) =>
      parseRegex(separator, ctx.stackTrace).map { regex =>
        split(self, regex)
      }.toQuery
  }
  defMethod("splitAt" -> tt.Tuple(tt.Str, tt.Str))("index" -> tt.Int) {
    (ctx, self, index) =>
      codepointToIndexInclusive(self, index, ctx.stackTrace).map { charIndex =>
        val left = Value.Str(self.value.slice(0, charIndex))
        val right = Value.Str(self.value.slice(charIndex, self.value.length))
        Value.Array(left, right)
      }.toQuery
  }

  private def parseRegex(
    pattern: Value.Str,
    stackTrace: FQLInterpreter.StackTrace): Result[Regex] = {
    try {
      Result.Ok(pattern.value.r)
    } catch {
      case err: PatternSyntaxException =>
        Result.Err(
          QueryRuntimeFailure.InvalidRegex(
            err.getDescription(),
            indexToCodepoint(pattern, err.getIndex()).value,
            stackTrace))
    }
  }

  private def matches0(
    self: Value.Str,
    pattern: Value.Str,
    stackTrace: FQLInterpreter.StackTrace): Result[ArraySeq[(Int, String)]] = {
    parseRegex(pattern, stackTrace).map {
      _.findAllMatchIn(self.value)
        .map { m => (m.start, m.matched) }
        .to(ArraySeq)
    }
  }

  private def checkRadix(radix: Int, stackTrace: FQLInterpreter.StackTrace) = {
    if (radix < 2 || radix > 36) {
      Result.Err(
        QueryRuntimeFailure.InvalidArgument(
          "format",
          "invalid radix, expected a number from 2-36",
          stackTrace))
    } else {
      Result.Ok(())
    }
  }

  private def parseNumber(self: Value.Str, span: Span): Value = {
    try {
      Value.Int(Integer.parseInt(self.value))
    } catch {
      case _: NumberFormatException =>
        try {
          Value.Long(JLong.parseLong(self.value))
        } catch {
          case _: NumberFormatException =>
            try {
              Value.Double(JDouble.parseDouble(self.value))
            } catch {
              case _: NumberFormatException =>
                Value.Null.invalidNumber(self.value, span)
            }
        }
    }
  }
  private def parseDouble(self: Value.Str, span: Span): Value = {
    try {
      Value.Double(JDouble.parseDouble(self.value))
    } catch {
      case _: NumberFormatException =>
        Value.Null.invalidNumber(self.value, span)
    }
  }
  private def parseLong(self: Value.Str, radix: Int, currentSpan: Span): Value = {
    try {
      Value.Long(JLong.parseLong(self.value, radix))
    } catch {
      case _: NumberFormatException =>
        Value.Null.invalidNumber(self.value, currentSpan)
    }
  }
  private def parseInt(self: Value.Str, radix: Int, currentSpan: Span): Value = {
    try {
      Value.Int(Integer.parseInt(self.value, radix))
    } catch {
      case _: NumberFormatException =>
        Value.Null.invalidNumber(self.value, currentSpan)
    }
  }

  /** Applies the given replacement `count` times. Note that a count of None will
    * replace as many times as possible (i.e. count = infinity).
    */
  private def replace0(
    self: Value.Str,
    matches: String => Option[(Int, Int)],
    replacement: String,
    count: Option[Int],
    stackTrace: FQLInterpreter.StackTrace): Result[Value.Str] = {
    if (count.exists(_ < 1)) {
      Result.Err(QueryRuntimeFailure
        .InvalidArgument("count", "must be greater than or equal to 1", stackTrace))
    } else {
      var prev = 0
      var remaining = count
      val sb = new StringBuilder
      Result.guard {
        while (remaining.map { _ > 0 }.getOrElse(true)) {
          matches(self.value.slice(prev, self.value.length)) match {
            case Some((start, end)) =>
              val resMinLen =
                sb.size + start + replacement.size
              if (resMinLen > MaxSize) {
                Result.fail(maxExceeded(resMinLen, stackTrace))
              }
              sb.append(self.value.slice(prev, prev + start))
              sb.append(replacement)
              prev += end
              remaining = remaining.map { _ - 1 }
            case None => remaining = Some(0)
          }
        }
        val resMinLen =
          sb.size + self.value.length - prev
        if (resMinLen > MaxSize) {
          Result.fail(maxExceeded(resMinLen, stackTrace))
        }
        sb.append(self.value.slice(prev, self.value.length))
        Result.Ok(Value.Str(sb.result()))
      }
    }
  }
  private def replace(
    self: Value.Str,
    pattern: String,
    replacement: String,
    count: Option[Int],
    stackTrace: FQLInterpreter.StackTrace): Result[Value.Str] =
    replace0(
      self,
      section => {
        val idx = section.indexOf(pattern)
        if (idx < 0) {
          None
        } else {
          Some((idx, idx + pattern.length))
        }
      },
      replacement,
      count,
      stackTrace)
  private def replace(
    self: Value.Str,
    pattern: Regex,
    replacement: String,
    count: Option[Int],
    stackTrace: FQLInterpreter.StackTrace): Result[Value.Str] =
    replace0(
      self,
      section => pattern.findFirstMatchIn(section).map { m => (m.start, m.end) },
      replacement,
      count,
      stackTrace)

  // java's split is wrong:
  // "f b ".split(" ") -> ["f", "b"]      (!!)
  // " f b".split(" ") -> ["", "f", "b"]
  // " ".split(" ") -> []
  // instead, we will follow javascripts impl:
  // "f b ".split(" ") -> ["f", "b", ""]
  // " f b".split(" ") -> ["", "f", "b"]
  // " ".split(" ") -> ["", ""]
  private def split0(
    self: Value.Str,
    matches: String => Option[(Int, Int)]): Value.Array = {
    var prev = 0
    var done = false
    val results = ArraySeq.newBuilder[Value.Str]
    while (!done && prev < self.value.length) {
      matches(self.value.slice(prev, self.value.length)) match {
        case Some((start, end)) =>
          // If the matcher matches zero characters, then we need to advance one
          // character.
          if (start == end) {
            results += Value.Str(self.value.slice(prev, prev + 1))
            prev += 1
            // avoids a trailing empty string
            if (prev == self.value.length - 1) {
              done = true
            }
          } else {
            results += Value.Str(self.value.slice(prev, prev + start))
            prev += end
          }
        case None => done = true
      }
    }
    results += Value.Str(self.value.slice(prev, self.value.length))
    Value.Array(results.result())
  }

  private def split(self: Value.Str, separator: String): Value.Array =
    split0(
      self,
      section => {
        val idx = section.indexOf(separator)
        if (idx == -1) {
          None
        } else {
          Some((idx, idx + separator.length))
        }
      })
  private def split(self: Value.Str, separator: Regex): Value.Array =
    split0(
      self,
      section => separator.findFirstMatchIn(section).map { m => (m.start, m.end) })

  private def at(
    self: Value.Str,
    idx: Value.Int,
    stackTrace: FQLInterpreter.StackTrace) =
    codepointToIndex(self, idx, stackTrace).map { charIndex =>
      val codepoint = self.value.codePointAt(charIndex)
      Value.Str(Character.toString(codepoint))
    }

  // This converts the given _codepoint_ index, to a _char_ index (or a failure),
  // which is the start of the given character.
  //
  // This fail if char index >= length.
  private def codepointToIndex(
    self: Value.Str,
    idx: Value.Int,
    stackTrace: FQLInterpreter.StackTrace,
    checked: Boolean = true): Result[Int] = {
    def err() = Result.Err(
      QueryRuntimeFailure
        .IndexOutOfBounds(idx.value, codepointLength(self), stackTrace))
    if (idx.value >= self.value.length || idx.value < 0) {
      if (checked) {
        err()
      } else {
        Result.Ok(idx.value)
      }
    } else {
      codepointToIndexInclusive(self, idx, stackTrace) match {
        // Because we checked for idx < 0 above, the only error can be if idx >
        // length, in which case we just round to self.value.length
        case Result.Err(_) if !checked => Result.Ok(self.value.length)
        case res if !checked           => res

        case Result.Ok(charIndex) if charIndex == self.value.length => err()
        case res                                                    => res
      }
    }
  }

  // This converts the given _codepoint_ index, to a _char_ index (or a failure),
  // which is the start of the given character.
  //
  // This will only fail if char index > length.
  private def codepointToIndexInclusive(
    self: Value.Str,
    idx: Value.Int,
    stackTrace: FQLInterpreter.StackTrace): Result[Int] = {
    def err() = Result.Err(
      QueryRuntimeFailure
        .IndexOutOfBounds(idx.value, codepointLength(self), stackTrace))
    if (idx.value > self.value.length || idx.value < 0) {
      err()
    } else {
      try {
        val charIndex = self.value.offsetByCodePoints(0, idx.value)
        // offsetByCodePoints will error if the number of codepoints in `self` is
        // _fewer_ than the `idx`. In the case where there are the same number of
        // codepoints in `self` as `idx`, this is valid.
        Result.Ok(charIndex)
      } catch {
        case _: IndexOutOfBoundsException =>
          // This can only happen in a case like so:
          // "f😍😍"
          //  ^ ^ ^
          //  1 2 2
          // - str.length: 5
          // - codepointLength: 3
          // - given index is 3 or 4
          err()
      }
    }
  }

  // This converts the given _char_ index to a _codepoint_ index. This will fail
  // if the char index is out of bounds.
  private def indexToCodepoint(self: Value.Str, charIndex: Int): Value.Int =
    Value.Int(self.value.codePointCount(0, charIndex))

  // Returns the length of the string in _codepoints_ (not chars, which is what
  // .length does)
  private def codepointLength(self: Value.Str) =
    self.value.codePointCount(0, self.value.length)
}
