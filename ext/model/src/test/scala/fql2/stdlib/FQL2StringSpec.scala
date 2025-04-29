package fauna.model.test

import fauna.model.runtime.fql2._
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import fql.error.TypeError

class FQL2StringSpec extends FQL2StdlibHelperSpec("String", stdlib.StringPrototype) {
  val auth = newDB

  def span(start: Int, end: Int) = Span(start, end, Src.Query(""))

  "+" - {
    // FIXME: What should this signature be?
    testSig("+(other: Any) => String")

    "concatenates another string" in {
      checkOk(auth, "'foo' + 'bar'", Value.Str("foobar"))
      checkOk(auth, "'foo' + ''", Value.Str("foo"))
      checkOk(auth, "'' + 'bar'", Value.Str("bar"))
    }

    "concatenates any other value" in {
      checkErr(
        auth,
        "'foo' + 3",
        QueryCheckFailure(
          Seq(TypeError("Type `Int` is not a subtype of `String`", span(8, 9)))))
      checkErr(
        auth,
        "'foo' + true",
        QueryCheckFailure(
          Seq(
            TypeError(
              "Type `Boolean` is not a subtype of `String | Number`",
              span(8, 12)))))
      checkErr(
        auth,
        "'foo' + 2.5",
        QueryCheckFailure(
          Seq(TypeError("Type `Double` is not a subtype of `String`", span(8, 11)))))
      checkErr(
        auth,
        "'foo' + Date('1970-01-01')",
        QueryCheckFailure(
          Seq(
            TypeError(
              "Type `Date` is not a subtype of `String | Number`",
              span(8, 26)))))

      evalOk(auth, "'foo' + 3", typecheck = false) shouldBe Value.Str("foo3")
      evalOk(auth, "'foo' + true", typecheck = false) shouldBe Value.Str("footrue")
      evalOk(auth, "'foo' + 2.5", typecheck = false) shouldBe Value.Str("foo2.5")
      evalOk(auth, "'foo' + Date('1970-01-01')", typecheck = false) shouldBe Value
        .Str("foo1970-01-01")
    }

    "limits the size of the result" in {
      val limit = stdlib.StringPrototype.MaxSize
      val s = "a" * (limit / 2)
      checkErr(
        auth,
        s"""'$s' + '$s' + 'a'""",
        QueryRuntimeFailure.ValueTooLarge(
          s"string size ${limit + 1} exceeds limit $limit",
          FQLInterpreter.StackTrace(Seq(Span(16777226, 16777229, Src.Query("")))))
      )
    }
  }

  "[]" - {
    testSig("[](index: Number) => String")
  }

  "at" - {
    testSig("at(index: Number) => String")

    "selects the character at the given index" in {
      checkOk(auth, "'foo'[0]", Value.Str("f"))
      checkOk(auth, "'foo'[1]", Value.Str("o"))
      checkOk(auth, "'foo'[2]", Value.Str("o"))

      checkOk(auth, "'foo'.at(0)", Value.Str("f"))
      checkOk(auth, "'foo'.at(1)", Value.Str("o"))
      checkOk(auth, "'foo'.at(2)", Value.Str("o"))
    }

    "selects surrogate pairs correctly" in {
      checkOk(auth, "'fo😍o'[0]", Value.Str("f"))
      checkOk(auth, "'fo😍o'[1]", Value.Str("o"))
      checkOk(auth, "'fo😍o'[2]", Value.Str("😍"))
      checkOk(auth, "'fo😍o'[3]", Value.Str("o"))
      checkErr(
        auth,
        "'fo😍o'[4]",
        QueryRuntimeFailure(
          "index_out_of_bounds",
          "index 4 out of bounds for length 4",
          FQLInterpreter.StackTrace(Seq(Span(7, 10, Src.Query("")))))
      )
      checkErr(
        auth,
        "'fo😍o'[5]",
        QueryRuntimeFailure(
          "index_out_of_bounds",
          "index 5 out of bounds for length 4",
          FQLInterpreter.StackTrace(Seq(Span(7, 10, Src.Query("")))))
      )
      checkErr(
        auth,
        "'fo😍😍'[5]",
        QueryRuntimeFailure(
          "index_out_of_bounds",
          "index 5 out of bounds for length 4",
          FQLInterpreter.StackTrace(Seq(Span(8, 11, Src.Query("")))))
      )
    }

    "disallows negative indexes" in {
      checkErr(
        auth,
        "'fo😍o'[-1]",
        QueryRuntimeFailure(
          "index_out_of_bounds",
          "index -1 out of bounds for length 4",
          FQLInterpreter.StackTrace(Seq(Span(7, 11, Src.Query("")))))
      )
    }
  }

  "concat" - {
    testSig("concat(other: String) => String")

    "concatenates another string" in {
      checkOk(auth, "'foo'.concat('bar')", Value.Str("foobar"))
      checkOk(auth, "'foo'.concat('')", Value.Str("foo"))
      checkOk(auth, "''.concat('bar')", Value.Str("bar"))
    }

    "limits the size of the result" in {
      checkErr(
        auth,
        // The original query from the bug report.
        s"""|let str = "Lorem ipsum dolor sit amet,Lorem ipsum dolor sit amet";
            |[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0].fold(
            |  str, (acc, val) => acc.concat(acc)
            |)""".stripMargin,
        QueryRuntimeFailure(
          "value_too_large",
          "Value too large: string size 27787264 exceeds limit 16777216.",
          FQLInterpreter.StackTrace(
            List(Span(158, 163, Src.Query("")), Span(125, 165, Src.Query(""))))
        )
      )
    }
  }

  "casefold" - {
    testSig("casefold() => String", "casefold(format: String) => String")

    "converts an ascii string to lowercase" in {
      checkOk(auth, "'FQL-X'.casefold()", Value.Str("fql-x"))
      checkOk(auth, "'foobar'.casefold()", Value.Str("foobar"))
    }
    "converts non-ascii characters to lowercase" in {
      // İstanbul. Note the double `\\` will have the FQLX parser handle this,
      // which is good to test.
      checkOk(auth, "'\\u0130stanbul'.casefold()", Value.Str("i̇stanbul"))

      checkOk(auth, "'John Doe'.casefold()", Value.Str("john doe"))
      checkOk(auth, "'İstanbul'.casefold()", Value.Str("i̇stanbul"))
      checkOk(auth, "'Diyarbakır'.casefold()", Value.Str("diyarbakır"))
      checkOk(auth, "'φιλόσοφος'.casefold()", Value.Str("φιλόσοφοσ"))
      checkOk(auth, "'Weißkopfseeadler'.casefold()", Value.Str("weisskopfseeadler"))
    }

    "allows a format specifier" in {
      checkOk(auth, "'foo'.casefold('NFKCCaseFold')", Value.Str("foo"))
      checkOk(auth, "'foo'.casefold('NFC')", Value.Str("foo"))
      checkOk(auth, "'foo'.casefold('NFD')", Value.Str("foo"))
      checkOk(auth, "'foo'.casefold('NFKC')", Value.Str("foo"))
      checkOk(auth, "'foo'.casefold('NFKD')", Value.Str("foo"))

      checkErr(
        auth,
        "'foo'.casefold('wrong')",
        QueryRuntimeFailure.InvalidArgument(
          "format",
          "expected a casefold format (one of NFKCCaseFold, NFC, NFD, NFKC, or NFKD)",
          FQLInterpreter.StackTrace(Seq(Span(14, 23, Src.Query("")))))
      )
    }
  }

  "endsWith" - {
    testSig("endsWith(suffix: String) => Boolean")

    "checks if string ends with another string" in {
      checkOk(auth, "'foo'.endsWith('o')", Value.True)
      checkOk(auth, "'foo'.endsWith('b')", Value.False)
    }

    "strings always start with themselves" in {
      checkOk(auth, "'foo'.endsWith('foo')", Value.True)
    }

    "longer strings will not work" in {
      checkOk(auth, "'foo'.endsWith('barfoo')", Value.False)
    }
  }

  "insert" - {
    testSig("insert(index: Number, other: String) => String")

    "inserts the given string into the source string" in {
      checkOk(auth, "'foo'.insert(0, 'bar')", Value.Str("barfoo"))
      checkOk(auth, "'foo'.insert(1, 'bar')", Value.Str("fbaroo"))
      checkOk(auth, "'foo'.insert(2, 'bar')", Value.Str("fobaro"))
      checkOk(auth, "'foo'.insert(3, 'bar')", Value.Str("foobar"))
    }

    "handles surrogate pairs correctly" in {
      checkOk(auth, "'f😍o'.insert(0, 'bar')", Value.Str("barf😍o"))
      checkOk(auth, "'f😍o'.insert(1, 'bar')", Value.Str("fbar😍o"))
      checkOk(auth, "'f😍o'.insert(2, 'bar')", Value.Str("f😍baro"))
      checkOk(auth, "'f😍o'.insert(3, 'bar')", Value.Str("f😍obar"))
    }

    "errors for an index larger than the length of the string" in {
      checkErr(
        auth,
        "'foo'.insert(4, 'bar')",
        QueryRuntimeFailure
          .IndexOutOfBounds(4, 3, FQLInterpreter.StackTrace(Seq(span(12, 22)))))
      checkErr(
        auth,
        "'f😍o'.insert(4, 'bar')",
        QueryRuntimeFailure
          .IndexOutOfBounds(4, 3, FQLInterpreter.StackTrace(Seq(span(13, 23)))))
      checkErr(
        auth,
        "'f😍😍'.insert(4, 'bar')",
        QueryRuntimeFailure
          .IndexOutOfBounds(4, 3, FQLInterpreter.StackTrace(Seq(span(14, 24)))))
    }

    "errors for negative indices" in {
      checkErr(
        auth,
        "'foo'.insert(-1, 'bar')",
        QueryRuntimeFailure
          .IndexOutOfBounds(-1, 3, FQLInterpreter.StackTrace(Seq(span(12, 23)))))
    }

    "limits the size of the result" in {
      val limit = stdlib.StringPrototype.MaxSize
      val s = "a" * limit
      checkErr(
        auth,
        s"""'$s'.insert(0, 'a')""",
        QueryRuntimeFailure.ValueTooLarge(
          s"string size ${limit + 1} exceeds limit $limit",
          FQLInterpreter.StackTrace(Seq(Span(16777225, 16777233, Src.Query("")))))
      )
    }
  }

  "includes" - {
    testSig("includes(pattern: String) => Boolean")

    "returns true if the given argument is included in self" in {
      checkOk(auth, "'foo'.includes('o')", Value.True)
      checkOk(auth, "'foo'.includes('f')", Value.True)
      checkOk(auth, "'foo'.includes('foo')", Value.True)
    }
    "returns false if the given argument is not included in self" in {
      checkOk(auth, "'foo'.includes('b')", Value.False)
      checkOk(auth, "'foo'.includes('bar')", Value.False)
      checkOk(auth, "'foo'.includes('fooo')", Value.False)
    }
  }
  "includesRegex" - {
    testSig("includesRegex(regex: String) => Boolean")

    "returns true if self matches the given regex" in {
      checkOk(auth, "'foo'.includesRegex('f')", Value.True)
      checkOk(auth, "'foo'.includesRegex('.')", Value.True)
      checkOk(auth, "'foo'.includesRegex('[a-z]')", Value.True)
    }
    "returns false if self does not match the given regex" in {
      checkOk(auth, "'foo'.includesRegex('b')", Value.False)
      checkOk(auth, "''.includesRegex('.')", Value.False)
      checkOk(auth, "''.includesRegex('[a-z]')", Value.False)
    }
  }

  "indexOf" - {
    testSig(
      "indexOf(pattern: String) => Number | Null",
      "indexOf(pattern: String, start: Number) => Number | Null")

    "returns the index of the first match" in {
      checkOk(auth, "'foo'.indexOf('o')", Value.Int(1))
      checkOk(auth, "'foo'.indexOf('f')", Value.Int(0))
    }

    "returns null when there is no match" in {
      checkOk(auth, "'foo'.indexOf('bar')", Value.Null(Span.Null))
    }

    "starts searching from the given index" in {
      checkOk(auth, "'foo'.indexOf('o', 0)", Value.Int(1))
      checkOk(auth, "'foo'.indexOf('o', 1)", Value.Int(1))
      checkOk(auth, "'foo'.indexOf('o', 2)", Value.Int(2))
    }

    "handles surrogate pairs correctly" in {
      checkOk(auth, "'f😍o'.indexOf('f')", Value.Int(0))
      checkOk(auth, "'f😍o'.indexOf('😍')", Value.Int(1))
      checkOk(auth, "'f😍o'.indexOf('o')", Value.Int(2))
    }

    "starts searching from the given index with surrogate pairs" in {
      checkOk(auth, "'f😍😍'.indexOf('😍', 0)", Value.Int(1))
      checkOk(auth, "'f😍😍'.indexOf('😍', 1)", Value.Int(1))
      checkOk(auth, "'f😍😍'.indexOf('😍', 2)", Value.Int(2))
    }

    "errors if the index is outside of the given string" in {
      checkErr(
        auth,
        "'foo'.indexOf('o', 5)",
        QueryRuntimeFailure
          .IndexOutOfBounds(5, 3, FQLInterpreter.StackTrace(Seq(span(13, 21)))))
      checkErr(
        auth,
        "'f😍😍'.indexOf('😍', 5)",
        QueryRuntimeFailure
          .IndexOutOfBounds(5, 3, FQLInterpreter.StackTrace(Seq(span(15, 24)))))
    }
  }

  "indexOfRegex" - {
    testSig(
      "indexOfRegex(regex: String) => Number | Null",
      "indexOfRegex(regex: String, start: Number) => Number | Null")

    "returns the index of the first match" in {
      checkOk(auth, raw"'foo 123'.indexOfRegex('o')", Value.Int(1))
      checkOk(auth, raw"'foo 123'.indexOfRegex('f')", Value.Int(0))
      checkOk(auth, raw"'foo 123'.indexOfRegex('[0-9]')", Value.Int(4))
      // length but worse
      checkOk(auth, raw"'foo 123'.indexOfRegex('$$')", Value.Int(7))
    }

    "returns null when there is no match" in {
      checkOk(auth, raw"'foo'.indexOfRegex('\\W')", Value.Null(Span.Null))
    }

    "starts searching from the given index" in {
      checkOk(auth, raw"'foo'.indexOfRegex('o', 0)", Value.Int(1))
      checkOk(auth, raw"'foo'.indexOfRegex('o', 1)", Value.Int(1))
      checkOk(auth, raw"'foo'.indexOfRegex('o', 2)", Value.Int(2))
    }

    "handles surrogate pairs correctly" in {
      checkOk(auth, "'f😍o'.indexOfRegex('f')", Value.Int(0))
      checkOk(auth, "'f😍o'.indexOfRegex('😍')", Value.Int(1))
      checkOk(auth, "'f😍o'.indexOfRegex('o')", Value.Int(2))
    }

    "starts searching from the given index with surrogate pairs" in {
      checkOk(auth, "'f😍😍'.indexOfRegex('😍', 0)", Value.Int(1))
      checkOk(auth, "'f😍😍'.indexOfRegex('😍', 1)", Value.Int(1))
      checkOk(auth, "'f😍😍'.indexOfRegex('😍', 2)", Value.Int(2))
    }

    "errors if the index is outside of the given string" in {
      checkErr(
        auth,
        "'foo'.indexOfRegex('o', 5)",
        QueryRuntimeFailure
          .IndexOutOfBounds(5, 3, FQLInterpreter.StackTrace(Seq(span(18, 26)))))
      checkErr(
        auth,
        "'f😍😍'.indexOfRegex('😍', 5)",
        QueryRuntimeFailure
          .IndexOutOfBounds(5, 3, FQLInterpreter.StackTrace(Seq(span(20, 29)))))
    }
  }

  "lastIndexOf" - {
    testSig(
      "lastIndexOf(pattern: String) => Number | Null",
      "lastIndexOf(pattern: String, end: Number) => Number | Null")

    "returns the index of the first match" in {
      checkOk(auth, "'foo'.lastIndexOf('o')", Value.Int(2))
      checkOk(auth, "'foo'.lastIndexOf('f')", Value.Int(0))
    }

    "returns null when there is no match" in {
      checkOk(auth, "'foo'.lastIndexOf('bar')", Value.Null(Span.Null))
    }

    "searchs backwards from the given index" in {
      checkOk(auth, "'foobar'.lastIndexOf('o', 0)", Value.Null(Span.Null))
      checkOk(auth, "'foobar'.lastIndexOf('o', 1)", Value.Int(1))
      checkOk(auth, "'foobar'.lastIndexOf('o', 2)", Value.Int(2))
      checkOk(auth, "'foobar'.lastIndexOf('o', 3)", Value.Int(2))
      checkOk(auth, "'foobar'.lastIndexOf('o', 4)", Value.Int(2))
      checkOk(auth, "'foobar'.lastIndexOf('o', 5)", Value.Int(2))
    }

    "handles surrogate pairs correctly" in {
      checkOk(auth, "'f😍o'.lastIndexOf('f')", Value.Int(0))
      checkOk(auth, "'f😍o'.lastIndexOf('😍')", Value.Int(1))
      checkOk(auth, "'f😍o'.lastIndexOf('o')", Value.Int(2))
    }

    "starts searching from the given index with surrogate pairs" in {
      checkOk(auth, "'f😍😍'.lastIndexOf('😍', 0)", Value.Null(Span.Null))
      checkOk(auth, "'f😍😍'.lastIndexOf('😍', 1)", Value.Int(1))
      checkOk(auth, "'f😍😍'.lastIndexOf('😍', 2)", Value.Int(2))
    }

    "errors if the index is outside of the given string" in {
      checkErr(
        auth,
        "'foo'.lastIndexOf('o', 5)",
        QueryRuntimeFailure
          .IndexOutOfBounds(5, 3, FQLInterpreter.StackTrace(Seq(span(17, 25)))))
      checkErr(
        auth,
        "'f😍😍'.lastIndexOf('😍', 5)",
        QueryRuntimeFailure
          .IndexOutOfBounds(5, 3, FQLInterpreter.StackTrace(Seq(span(19, 28)))))
    }
  }

  "length" - {
    testSig("Number")

    "returns the length of a string" in {
      checkOk(auth, "''.length", Value.Int(0))
      checkOk(auth, "'foo'.length", Value.Int(3))
    }

    "counts surrogate pairs as one character in the length" in {
      checkOk(auth, "'fo😍o'.length", Value.Int(4))
      checkOk(auth, "'fo😍😍'.length", Value.Int(4))
    }
  }

  "matches" - {
    testSig("matches(regex: String) => Array<String>")

    "returns matches of the given regex" in {
      evalOk(auth, raw"'foo bar baz'.matches('\\w+')") shouldBe Value.Array(
        Value.Str("foo"),
        Value.Str("bar"),
        Value.Str("baz"))
    }

    "returns an error for an invalid regex" in {
      checkErr(
        auth,
        raw"'foo bar baz'.matches('+')",
        QueryRuntimeFailure
          .InvalidRegex(
            "Dangling meta character '+'",
            0,
            FQLInterpreter.StackTrace(Seq(span(21, 26)))))
    }
  }

  "matchIndexes" - {
    testSig("matchIndexes(regex: String) => Array<[Number, String]>")

    "returns matches of the given regex" in {
      evalOk(auth, raw"'foo bar baz'.matchIndexes('\\w+')") shouldBe Value.Array(
        Value.Array(Value.Int(0), Value.Str("foo")),
        Value.Array(Value.Int(4), Value.Str("bar")),
        Value.Array(Value.Int(8), Value.Str("baz")))
    }

    "returned indexes handle surrogates correctly" in {
      evalOk(auth, raw"'f😍😍 bar baz'.matchIndexes('\\S+')") shouldBe Value.Array(
        Value.Array(Value.Int(0), Value.Str("f😍😍")),
        Value.Array(Value.Int(4), Value.Str("bar")),
        Value.Array(Value.Int(8), Value.Str("baz")))
    }

    "returns an error for an invalid regex" in {
      checkErr(
        auth,
        raw"'foo bar baz'.matchIndexes('+')",
        QueryRuntimeFailure
          .InvalidRegex(
            "Dangling meta character '+'",
            0,
            FQLInterpreter.StackTrace(Seq(span(26, 31))))
      )
    }
  }

  "parseDouble" - {
    testSig("parseDouble() => Number | Null")
    testSigPending("parseDouble() => Double | Null")

    "parses the given value as a double" in {
      checkOk(auth, "'3'.parseDouble()", Value.Double(3.0))
      checkOk(auth, "'4294967297'.parseDouble()", Value.Double(4294967297.0))
      checkOk(auth, "'50.0'.parseNumber()", Value.Double(50.0))
      checkOk(auth, "'50.123'.parseNumber()", Value.Double(50.123))

      checkOk(auth, "'5a'.parseDouble()", Value.Null(Span.Null))
    }
  }
  "parseInt" - {
    testSig(
      "parseInt() => Number | Null",
      "parseInt(radix: Number) => Number | Null")
    testSigPending("parseInt() => Int | Null", "parseInt(radix: Int) => Int | Null")

    "parses the given value as an integer" in {
      checkOk(auth, "'3'.parseInt()", Value.Int(3))
      checkOk(auth, "'4294967297'.parseInt()", Value.Null(Span.Null))
      checkOk(auth, "'50.0'.parseInt()", Value.Null(Span.Null))

      checkOk(auth, "'5a'.parseInt()", Value.Null(Span.Null))

    }

    "accepts a radix argument" in {
      checkOk(auth, "'1a3'.parseInt(16)", Value.Int(0x1a3))
      checkOk(auth, "'1az3'.parseInt(16)", Value.Null(Span.Null))
    }
  }
  "parseLong" - {
    testSig(
      "parseLong() => Number | Null",
      "parseLong(radix: Number) => Number | Null")
    testSigPending(
      "parseLong() => Long | Null",
      "parseLong(radix: Int) => Long | Null")

    "parses the given value as a long" in {
      checkOk(auth, "'3'.parseLong()", Value.Long(3))
      checkOk(auth, "'4294967297'.parseLong()", Value.Long(4294967297L))
      checkOk(auth, "'50.0'.parseLong()", Value.Null(Span.Null))

      checkOk(auth, "'5a'.parseLong()", Value.Null(Span.Null))
    }

    "accepts a radix argument" in {
      checkOk(auth, "'1a3'.parseLong(16)", Value.Long(0x1a3))
      checkOk(auth, "'1az3'.parseLong(16)", Value.Null(Span.Null))
    }
  }
  "parseNumber" - {
    testSig("parseNumber() => Number | Null")

    "parses the given value as a number" in {
      checkOk(auth, "'3'.parseNumber()", Value.Int(3))
      checkOk(auth, "'4294967297'.parseNumber()", Value.Long(4294967297L))
      checkOk(auth, "'50.0'.parseNumber()", Value.Double(50.0))

      checkOk(auth, "'5a'.parseNumber()", Value.Null(Span.Null))
    }
  }

  "replace" - {
    testSig(
      "replace(pattern: String, replacement: String) => String",
      "replace(pattern: String, replacement: String, amount: Number) => String")

    "replaces the first occurrence of text in the given string" in {
      checkOk(auth, "'foobar'.replace('foo', 'bar')", Value.Str("barbar"))
      checkOk(auth, "'foobar'.replace('o', 'oo')", Value.Str("fooobar"))
    }

    "accepts a count for how many replacements to make" in {
      checkOk(auth, "'foobar'.replace('o', 'oo', 1)", Value.Str("fooobar"))
      checkOk(auth, "'foobar'.replace('o', 'oo', 2)", Value.Str("foooobar"))
      checkOk(auth, "'foobar'.replace('o', 'oo', 3)", Value.Str("foooobar"))
    }

    "disallows a count less than 1" in {
      checkErr(
        auth,
        "'foo bar'.replace('o', 'z', 0)",
        QueryRuntimeFailure(
          "invalid_argument",
          "invalid argument `count`: must be greater than or equal to 1",
          FQLInterpreter.StackTrace(Seq(Span(17, 30, Src.Query("")))))
      )
    }

    "limits the size of the result" in {
      val limit = stdlib.StringPrototype.MaxSize
      val as = "a" * (limit / 4)
      val base = Seq(as, "b", as, "b", "c").mkString

      // Long but OK.
      checkOk(
        auth,
        s"'$base'.replace('b', '$as', 1)",
        Value.Str(Seq(as, as, as, "b", "c").mkString))

      // No.
      checkErr(
        auth,
        s"'$base'.replace('b', '$as', 2)",
        QueryRuntimeFailure.ValueTooLarge(
          "string size 16777217 exceeds limit 16777216",
          FQLInterpreter.StackTrace(Seq(Span(8388621, 12582937, Src.Query("")))))
      )
    }
  }

  "replaceRegex" - {
    testSig(
      "replaceRegex(regex: String, replacement: String) => String",
      "replaceRegex(regex: String, replacement: String, amount: Number) => String")

    "replaces the first occurance of text in the given string" in {
      checkOk(auth, raw"'foo bar'.replaceRegex('\\w+', 'bar')", Value.Str("bar bar"))
      checkOk(
        auth,
        raw"'foo1234bar'.replaceRegex('[0-9]', 'z')",
        Value.Str("fooz234bar"))
    }

    "accepts a count for how many replacements to make" in {
      checkOk(auth, raw"'foo bar'.replaceRegex('\\w', 'z', 1)", Value.Str("zoo bar"))
      checkOk(auth, raw"'foo bar'.replaceRegex('\\w', 'z', 2)", Value.Str("zzo bar"))
      checkOk(auth, raw"'foo bar'.replaceRegex('\\w', 'z', 3)", Value.Str("zzz bar"))
      checkOk(auth, raw"'foo bar'.replaceRegex('\\w', 'z', 4)", Value.Str("zzz zar"))
    }

    "disallows a count less than 1" in {
      checkErr(
        auth,
        raw"'foo bar'.replaceRegex('\\w', 'z', 0)",
        QueryRuntimeFailure(
          "invalid_argument",
          "invalid argument `count`: must be greater than or equal to 1",
          FQLInterpreter.StackTrace(Seq(Span(22, 37, Src.Query("")))))
      )
    }
  }

  "replaceAll" - {
    testSig("replaceAll(pattern: String, replacement: String) => String")

    "replaces all occurances of text in the given string" in {
      checkOk(auth, "'foobar'.replaceAll('foo', 'bar')", Value.Str("barbar"))
      checkOk(auth, "'foobar'.replaceAll('o', 'oo')", Value.Str("foooobar"))
    }
  }

  "replaceAllRegex" - {
    testSig("replaceAllRegex(pattern: String, replacement: String) => String")

    "replaces all occurances of the given regex in the string" in {
      checkOk(auth, raw"'foobar'.replaceAllRegex('\\w', 'z')", Value.Str("zzzzzz"))
      checkOk(auth, raw"'1 2 3'.replaceAllRegex('\\W', '::')", Value.Str("1::2::3"))
    }
  }

  "startsWith" - {
    testSig("startsWith(prefix: String) => Boolean")

    "checks if string starts with another string" in {
      checkOk(auth, "'foo'.startsWith('f')", Value.True)
      checkOk(auth, "'foo'.startsWith('b')", Value.False)
    }

    "strings always start with themselves" in {
      checkOk(auth, "'foo'.startsWith('foo')", Value.True)
    }

    "longer strings will not work" in {
      checkOk(auth, "'foo'.startsWith('foobar')", Value.False)
    }
  }

  "slice" - {
    testSig(
      "slice(start: Number) => String",
      "slice(start: Number, end: Number) => String")

    "returns the characters from the first to the second index" in {
      checkOk(auth, "'foo'.slice(0, 0)", Value.Str(""))
      checkOk(auth, "'foo'.slice(0, 1)", Value.Str("f"))
      checkOk(auth, "'foo'.slice(0, 2)", Value.Str("fo"))
      checkOk(auth, "'foo'.slice(1, 0)", Value.Str(""))
      checkOk(auth, "'foo'.slice(1, 1)", Value.Str(""))
      checkOk(auth, "'foo'.slice(1, 2)", Value.Str("o"))
    }

    "allows indexes out of bounds" in {
      checkOk(auth, "'foo'.slice(0, 3)", Value.Str("foo"))
      checkOk(auth, "'foo'.slice(-1, 3)", Value.Str("foo"))
      checkOk(auth, "'foo'.slice(-1, 10)", Value.Str("foo"))
    }

    "accepts a single argument" in {
      checkOk(auth, "'foo'.slice(0)", Value.Str("foo"))
      checkOk(auth, "'foo'.slice(1)", Value.Str("oo"))
      checkOk(auth, "'foo'.slice(2)", Value.Str("o"))
      checkOk(auth, "'foo'.slice(3)", Value.Str(""))
      checkOk(auth, "'foo'.slice(4)", Value.Str(""))

      checkOk(auth, "'f😍o'.slice(0)", Value.Str("f😍o"))
      checkOk(auth, "'f😍o'.slice(1)", Value.Str("😍o"))
      checkOk(auth, "'f😍o'.slice(2)", Value.Str("o"))
      checkOk(auth, "'f😍o'.slice(3)", Value.Str(""))
      checkOk(auth, "'f😍o'.slice(4)", Value.Str(""))
    }

    "handles surrogate pairs correctly" in {
      checkOk(auth, "'fo😍o'.slice(0, 0)", Value.Str(""))
      checkOk(auth, "'fo😍o'.slice(0, 1)", Value.Str("f"))
      checkOk(auth, "'fo😍o'.slice(0, 2)", Value.Str("fo"))
      checkOk(auth, "'fo😍o'.slice(0, 3)", Value.Str("fo😍"))
      checkOk(auth, "'fo😍o'.slice(0, 4)", Value.Str("fo😍o"))
      checkOk(auth, "'fo😍o'.slice(0, 5)", Value.Str("fo😍o"))

      checkOk(auth, "'fo😍😍'.slice(0, 0)", Value.Str(""))
      checkOk(auth, "'fo😍😍'.slice(0, 1)", Value.Str("f"))
      checkOk(auth, "'fo😍😍'.slice(0, 2)", Value.Str("fo"))
      checkOk(auth, "'fo😍😍'.slice(0, 3)", Value.Str("fo😍"))
      checkOk(auth, "'fo😍😍'.slice(0, 4)", Value.Str("fo😍😍"))
      checkOk(auth, "'fo😍😍'.slice(0, 5)", Value.Str("fo😍😍"))
    }
  }

  "split" - {
    testSig("split(separator: String) => Array<String>")

    "splits text by the given delimiter" in {
      checkOk(
        auth,
        "'foo bar baz'.split(' ')",
        Value.Array(Value.Str("foo"), Value.Str("bar"), Value.Str("baz")))
    }

    "multiple delimiters in the source string will result in empty strings in the result" in {
      checkOk(
        auth,
        "'foo::bar'.split(':')",
        Value.Array(Value.Str("foo"), Value.Str(""), Value.Str("bar")))
    }

    "trailing/preceding delimiters will be counted" in {
      checkOk(
        auth,
        "'1:2:'.split(':')",
        Value.Array(Value.Str("1"), Value.Str("2"), Value.Str("")))
      checkOk(
        auth,
        "':1:2'.split(':')",
        Value.Array(Value.Str(""), Value.Str("1"), Value.Str("2")))
      checkOk(
        auth,
        "':1:2:'.split(':')",
        Value.Array(Value.Str(""), Value.Str("1"), Value.Str("2"), Value.Str("")))
    }

    "empty delimiter will return all chars in the source string" in {
      checkOk(
        auth,
        "'foo'.split('')",
        Value.Array(Value.Str("f"), Value.Str("o"), Value.Str("o")))
    }

    "zero-character edge cases" in {
      checkOk(auth, "'12'.split('')", Value.Array(Value.Str("1"), Value.Str("2")))
    }
  }

  "splitRegex" - {
    testSig("splitRegex(regex: String) => Array<String>")

    "splits text by the given regex" in {
      checkOk(
        auth,
        raw"'foo bar  \t  baz'.splitRegex('\\W+')",
        Value.Array(Value.Str("foo"), Value.Str("bar"), Value.Str("baz")))
    }

    "multiple delimiters in the source string will result in empty strings in the result" in {
      checkOk(
        auth,
        "'foo::bar'.splitRegex(':')",
        Value.Array(Value.Str("foo"), Value.Str(""), Value.Str("bar")))
    }

    "trailing/preceding delimiters will be counted" in {
      checkOk(
        auth,
        "'1:2:'.splitRegex(':')",
        Value.Array(Value.Str("1"), Value.Str("2"), Value.Str("")))
      checkOk(
        auth,
        "':1:2'.splitRegex(':')",
        Value.Array(Value.Str(""), Value.Str("1"), Value.Str("2")))
      checkOk(
        auth,
        "':1:2:'.splitRegex(':')",
        Value.Array(Value.Str(""), Value.Str("1"), Value.Str("2"), Value.Str("")))
    }

    "empty delimiter will return all chars in the source string" in {
      checkOk(
        auth,
        "'foo'.splitRegex('')",
        Value.Array(Value.Str("f"), Value.Str("o"), Value.Str("o")))
    }

    "zero-character edge cases" in {
      checkOk(
        auth,
        "'12'.splitRegex('(1|)')",
        Value.Array(Value.Str(""), Value.Str("2"), Value.Str("")))
      checkOk(
        auth,
        "'12'.splitRegex('')",
        Value.Array(Value.Str("1"), Value.Str("2")))
    }
  }

  "splitAt" - {
    testSig("splitAt(index: Number) => [String, String]")

    "splits text at the given index" in {
      checkOk(
        auth,
        "'foobar'.splitAt(3)",
        Value.Array(Value.Str("foo"), Value.Str("bar")))
      checkOk(
        auth,
        "'foobar'.splitAt(0)",
        Value.Array(Value.Str(""), Value.Str("foobar")))
    }

    "allows index at length of string" in {
      checkOk(
        auth,
        "'foobar'.splitAt(6)",
        Value.Array(Value.Str("foobar"), Value.Str("")))
    }
  }

  "toString" - {
    testSig("toString() => String")

    "returns the original string" in {
      checkOk(auth, "'FQL-X'.toString()", Value.Str("FQL-X"))
      checkOk(auth, "'foobar'.toString()", Value.Str("foobar"))
    }
  }

  "toLowerCase" - {
    testSig("toLowerCase() => String")

    "converts string to lowercase" in {
      checkOk(auth, "'FQL-X'.toLowerCase()", Value.Str("fql-x"))
      checkOk(auth, "'foobar'.toLowerCase()", Value.Str("foobar"))
    }
  }
  "toUpperCase" - {
    testSig("toUpperCase() => String")

    "converts string to uppercase" in {
      checkOk(auth, "'FQL-X'.toUpperCase()", Value.Str("FQL-X"))
      checkOk(auth, "'foobar'.toUpperCase()", Value.Str("FOOBAR"))
    }
  }
}
