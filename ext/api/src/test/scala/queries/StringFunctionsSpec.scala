package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop._

class StringFunctionsSpec extends QueryAPI21Spec {
  "concat" - {
    once("adds strings together") {
      for {
        db <- aDatabase
      } {
        qequals("42", Concat(JSArray("4", "2")), db)
        qequals("5", Concat("5"), db)
        qequals("4,2", Concat(JSArray("4", "2"), ","), db)
        qequals("", Concat(JSArray()), db)
        qassertErr(Concat(JSArray("4", "2"), 5), "invalid argument", JSArray("separator"), db)
        qassertErr(Concat(JSArray(5)), "invalid argument", JSArray("concat", 0), db)
      }
    }
  }

  "casefold" - {
    prop("normalizes strings") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        qequals(CaseFold(str), CaseFold(str), db)
      }
    }

    once("normalizes oddities") {
      for {
        db <- aDatabase
      } {
        qequals("john doe", CaseFold("John Doe"), db)
        qequals("i̇stanbul", CaseFold("İstanbul"), db)
        qequals("diyarbakır", CaseFold("Diyarbakır"), db)
        qequals("φιλόσοφοσ", CaseFold("φιλόσοφος"), db)
        qequals("weisskopfseeadler", CaseFold("Weißkopfseeadler"), db)
      }
    }

    prop("can use different normalizers") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        Seq(
          "NFKCCaseFold",
          "NFC",
          "NFD",
          "NFKC",
          "NFKD"
        ) foreach { n => qequals(CaseFold(str, n), CaseFold(str, n), db) }

        qassertErr(CaseFold("foo", "nope"), "invalid argument", JSArray("normalizer"), db)
      }
    }
  }

  "startswith" - {
    prop("finds prefixes") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 1)
      } {
        str.inits foreach { prefix =>
          qassert(StartsWith(str, prefix), db)
        }

        qassert(Not(StartsWith(str, s"$str$str")), db)
      }
    }(propConfig.copy(minSuccessful = propConfig.minSuccessful / 3))

    once("finds empty string prefix") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 1)
      } {
        qassert(StartsWith("", ""), db)
        qassert(StartsWith(str, ""), db)
      }
    }
  }

  "endswith" - {
    prop("finds suffixes") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 1)
      } {
        str.tails foreach { suffix =>
          qassert(EndsWith(str, suffix), db)
        }

        qassert(Not(EndsWith(str, s"$str$str")), db)
      }
    }(propConfig.copy(minSuccessful = propConfig.minSuccessful / 2))

    once("finds empty string suffix") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 1)
      } {
        qassert(EndsWith("", ""), db)
        qassert(EndsWith(str, ""), db)
      }
    }
  }

  "containsstr" - {
    prop("finds substrings") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        str.toSeq.sliding(5).map(_.unwrap) foreach {
          s => qassert(ContainsStr(str, s), db)
        }
      }
    }(propConfig.copy(minSuccessful = propConfig.minSuccessful / 2))

    prop("doesn't find duplicated string as substring") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 1)
      } {
        qassert(Not(ContainsStr(str, s"$str$str")), db)
      }
    }
  }

  "containsstrregex" - {
    once("finds patterns") {
      for {
        db <- aDatabase
      } {
        val str = "One 2\n3 Four"
        qassert(ContainsStrRegex(str, ".*"), db)
        qassert(ContainsStrRegex(str, "."), db)
        qassert(ContainsStrRegex(str, "\\d"), db)
        qassert(ContainsStrRegex(str, "\\D"), db)
        qassert(ContainsStrRegex(str, "\\w"), db)
        qassert(ContainsStrRegex(str, "\\W"), db)
        qassert(ContainsStrRegex(str, "\\s"), db)
        qassert(ContainsStrRegex(str, "\\S"), db)

        qassert(ContainsStrRegex(str, "(?m)2$"), db)
        qassert(Not(ContainsStrRegex(str, "2$")), db)

        qassert(ContainsStrRegex(str, "(?m)^3"), db)
        qassert(Not(ContainsStrRegex(str, "^3")), db)

        qassert(ContainsStrRegex(str, "\\AOne 2\n3 Four\\Z"), db)
        qassert(ContainsStrRegex(str, "^One 2\n3 Four$"), db)
      }
    }

    once("reports invalid regex pattern") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          ContainsStrRegex("", "{"),
          "invalid argument",
          "Search pattern /{/ is not a valid regular expression.",
          JSArray("pattern"),
          db)
      }
    }

    prop("finds escaped substrings") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 1)
      } {
        // find the wildcard matcher
        qassert(ContainsStrRegex(str, ".*"), db)

        str.toSeq.sliding(5).map(_.unwrap) foreach {
          s => qassert(ContainsStrRegex(str, RegexEscape(s)), db)
        }

        qassert(ContainsStrRegex("", RegexEscape("")), db)
        qassert(Not(ContainsStrRegex(str, RegexEscape(s"$str$str"))), db)
      }
    }
  }

  "findstr" - {
    once("test findstr string function") {
      for {
        db <- aDatabase
      } {
        qequals(0,  FindStr("a big apple","a"), db)
        qequals(0,  FindStr("ABC","A", 0), db)
        qequals(-1, FindStr("ABC","A", 1), db)
        qequals(6,  FindStr("a big apple","a", 2), db)
        qequals(-1, FindStr("a big apple","bad"), db)
        qequals(4,  FindStr("Bob f smith sonny Jones BILL WALSH","f"), db)
        qequals(2,  FindStr("apple", "ple"), db)
        qequals(6,  FindStr(Concat(JSArray("three strings", " ", "combine TOGETHER by ANOTHER")), "strings"), db)
        qequals(-1, FindStr("","find"), db)
        qequals(2,  FindStr("\uD801\uDC00 ABC", "ABC"), db)
        qequals(8,  FindStr("\uD801\uDC00 ABC \uD801\uDC00 ABC", "ABC", 3), db)
        qassertErr(FindStr(7, 7), "invalid argument", JSArray("findstr"), db)
        qassertErr(FindStr("ABC", 7), "invalid argument", JSArray("find"), db)
      }
    }
  }

  "findstrregex" - {
    once("test findstrregex string function") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(), FindStrRegex("One Fish Two Fish","Fish",16), db)

        val res = runQuery( FindStrRegex("One Fish Two fish","Fish"), db)
        (res.get(0) / "start") should equal (JSLong(4))
        (res.get(0) / "end") should equal (JSLong(7))
        (res.get(0) / "data") should equal (JSString("Fish"))

        val res2 = runQuery( FindStrRegex("One Fish Two fish","[fF]ish"), db)
        (res2.get(0) / "start") should equal (JSLong(4))
        (res2.get(0) / "end") should equal (JSLong(7))
        (res2.get(0) / "data") should equal (JSString("Fish"))
        (res2.get(1) / "start") should equal (JSLong(13))
        (res2.get(1) / "end") should equal (JSLong(16))
        (res2.get(1) / "data") should equal (JSString("fish"))

        val res3 = runQuery( FindStrRegex("One Fish Two fish","[fF]ish", 9), db)
        (res3.get(0) / "start") should equal (JSLong(13))
        (res3.get(0) / "end") should equal (JSLong(16))
        (res3.get(0) / "data") should equal (JSString("fish"))

        val res4 = runQuery( FindStrRegex("One Fish Two fish","[Oo][Nn]e" ), db)
        (res4.get(0) / "start") should equal (JSLong(0))

        val res5 = runQuery( FindStrRegex("One Fish Two fish","Two", 9), db)
        (res5.get(0) / "start") should equal (JSLong(9))
        qassertErr(FindStrRegex(7,"7"), "invalid argument", JSArray("findstrregex"), db)

        val res6 = runQuery( FindStrRegex("\uD801\uDC00 ABC \uD801\uDC00 ABc", "AB[cC]"), db)
        (res6.get(0) / "start") should equal (JSLong(2))
        (res6.get(0) / "end") should equal (JSLong(4))
        (res6.get(0) / "data") should equal (JSString("ABC"))
        (res6.get(1) / "start") should equal (JSLong(8))
        (res6.get(1) / "end") should equal (JSLong(10))
        (res6.get(1) / "data") should equal (JSString("ABc"))

        val res7 = runQuery( FindStrRegex("\uD801\uDC00 ABC \uD801\uDC00 ABc", "AB[cC]", 3), db)
        (res7.get(0) / "start") should equal (JSLong(8))

        val res8 = runQuery( FindStrRegex("One Fish to Fish", "^|man"), db)
        (res8.get(0) / "start") should equal (JSLong(0))
        (res8.get(0) / "end") should equal (JSLong(0))
        (res8.get(0) / "data") should equal (JSString(""))

        val res9 = runQuery( FindStrRegex("fauna,graphql,serverless,database", "^|$|database"), db)
        (res9.get(0) / "start") should equal (JSLong(0))
        (res9.get(0) / "end") should equal (JSLong(0))
        (res9.get(0) / "data") should equal (JSString(""))
        (res9.get(1) / "start") should equal (JSLong(25))
        (res9.get(1) / "end") should equal (JSLong(32))
        (res9.get(1) / "data") should equal (JSString("database"))
        (res9.get(2) / "start") should equal (JSLong(33))
        (res9.get(2) / "end") should equal (JSLong(33))
        (res9.get(2) / "data") should equal (JSString(""))
      }
    }

    once("reports invalid regex pattern") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          FindStrRegex("", "{"),
          "invalid argument",
          "Search pattern /{/ is not a valid regular expression.",
          JSArray("pattern"),
          db)
      }
    }
  }

  "length" - {
    once("test length string function") {
      for {
        db <- aDatabase
        str <- Prop.string(minSize = 500, maxSize = 600 )
        strnum <- Prop.numericString(minSize = 20, maxSize = 27 )
        stralpha <- Prop.numericString(minSize = 20, maxSize = 27 )
        stralphanum <- Prop.numericString(minSize = 20, maxSize = 27 )
        strhex <- Prop.numericString(minSize = 20, maxSize = 27 )
      } {
        qequals(0,  Length(""), db)
        qequals(1,  Length("a"), db)
        qequals(5,  Length("apple"), db)
        qequals(34, Length("Bob f smith sonny Jones BILL WALSH"), db)
        qequals(1,  Length(ToString(7)), db)
        qequals(5,  Length("\uD801\uDC00 ABC"), db)

        qassert(JSObject("lte" -> JSArray(500, Length(str))), db)
        qassert(JSObject("gte" -> JSArray(600, Length(str))), db)

        qassert(JSObject("lte" -> JSArray(20, Length(strnum))), db)
        qassert(JSObject("gte" -> JSArray(27, Length(strnum))), db)

        qassert(JSObject("lte" -> JSArray(20, Length(stralpha))), db)
        qassert(JSObject("gte" -> JSArray(27, Length(stralpha))), db)

        qassert(JSObject("lte" -> JSArray(20, Length(stralphanum))), db)
        qassert(JSObject("gte" -> JSArray(27, Length(stralphanum))), db)

        qassert(JSObject("lte" -> JSArray(20, Length(strhex))), db)
        qassert(JSObject("gte" -> JSArray(27, Length(strhex))), db)
      }
    }
  }

  "lowercase" - {
    once("test lowercase string function") {
      for {
        db <- aDatabase
      } {
        qequals("",  LowerCase(""), db)
        qequals("a",  LowerCase("A"), db)
        qequals("apple",  LowerCase("ApPle"), db)
        qequals("bob f smith sonny jones bill walsh", LowerCase("Bob f smITH soNny Jones BILL WALSH"), db)
        qequals("7",  LowerCase(ToString(7)), db)
        qequals("john doe", LowerCase("John Doe"), db)
        qequals("i̇stanbul", LowerCase("İstanbul"), db)
        qequals("diyarbakır", LowerCase("Diyarbakır"), db)
        qassertErr(LowerCase(7), "invalid argument", JSArray("lowercase"), db)
      }
    }
  }

  "ltrim" - {
    once("test ltrim string function") {
      for {
        db <- aDatabase
      } {
        qequals("", LTrim(""), db)
        qequals("", LTrim(" "), db)
        qequals("", LTrim("  "), db)
        qequals("A", LTrim("    A"), db)
        qequals("Apple", LTrim("\t\n\t\n\n\nApple"), db)
        qequals(
          "bob f smith sonny jones bill walsh",
          LTrim("    bob f smith sonny jones bill walsh"),
          db)
        qequals("ABC", LTrim("\u0009ABC"), db)
        qequals("ABD", LTrim("\u1680ABD"), db)
        qequals("7", LTrim(ToString(7)), db)
        qassertErr(LTrim(7), "invalid argument", JSArray("ltrim"), db)
      }
    }
  }

  "ngram" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qequals(NGram("apple"), JSArray("a","ap","p","pp","p","pl","l","le","e"), db)

        qequals(NGram("apple", 1, 2), JSArray("a","ap","p","pp","p","pl","l","le","e"), db)
        qequals(NGram("apple", 2, 3), JSArray("ap","app","pp","ppl","pl","ple","le"), db)
        qequals(NGram("apple", 3, 4), JSArray("app","appl","ppl","pple","ple"), db)
        qequals(NGram("apple", 4, 5), JSArray("appl","apple","pple"), db)
        qequals(NGram("apple", 5, 6), JSArray("apple"), db)
      }
    }

    once("rejects invalid minimum size") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        qassertErr(NGram(str, Int.MinValue), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, 0), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, Int.MaxValue), "invalid argument", JSArray.empty, db)
      }
    }

    once("rejects invalid maximum size") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        qassertErr(NGram(str, 1, Int.MinValue), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, 1, 0), "invalid argument", JSArray.empty, db)
      }
    }

    once("rejects invalid size range") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        qassertErr(NGram(str, Int.MinValue, Int.MinValue), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, 0, Int.MinValue), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, 10, 3), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, Int.MaxValue, Int.MinValue), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, 1, 3), "invalid argument", JSArray.empty, db)
        qassertErr(NGram(str, 20, 22), "invalid argument", JSArray.empty, db)
      }
    }
  }
  "repeat" - {
    once("test repeat string function") {
      for {
        db <- aDatabase
      } {
        qequals("AA",  RepeatString("A"), db)
        qequals("AAAAA",  RepeatString("A", 5), db)
        qequals("ABABAB",  RepeatString("AB",3), db)
        qassertErr(RepeatString(7), "invalid argument", JSArray("repeat"), db)
      }
    }
  }

  "replacestr" - {
    once("test ReplaceString string function") {
      for {
        db <- aDatabase
      } {
        qequals("One Fish Blue Fish",  ReplaceStr("One Fish Two Fish","Two","Blue"), db)
        // Check to ensure punctuation is not taken as patterns
        qequals("One Cat Two Fish",  ReplaceStr("One Fis? Two Fish","Fis?","Cat"), db)
        qequals("One Cat Two Fish",  ReplaceStr("One Fis. Two Fish","Fis.","Cat"), db)
        // Check multiple subsitutions
        qequals("One Car Two Car",  ReplaceStr("One Fish Two Fish","Fish","Car"), db)
        qassertErr(ReplaceStr(7,"to","two"), "invalid argument", JSArray("replacestr"), db)
      }
    }
  }

  "replacestrregex" - {
    once("test replace string with regex string function") {
      for {
        db <- aDatabase
      } {
        qequals("One Fish Blue Fish",  ReplaceStrRegex("One Fish Two Fish", "Two", "Blue"), db)
        qequals("One Car Two Car",  ReplaceStrRegex("One Fish Two Fish","Fish","Car"), db)
        qequals("One Car Two Fish",  ReplaceStrRegex("One Fish Two Fish","Fish","Car",true), db)
        // Check to ensure punctuation and formal patterns are accepted
        qequals("OneCarTwoCar",  ReplaceStrRegex("One Car Two Car", "\\s", ""), db)
        qequals("1 FISH 2 FISH 3 FISH",  ReplaceStrRegex("1 Cat 2 Cap 3 cam","[cC]a.", "FISH"), db)
        qequals("1 FISH 2 Cap 3 cam",  ReplaceStrRegex("1 Cat 2 Cap 3 cam","[cC]a.", "FISH", true), db)
        qassertErr(ReplaceStrRegex(7,"7","8"), "invalid argument", JSArray("replacestrregex"), db)
      }
    }

    once("reports invalid regex pattern") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          ReplaceStrRegex("", "{", ""),
          "invalid argument",
          "Search pattern /{/ is not a valid regular expression.",
          JSArray("pattern"),
          db)
      }
    }
  }

  "rtrim" - {
    once("test rtrim string function") {
      for {
        db <- aDatabase
      } {
        qequals("", RTrim(""), db)
        qequals("", RTrim(" "), db)
        qequals("", RTrim("  "), db)
        qequals("A", RTrim("A    "), db)
        qequals("Apple", RTrim("Apple \t\n\t\n\n\n"), db)
        qequals(
          "bob f smith sonny jones bill walsh",
          RTrim("bob f smith sonny jones bill walsh    "),
          db)
        qequals("ABC", RTrim("ABC\u0009"), db)
        qequals("ABD", RTrim("ABD\u1680"), db)
        qequals("7", RTrim(ToString(7)), db)
        qassertErr(RTrim(7), "invalid argument", JSArray("rtrim"), db)
      }
    }
  }

  "space" - {
    once("test space string function") {
      for {
        db <- aDatabase
      } {
        qequals("", Space(-10), db)
        qequals("    ", Space(4), db)
        qequals("1  3", Concat(JSArray("1", Space(2), "3")), db)
        qequals("4          2", Concat(JSArray("4", Space(10), "2")), db)
        qassertErr(Space("Hello"), "invalid argument", JSArray("space"), db)
      }
    }
  }

  "substring" - {
    once("test substring string function") {
      for {
        db <- aDatabase
      } {
        qequals("ABC",  SubString("ABC"), db)
        qequals("ABC",  SubString("DEABC", 2), db)
        qequals("ABC",  SubString3("DEABCHIJ", 2, 3), db)
        qequals("ABC",  SubString("DEFABC", -3), db)
        qequals("ABC",  SubString3("DEFABCHIJ", -6, 3), db)
        qequals("ABC",  SubString("\uD801\uDC00 ABC", 2), db)
        qequals("ABC",  SubString3("\uD801\uDC00 ABC \uD801\uDC00", 2, 3), db)
        qequals("", SubString3("ABCDE", 5, 2), db)
        qassertErr(SubString(7), "invalid argument", JSArray("substring"), db)
        qassertErr(
          SubString3("ABCDE", 6, 2),
          "invalid argument",
          "start must be less than or equal to the string length",
          JSArray(),
          db)
      }
    }
  }

  "titlecase" - {
    once("test titlecase string function") {
      for {
        db <- aDatabase
      } {
        qequals("Bob F Smith Sonny Jones Bill Walsh", TitleCase("Bob f smith sonny Jones BILL WALSH"), db)
        qequals("A", TitleCase("a"), db)
        qequals("Apple", TitleCase("apple"), db)
        qequals("Three Strings Combine Together By Another", TitleCase(Concat(JSArray("three strings", " ", "combine TOGETHER by ANOTHER"))), db)
        qequals("", TitleCase(""), db)
        qequals("7", TitleCase(ToString(7)), db)
        qassertErr(TitleCase(7), "invalid argument", JSArray("titlecase"), db)
      }
    }
  }

  "trim" - {
    once("test trim string function") {
      for {
        db <- aDatabase
      } {
        qequals("", Trim(""), db)
        qequals("", Trim(" "), db)
        qequals("", Trim("  "), db)
        qequals("ABC", Trim("ABC"), db)
        qequals("A B C", Trim("A B C"), db)
        qequals("A", Trim("    A    "), db)
        qequals("Apple", Trim("\t\t\n\n     Apple \t\n\t\n\n\n"), db)
        qequals(
          "bob f smith sonny jones bill walsh",
          Trim("\tbob f smith sonny jones bill walsh    "),
          db)
        qequals("ABC", Trim("\u0009ABC\u0009"), db)
        qequals("ABD", Trim("\u1680ABD\u1680"), db)
        qequals("7", Trim(ToString(7)), db)
        qassertErr(Trim(7), "invalid argument", JSArray("trim"), db)
      }
    }
  }

  "uppercase" - {
    once("test upper string function") {
      for {
        db <- aDatabase
      } {
        qequals("",  UpperCase(""), db)
        qequals("A",  UpperCase("a"), db)
        qequals("APPLE",  UpperCase("Apple"), db)
        qequals("BOB F SMITH SONNY JONES BILL WALSH", UpperCase("Bob f smITH soNny Jones BILL WALSH"), db)
        qequals("7",  UpperCase(ToString(7)), db)
        qassertErr(UpperCase(7), "invalid argument", JSArray("uppercase"), db)
      }
    }
  }

  "format" - {
    once("test format function") {
      for {
        db  <- aDatabase
      } {
        qequals("str true false null", Format("%s %s %s %s", JSArray("str", true, false, JSNull)), db)
        qequals("15 17 f", Format("%d %o %x", JSArray(15L, 15L, 15L)), db)
        qequals("3.140000e+00 3.140000 3.14000", Format("%e %f %g", JSArray(3.14D, 3.14D, 3.14D)), db)
        qequals("true true true true false false", Format("%b %b %b %b %b %b", JSArray(true, "str", 10L, 3.14D, false, JSNull)), db)
      }
    }

    once("test format function with uppercase") {
      for {
        db  <- aDatabase
      } {
        qequals("STR TRUE FALSE NULL", Format("%S %S %S %S", JSArray("str", true, false, JSNull)), db)
        qequals("FF", Format("%X", JSArray(255L)), db)
        qequals("3.140000E+00 3.14000E+07", Format("%E %G", JSArray(3.14D, 3.14D*10_000_000)), db)
        qequals("TRUE TRUE TRUE TRUE FALSE FALSE", Format("%B %B %B %B %B %B", JSArray(true, "str", 10L, 3.14D, false, JSNull)), db)
      }
    }

    once("test format function with special chars") {
      for {
        db  <- aDatabase
      } {
        qequals("%", Format("%%", JSArray()), db)
        qequals(System.lineSeparator(), Format("%n", JSArray()), db)

        qequals(
          """{"x":10,"y":3.14,"z":["str"]}""",
          Format("%@", MkObject("x" -> 10L, "y" -> 3.14D, "z" -> JSArray("str"))),
          db)
      }
    }

    once("test format function with width") {
      for {
        db  <- aDatabase
      } {
        qequals("                 str", Format("%20s", "str"), db)
        qequals("                  10", Format("%20d", 10L), db)
        qequals("            3.140000", Format("%20f", 3.14D), db)
        qequals("                true", Format("%20b", true), db)
        qequals("               false", Format("%20b", false), db)
      }
    }

    once("test format function with width/left-justify") {
      for {
        db  <- aDatabase
      } {
        qequals("str                 ", Format("%-20s", "str"), db)
        qequals("10                  ", Format("%-20d", 10L), db)
        qequals("3.140000            ", Format("%-20f", 3.14D), db)
        qequals("true                ", Format("%-20b", true), db)
        qequals("false               ", Format("%-20b", false), db)
      }
    }

    once("test format function with precision") {
      for {
        db  <- aDatabase
      } {
        qequals("3.14", Format("%.2f", 3.14159D), db)
        qequals("3.142", Format("%.3f", 3.14159D), db)
        qequals("3.14159", Format("%.5f", 3.14159D), db)
      }
    }

    once("test format function with leading sign") {
      for {
        db  <- aDatabase
      } {
        qequals("+3.141590", Format("%+f", +3.14159D), db)
        qequals("-3.141590", Format("%+f", -3.14159D), db)
        qequals("+10", Format("%+d", +10L), db)
        qequals("-10", Format("%+d", -10L), db)
      }
    }

    once("test format function with parentheses") {
      for {
        db  <- aDatabase
      } {
        qequals("3.141590", Format("%(f", +3.14159D), db)
        qequals("(3.141590)", Format("%(f", -3.14159D), db)
        qequals("10", Format("%(d", +10L), db)
        qequals("(10)", Format("%(d", -10L), db)
      }
    }

    once("test format function with leading space") {
      for {
        db  <- aDatabase
      } {
        qequals(" 3.141590", Format("% f", +3.14159D), db)
        qequals("-3.141590", Format("% f", -3.14159D), db)
        qequals(" 10", Format("% d", +10L), db)
        qequals("-10", Format("% d", -10L), db)
      }
    }

    once("test format function with dates") {
      for {
        db  <- aDatabase
      } {
        val date = DateF("1970-03-01")

        qequals("1970-03-01", Format("%t", date), db) // non-standard
        qequals("1970-03-01", Format("%s", date), db)

        qequals("March", Format("%tB", date), db)
        qequals("Mar", Format("%tb", date), db)
        qequals("Mar", Format("%th", date), db)
        qequals("MARCH", Format("%TB", date), db)
        qequals("MAR", Format("%Tb", date), db)
        qequals("MAR", Format("%Th", date), db)

        qequals("Sunday", Format("%tA", date), db)
        qequals("Sun", Format("%ta", date), db)
        qequals("SUNDAY", Format("%TA", date), db)
        qequals("SUN", Format("%Ta", date), db)

        qequals("19", Format("%tC", date), db)
        qequals("70", Format("%ty", date), db)
        qequals("1970", Format("%tY", date), db)

        qequals("01", Format("%td", date), db)
        qequals("1", Format("%te", date), db)
        qequals("03", Format("%tm", date), db)
        qequals("060", Format("%tj", date), db)

        qequals("03/01/70", Format("%tD", date), db)
        qequals("1970-03-01", Format("%tF", date), db)
      }
    }

    once("test format function with times") {
      for {
        db  <- aDatabase
      } {
        val time = Time("1970-03-01T15:30:45.123456789Z")

        qequals("1970-03-01T15:30:45.123456789Z", Format("%t", time), db) // non-standard
        qequals("1970-03-01T15:30:45.123456789Z", Format("%s", time), db)

        qequals("15", Format("%tH", time), db)
        qequals("03", Format("%tI", time), db)
        qequals("15", Format("%tk", time), db)
        qequals("3", Format("%tl", time), db)

        qequals("30", Format("%tM", time), db)
        qequals("45", Format("%tS", time), db)

        qequals("123", Format("%tL", time), db)
        qequals("123456789", Format("%tN", time), db)

        qequals("pm", Format("%tp", time), db)
        qequals("PM", Format("%Tp", time), db)

        qequals("5153445", Format("%ts", time), db)
        qequals("5153445123", Format("%tQ", time), db)

        qequals("+0000", Format("%tz", time), db)
        qequals("Z", Format("%tZ", time), db)

        qequals("March", Format("%tB", time), db)
        qequals("Mar", Format("%tb", time), db)
        qequals("Mar", Format("%th", time), db)
        qequals("MARCH", Format("%TB", time), db)
        qequals("MAR", Format("%Tb", time), db)
        qequals("MAR", Format("%Th", time), db)

        qequals("Sunday", Format("%tA", time), db)
        qequals("Sun", Format("%ta", time), db)
        qequals("SUNDAY", Format("%TA", time), db)
        qequals("SUN", Format("%Ta", time), db)

        qequals("19", Format("%tC", time), db)
        qequals("70", Format("%ty", time), db)
        qequals("1970", Format("%tY", time), db)

        qequals("01", Format("%td", time), db)
        qequals("1", Format("%te", time), db)
        qequals("03", Format("%tm", time), db)
        qequals("060", Format("%tj", time), db)

        qequals("15:30", Format("%tR", time), db)
        qequals("15:30:45", Format("%tT", time), db)
        qequals("03:30:45 PM", Format("%tr", time), db)
        qequals("03/01/70", Format("%tD", time), db)
        qequals("1970-03-01", Format("%tF", time), db)
        qequals("Sun Mar 01 15:30:45 Z 1970", Format("%tc", time), db)
      }
    }

    once("test format function with explicit index") {
      for {
        db  <- aDatabase
      } {
        qequals("FaunaDB rocks", Format("%2$s%3$s %1$s", JSArray("rocks", "Fauna", "DB")), db)
      }
    }

    once("errors") {
      for {
        db  <- aDatabase
      } {
        qassertErr(Format("%Z", ""),
          "invalid argument",
          "Invalid conversion format '%Z'.",
          JSArray("format"),
          db)

        qassertErr(Format("% s", ""),
          "invalid argument",
          "Invalid conversion format '% s'.",
          JSArray("format"),
          db)

        qassertErr(Format("%t", 10),
          "invalid argument",
          "Date or Time expected, Integer provided.",
          JSArray("values", 0),
          db)

        qassertErr(Format("%d %d", 10),
          "invalid argument",
          "format specifiers count must be <= arguments size",
          JSArray("format"),
          db)

        qassertErr(Format("%4$d", 10),
          "invalid argument",
          "index must be >= 1 and <= arguments size",
          JSArray("format"),
          db)
      }
    }
  }
}

class StringFunctionsUnstableSpec extends QueryAPIUnstableSpec {

  "split_str" - {

    once("count param. cannot be zero") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        str2 <- Prop.string
        c <- Prop.char
      } {
        qassertErr(
          SplitStr(str1 + c + str2, c, 0),
          "invalid argument",
          "count must be count > 0",
          JSArray("count"),
          db)
      }
    }

    once("count param. must be positive") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        str2 <- Prop.string
        c <- Prop.char
        count <- Prop.negativeInt
      } {
        qassertErr(
          SplitStr(str1 + c + str2, c, count),
          "invalid argument",
          "count must be count > 0",
          JSArray("count"),
          db)
      }
    }

    once("count param. must be less or equal to 1024") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        str2 <- Prop.string
        c <- Prop.char
        count <- Prop.choose(Range(1025, Int.MaxValue))
      } {
        qassertErr(
          SplitStr(str1 + c + str2, c, count),
          "invalid argument",
          "count must be count <= 1024",
          JSArray("count"),
          db)
      }
    }

    prop("split on a single char") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        str2 <- Prop.string
        c <- Prop.char.filter { c => !str1.contains(c) && !str2.contains(c) }
      } {
        qequals(SplitStr(str1 + c + str2, c), JSArray(str1, str2), db)
      }
    }

    prop("split on a non-empty String") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        str2 <- Prop.string
        token <- Prop.string.filter { c => c != str1 && c != str2 && c.nonEmpty }
      } {
        qequals(SplitStr(str1 + token + str2, token), JSArray(str1, str2), db)
      }
    }

    prop("split on an empty String") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        str2 <- Prop.string
      } {
        // Expect List of Strings terminated by ""
        val expectedChars = Seq.newBuilder[String]
        str1.foreach { c => expectedChars.addOne(c.toString) }
        str2.foreach { c => expectedChars.addOne(c.toString) }
        expectedChars.addOne("")
        qequals(SplitStr(str1 + str2, ""), JSArray(expectedChars.result().map(JSString):_*), db)
      }
    }

    prop("split on a String at the end adds empty value") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        token <- Prop.string.filter { s => !str1.contains(s) && s.nonEmpty }
      } {
        qequals(SplitStr(str1 + token, token), JSArray(str1, ""), db)
      }
    }

    prop("split never produces an array bigger than count") {
      for {
        db <- aDatabase
        str <- Prop.string
        c <- Prop.char
        count <- Prop.choose(Range(1, 1024))
      } {
        runQuery(SplitStr(str, c, count), db) match {
          case a: JSArray if a.value.size > count =>
            fail("result bigger than count")
          case _: JSArray => ()
          case x          => fail(s"Expected JSArray but got $x")
        }
      }
    }

    prop("split on a String with max count returns the complete remainder last") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        token <- Prop.string.filter { _ != str1 }
        count <- Prop.choose(Range(1, 100)) //avoid "Request entity is too large."
      } {
        // input text with (count * 2) potential places to split
        val txt = List.fill(count * 2)(str1 + token).mkString("")
        // beginning of result array with (count - 1) reported split
        val expectedMatched = Seq.fill(count - 1)(JSString(str1))
        // last element of the result array with (count + 1) un-split entries
        val expectedRemaining =
          JSString(List.fill(count + 1)(str1 + token).mkString(""))
        val expected = expectedMatched :+ expectedRemaining
        qequals(SplitStr(txt, token, count), JSArray(expected: _*), db)
      }
    }
  }

  "split_str_regex" - {

    once("count param. cannot be zero") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        r <- Prop.regex
      } {
        qassertErr(
          SplitStrRegex(str1, r.regex, 0),
          "invalid argument",
          "count must be count > 0",
          JSArray("count"),
          db)
      }
    }

    once("count param. must be positive") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        r <- Prop.regex
        count <- Prop.negativeInt
      } {
        qassertErr(
          SplitStrRegex(str1, r.regex, count),
          "invalid argument",
          "count must be count > 0",
          JSArray("count"),
          db)
      }
    }

    once("count param. must be less or equal to 1024") {
      for {
        db <- aDatabase
        str1 <- Prop.string
        r <- Prop.regex
        count <- Prop.choose(Range(1025, Int.MaxValue))
      } {
        qassertErr(
          SplitStrRegex(str1, r.regex, count),
          "invalid argument",
          "count must be count <= 1024",
          JSArray("count"),
          db)
      }
    }

    once("reports invalid regex pattern") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          SplitStrRegex("", "{"),
          "invalid argument",
          "Search pattern /{/ is not a valid regular expression.",
          JSArray("pattern"),
          db)
      }
    }

    prop("never produces an array bigger than count") {
      for {
        db <- aDatabase
        str <- Prop.string
        r <- Prop.regex
        count <- Prop.choose(Range(1, 1024))
      } {
        runQuery(SplitStrRegex(str, r.regex, count), db) match {
          case a: JSArray if a.value.size > count =>
            fail("result bigger than count")
          case _: JSArray => ()
          case x          => fail(s"Expected JSArray but got $x")
        }
      }
    }

    once("finds common patterns") {
      for {
        db <- aDatabase
      } {
        val str = "One 2\n3 Four"
        qequals(SplitStrRegex(str, ".*"), JSArray("", "", "\n", "", ""), db)
        qequals(
          SplitStrRegex(str, "."),
          JSArray("", "", "", "", "", "\n", "", "", "", "", "", ""),
          db)
        qequals(SplitStrRegex(str, "\\d"), JSArray("One ", "\n", " Four"), db)
        qequals(
          SplitStrRegex(str, "\\D"),
          JSArray("", "", "", "", "2", "3", "", "", "", "", ""),
          db)
        qequals(
          SplitStrRegex(str, "\\w"),
          JSArray("", "", "", " ", "\n", " ", "", "", "", ""),
          db)
        qequals(SplitStrRegex(str, "\\W"), JSArray("One", "2", "3", "Four"), db)
        qequals(SplitStrRegex(str, "\\s"), JSArray("One", "2", "3", "Four"), db)
        qequals(
          SplitStrRegex(str, "\\S"),
          JSArray("", "", "", " ", "\n", " ", "", "", "", ""),
          db)
        qequals(SplitStrRegex(str, "(?m)2$"), JSArray("One ", "\n3 Four"), db)
        qequals(SplitStrRegex(str, "2$"), JSArray("One 2\n3 Four"), db)
        qequals(SplitStrRegex(str, "(?m)^3"), JSArray("One 2\n", " Four"), db)
        qequals(SplitStrRegex(str, "\\AOne 2\n3 Four\\Z"), JSArray("", ""), db)
        qequals(SplitStrRegex(str, "^One 2\n3 Four$"), JSArray("", ""), db)
      }
    }

    once("finds common patterns up to count") {
      for {
        db <- aDatabase
      } {
        val str = "One 2\n3 Four"
        qequals(SplitStrRegex(str, ".*", 2), JSArray("", "\n3 Four"), db)
        qequals(SplitStrRegex(str, ".", 2), JSArray("", "ne 2\n3 Four"), db)
        qequals(SplitStrRegex(str, "\\d", 2), JSArray("One ", "\n3 Four"), db)
        qequals(SplitStrRegex(str, "\\D", 2), JSArray("", "ne 2\n3 Four"), db)
        qequals(SplitStrRegex(str, "\\w", 2), JSArray("", "ne 2\n3 Four"), db)
        qequals(SplitStrRegex(str, "\\W", 2), JSArray("One", "2\n3 Four"), db)
        qequals(SplitStrRegex(str, "\\s", 2), JSArray("One", "2\n3 Four"), db)
        qequals(SplitStrRegex(str, "\\S", 2), JSArray("", "ne 2\n3 Four"), db)
        qequals(SplitStrRegex(str, "(?m)2$", 2), JSArray("One ", "\n3 Four"), db)
        qequals(SplitStrRegex(str, "2$", 2), JSArray("One 2\n3 Four"), db)
        qequals(SplitStrRegex(str, "(?m)^3", 2), JSArray("One 2\n", " Four"), db)
        qequals(SplitStrRegex(str, "\\AOne 2\n3 Four\\Z", 2), JSArray("", ""), db)
        qequals(SplitStrRegex(str, "^One 2\n3 Four$", 2), JSArray("", ""), db)
      }
    }
  }

}

class StringFunctions21Spec extends QueryAPI21Spec {
  once("debug format should use api version") {
    for {
      db <- aDatabase
    } {
      qequals(
        """{"@ref":{"id":"my_index","class":{"@ref":{"id":"indexes"}}}}""",
        Format("%@", IdxRefV("my_index")),
        db)

      qequals(
        """{"@ref":{"id":"my_index","class":{"@ref":{"id":"indexes"}}}}""",
        Format("%^", IdxRefV("my_index")),
        db)
    }
  }
}

class StringFunctions30Spec extends QueryAPI3Spec {
  once("debug format should use api version") {
    for {
      db <- aDatabase
    } {
      qequals(
        """{"@ref":{"id":"my_index","class":{"@ref":{"id":"indexes"}}}}""",
        Format("%@", IdxRefV("my_index")),
        db)

      qequals(
        """{"@ref":{"id":"my_index","collection":{"@ref":{"id":"indexes"}}}}""",
        Format("%^", IdxRefV("my_index")),
        db)
    }
  }
}
