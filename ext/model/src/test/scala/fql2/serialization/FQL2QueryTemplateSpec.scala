package fauna.model.test

import fauna.codex.json2.{ JSON, JSONParseException }
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.fql2.serialization.{
  FQL2Query,
  UndecidedValue,
  ValueFormat
}
import fauna.model.runtime.Effect
import fauna.repo.values.Value
import fql.ast.display._
import fql.parser.TemplateSigil.{ Value => ValueSigil }
import org.scalactic.source.Position
import scala.collection.immutable.SeqMap

class FQL2QueryTemplateSpec extends FQL2Spec {
  private def decode(json: String) = {
    val bytes = json.getBytes()
    JSON.parse[FQL2Query](bytes)
  }

  val auth = newAuth

  private def eval0(json: String)(implicit pos: Position) = {
    val bytes = json.getBytes()
    val query = JSON.parse[FQL2Query](bytes)
    val intp = new FQLInterpreter(auth)
    (ctx ! query.parse(intp)) match {
      case Result.Ok((vars, expr)) =>
        val out =
          eval(
            auth,
            expr,
            FQLInterpreter.TypeMode.InferType,
            vars,
            Effect.Write,
            performanceHintsEnabled = false)
        out.res.getOrElse(fail())
      case Result.Err(e) => fail(e.toString)
    }
  }

  private def eval(json: String)(implicit pos: Position): Value =
    eval0(json).value

  private def evalType(json: String)(implicit pos: Position): String =
    eval0(json).ty.display

  private def evalErr(json: String)(implicit pos: Position): String = {
    val bytes = json.getBytes()
    val query =
      try {
        JSON.parse[FQL2Query](bytes)
      } catch {
        case JSONParseException(e) => return e.toString()
      }
    val intp = new FQLInterpreter(auth)
    (ctx ! query.parse(intp)) match {
      case Result.Ok((vars, expr)) =>
        val out =
          eval(
            auth,
            expr,
            FQLInterpreter.TypeMode.CheckOnly,
            vars,
            Effect.Write,
            performanceHintsEnabled = false)
        val err = out.res match {
          case Result.Ok(_)  => fail()
          case Result.Err(e) => e
        }
        err.errors.map { _.renderWithSource(Map.empty) }.mkString("\n")
      case Result.Err(err) =>
        err.errors.map { _.renderWithSource(Map.empty) }.mkString("\n")
    }
  }

  "FQL2Query decodes JSON" - {
    import UndecidedValue._

    "decodes a plain string" in {
      val res = decode("\"hello world\"")
      res.queryString shouldEqual "hello world"
      res.values shouldEqual Nil
    }

    "decodes a list of fql strings" in {
      val res = decode("""{"fql": ["hello", "world"]}""")
      res.queryString shouldEqual "helloworld"
      res.values shouldEqual Nil
    }

    "decodes a value between fql strings" in {
      val res = decode("""{"fql": ["'hello ' +", { "value": "world" }]}""")
      res.queryString shouldEqual s"'hello ' +$ValueSigil"
      res.values shouldEqual Seq(Str("world") -> 10)
    }

    "decodes nested fql strings" in {
      val res =
        decode("""{"fql": ["hello ", { "fql": ["foo ", "bar"] }, " world"]}""")
      res.queryString shouldEqual "hello foo bar world"
      res.values shouldEqual Nil
    }

    "decodes example query" in {
      val res = decode(
        """{ "fql": ["Author.all().where(.name ==", { "value": "Alice" }, ")"] }""")
      res.queryString shouldEqual s"Author.all().where(.name ==$ValueSigil)"
      res.values shouldEqual Seq(Str("Alice") -> 27)
    }

    "value-converts array if possible" in {
      val res1 = decode("""{ "array": ["1", "2", "3"] }""")
      res1.queryString shouldEqual "[(1), (2), (3)]"
      res1.values shouldEqual Nil

      val res2 = decode(
        """{ "array": [{ "value": "1" }, { "value": "2" }, { "value": "3" }] }""")
      res2.queryString shouldEqual ValueSigil
      res2.values shouldEqual Seq(Array(Seq(Str("1"), Str("2"), Str("3"))) -> 0)

      val res3 = decode("""{ "array": [{ "value": "1" }, "2", { "value": "3" }] }""")
      res3.queryString shouldEqual s"[$ValueSigil, (2), $ValueSigil]"
      res3.values shouldEqual Seq(Str("1") -> 1, Str("3") -> 15)

      val res4 = decode("""{ "array": [
        "1",
        { "array": [{ "value": "1" }, { "value": "2" }, { "value": "3" }] },
        "3"
      ] }""")
      res4.queryString shouldEqual s"[(1), $ValueSigil, (3)]"
      res4.values shouldEqual Seq(Array(Seq(Str("1"), Str("2"), Str("3"))) -> 6)
    }

    "value-converts object if possible" in {
      val res1 = decode("""{ "object": { "a": "1", "b": "2", "c": "3" } }""")
      res1.queryString shouldEqual """{ "a": (1), "b": (2), "c": (3) }"""
      res1.values shouldEqual Nil

      val res2 = decode(
        """{ "object": { "a": { "value": "1" }, "b": { "value": "2" }, "c": { "value": "3" } } }""")
      res2.queryString shouldEqual ValueSigil
      res2.values shouldEqual Seq(
        Object(SeqMap("@object" -> Object(
          SeqMap("a" -> Str("1"), "b" -> Str("2"), "c" -> Str("3"))))) -> 0)

      val res3 = decode(
        """{ "object": { "a": { "value": "1" }, "b": "2", "c": { "value": "3" } } }""")
      res3.queryString shouldEqual s"""{ "a": $ValueSigil, "b": (2), "c": $ValueSigil }"""
      res3.values shouldEqual Seq(Str("1") -> 7, Str("3") -> 31)

      val res4 = decode("""{ "object": {
        "a": "1",
        "b": { "object": { "a": { "value": "1" }, "b": { "value": "2" }, "c": { "value": "3" } } },
        "c": "3"
      } }""")
      res4.queryString shouldEqual s"""{ "a": (1), "b": $ValueSigil, "c": (3) }"""
      res4.values shouldEqual Seq(
        Object(SeqMap("@object" -> Object(
          SeqMap("a" -> Str("1"), "b" -> Str("2"), "c" -> Str("3"))))) -> 17)
    }

    "disallows duplicate keys" in {
      val ex1 = the[JSONParseException] thrownBy {
        decode("""{ "object": { "a": "1", "a": "2" } }""")
      }
      ex1.getMessage shouldEqual "Duplicate object field `a`"

      val ex2 = the[JSONParseException] thrownBy {
        decode("""{ "object": { "a": { "value": "1" }, "a": { "value": "2" } } }""")
      }
      ex2.getMessage shouldEqual "Duplicate object field `a`"
    }

    "decodes nested keys with @ in the replace with value path" in {
      val res0 = decode(
        """{ "fql": ["'hi' + ", { "object": { "@ref": { "value": "1234" } } }] }""")
      val intp = new FQLInterpreter(auth)
      val value = ctx ! res0
        .values(0)
        ._1
        .toValue(intp, ValueFormat.Tagged)

      value shouldBe Right(Value.Struct("@ref" -> Value.Str("1234")))
    }
  }

  "FQL2Query evals values" - {
    "evals a plain string" in {
      eval("\"'foo'\"") shouldEqual Value.Str("foo")
    }

    "evals a list of fql strings" in {
      eval("""{"fql": ["'foo'", ".length"]}""") shouldEqual Value.Int(3)
    }

    "evals a value between fql strings" in {
      eval("""{"fql": ["'hello ' +", { "value": "world" }]}""") shouldEqual Value
        .Str("hello world")
    }

    "evals nested fql strings" in {
      eval("""{"fql": ["2 +", { "fql": ["3 +", "4 +"] }, "5"]}""") shouldEqual Value
        .Int(2 + 3 + 4 + 5)
    }

    "evals example query" in {
      evalOk(auth, "Collection.create({ name: 'Author' })")
      evalOk(auth, "Author.create({ name: 'Alice' })")
      evalOk(auth, "Author.create({ name: 'Bob' })")
      evalOk(auth, "Author.create({ name: 'Carol' })")
      eval(
        """{ "fql": ["(Author.all().where(.name ==", { "value": "Alice" }, ") { name }).toArray()"] }""") shouldEqual Value
        .Array(Value.Struct("name" -> Value.Str("Alice")))
    }

    "evals arrays" in {
      eval("""{ "array": ["2", "3"] }""") shouldEqual Value.Array(
        Value.Int(2),
        Value.Int(3))
      eval(
        """{ "array": ["1 + 1", { "value": { "@int": "3" } }] }""") shouldEqual Value
        .Array(Value.Int(2), Value.Int(3))
    }

    "evals objects" in {
      eval("""{ "object": { "a": "2", "b": "3" } }""") shouldEqual Value.Struct(
        "a" -> Value.Int(2),
        "b" -> Value.Int(3)
      )
      eval(
        """{ "object": { "a": "1 + 1", "b": { "value": { "@int": "3" } } } }""") shouldEqual Value
        .Struct(
          "a" -> Value.Int(2),
          "b" -> Value.Int(3)
        )

      // preserves order
      eval(
        (1 to 1000)
          .map(i => "\"" + i + "\": \"1\"")
          .mkString("""{ "object": {""", ", ", "} }")) shouldEqual {
        Value.Struct((1 to 1000).map(i => i.toString -> Value.Int(1)): _*)
      }
    }

    "object fields are escaped" in {
      val nulEscaped = "\\u0000"
      eval(s"""{ "object": { "$nulEscaped": "2" } }""") shouldEqual Value.Struct(
        "\u0000" -> Value.Int(2))
      val quoteEscaped = "\\\""
      eval(s"""{ "object": { "$quoteEscaped": "2" } }""") shouldEqual Value.Struct(
        "\"" -> Value.Int(2))
    }

    "query can be a single value" in {
      eval("""{ "value": { "@int": "5" } }""") shouldEqual Value.Int(5)
    }
  }

  "FQL2Query renders errors" - {
    "renders a normal error" in {
      evalErr("\"'foo'.bar\"") shouldEqual
        """|error: Type `String` does not have field `bar`
           |at *query*:1:7
           |  |
           |1 | 'foo'.bar
           |  |       ^^^
           |  |""".stripMargin
    }

    "renders an with a templated value" in {
      evalErr("""{ "fql": [{ "value": "foo" }, ".bar!"] }""") shouldEqual
        """|error: Type `String` does not have field `bar`
           |at *query*:1:9
           |  |
           |1 | <value>.bar!
           |  |         ^^^
           |  |""".stripMargin
    }

    "renders invalid value errors at the correct location" in {
      evalErr(
        """{ "fql": [{ "value": { "@blah": "foo" } }, ".bar!"] }""") shouldEqual
        """|error: invalid tag '@blah'
           |at *query*:1:1
           |  |
           |1 | <value>.bar!
           |  | ^^^^^^^
           |  |""".stripMargin
    }

    "renders errors with array" in {
      evalErr("""{ "array": [{ "value": "foo" }, "2.bar"] }""") shouldEqual
        """|error: Type `Int` does not have field `bar`
           |at *query*:1:14
           |  |
           |1 | [<value>, (2.bar)]
           |  |              ^^^
           |  |""".stripMargin
    }

    "renders errors with object" in {
      evalErr(
        """{ "object": { "a": { "value": "foo" }, "b": "2.bar" } }""") shouldEqual
        """|error: Type `Int` does not have field `bar`
           |at *query*:1:25
           |  |
           |1 | { "a": <value>, "b": (2.bar) }
           |  |                         ^^^
           |  |""".stripMargin
    }

    "error for any NUL characters in the query source" in {
      val json = s"""{ "fql": [ { "value": { "@int": "5" } }, ".foo\\u0000" ] }"""
      val error = "FQL strings cannot contain NUL characters"
      evalErr(json) shouldEqual error
    }
  }

  "typechecks query arguments correctly" in {
    val auth = newDB
    evalOk(auth, "Collection.create({ name: 'Author' })")
    evalOk(auth, "Author.create({ id: '1234' })")

    evalType("""{ "value": "foo" }""") shouldEqual "\"foo\""
    evalType("""{ "value": { "@int": "5" } }""") shouldEqual "5"
    evalType("""{ "value": { "@double": "5.5" } }""") shouldEqual "5.5"
    evalType("""{ "value": true }""") shouldEqual "Boolean"
    evalType("""{ "value": null }""") shouldEqual "Null"
    evalType(
      """{ "value": { "@time": "2022-02-02T02:02:02Z" } }""") shouldEqual "Time"
    evalType("""{ "value": { "@date": "2022-02-02" } }""") shouldEqual "Date"
    evalType("""{ "value": { "foo": "hi" } }""") shouldEqual "{ foo: \"hi\" }"
    evalType("""{ "value": ["foo", "bar"] }""") shouldEqual "[\"foo\", \"bar\"]"
    evalType("""{ "value": { "@mod": "Author" } }""") shouldEqual "AuthorCollection"
    evalType(
      """{ "value": { "@doc": { "coll": { "@mod": "Author" }, "id": "1234" } } }""") shouldEqual "Author"
    evalType(
      """{ "value": { "@doc": { "coll": { "@mod": "Collection" }, "name": "Author" } } }""") shouldEqual "CollectionDef"

    evalType("""{ "value": { "@double": "NaN" } }""") shouldEqual "Double"
    evalType("""{ "value": { "@double": "Infinity" } }""") shouldEqual "Double"
    evalType("""{ "value": { "@double": "-Infinity" } }""") shouldEqual "Double"
  }
}
