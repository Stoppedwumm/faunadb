package fauna.model.test

import fauna.auth.NullPermissions
import fauna.flags.EvalV4FromV10
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.serialization.FQL2Query
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.repo.values.{ Value, ValueType }
import fql.ast.{ Expr, Name, Span, Src }
import fql.error.{ Hint, TypeError }
import java.util.concurrent.atomic.LongAdder
import scala.collection.immutable.{ ArraySeq, SeqMap }

class FQL2InterpreterSpec extends FQL2WithV4Spec {
  import Hint.HintType
  import Result._

  "FQLInterpreter" - {
    "successfully evaluates a valid query string" in { pending }

    "successfully evaluates a valid query template" in { pending }

    "returns a parse error referencing the invalid source" in { pending }
    "returns a runtime error referencing the invalid source" in { pending }
    "returns a runtime error with a stack trace" - {
      "when the error is in lambda" in { pending }
      "when the error is in a user function" in { pending }
    }

    "effects" - {
      "read limit still allows reads" in {
        val auth = newAuth

        evalV4Ok(
          auth,
          CreateFunction(
            MkObject(
              "name" -> "foo",
              "body" -> QueryF(Lambda("x" -> Paginate(Documents(ClassesRef)))))))

        // takes advantage of the fact that there is one function, so map body is
        // executed once.
        evalOk(
          auth,
          "Function.all().map(_ => { foo(1); 1}).toArray()") shouldEqual Value.Array(
          Value.Number(1))
      }
    }

    "syntax" - {
      val auth = newAuth

      "if" - {
        "returns then block if pred is true" in {
          evalOk(auth, "if (true) 2") shouldEqual Value.Int(2)
        }

        "returns `null` if pred is false" in {
          evalOk(auth, "if (false) 2") shouldBe a[Value.Null]
        }

        "if(null) is disallowed" in {
          evalErr(
            auth,
            "if (null) 2",
            typecheck = false) shouldEqual QueryRuntimeFailure
            .InvalidType(
              ValueType.BooleanType,
              Value.Null(Span.Null),
              FQLInterpreter.StackTrace(Seq(Span(4, 8, Src.Query("")))))
          evalErr(
            auth,
            "if (null) 2 else 3",
            typecheck = false) shouldEqual QueryRuntimeFailure
            .InvalidType(
              ValueType.BooleanType,
              Value.Null(Span.Null),
              FQLInterpreter.StackTrace(Seq(Span(4, 8, Src.Query("")))))
        }

        "variables defined inside of if block can't be referenced outside of it" in {
          evalErr(
            auth,
            """|{
               |  let test = if (true) {
               |    let x = "hello"
               |    x
               |  }
               |  x
               |}""".stripMargin,
            typecheck = false
          ) shouldEqual QueryRuntimeFailure.UnboundVariable(
            "x",
            FQLInterpreter.StackTrace(Seq(Span(59, 60, Src.Query("")))))
        }
        "variables defined inside of else block can't be referenced outside of it" in {
          evalErr(
            auth,
            """|{
               |  let test = if (false) {
               |    "hey"
               |  } else {
               |    let x = "hello"
               |    x
               |  }
               |  x
               |}""".stripMargin,
            typecheck = false
          ) shouldEqual QueryRuntimeFailure.UnboundVariable(
            "x",
            FQLInterpreter.StackTrace(Seq(Span(81, 82, Src.Query("")))))
        }

        "returns QueryRuntimeFailure type_mismatch when pred doesn't evaluate to a boolean" in {
          val err = evalErr(auth, "if (2) 2", typecheck = false)
          err shouldBe a[QueryRuntimeFailure]
          err.code shouldEqual ("type_mismatch")
        }
      }

      "if-else" - {
        "returns then block if pred is true" in {
          evalOk(auth, "if (true) 2 else 3") shouldEqual Value.Int(2)
        }
        "returns else block if pred is false" in {
          evalOk(auth, "if (false) 2 else 3") shouldEqual Value.Int(3)
        }
        "variables defined inside of if block can't be referenced inside else block" in {
          evalErr(
            auth,
            """|{
               |  if (false) {
               |    let x = "hello"
               |    x
               |  } else {
               |    x
               |  }
               |}""".stripMargin,
            typecheck = false
          ) shouldEqual QueryRuntimeFailure.UnboundVariable(
            "x",
            FQLInterpreter.StackTrace(Seq(Span(58, 59, Src.Query("")))))
        }
        "values returned from if else expressions can be used in variables" in {
          evalOk(
            auth,
            """|{
               |  let x = "boom"
               |  let test = if (false) {
               |    x
               |  } else {
               |    "pow"
               |  }
               |
               |  test.concat(" day")
               |}""".stripMargin
          ) shouldEqual Value.Str("pow day")
        }
        "returns QueryRuntimeFailure type_mismatch when pred doesn't evaluate to a boolean" in {
          val err = evalErr(auth, "if (2) 2 else 2", typecheck = false)
          err shouldBe a[QueryRuntimeFailure]
          err.code shouldEqual ("type_mismatch")
        }
      }

      "?? operator" - {
        "returns left side if not null" in {
          evalOk(auth, "1 ?? 10") shouldEqual Value.Int(1)
          evalOk(auth, "1 ?? 1 == 1") shouldEqual Value.Int(1)
          evalOk(auth, "1 == 1 ?? 1 != 1") shouldEqual Value.True
        }

        "returns right side if left side is null" in {
          evalOk(auth, "null ?? 10") shouldEqual Value.Int(10)
          evalOk(auth, "let x = () => null; x() ?? 10") shouldEqual Value.Int(10)
        }
      }

      "== operator compares objects" in {
        evalOk(auth, "{ a: 2 } == { a: 2 }") shouldEqual Value.True
        evalOk(auth, "{ a: 2 } == { a: 3 }") shouldEqual Value.False
      }

      "can compare partials" in {
        evalOk(
          auth,
          "Collection.create({ name: 'Author', indexes: { byName: { values: [{ field: 'name' }] } } })")
        evalOk(auth, "Author.create({ name: 'Alice', extra: 3 })")
        evalOk(auth, "Author.create({ name: 'Bob', extra: 4 })")
        evalOk(auth, "Author.create({ name: 'Carol', extra: 5 })")

        evalOk(
          auth,
          "Author.byName().map(v => v.data == v.data).first()") shouldBe Value.True
        evalOk(
          auth,
          "Author.byName().map(.data == { name: 'Alice', extra: 3 }).first()") shouldBe Value.True
        evalOk(auth, "Author.byName().map(.data == {}).first()") shouldBe Value.False
        evalOk(auth, "Author.byName().map(.data > {}).first()") shouldBe Value.True

        evalOk(
          auth,
          "Author.byName().map(v => [v.data] == [v.data]).first()") shouldBe Value.True
        evalOk(
          auth,
          "Author.byName().map(v => [v.data] == [{ name: 'Alice', extra: 3 }]).first()") shouldBe Value.True
        evalOk(
          auth,
          "Author.byName().map(v => [v.data] == []).first()") shouldBe Value.False

        evalOk(
          auth,
          "Author.byName().map(v => { foo: v.data } == { foo: v.data }).first()") shouldBe Value.True
        evalOk(
          auth,
          "Author.byName().map(v => { foo: v.data } == { foo: { name: 'Alice', extra: 3 } }).first()") shouldBe Value.True
        evalOk(
          auth,
          "Author.byName().map(v => { foo: v.data } == {}).first()") shouldBe Value.False
      }

      "lambda & lambda apply" - {
        "lambdas can be created" in {
          evalOk(auth, "x => x") shouldEqual Value.Lambda(
            ArraySeq(Some("x")),
            None,
            Expr.Id("x", Span(5, 6, Src.Query(""))),
            Map())
        }

        "variadic lambdas can be created" in {
          evalOk(auth, "(a, ...x) => x") shouldEqual Value.Lambda(
            ArraySeq(Some("a")),
            Some(Some("x")),
            Expr.Id("x", Span(13, 14, Src.Query(""))),
            Map())
          evalOk(auth, "(a, ..._) => a") shouldEqual Value.Lambda(
            ArraySeq(Some("a")),
            Some(None),
            Expr.Id("a", Span(13, 14, Src.Query(""))),
            Map())
        }

        "lambdas can be applied" in {
          evalOk(auth, "(x => x)(2)") shouldEqual Value.Int(2)
        }

        "variadic lambdas can be applied" in {
          evalOk(auth, "((x, ...xs) => x + xs.length)(1)") shouldEqual Value.Int(1)
          evalOk(auth, "((x, ...xs) => x + xs.length)(1, 0)") shouldEqual Value.Int(
            2)

          evalOk(auth, "((...xs) => xs.length)()") shouldEqual Value.Int(0)
          evalOk(auth, "((...xs) => xs.length)(0)") shouldEqual Value.Int(1)
          evalOk(auth, "((...xs) => xs.length)(0, 0)") shouldEqual Value.Int(2)
          evalOk(auth, "((...xs) => xs.length)(0, 0, 0)") shouldEqual Value.Int(3)
        }

        "lambdas close over variables" in {
          evalOk(
            auth,
            """|{
               |  let returnTwo = {
               |    let two = 2
               |    x => two
               |  }
               |  returnTwo(3)
               |}""".stripMargin
          ) shouldEqual Value.Int(2)
        }
        "lambda preserves variable values at definition time" in {
          evalOk(
            auth,
            """|{
               |  let two = 2
               |  let returnTwo = () => {
               |    two
               |  }
               |  let two = 1
               |  returnTwo()
               |}""".stripMargin
          ) shouldEqual Value.Int(2)
        }
        "variables defined in a lambda can't be referenced outside the lambda" in {
          evalErr(
            auth,
            """|{
               |  let returnTwo = () => {
               |    let x = 5
               |    x
               |  }
               |  x
               |}""".stripMargin,
            typecheck = false
          ) shouldEqual QueryRuntimeFailure.UnboundVariable(
            "x",
            FQLInterpreter.StackTrace(Seq(Span(54, 55, Src.Query("")))))
        }

        "short lambdas can be created" in {
          evalOk(auth, "(.length)") shouldEqual Value.Lambda(
            ArraySeq(Some(Expr.This.name)),
            None,
            Expr.MethodChain(
              Expr.This(Span(1, 1, Src.Query(""))),
              Seq(
                Expr.MethodChain.Select(
                  Span(1, 2, Src.Query("")),
                  Name("length", Span(2, 8, Src.Query(""))),
                  false
                )
              ),
              Span(1, 8, Src.Query(""))
            ),
            Map()
          )
        }
        "short lambdas can be evaluated" in {
          evalOk(
            auth,
            """|{
               |  (.length)("hi")
               |}""".stripMargin
          ) shouldEqual Value.Int(2)
        }
        "short lambdas close over variables" in {
          evalOk(
            auth,
            """|{
               |  let getMessage = {
               |    let x = " hello"
               |    (.concat(x))
               |  }
               |  getMessage("hi")
               |}""".stripMargin
          ) shouldEqual Value.Str("hi hello")
        }
        "short lambdas preserve variable values at definition time" in {
          evalOk(
            auth,
            """|{
               |  let x = " hello"
               |  let concatHello = (.concat(x))
               |  let x = " goodbye"
               |  concatHello("hi")
               |}""".stripMargin
          ) shouldEqual Value.Str("hi hello")
        }
      }

      "blocks and let" - {
        "variables can be defined at the top level" in {
          evalOk(
            auth,
            """|
               |let x = 3
               |x
               |""".stripMargin
          ) shouldEqual Value.Int(3)
        }

        "return value of last expr" in {
          evalOk(
            auth,
            """|{
               |  1
               |  2
               |  3
               |}
               |""".stripMargin
          ) shouldEqual Value.Int(3)
        }

        "can refer to variables defined previously" in {
          evalOk(
            auth,
            """|{
               |  let x = "foo"
               |  x
               |}
               |""".stripMargin
          ) shouldEqual Value.Str("foo")
        }
        "cannot refer to variables before they have been defined" in {
          evalErr(
            auth,
            """|{
               |  x
               |  let x = "foo"
               |  2
               |}
               |""".stripMargin,
            typecheck = false
          ) shouldEqual QueryRuntimeFailure.UnboundVariable(
            "x",
            FQLInterpreter.StackTrace(Seq(Span(4, 5, Src.Query("")))))
        }
      }

      "str-template" - {
        "correctly evaluates expressions in string template" in {
          evalOk(
            auth,
            """|{
              |  let a = "world"
              |  let b = 42
              |  let c = 42.3
              |  "hello #{a} I am #{b} well technically #{c}"
              |}
              |""".stripMargin
          ) shouldEqual Value.Str("hello world I am 42 well technically 42.3")
        }
        "correctly prints an array" in {
          evalOk(
            auth,
            """|{
              |  let a = ["world", 1, if(true) 2, "hello"]
              |  "This is an array: #{a}"
              |}
              |""".stripMargin
          ) shouldEqual Value.Str("This is an array: [\"world\", 1, 2, \"hello\"]")
        }
        "correctly prints an object" in {
          evalOk(
            auth,
            """|{
              |  let a = {world: 1, hello: 0}
              |  "This is an object: #{a}"
              |}
              |""".stripMargin
          ) shouldEqual Value.Str("This is an object: { world: 1, hello: 0 }")

        }
        "returns a QueryFailure if any of the expressions in the string fail" in {
          evalErr(
            auth,
            """|{
              |  let a = "world"
              |  let c = 42.3
              |  "hello #{a} I am #{b} well technically #{c}"
              |}
              |""".stripMargin
          ) shouldBe a[QueryFailure]
        }
        "enforces a limit on result size" in {
          val limit = stdlib.StringPrototype.MaxSize
          val as = "a" * (limit / 2)
          evalErr(
            auth,
            s"""|
               |let as = "$as"
               |"#{as}b#{as}"
               |""".stripMargin
          ) shouldEqual QueryRuntimeFailure.ValueTooLarge(
            s"string size ${limit + 1} exceeds limit $limit",
            FQLInterpreter.StackTrace(Seq(Span(8388621, 8388634, Src.Query((""))))))
        }

        "un-indent to the smallest indented line" in {
          // interpolated version
          evalOk(
            auth,
            """|<<+STR
               |  a multiline
               |    with different indentation
               |      plus #{'interpolation'}
               |STR""".stripMargin
          ) shouldBe Value.Str("""|a multiline
               |  with different indentation
               |    plus interpolation
               |""".stripMargin)

          evalOk(
            auth,
            """|<<+STR
               |  #{'interpolation'}
               |    after spaces
               |STR""".stripMargin
          ) shouldBe Value.Str("""|interpolation
                                  |  after spaces
                                  |""".stripMargin)

          evalOk(
            auth,
            """|<<+STR
               |#{'interpolation'}
               |    in the beginning
               |STR""".stripMargin
          ) shouldBe Value.Str("""|interpolation
                                  |    in the beginning
                                  |""".stripMargin)

          // non-interpolated version
          evalOk(
            auth,
            """|<<-STR
               |  a multiline
               |    with different indentation
               |STR""".stripMargin
          ) shouldBe Value.Str("""|a multiline
                                  |  with different indentation
                                  |""".stripMargin)
        }

        "un-indent accepts whitespaces between tokens" in {
          // interpolated version
          evalOk(
            auth,
            s"""|  \t  <<+  \t  STR  \t  \t
                |      a multiline string
                |  \t  STR""".stripMargin
          ) shouldBe Value.Str("a multiline string\n")

          // non-interpolated version
          evalOk(
            auth,
            s"""|  \t  <<-  \t  STR  \t  \t
                |      a multiline string
                |  \t  STR""".stripMargin
          ) shouldBe Value.Str("a multiline string\n")
        }

        "multiline string inside block" in {
          // interpolated version
          evalOk(
            auth,
            """|let x = {
               |  let str = <<+STR
               |    a multiline
               |    string
               |  STR
               |
               |  str
               |}
               |
               |x
               |""".stripMargin
          ) shouldBe Value.Str("a multiline\nstring\n")

          // non-interpolated version
          evalOk(
            auth,
            """|let x = {
               |  let str = <<-STR
               |    a multiline
               |    string
               |  STR
               |
               |  str
               |}
               |
               |x
               |""".stripMargin
          ) shouldBe Value.Str("a multiline\nstring\n")
        }

        "multiline string inside lambda" in {
          // interpolated version
          evalOk(
            auth,
            """|let lambda = () => {
               |  <<+STR
               |    a multiline
               |    string
               |  STR
               |}
               |
               |lambda()
               |""".stripMargin
          ) shouldBe Value.Str("a multiline\nstring\n")

          // non-interpolated version
          evalOk(
            auth,
            """|let lambda = () => {
               |  <<-STR
               |    a multiline
               |    string
               |  STR
               |}
               |
               |lambda()
               |""".stripMargin
          ) shouldBe Value.Str("a multiline\nstring\n")
        }

        "multiline string on argument" in {
          // interpolated version
          evalOk(
            auth,
            """|let lambda = arg => arg
               |
               |lambda(<<+STR
               |  a multiline
               |  string
               |STR
               |)
               |""".stripMargin
          ) shouldBe Value.Str("a multiline\nstring\n")

          // non-interpolated version
          evalOk(
            auth,
            """|let lambda = arg => arg
               |
               |lambda(<<-STR
               |  a multiline
               |  string
               |STR
               |)
               |""".stripMargin
          ) shouldBe Value.Str("a multiline\nstring\n")
        }
      }

      "array" - {
        "returns evaluated array" in {
          val arr = evalOk(
            auth,
            """|
              |{
              |  let a = true
              |  [1, "hi #{a}", if (a) { 2 } else { 4 }, 3]
              |}
              |
              |""".stripMargin
          ).to[Value.Array]

          arr.elems should contain.inOrderOnly(
            Value.Int(1),
            Value.Str("hi true"),
            Value.Int(2),
            Value.Int(3))
        }
        "returns a QueryFailure if an expression in the array fails" in {
          evalErr(
            auth,
            """|
              |{
              |  [1, 2, 3, if (2) { 2 }, 4, 5, 6]
              |}
              |
              |""".stripMargin
          ) shouldBe a[QueryFailure]
        }
      }

      "object" - {
        "returns an evaluated struct" in {
          val obj = evalOk(
            auth,
            """|
               |{
               |  let a = true
               |  {keyone: 1, keytwo: "hi #{a}", keythree: if (a) { 2 } else { 4 }}
               |}
               |
               |""".stripMargin
          ).to[Value.Struct.Full]
          val fields = obj.fields
          fields.size shouldEqual 3
          fields should contain.allOf(
            ("keyone" -> Value.Int(1)),
            ("keytwo" -> Value.Str("hi true")),
            ("keythree" -> Value.Int(2))
          )
        }
        "returns an empty struct" in {
          val obj = evalOk(
            auth,
            """|
               |{
               |  {}
               |}
               |
               |""".stripMargin
          ).to[Value.Struct.Full]
          obj.fields shouldBe empty
        }
        "can successfully reference an object field" in {
          evalOk(
            auth,
            """|
               |{
               |  let obj = {foo: "bar", hello: "world"}
               |  obj.hello
               |}
               |
               |""".stripMargin
          ).to[Value.Str] shouldEqual Value.Str("world")
        }
        "returns null when a non-existent field is accessed" in {
          val nonExistentFieldRef = evalOk(
            auth,
            """|
               |{
               |  let obj = {foo: "bar", hello: "yolo"}
               |  obj.whoami
               |}
               |
               |""".stripMargin,
            typecheck = false
          ).to[Value.Null]
          nonExistentFieldRef.cause shouldBe a[Value.Null.Cause.MissingField]
          // TODO: better cause validation here
          nonExistentFieldRef.cause
            .asInstanceOf[Value.Null.Cause.MissingField]
            .field
            .str shouldEqual "whoami"
        }
        "supports nested structs" in {
          val objFields = evalOk(
            auth,
            """|
               |{
               |  {foo: "bar", hello: { nested: "struct" }}
               |}
               |
               |""".stripMargin
          ).to[Value.Struct.Full].fields
          objFields.size shouldEqual 2
          objFields should contain.allOf(
            ("foo" -> Value.Str("bar")),
            ("hello" -> Value.Struct(SeqMap(("nested", Value.Str("struct")))))
          )
        }
        "returns a QueryFailure if one of the expressions fails" in {
          evalErr(
            auth,
            """|
               |{
               |  {foo: "bar", hello: "#{nonExistentVar}"}
               |}
               |
               |""".stripMargin
          ) shouldBe a[QueryFailure]
        }
      }

      "field access" - {
        "can access fields on a string" in {
          evalOk(auth, "'foo'.length") shouldEqual Value.Int(3)
        }
        "returns null when the field does not exist" in {
          val Value.Null(clause) =
            evalOk(auth, "'foo'.notafield", typecheck = false).to[Value.Null]
          clause shouldBe a[Value.Null.Cause.MissingField]

          val Value.Null.Cause.MissingField(field, name) = clause
          field shouldBe a[Value.Str]
          field.asInstanceOf[Value.Str].value shouldEqual "foo"
          name.str shouldEqual "notafield"
        }
        "error on null" in {
          evalErr(auth, "null.foo", typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `foo` on null.",
                  Span(5, 8, _),
                  Seq()) =>
          }

          evalErr(
            auth,
            """|let foo = null
               |foo.bar""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `bar` on null.",
                  Span(19, 22, _),
                  Seq()) =>
          }
        }
      }

      "! operator" - {
        "chain multiple operators" in {
          evalOk(
            auth,
            """|let foo = {bar: [() => null]}
               |
               |foo!.bar![0]!()
               |""".stripMargin) shouldBe a[Value.Null]
        }

        "chains with ?. correctly" in {
          evalErr(
            auth,
            """|let foo = {}
               |
               |foo?.bar?.baz!
               |""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to missing field `bar`",
                  Span(24, 28, _),
                  Seq()) =>
          }
        }

        "null literal" in {
          evalErr(auth, """null!""".stripMargin) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null literal value",
                  Span(0, 5, _),
                  Seq()) =>
          }

          evalErr(
            auth,
            """|let foo = () => null
               |
               |foo()!
               |""".stripMargin) should matchPattern {
            case QueryRuntimeFailure(
                  "null_value",
                  "Null literal value",
                  FQLInterpreter.StackTrace(Seq(Span(25, 28, _))),
                  Seq(
                    Hint(
                      "Null value created here",
                      Span(16, 20, _),
                      None,
                      HintType.General)),
                  Seq(),
                  None
                ) =>
          }
        }

        "missing field" in {
          evalErr(
            auth,
            """|let foo = {}
               |
               |foo.bar!
               |""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to missing field `bar`",
                  Span(18, 22, _),
                  Seq()) =>
          }

          evalErr(
            auth,
            """|let foo = {}
               |let bar = foo.bar
               |
               |bar!
               |""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to missing field `bar`",
                  Span(32, 36, _),
                  Seq()) =>
          }
        }

        "doc not found" in {
          val auth = newAuth

          evalOk(
            auth,
            """|Collection.create({name: "Book"})
               |Collection.create({name: "Author"})
               |""".stripMargin)

          // access non existent document
          evalErr(auth, "Author.byId('1')!") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "document_not_found",
                  "Collection `Author` does not contain document with id 1.",
                  Span(11, 17, _),
                  Seq()) =>
          }

          // creates an author
          val author = evalOk(
            auth,
            """|Author.create({name: "Alfred V. Aho"})
               |""".stripMargin).to[Value.Doc].id

          // ?? operator returns RHS if LHS doc does not exist.
          evalOk(auth, "Author.byId('0') ?? 1") shouldBe Value.Number(1)

          // ?? returns LHS if LHS doc exists.
          evalOk(
            auth,
            s"Author.byId('${author.subID.toLong}') ?? 0") should matchPattern {
            case Value.Doc(_, _, _, _, _) =>
          }

          // creates a book that references the author
          val book = evalOk(
            auth,
            s"""|Book.create({
                |  title: "Compilers",
                |  author: Author.byId("${author.subID.toLong}")!
                |})
                |""".stripMargin
          ).to[Value.Doc].id

          // deletes the author, leaving the book pointing to a deleted reference
          evalOk(auth, s"""Author.byId("${author.subID.toLong}")!.delete()""")

          val msg =
            s"Collection `Author` does not contain document with id ${author.subID.toLong}."
          evalErr(
            auth,
            s"""|let book = Book.byId("${book.subID.toLong}")!
                |
                |book.author!
                |""".stripMargin) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "document_not_found",
                  `msg`,
                  Span(50, 57, _),
                  Seq()) =>
          }
        }

        "empty set" in {
          val auth = newAuth

          evalOk(auth, "Collection.create({ name: 'Person' })")

          evalErr(auth, "Person.all().first()!") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to empty set",
                  Span(18, 21, _),
                  Seq()) =>
          }
        }

        "definition not found" in {
          val auth = newAuth

          evalErr(auth, """Collection.byName("Person")!""") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "document_not_found",
                  "Collection `Person` not found.",
                  Span(17, 28, _),
                  Seq()) =>
          }

          evalErr(auth, """AccessProvider.byName("auth0")!""") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to permission denied",
                  Span(21, 31, _),
                  Seq()) =>
          }
        }

        "permission denied" in {
          val auth = newAuth

          evalOk(
            auth,
            """|Collection.create({name: "Person"})
               |""".stripMargin)

          val person =
            evalOk(auth, """Person.create({name: "John Doe"})""").to[Value.Doc]

          evalErr(
            auth.withPermissions(NullPermissions),
            s"""Person.byId('${person.id.subID.toLong}')!"""
          ) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null value, due to permission denied",
                  Span(11, 34, _),
                  Seq()) =>
          }
        }
      }

      "?. operator" - {
        "access field" in {
          evalOk(
            auth,
            """|let foo = {bar: "baz"}
               |
               |foo?.bar
               |""".stripMargin) shouldBe Value.Str("baz")
        }

        "chain multiple operators" in {
          evalOk(
            auth,
            """|let foo = {bar: [() => null]}
               |
               |foo!.bar![0]!()?.baz
               |""".stripMargin) shouldBe a[Value.Null]
        }

        "short-circuit on nulls" in {
          evalOk(auth, "null?.foo") shouldBe a[Value.Null]
          evalOk(auth, "null?.foo()") shouldBe a[Value.Null]
          evalOk(auth, "null?.foo[0]") shouldBe a[Value.Null]

          evalOk(auth, "{ foo: { bar: null } }.foo.bar?.baz") shouldBe a[Value.Null]
        }

        "short-circuit function calls and index access" in {
          evalOk(auth, "null?.()") shouldBe a[Value.Null]
          evalOk(auth, "null?.[0]") shouldBe a[Value.Null]

          evalOk(
            auth,
            """|let foo = { bar: null }
               |
               |foo.bar?.("bar")
               |""".stripMargin,
            typecheck = false) shouldBe a[Value.Null]

          evalOk(
            auth,
            """|let foo = { bar: null }
               |
               |foo.bar?.[0]
               |""".stripMargin,
            typecheck = false) shouldBe a[Value.Null]
        }

        "short-circuit nested things (except bang) after ?." in {
          evalOk(auth, "null?.foo[0].bar().baz") shouldBe a[Value.Null]
          evalErr(
            auth,
            "null?.foo[0].bar().baz!",
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "null_value",
                  "Null literal value",
                  Span(18, 23, _),
                  Seq()) =>
          }
        }

        "only short-circuit if recipient evals to null" in {
          evalErr(
            auth,
            """|let x = "1"
               |x?.foo()
               |""".stripMargin,
            typecheck = false
          ) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_function_invocation",
                  "The function `foo` does not exist on `String`",
                  Span(15, 18, _),
                  Seq()) =>
          }

          evalErr(
            auth,
            """|let x = {foo: null}
               |x?.foo.bar""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `bar` on null.",
                  Span(27, 30, _),
                  Seq()) =>
          }

          evalErr(
            auth,
            """|let x = {foo: null}
               |x?.foo.bar()""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `bar` on null.",
                  Span(27, 30, _),
                  Seq()) =>
          }
        }

        "different statements doesn't short-circuit" in {
          evalErr(
            auth,
            """|let foo = null?.foo.bar[0].baz()
               |foo.bar
               |""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `bar` on null.",
                  Span(37, 40, _),
                  Seq()) =>
          }

          evalErr(
            auth,
            """|let foo = () => null?.foo.bar[0].baz()
               |foo().bar
               |""".stripMargin,
            typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `bar` on null.",
                  Span(45, 48, _),
                  Seq()) =>
          }
        }

        "parenthesized expression breaks the short-circuit chain" in {
          evalErr(auth, "(null?.foo).bar", typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_null_access",
                  "Cannot access `bar` on null.",
                  Span(12, 15, _),
                  Seq()) =>
          }
        }

        "doesn't short circuit binary operators" in {
          evalErr(auth, "null?.foo + 1", typecheck = false) should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "invalid_function_invocation",
                  "The function `+` does not exist on `Null`",
                  Span(10, 11, _),
                  Seq()) =>
          }
        }
      }
    }

    "global env" - {
      val auth = newAuth

      "can access singletons in the global env" in {
        evalOk(auth, "Math.abs(-1)") shouldEqual Value.Int(1)
      }

      "does not expose user objects in FQL namespace" in {
        evalOk(auth, "Collection.create({ name: 'GlobalEnvGoblin' })")

        evalOk(auth, "GlobalEnvGoblin")

        eval(
          auth,
          "FQL.GlobalEnvGoblin!",
          typecheck = true).res should matchPattern {
          case Result.Err(QueryCheckFailure(List(TypeError(_, _, _, _)))) =>
        }

        eval(
          auth,
          "FQL.GlobalEnvGoblin!",
          typecheck = false).res should matchPattern {
          case Result.Err(QueryRuntimeFailure.Simple("null_value", _, _, Seq())) =>
        }
      }
    }

    "effects" - {
      "pure" in {
        val auth = newAuth

        evalOk(auth, "Collection.create({ name: 'Foo' })")
        eval(auth, "1", effect = Effect.Pure).res
          .getOrElse(fail())
          .value shouldBe Value.Int(1)

        eval(
          auth,
          """Foo.create({})""",
          effect = Effect.Pure).res should matchPattern {
          case Result.Err(
                QueryRuntimeFailure.Simple(
                  "invalid_effect",
                  "`create` performs a write, which is not allowed in model tests.",
                  _,
                  Seq())) =>
        }

        eval(
          auth,
          """Foo.definition!""",
          effect = Effect.Pure
        ).res should matchPattern {
          case Result.Err(
                QueryRuntimeFailure.Simple(
                  "invalid_effect",
                  "`!` performs a read, which is not allowed in model tests.",
                  _,
                  Seq())) =>
        }
      }

      "read" in {
        val auth = newAuth

        evalOk(auth, "Collection.create({ name: 'Foo' })")

        eval(auth, "1", effect = Effect.Read).res.getOrElse(fail())
        eval(auth, "Foo.definition!", effect = Effect.Read).res.getOrElse(fail())
        eval(
          auth,
          """Foo.create({})""",
          effect = Effect.Read).res should matchPattern {
          case Result.Err(
                QueryRuntimeFailure.Simple(
                  "invalid_effect",
                  "`create` performs a write, which is not allowed in model tests.",
                  _,
                  Seq())) =>
        }
      }

      "write" in {
        val auth = newAuth

        evalOk(auth, "Collection.create({ name: 'Foo' })")

        eval(auth, "1", effect = Effect.Write).res.getOrElse(fail())
        eval(auth, "Foo.definition!", effect = Effect.Write).res.getOrElse(fail())
        eval(auth, "Foo.create({})", effect = Effect.Write).res.getOrElse(fail())
      }
    }

    "v4 compat" - {
      "can eval v4 from v10 with flag set to true" in {
        // need a proper db for flags to work
        var auth = newDB

        withAccountFlag(EvalV4FromV10, true) {
          // reload auth to get flag changes
          auth = loadAuth(auth.scopeID)
          evalOk(
            auth,
            "FQL.evalV4({ equals: [{ add: [1, 1] }, 2] })") shouldEqual Value.True
        }
      }

      "disallowed when flag is false" in {
        // need a proper db for flags to work
        var auth = newDB

        withAccountFlag(EvalV4FromV10, false) {
          // reload auth to get flag changes
          auth = loadAuth(auth.scopeID)
          val err = evalErr(auth, "FQL.evalV4({ equals: [{ add: [1, 1] }, 2] })")
          err.code shouldEqual "invalid_function_invocation"
        }
      }
    }

    "hooks" - {
      val auth = newAuth

      def runWith(intp: FQLInterpCtx) = {
        val output = ctx ! {
          FQLInterpreter.evalQuery(
            intp,
            FQL2Query("null"),
            Map.empty,
            typecheck = Some(false)
          )
        }
        output.result
      }

      "post eval" - {
        def addHook(intp: FQLInterpCtx, keyStr: String)(q: => Query[Result[Unit]]) =
          intp.addPostEvalHook(new PostEvalHook {
            val key = keyStr
            def merge(last: Self) = last
            def run() = q
          })

        "can add" in {
          val intp = new FQLInterpreter(auth)
          @volatile var ran = false

          addHook(intp, "foo") {
            ran = true
            Ok(()).toQuery
          }

          assert(!ran, "before query eval") // ensure hook's body didn't ran on add
          runWith(intp)
          assert(ran, "after query eval")
        }

        "can fail" in {
          val intp = new FQLInterpreter(auth)
          val err = Err(QueryRuntimeFailure("foo", "bar", Span.Null))
          addHook(intp, "foo") { Query.value(err) }
          runWith(intp) shouldBe err
        }

        "de-duplicates hooks" in {
          val runs = new LongAdder()
          val intp = new FQLInterpreter(auth)
          addHook(intp, "foo") { Query.value(Ok(runs.increment())) }
          addHook(intp, "foo") { Query.value(Ok(runs.increment())) }
          addHook(intp, "bar") { Query.value(Ok(runs.increment())) }
          runWith(intp)
          runs.sum() shouldBe 2
        }

        "merges hooks" in {
          val runs = new LongAdder()
          @volatile var box = ""
          val intp = new FQLInterpreter(auth)

          case class MergeHook(msg: String, count: Int) extends PostEvalHook {
            type Self = MergeHook
            def key = "foo"
            def run() = {
              box = msg
              runs.add(count)
              Ok(()).toQuery
            }
            def merge(last: MergeHook): MergeHook =
              last.copy(count = last.count + count).asInstanceOf[this.type]
          }

          intp.addPostEvalHook(MergeHook("one", 1))
          intp.addPostEvalHook(MergeHook("two", 1))
          runWith(intp)

          runs.sum() shouldEqual 2
          box shouldEqual "two"
        }
      }
    }
  }

  "not stack overflow" in pendingUntilFixed {
    val auth = newDB
    val q = (0 to 2000).mkString("+")
    try {
      evalOk(auth, q)
    } catch {
      case _: StackOverflowError => fail("typechecking stack overflowed :(")
    }
  }

  "newId" in {
    val auth = newDB
    evalOk(auth, "newId()") should matchPattern { case Value.ID(_) => }
    evalOk(auth, "FQL.newId()") should matchPattern { case Value.ID(_) => }
  }

  /** Writing these down so we don't lose the, but they need to be moved to
    * appropriate test modules.
    */
  "Unfiled" - {

    // how far down the road of transaction time transformation do we want to
    // go? Could this be generalized into a deferred eval scheme (consider
    // handling an increment result in-transaction).
    "transaction times can be transformed" in { pending }
  }
}
