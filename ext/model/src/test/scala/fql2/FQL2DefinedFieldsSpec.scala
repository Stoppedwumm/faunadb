package fauna.model.test

import fauna.ast._
import fauna.atoms.APIVersion
import fauna.auth.{ AdminPermissions, Auth }
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.model.schema.SchemaCollection
import fauna.prop.api.DefaultQueryHelpers
import fauna.repo.query.Query
import fauna.repo.values._
import fauna.repo.Store
import fauna.storage.doc._
import fauna.storage.ir._

class FQL2DefinedFieldsSpec extends FQL2Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "defined fields" - {
    "can be declared" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    my_field: { signature: 'Int' }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.definition.fields.my_field.signature", typecheck = false)
        .as[String] shouldEqual "Int"
    }

    "reject invalid field names" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    '1234': {
           |      signature: "Number"
           |    }
           |  }
           |})
           |""".stripMargin
      )

      err.code shouldBe "constraint_failure"
      err.errors.head.message should include("Invalid identifier")
    }

    "reject reserved field names" in {
      val err0 = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    update: {
           |      signature: "String"
           |    }
           |  }
           |})
           |""".stripMargin
      )

      val err1 = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    ttl: {
           |      signature: "Number"
           |    }
           |  }
           |})
           |""".stripMargin
      )

      val errs = Seq(err0, err1)
      all(errs map { _.code }) shouldBe "constraint_failure"
      all(errs map { _.errors.head.message }) should
        include regex "The name '\\w+' is reserved"
    }

    "rejects non-persistable fields" - {
      "functions" in {
        val err = evalErr(
          auth,
          """|Collection.create({
            |  name: "Foo",
            |  fields: {
            |    foo: { signature: "Number" },
            |    bar: { signature: "String => Number" }
            |  }
            |})""".stripMargin
        )
        err.errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Invalid database schema update.
             |    error: Field `bar` in collection Foo is not persistable
             |    at main.fsl:6:8
             |      |
             |    6 |   bar: String => Number
             |      |        ^^^^^^^^^^^^^^^^
             |      |
             |    cause: Type String => Number is not persistable
             |      |
             |    6 |   bar: String => Number
             |      |        ^^^^^^^^^^^^^^^^
             |      |
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: "Foo",
             |3 | |   fields: {
             |4 | |     foo: { signature: "Number" },
             |5 | |     bar: { signature: "String => Number" }
             |6 | |   }
             |7 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "array of non-persistable" in {
        val err = evalErr(
          auth,
          """|Collection.create({
            |  name: "Foo",
            |  fields: {
            |    foo: { signature: "{ a: Array<String => Number> }" }
            |  }
            |})""".stripMargin
        )
        err.errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Invalid database schema update.
             |    error: Field `foo` in collection Foo is not persistable
             |    at main.fsl:5:8
             |      |
             |    5 |   foo: { a: Array<String => Number> }
             |      |        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |      |
             |    cause: Type String => Number is not persistable
             |      |
             |    5 |   foo: { a: Array<String => Number> }
             |      |                   ^^^^^^^^^^^^^^^^
             |      |
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: "Foo",
             |3 | |   fields: {
             |4 | |     foo: { signature: "{ a: Array<String => Number> }" }
             |5 | |   }
             |6 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "union of non-persistable" in {
        evalErr(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    foo: { signature: "String | (String => String)" }
             |  }
             |})""".stripMargin
        ).errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Invalid database schema update.
             |    error: Field `foo` in collection Foo is not persistable
             |    at main.fsl:5:8
             |      |
             |    5 |   foo: String | (String => String)
             |      |        ^^^^^^^^^^^^^^^^^^^^^^^^^^
             |      |
             |    cause: Type String => String is not persistable
             |      |
             |    5 |   foo: String | (String => String)
             |      |                  ^^^^^^^^^^^^^^^^
             |      |
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: "Foo",
             |3 | |   fields: {
             |4 | |     foo: { signature: "String | (String => String)" }
             |5 | |   }
             |6 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }
    }

    "ID is not persistable" in {
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    a: { signature: "ID" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Field `a` in collection Foo is not persistable
           |    at main.fsl:5:6
           |      |
           |    5 |   a: ID
           |      |      ^^
           |      |
           |    cause: Type ID is not persistable
           |      |
           |    5 |   a: ID
           |      |      ^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     a: { signature: "ID" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "non-persistable types don't explode default evaluation" in {
      val auth = newDB(typeChecked = false)
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    a: { signature: "ID", default: "newId()" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  a: Default field evaluation failed.
           |      Value has type ID, which cannot be persisted
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     a: { signature: "ID", default: "newId()" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "recognizes (doc | nulldoc) types as persistable" in {
      evalOk(auth, "Collection.create({ name: 'Bar' })")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    a: { signature: "Bar | NullBar | Number" },
           |    b: { signature: "Array<Bar | NullBar> | String" }
           |  }
           |})""".stripMargin
      )
    }

    "recognizes doc types as unpersistable" in {
      evalOk(auth, "Collection.create({ name: 'Bar' })")

      evalErr(
        auth,
        """|Collection.create({
           |  name: "Baaz2",
           |  fields: {
           |    a: { signature: "Array<Bar> | String" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Field `a` in collection Baaz2 is not persistable
           |    at main.fsl:8:6
           |      |
           |    8 |   a: Array<Bar> | String
           |      |      ^^^^^^^^^^^^^^^^^^^
           |      |
           |    cause: Type Bar is not persistable
           |      |
           |    8 |   a: Array<Bar> | String
           |      |            ^^^
           |      |
           |    hint: Use Ref<Bar> as the type of a reference to a Bar document
           |      |
           |    8 |   a: Array<Ref<Bar>> | String
           |      |            ~~~~~~~~
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Baaz2",
           |3 | |   fields: {
           |4 | |     a: { signature: "Array<Bar> | String" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "doc and null doc must be paired from the same collection" in {
      evalOk(auth, "Collection.create({ name: 'Bar' })")
      evalOk(auth, "Collection.create({ name: 'Bar2' })")
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Baaz3",
           |  fields: {
           |    a: { signature: "Bar | NullBar2" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Field `a` in collection Baaz3 is not persistable
           |    at main.fsl:11:6
           |       |
           |    11 |   a: Bar | NullBar2
           |       |      ^^^^^^^^^^^^^^
           |       |
           |    cause: Type Bar is not persistable
           |       |
           |    11 |   a: Bar | NullBar2
           |       |      ^^^
           |       |
           |    hint: Use Ref<Bar> as the type of a reference to a Bar document
           |       |
           |    11 |   a: Ref<Bar> | NullBar2
           |       |      ~~~~~~~~
           |       |
           |    cause: Type NullBar2 is not persistable
           |       |
           |    11 |   a: Bar | NullBar2
           |       |            ^^^^^^^^
           |       |
           |    hint: Use Ref<Bar2> as the type of a reference to a Bar2 document
           |       |
           |    11 |   a: Bar | Ref<Bar2>
           |       |            ~~~~~~~~~
           |       |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Baaz3",
           |3 | |   fields: {
           |4 | |     a: { signature: "Bar | NullBar2" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "upcasts numeric types in create/update/replace methods" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    a: { signature: 'Int' },
           |    b: { signature: 'Long' },
           |    c: { signature: 'Float' },
           |    d: { signature: 'Double' }
           |  }
           |})""".stripMargin
      )

      evalOk(
        auth,
        """|let a: Number = 1
           |let b: Number = 2
           |let c: Number = 3.0
           |let d: Number = 4.0
           |Foo.create({ a: a, b: b, c: c, d: d }) { a, b, c, d }
           |""".stripMargin
      ) shouldEqual {
        Value.Struct(
          "a" -> Value.Int(1),
          "b" -> Value.Int(2),
          "c" -> Value.Double(3),
          "d" -> Value.Double(4))
      }

      // Set.sequence case
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Bar',
           |  fields: { a: { signature: 'Int' } }
           |})""".stripMargin
      )

      evalOk(auth, "Set.sequence(1, 10).forEach(i => Bar.create({ a: i }))")
    }

    "integer literals should be accepted in Double fields (pending)" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    a: { signature: 'Float' },
           |    b: { signature: 'Double' }
           |  }
           |})""".stripMargin
      )

      pendingUntilFixed {
        evalOk(
          auth,
          """|let a: Number = 1
             |let b: Number = 2
             |Foo.create({ a: a, b: b }) { a, b }
             |""".stripMargin
        ) shouldEqual {
          Value.Struct("a" -> Value.Double(1), "b" -> Value.Double(2))
        }
      }
    }

    "singleton fields accept longs" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    a: { signature: '0 | 42 | 2289521863497489' }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ a: 0 })")
      evalOk(auth, "Foo.create({ a: 42 })")
      evalOk(auth, "Foo.create({ a: 2289521863497489 })")

      evalErr(auth, "Foo.create({ a: 3 })")
    }

    "makes nice errors for invalid signatures" in {
      val err1 = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    my_field: { signature: '2 $$ 3' }
           |  }
           |})
           |""".stripMargin
      )

      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  fields.my_field.signature: Failed parsing user-provided signature.
           |      error: Expected end-of-input
           |      at *signature*:1:3
           |        |
           |      1 | 2 $$ 3
           |        |   ^
           |        |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: 'Foo',
           |3 | |   fields: {
           |4 | |     my_field: { signature: '2 $$ 3' }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "makes nice errors for valid signature exprs, but invalid type exprs" in {
      val err1 = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    my_field: { signature: 'Array<2, 3>' }
           |  }
           |})
           |""".stripMargin
      )

      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Type constructor `Array<>` accepts 1 parameters, received 2
           |    at main.fsl:5:13
           |      |
           |    5 |   my_field: Array<2, 3>
           |      |             ^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: 'Foo',
           |3 | |   fields: {
           |4 | |     my_field: { signature: 'Array<2, 3>' }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "requires fields to exist in a collection" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    my_field: { signature: 'Number' }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ my_field: 5 })")

      val err0 = evalErr(auth, "Foo.create({})", typecheck = false)
      err0.code shouldBe "constraint_failure"
      err0.failureMessage shouldBe "Failed to create document in collection `Foo`."

      err0.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Missing required field of type Number
           |at *query*:1:11
           |  |
           |1 | Foo.create({})
           |  |           ^^^^
           |  |""".stripMargin
      )

      val err1 = evalErr(auth, "Foo.create({ my_field: 'hi' })", typecheck = false)
      err1.code shouldBe "constraint_failure"
      err1.failureMessage shouldBe "Failed to create document in collection `Foo`."

      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Expected Number, provided String
           |at *query*:1:11
           |  |
           |1 | Foo.create({ my_field: 'hi' })
           |  |           ^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "properly removes the data prefix from error messages" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    a: { signature: 'Int' }
           |  }
           |})""".stripMargin
      )

      // `a`, not `data.a`.
      evalErr(auth, "Foo.create({ a: 'hello' })", typecheck = false).errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  a: Expected Int, provided String
           |at *query*:1:11
           |  |
           |1 | Foo.create({ a: 'hello' })
           |  |           ^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // Kinda should be `data.a` but whatever.
      evalErr(auth, "Foo.createData({ a: 'hello' })", typecheck = false).errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  a: Expected Int, provided String
           |at *query*:1:15
           |  |
           |1 | Foo.createData({ a: 'hello' })
           |  |               ^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // Meta field `ttl`, so no `data`.
      evalErr(auth, "Foo.create({ a: 0, ttl: 0 })", typecheck = false).errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  ttl: Expected Time | Null, provided Int
           |at *query*:1:11
           |  |
           |1 | Foo.create({ a: 0, ttl: 0 })
           |  |           ^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // User field `ttl`, so include `data` to disambiguate.
      evalErr(
        auth,
        "Foo.createData({ a: 0, ttl: 0 })",
        typecheck = false).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  data.ttl: Unexpected field provided
           |at *query*:1:15
           |  |
           |1 | Foo.createData({ a: 0, ttl: 0 })
           |  |               ^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // Non-user collections don't trim `data`.
      evalErr(
        auth,
        "Key.create({ role: 'server', data: { foo: Null } })",
        typecheck = false).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Value at `data.foo` has type Null, which cannot be persisted.
           |at *query*:1:11
           |  |
           |1 | Key.create({ role: 'server', data: { foo: Null } })
           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    // Tested more extensively in repo/SchemaSpec.
    "works with ints" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    my_field: { signature: 'Int' }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ my_field: 5 })")
      val err0 =
        evalErr(auth, "Foo.create({ my_field: 2147483648 })", typecheck = false)
      err0.code shouldBe "constraint_failure"
      err0.failureMessage shouldBe "Failed to create document in collection `Foo`."

      err0.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Expected Int, provided Long
           |at *query*:1:11
           |  |
           |1 | Foo.create({ my_field: 2147483648 })
           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      val err1 = evalErr(auth, "Foo.create({ my_field: 2.0 })", typecheck = false)
      err1.code shouldBe "constraint_failure"
      err1.failureMessage shouldBe "Failed to create document in collection `Foo`."

      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Expected Int, provided Double
           |at *query*:1:11
           |  |
           |1 | Foo.create({ my_field: 2.0 })
           |  |           ^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "works with singletons" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "'a' | 'b'" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ my_field: 'a' })")
      evalOk(auth, "Foo.create({ my_field: 'b' })")
      renderErr(auth, "Foo.create({ my_field: 'c' })", typecheck = false) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Expected "a" | "b", provided "c"
           |at *query*:1:11
           |  |
           |1 | Foo.create({ my_field: 'c' })
           |  |           ^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "does not require nullable fields to exist" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    my_field: { signature: 'Number | Null' }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({})")
      evalOk(auth, "Foo.create({ my_field: 5 })")

      val err0 = evalErr(auth, "Foo.create({ my_field: 'hi' })", typecheck = false)
      err0.code shouldBe "constraint_failure"
      err0.failureMessage shouldBe "Failed to create document in collection `Foo`."

      // TODO: This error needs improving. It should include the expected type at
      // the very least.
      err0.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Expected Number | Null, provided String
           |at *query*:1:11
           |  |
           |1 | Foo.create({ my_field: 'hi' })
           |  |           ^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "result type shows up in typechecking" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalRes(auth, "Foo.create({ my_field: 5 }).my_field").typeStr shouldBe "Number"
      evalRes(auth, "Foo.byId(1234)?.my_field").typeStr shouldBe "Number | Null"
    }

    "wildcard shows up when typechecking doc" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  wildcard: "Any",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalRes(
        auth,
        "Foo.create({ my_field: 5 }) { my_field, foo }").typeStr shouldBe "{ my_field: Number, foo: Any }"

      evalOk(auth, "Foo.create({ id: '0', my_field: 5 })")
      evalRes(auth, "Foo.byId(0)!.foo").typeStr shouldBe "Any"
    }

    "expected type is in the create signature" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalRes(auth, "Foo.create").typeStr shouldBe (
        "(data: { id: ID | Null, my_field: Number }) => Foo"
      )
      evalRes(auth, "Foo.createData").typeStr shouldBe (
        "(data: { my_field: Number }) => Foo"
      )

      val err0 = evalErr(auth, "Foo.create({})")
      err0.code shouldBe "invalid_query"
      err0.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Type `{}` does not have field `my_field`
           |at *query*:1:12
           |  |
           |1 | Foo.create({})
           |  |            ^^
           |  |""".stripMargin
      )

      val err1 = evalErr(auth, "Foo.create({ my_field: 'hi' })")
      err1.code shouldBe "invalid_query"
      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `String` is not a subtype of `Number`
           |at *query*:1:24
           |  |
           |1 | Foo.create({ my_field: 'hi' })
           |  |                        ^^^^
           |  |""".stripMargin
      )
    }

    "update signature shows up" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 0, my_field: 5 })")

      evalRes(
        auth,
        "Foo.byId(0)!.update").typeStr shouldBe "(data: { my_field: Number | Null }) => Foo"

      evalOk(auth, "Foo.create({ my_field: 3 }).update({})")
      evalOk(auth, "Foo.create({ my_field: 3 }).update({ my_field: 5 })")

      val err =
        evalErr(auth, "Foo.create({ my_field: 3 }).update({ my_field: 'hi' })")
      err.code shouldBe "invalid_query"
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `String` is not a subtype of `Null | Number`
           |at *query*:1:48
           |  |
           |1 | Foo.create({ my_field: 3 }).update({ my_field: 'hi' })
           |  |                                                ^^^^
           |  |""".stripMargin
      )
    }

    "updateData signature shows up" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 0, my_field: 5 })")

      evalRes(
        auth,
        "Foo.byId(0)!.updateData").typeStr shouldBe "(data: { my_field: Number | Null }) => Foo"

      evalOk(auth, "Foo.create({ my_field: 3 }).updateData({})")
      evalOk(auth, "Foo.create({ my_field: 3 }).updateData({ my_field: 5 })")

      val err =
        evalErr(auth, "Foo.create({ my_field: 3 }).updateData({ my_field: 'hi' })")
      err.code shouldBe "invalid_query"
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `String` is not a subtype of `Null | Number`
           |at *query*:1:52
           |  |
           |1 | Foo.create({ my_field: 3 }).updateData({ my_field: 'hi' })
           |  |                                                    ^^^^
           |  |""".stripMargin
      )
    }

    "replace signature shows up" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 0, my_field: 5 })")

      evalRes(
        auth,
        "Foo.byId(0)!.replace").typeStr shouldBe "(data: { my_field: Number }) => Foo"

      evalOk(auth, "Foo.create({ my_field: 3 }).replace({ my_field: 5 })")

      val err0 =
        evalErr(auth, "Foo.create({ my_field: 3 }).replace({})")
      err0.code shouldBe "invalid_query"
      err0.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{}` does not have field `my_field`
           |at *query*:1:37
           |  |
           |1 | Foo.create({ my_field: 3 }).replace({})
           |  |                                     ^^
           |  |""".stripMargin
      )

      val err1 =
        evalErr(auth, "Foo.create({ my_field: 3 }).replace({ my_field: 'hi' })")
      err1.code shouldBe "invalid_query"
      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `String` is not a subtype of `Number`
           |at *query*:1:49
           |  |
           |1 | Foo.create({ my_field: 3 }).replace({ my_field: 'hi' })
           |  |                                                 ^^^^
           |  |""".stripMargin
      )
    }

    "replaceData signature shows up" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 0, my_field: 5 })")

      evalRes(
        auth,
        "Foo.byId(0)!.replaceData").typeStr shouldBe "(data: { my_field: Number }) => Foo"

      evalOk(auth, "Foo.create({ my_field: 3 }).replaceData({ my_field: 5 })")

      val err0 =
        evalErr(auth, "Foo.create({ my_field: 3 }).replaceData({})")
      err0.code shouldBe "invalid_query"
      err0.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{}` does not have field `my_field`
           |at *query*:1:41
           |  |
           |1 | Foo.create({ my_field: 3 }).replaceData({})
           |  |                                         ^^
           |  |""".stripMargin
      )

      val err1 =
        evalErr(auth, "Foo.create({ my_field: 3 }).replace({ my_field: 'hi' })")
      err1.code shouldBe "invalid_query"
      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `String` is not a subtype of `Number`
           |at *query*:1:49
           |  |
           |1 | Foo.create({ my_field: 3 }).replace({ my_field: 'hi' })
           |  |                                                 ^^^^
           |  |""".stripMargin
      )
    }
  }

  "default defined fields" - {
    "can be declared" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: "1 + 2" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.definition.fields.my_field.default", typecheck = false)
        .as[String] shouldBe "1 + 2"
    }

    "typecheck index definitions" - {
      "accept defined fields and reject undefined" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    my_field: { signature: "Number" }
             |  }
             |})""".stripMargin
        )

        evalOk(
          auth,
          """|Foo.definition.update({
             |  indexes: {
             |    byMyField: {
             |      terms: [{ field: ".my_field" }]
             |    }
             |  }
             |})""".stripMargin
        )

        val err = evalErr(
          auth,
          """|Foo.definition.update({
             |  indexes: {
             |    byXY: {
             |      terms: [{ field: ".x.y" }]
             |    }
             |  }
             |})""".stripMargin
        )
        err.errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Invalid database schema update.
             |    error: Type `{ id: ID, ts: Time, ttl: Time | Null, my_field: Number }` does not have field `x`
             |    at main.fsl:10:13
             |       |
             |    10 |     terms [.x.y]
             |       |             ^
             |       |
             |at *query*:1:22
             |  |
             |1 |   Foo.definition.update({
             |  |  ______________________^
             |2 | |   indexes: {
             |3 | |     byXY: {
             |4 | |       terms: [{ field: ".x.y" }]
             |5 | |     }
             |6 | |   }
             |7 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "works with wildcards" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    my_field: { signature: "Number" }
             |  },
             |  wildcard: "Any",
             |  indexes: {
             |    byOurFields: { terms: [
             |      { field: ".my_field" },
             |      { field: ".your_field" }
             |    ] }
             |  }
             |})""".stripMargin
        )
      }

      "works for unique constraints" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    my_field: { signature: "Number" },
             |  },
             |  constraints: [
             |    { unique: [".my_field"] }
             |  ]
             |})""".stripMargin
        )

        val err = evalErr(
          auth,
          """|Collection.create({
             |  name: "FooBad",
             |  fields: {
             |    my_field: { signature: "Number" },
             |  },
             |  constraints: [
             |    { unique: [".your_field"] }
             |  ]
             |})""".stripMargin
        )
        err.errors.head.renderWithSource(Map.empty) shouldBe (
          """|error: Invalid database schema update.
             |    error: Type `{ id: ID, ts: Time, ttl: Time | Null, my_field: Number }` does not have field `your_field`
             |    at main.fsl:11:12
             |       |
             |    11 |   unique [.your_field]
             |       |            ^^^^^^^^^^
             |       |
             |at *query*:1:18
             |  |
             |1 |   Collection.create({
             |  |  __________________^
             |2 | |   name: "FooBad",
             |3 | |   fields: {
             |4 | |     my_field: { signature: "Number" },
             |5 | |   },
             |6 | |   constraints: [
             |7 | |     { unique: [".your_field"] }
             |8 | |   ]
             |9 | | })
             |  | |__^
             |  |""".stripMargin
        )
      }

      "works for nested fields and arrays" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    my_field: { signature: "Array<{ a: String, b: Number }>" },
             |  },
             |  indexes: {
             |    byAB: {
             |      terms: [
             |        { field:".my_field[0].a" },
             |        { field: ".my_field[1].b" }
             |      ]
             |    }
             |  }
             |})""".stripMargin
        )

        val err = evalErr(
          auth,
          """|Collection.create({
             |  name: "FooBad",
             |  fields: {
             |    my_field: { signature: "Array<{ a: String, b: Number }>" },
             |  },
             |  indexes: {
             |    byAB: {
             |      terms: [{ field:".my_field[0].a" }],
             |      values: [{ field: ".my_field[1].c" }]
             |    }
             |  }
             |})""".stripMargin
        )
        err.errors.head.renderWithSource(Map.empty) shouldBe (
          """error: Invalid database schema update.
             |    error: Type `{ a: String, b: Number }` does not have field `c`
             |    at main.fsl:15:26
             |       |
             |    15 |     values [.my_field[1].c]
             |       |                          ^
             |       |
             |    hint: Type `{ a: String, b: Number }` inferred here
             |    at main.fsl:12:19
             |       |
             |    12 |   my_field: Array<{ a: String, b: Number }>
             |       |                   ^^^^^^^^^^^^^^^^^^^^^^^^
             |       |
             |at *query*:1:18
             |   |
             | 1 |   Collection.create({
             |   |  __________________^
             | 2 | |   name: "FooBad",
             | 3 | |   fields: {
             | 4 | |     my_field: { signature: "Array<{ a: String, b: Number }>" },
             | 5 | |   },
             | 6 | |   indexes: {
             | 7 | |     byAB: {
             | 8 | |       terms: [{ field:".my_field[0].a" }],
             | 9 | |       values: [{ field: ".my_field[1].c" }]
             |10 | |     }
             |11 | |   }
             |12 | | })
             |   | |__^
             |   |""".stripMargin
        )
      }

      "works for all term formats" in {
        // NB: No leading '.'.
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    a: {
             |      signature: "{ 'b c': Array<Number> }"
             |    }
             |  },
             |  indexes: {
             |    byWWW: {
             |      terms: [{ field: '["a"]["b c"][0]' }]
             |    }
             |  }
             |})""".stripMargin
        )

        // TODO: When I was messing up the path this test faked that it
        // worked correctly, but there seems to be some problem with the
        // typecheck with the expanded syntax.
        pendingUntilFixed {
          val err = evalErr(
            auth,
            """|Collection.create({
              |  name: "FooBad",
              |  fields: {
              |    a: {
              |      signature: "{ 'b c': Array<Number> }"
              |    }
              |  },
              |  indexes: {
              |    byWWW: {
              |      terms: [{ field: '["a"]["bc"][0]' }]
              |    }
              |  }
              |})""".stripMargin
          )
          err.errors.head.renderWithSource(Map.empty) shouldBe (
            """error: Invalid database schema update.
              |    error: Type `{ "b c": Array<Number> }` does not have field `bc`
              |    at *FooBad.byWWW*:1:5
              |      |
              |    1 | (.a.bc[0])
              |      |     ^^
              |      |
              |    hint: Type `{ "b c": Array<Number> }` inferred here
              |    at *a*:1:1
              |      |
              |    1 | { 'b c': Array<Number> }
              |      | ^^^^^^^^^^^^^^^^^^^^^^^^
              |      |
              |at *query*:1:18
              |   |
              | 1 |   Collection.create({
              |   |  __________________^
              | 2 | |   name: "FooBad",
              | 3 | |   fields: {
              | 4 | |     a: {
              | 5 | |       signature: "{ 'b c': Array<Number> }"
              | 6 | |     }
              | 7 | |   },
              | 8 | |   indexes: {
              | 9 | |     byWWW: {
              |10 | |       terms: [{ field: '["a"]["bc"][0]' }]
              |11 | |     }
              |12 | |   }
              |13 | | })
              |   | |__^
              |   |""".stripMargin
          )
        }
      }
    }

    "default values are required for non-nullable fields" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    _no_wildcard: { signature: "Null" }
           |  }
           |})""".stripMargin
      )
      // Make a doc so that migrations are validated.
      evalOk(auth, "Foo.create({})")

      renderErr(
        auth,
        """|Collection.byName("Foo")!.update({
           |  fields: {
           |    _no_wilcard: { signature: "Null" },
           |    foo: { signature: "Int" }
           |  }
           |})""".stripMargin
      ) shouldBe (
        """|error: Invalid database schema update.
           |    error: Field `._no_wilcard` is not present in the live schema
           |    at <no source>
           |    hint: Provide an `add` migration for this field
           |    at <no source>
           |    error: Field `.foo` is not present in the live schema
           |    at <no source>
           |    hint: Provide an `add` migration for this field
           |    at <no source>
           |    hint: Add a default value to this field
           |    at *field:Foo:foo*:1:4
           |      |
           |    1 | Int = <expr>
           |      |    +++++++++
           |      |
           |    hint: Make the field nullable
           |    at *field:Foo:foo*:1:4
           |      |
           |    1 | Int?
           |      |    +
           |      |
           |at *query*:1:33
           |  |
           |1 |   Collection.byName("Foo")!.update({
           |  |  _________________________________^
           |2 | |   fields: {
           |3 | |     _no_wilcard: { signature: "Null" },
           |4 | |     foo: { signature: "Int" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "default values are not required for nullable fields" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    foo: {
           |      signature: "Int | Null"
           |    },
           |  }
           |})""".stripMargin
      )
    }

    "disallows non-strings (typechecked)" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: 3 }
           |  }
           |})
           |""".stripMargin
      )
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ name: "Foo", fields: { my_field: { signature: "Number", default: 3 } } }` is not a subtype of `{ name: String, alias: String | Null, computed_fields: { *: { body: String, signature: String | Null } } | Null, fields: { *: { signature: String, default: String | Null } } | Null, migrations: Array<{ backfill: { field: String, value: String } } | { drop: { field: String } } | { split: { field: String, to: Array<String> } } | { move: { field: String, to: String } } | { add: { field: String } } | { move_conflicts: { into: String } } | { move_wildcard: { into: String } } | { add_wildcard: {} }> | Null, wildcard: String | Null, history_days: Number | Null, ttl_days: Number | Null, document_ttls: Boolean | Null, indexes: { *: { terms: Array<{ field: String, mva: Boolean | Null }> | Null, values: Array<{ field: String, mva: Boolean | Null, order: "asc" | "desc" | Null }> | Null, queryable: Boolean | Null } } | Null, constraints: Array<{ unique: Array<String> | Array<{ field: String, mva: Boolean | Null }> } | { check: { name: String, body: String } }> | Null, data: { *: Any } | Null }`
           |at *query*:1:19
           |  |
           |1 |   Collection.create({
           |  |  ___________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     my_field: { signature: "Number", default: 3 }
           |5 | |   }
           |6 | | })
           |  | |_^
           |  |
           |cause: Type `{ my_field: { signature: "Number", default: 3 } }` is not a subtype of `Null`
           |at *query*:3:11
           |  |
           |3 |     fields: {
           |  |  ___________^
           |4 | |     my_field: { signature: "Number", default: 3 }
           |5 | |   }
           |  | |___^
           |  |
           |cause: Type `Int` is not a subtype of `String | Null`
           |at *query*:4:47
           |  |
           |4 |     my_field: { signature: "Number", default: 3 }
           |  |                                               ^
           |  |""".stripMargin
      )
    }

    "disallows non-strings (not typechecked)" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: 3 }
           |  }
           |})
           |""".stripMargin,
        typecheck = false
      )
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  fields.my_field.default: Expected String | Null, provided Int
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     my_field: { signature: "Number", default: 3 }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "works on create" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: "1 + 2" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({}).my_field").as[Int] shouldBe 3
      evalOk(auth, "Foo.create({ my_field: 5 }).my_field").as[Int] shouldBe 5
    }

    "default evaluation fails produce a nice error" in {
      val doc = evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      val id = doc.asInstanceOf[Value.Doc].id

      // Add in a default that fails at runtime. This is possible for end-users to do
      // by changing the behavior of their default field by reading `Time.now()`.
      val diff = MapV("fields" -> MapV("my_field" -> MapV("default" -> "1/0")))

      val cfg = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(cfg.Schema, id, Diff(diff))

      val err = evalErr(auth, "Foo.create({}).my_field")
      err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  my_field: Default field evaluation failed.
           |      error: Attempted integer division by zero.
           |      at *field:Foo:my_field*:1:3
           |        |
           |      1 | 1/0
           |        |   ^
           |        |
           |at *query*:1:11
           |  |
           |1 | Foo.create({}).my_field
           |  |           ^^^^
           |  |""".stripMargin
      )

      // Providing a value skips evaluating default.
      evalOk(auth, "Foo.create({ my_field: 5 }).my_field").as[Int] shouldBe 5
    }

    "default values must be pure" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: "Collection.all().count()" }
           |  }
           |})
           |""".stripMargin
      )
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  my_field: Default field evaluation failed.
           |      error: `all` performs a read, which is not allowed in default values.
           |      at *field:my_field*:1:15
           |        |
           |      1 | Collection.all().count()
           |        |               ^^
           |        |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     my_field: { signature: "Number", default: "Collection.all().count()" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "Time.now() is allowed in default fields" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: "Time.now().toMicros()" }
           |  }
           |})
           |""".stripMargin
      )

      val struct1 = evalOk(
        auth,
        """|{
           |  now: Time.now().toMicros(),
           |  field: Foo.create({ id: 1 }).my_field
           |}""".stripMargin)
      (struct1 / "now").as[Long] shouldBe (struct1 / "field").as[Long]

      val struct2 = evalOk(
        auth,
        """|{
           |  now: Time.now().toMicros(),
           |  field: Foo.create({ id: 2 }).my_field
           |}""".stripMargin)
      (struct2 / "now").as[Long] shouldBe (struct2 / "field").as[Long]

      // Make sure we actually got a different `Time.now()`.
      ((struct2 / "now").as[Long] > (struct1 / "now").as[Long]) shouldBe true

      // Make sure those times actually got stored.
      evalOk(auth, "Foo.byId(1)!.my_field") shouldBe struct1 / "now"
      evalOk(auth, "Foo.byId(2)!.my_field") shouldBe struct2 / "now"
    }

    "adding, changing, and removing defaults works" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    a: { signature: "Number" }
           |  }
           |})""".stripMargin
      )

      evalErr(auth, "Foo.create({})").code shouldBe "invalid_query"

      evalOk(
        auth,
        """|Foo.definition.update({
           |  fields: {
           |    a: { signature: "Number", default: "0" }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldEqual Value.Int(0)

      evalOk(
        auth,
        """|Foo.definition.update({
           |  fields: {
           |    a: { signature: "Number", default: "1" }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldEqual Value.Int(1)

      evalOk(
        auth,
        """|Foo.definition.update({
           |  fields: {
           |    a: { signature: "Number", default: null }
           |  }
           |})""".stripMargin
      )

      evalErr(auth, "Foo.create({})").code shouldBe "invalid_query"
    }

    "default values are typechecked" in {
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    foo: { signature: "String", "default": "0" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Type `Int` is not a subtype of `String`
           |    at main.fsl:5:17
           |      |
           |    5 |   foo: String = 0
           |      |                 ^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     foo: { signature: "String", "default": "0" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }
  }

  "named types work" in {
    evalOk(
      auth,
      """|Collection.create({
         |  name: "Foo",
         |  fields: {
         |    t: { signature: "Time" }
         |  }
         |})""".stripMargin
    )

    evalOk(auth, "Foo.create({ t: Time.now() })")
    val err0 = evalErr(auth, "Foo.create({ t: 2 })")
    err0.errors.head.renderWithSource(Map.empty) shouldBe (
      """|error: Type `Int` is not a subtype of `Time`
         |at *query*:1:17
         |  |
         |1 | Foo.create({ t: 2 })
         |  |                 ^
         |  |""".stripMargin
    )

    val err1 = evalErr(auth, "Foo.create({ t: 2 })", typecheck = false)
    err1.errors.head.renderWithSource(Map.empty) shouldBe (
      """|error: Failed to create document in collection `Foo`.
         |constraint failures:
         |  t: Expected Time, provided Int
         |at *query*:1:11
         |  |
         |1 | Foo.create({ t: 2 })
         |  |           ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "the bytes type works" in {
    // NB: One cannot actually make a value of type bytes...
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |    x: Bytes?
           |    y: Array<Bytes> = []
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ })")
    evalOk(auth, "Foo.create({ y: [] })")

    // Try the schema and queries from the original report, which
    // turned out to have a lot of irrelevant parts.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  a: Array<{
           |    x: String,
           |    y: String,
           |    z: { r: Number, s: Bytes }?
           |  }> = []
           |}""".stripMargin
    )

    evalOk(auth, "User.createData({ a: [{ x: 'foo', y: 'bar' }] })")
    evalOk(auth, "User.createData({ a: [] })")
    evalOk(auth, "User.createData({})")
  }

  "wildcard" - {
    "can be declared" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  wildcard: 'Any'
           |})""".stripMargin
      )

      evalOk(auth, "Foo.definition.wildcard", typecheck = false)
        .as[String] shouldEqual "Any"
    }

    "makes nice errors for invalid signatures" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  wildcard: '>%%%>'
           |})
           |""".stripMargin
      )

      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  wildcard: Failed parsing user-provided signature.
           |      error: Expected type
           |      at *wildcard*:1:1
           |        |
           |      1 | >%%%>
           |        | ^
           |        |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: 'Foo',
           |3 | |   wildcard: '>%%%>'
           |4 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "allows only Any" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "AnythingGoes",
           |
           |  wildcard: "Any"
           |})""".stripMargin
      )

      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |
           |  wildcard: "String"
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Top-level wildcard must have type `Any`
           |    at main.fsl:9:6
           |      |
           |    9 |   *: String
           |      |      ^^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |
           |4 | |   wildcard: "String"
           |5 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "field type overwrites wildcard type" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    z: { signature: 'Number' }
           |  },
           |  wildcard: 'Any'
           |})""".stripMargin
      )

      // Make sure the field type "overwrites" the type in the wildcard.
      evalOk(auth, "Foo.create({ z: 3 })")
      val err = evalErr(auth, "Foo.create({ z: 'hi' })")
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `String` is not a subtype of `Number`
           |at *query*:1:17
           |  |
           |1 | Foo.create({ z: 'hi' })
           |  |                 ^^^^
           |  |""".stripMargin
      )
    }

    "doesn't apply to nested fields" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    z: { signature: '{ a: Number }' }
           |  },
           |  wildcard: 'Any'
           |})""".stripMargin
      )

      val err =
        evalErr(auth, "Foo.create({ z: { a: 0, b: 'x' } })", typecheck = false)
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  z.b: Unexpected field provided
           |at *query*:1:11
           |  |
           |1 | Foo.create({ z: { a: 0, b: 'x' } })
           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "wildcards work in nested fields" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  fields: {
           |    z: { signature: '{ *: Any }' }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ z: { foo: 3, bar: 'hi' } })")

      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Bar',
           |  fields: {
           |    z: { signature: '{ *: String }' }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Bar.create({ z: { foo: 'hi', bar: 'hello' } })")
      renderErr(
        auth,
        "Bar.create({ z: { foo: 3, bar: 'hi' } })",
        typecheck = false) shouldBe (
        """|error: Failed to create document in collection `Bar`.
           |constraint failures:
           |  z.foo: Expected String, provided Int
           |at *query*:1:11
           |  |
           |1 | Bar.create({ z: { foo: 3, bar: 'hi' } })
           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }
  }

  "refs" - {
    "works" in {
      val auth = newDB

      evalOk(
        auth,
        """|Collection.create({ name: "Bar", fields: { a: { signature: "Int?" } } })
           |Collection.create({ name: "Baz", fields: { b: { signature: "Int?" } } })
           |
           |Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar_ref: { signature: "Ref<Bar>" }
           |  },
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ bar_ref: Bar.create({}) })")
      val err =
        evalErr(auth, "Foo.create({ bar_ref: Baz.create({}) })", typecheck = false)
      // TODO: Improve this error so it says Baz instead of Document.
      err.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  bar_ref: Expected Bar | NullBar, provided Document
           |at *query*:1:11
           |  |
           |1 | Foo.create({ bar_ref: Baz.create({}) })
           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "union refs work" in {
      val auth = newDB
      evalOk(
        auth,
        """|Collection.create({ name: "Bar", fields: { a: { signature: "Int?" } } })
           |Collection.create({ name: "Baz", fields: { b: { signature: "Int?" } } })
           |
           |Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar_ref: { signature: "Ref<Bar> | Int" }
           |  },
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ bar_ref: Bar.create({}) })")
      evalOk(auth, "Foo.create({ bar_ref: 3 })")

      val err1 =
        evalErr(auth, "Foo.create({ bar_ref: Baz.create({}) })", typecheck = false)

      err1.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Failed to create document in collection `Foo`.
           |constraint failures:
           |  bar_ref: Expected Bar | NullBar | Int, provided Document
           |at *query*:1:11
           |  |
           |1 | Foo.create({ bar_ref: Baz.create({}) })
           |  |           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      val err2 = evalErr(auth, "Foo.create({ bar_ref: Baz.create({}) })")

      // FIXME: this type error is pretty unhelpful
      err2.errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ bar_ref: Baz }` is not a subtype of `{ id: ID | Null, bar_ref: Ref<Bar> | Number }`
           |at *query*:1:12
           |  |
           |1 | Foo.create({ bar_ref: Baz.create({}) })
           |  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |
           |cause: Type `{ id: ID, ts: Time, ttl: Time | Null, b: Int | Null }` contains extra field `b`
           |  |
           |1 | Foo.create({ bar_ref: Baz.create({}) })
           |  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |
           |cause: Type `{ id: ID, ts: Time, ttl: Time | Null, b: Int | Null }` is not a subtype of `Number | Null`
           |  |
           |1 | Foo.create({ bar_ref: Baz.create({}) })
           |  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "aliased nulldocs work" in {
      val auth = newDB

      // This test has some stupid names, but its all to test the string munging to
      // figure out nulldoc types.
      evalOk(
        auth,
        """|Collection.create({
           |  name: "NullBar",
           |  alias: "NullAliased",
           |  fields: { a: { signature: "Int?" } }
           |})
           |
           |Collection.create({
           |  name: "Foo",
           |  fields: {
           |    doc_ref: { signature: "Ref<NullBar>" },
           |    aliased_doc_ref: { signature: "Ref<NullAliased>" },
           |  },
           |})""".stripMargin
      )

      evalOk(
        auth,
        """|Foo.create({
           |  doc_ref: NullBar.create({}),
           |  aliased_doc_ref: NullBar.create({}),
           |})""".stripMargin
      )
    }

    "works with union of doc | nulldoc" in {
      val auth = newDB

      evalOk(
        auth,
        """|Collection.create({ name: "Bar", fields: { a: { signature: "Int?" } } })
           |
           |Collection.create({
           |  name: "Foo",
           |  fields: {
           |    doc_ref: { signature: "Bar | NullBar" },
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ doc_ref: Bar.create({}) })")
      evalOk(auth, "Foo.create({ doc_ref: Bar.byId(0) })")
    }

    "two refs work" in {
      val auth = newDB()

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo { a: Int }
             |collection Bar { b: Int }
             |
             |collection Baz {
             |  foo: Ref<Foo>
             |  bar: Ref<Bar>
             |}
             |
             |function doit() {
             |  let getFoo: (String) => Foo = id => Foo(0)!
             |  let getBar: (String) => Bar = id => Bar(0)!
             |
             |  let foo = getFoo("")
             |  let bar = getBar("")
             |
             |  Baz.create({ foo: foo, bar: bar })
             |}
             |""".stripMargin
      )
    }
  }

  "backfill value" - {
    "schema update is disallowed if the default fails" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: "1/0" }
           |  }
           |})
           |""".stripMargin
      )
      err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Failed to create Collection.
           |constraint failures:
           |  my_field: Default field evaluation failed.
           |      error: Attempted integer division by zero.
           |      at *field:my_field*:1:3
           |        |
           |      1 | 1/0
           |        |   ^
           |        |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     my_field: { signature: "Number", default: "1/0" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "nested internal fields don't show up" in {
      val doc = evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})
           |""".stripMargin
      )

      val id = doc.asInstanceOf[Value.Doc].id

      val backfill0 = evalOk(
        auth,
        "Foo.definition.fields.my_field.backfill_value",
        typecheck = false)
      backfill0.isNull shouldBe true

      // Insert the field, and it should not show up, because it's internal.
      val diff =
        MapV("fields" -> MapV("my_field" -> MapV("backfill_value" -> "bar")))

      val cfg = ctx ! SchemaCollection.Collection(auth.scopeID)
      ctx ! Store.internalUpdate(cfg.Schema, id, Diff(diff))

      val doc0 = ctx ! Store.getUnmigrated(auth.scopeID, id)
      doc0.get.data.fields
        .get(List("fields", "my_field", "backfill_value")) shouldBe Some(
        StringV("bar"))

      evalOk(auth, "Foo.definition.fields.my_field") shouldBe Value.Struct(
        "signature" -> Value.Str("Number"))

      val backfill1 = evalOk(
        auth,
        "Foo.definition.fields.my_field.backfill_value",
        typecheck = false)
      backfill1.isNull shouldBe true

      // `.replace()` should leave the field alone.
      evalOk(
        auth,
        """|Foo.definition.replace({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number" }
           |  }
           |})""".stripMargin
      )

      val doc1 = ctx ! Store.getUnmigrated(auth.scopeID, id)
      doc1.get.data.fields
        .get(List("fields", "my_field", "backfill_value")) shouldBe Some(
        StringV("bar"))
    }

    "backfill_value is stored internally" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    my_field: { signature: "Number", default: "1 + 2" }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.definition.fields") shouldBe Value.Struct(
        "my_field" -> Value.Struct(
          "signature" -> Value.Str("Number"),
          "default" -> Value.Str("1 + 2")
        )
      )

      val ref = evalOk(auth, "Foo.definition")
      val doc =
        ctx ! Store.getUnmigrated(auth.scopeID, ref.asInstanceOf[Value.Doc].id)
      (doc.get.data.fields.get(List("fields", "my_field"))) shouldBe Some(
        MapV(
          "signature" -> "Number",
          "default" -> "1 + 2",
          "backfill_value" -> 3
        ))
    }
  }

  "syntax sugar" - {
    // This seemed like the most authentic place to test type operator ?.
    "sweet ?" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    a: { signature: "Int?" }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({})")
      evalOk(auth, "Foo.create({ id: 0, a: 1 })")
      evalOk(auth, "Foo.byId(0)!.a!") shouldBe Value.Int(1)
    }
  }
}

class FQL2PersistedFieldsWithV4Spec extends FQL2Spec with DefaultQueryHelpers {

  val auth = newDB

  def evalQuery(auth: Auth, q: JSValue) =
    Query.snapshotTime.flatMap { ts =>
      EvalContext
        .write(auth, ts, APIVersion.V4)
        .parseAndEvalTopLevel(JSON.parse[Literal](q.toByteBuf))
    }

  "nested internal fields don't show up in v4" in {
    val doc = evalOk(
      auth,
      """|Collection.create({
         |  name: "Foo",
         |  fields: {
         |    my_field: { signature: "Number" }
         |  }
         |})
         |""".stripMargin
    )

    val id = doc.asInstanceOf[Value.Doc].id

    val res0 = ctx ! evalQuery(
      auth,
      Select(Seq("fields", "my_field", "backfill_value"), Get(ClsRefV("Foo"))))
    res0.isLeft shouldBe true

    // Insert the field, and it should not show up, because it's internal.
    val diff =
      MapV("fields" -> MapV("my_field" -> MapV("backfill_value" -> "bar")))

    val cfg = ctx ! SchemaCollection.Collection(auth.scopeID)
    ctx ! Store.internalUpdate(cfg.Schema, id, Diff(diff))

    val res1 =
      ctx ! evalQuery(auth, Select(Seq("fields", "my_field"), Get(ClsRefV("Foo"))))
    res1.isRight shouldBe true
    res1.toOption.get shouldBe ObjectL("signature" -> StringL("Number"))

    val res2 = ctx ! evalQuery(
      auth,
      Select(Seq("fields", "my_field", "backfill_value"), Get(ClsRefV("Foo"))))
    res2.isLeft shouldBe true

    // `.replace()` in v4 cannot set this field, so it'll leave the fields alone.
    val res3 =
      ctx ! evalQuery(auth, Replace(ClsRefV("Foo"), MkObject("name" -> "Foo")))
    res3.isRight shouldBe true

    val doc1 = ctx ! Store.getUnmigrated(auth.scopeID, id)
    doc1.get.data.fields
      .get(List("fields", "my_field", "backfill_value")) shouldBe Some(
      StringV("bar"))
  }
}
