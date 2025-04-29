package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.repo.values.Value
import fql.parser.Tokens
import scala.collection.SeqMap

class SchemaCollectionSpec extends FQL2WithV4Spec {
  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "document_ttls" - {
    "infers the correct setting based on defined fields" in {
      // "There are defined fields" equivalent to "document_ttls = false".
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection A0 {
             |  name: String
             |}""".stripMargin
      )
      evalOk(auth, "A0.definition.document_ttls").isNull shouldBe true
      evalErr(auth, "A0.create({ ttl: Time.now() })").errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ ttl: Time }` is not a subtype of `{ id: ID | Null, name: String }`
           |at *query*:1:11
           |  |
           |1 | A0.create({ ttl: Time.now() })
           |  |           ^^^^^^^^^^^^^^^^^^^
           |  |
           |cause: Type `{ ttl: Time }` contains extra field `ttl`
           |  |
           |1 | A0.create({ ttl: Time.now() })
           |  |           ^^^^^^^^^^^^^^^^^^^
           |  |
           |cause: Type `{ ttl: Time }` does not have field `name`
           |  |
           |1 | A0.create({ ttl: Time.now() })
           |  |           ^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // "There are no defined fields" equivalent to "document_ttls = true".
      updateSchemaOk(
        auth,
        "main.fsl" ->
          "collection A1 {}"
      )
      evalOk(auth, "A1.definition.document_ttls").isNull shouldBe true
      evalOk(auth, "A1.create({ ttl: Time.now() })")

      // Setting it explicitly stores it and overrides the inferred default.
      // = true.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection A2 {
             |  name: String
             |
             |  document_ttls true
             |}""".stripMargin
      )
      evalOk(auth, "A2.definition.document_ttls") shouldBe Value.True
      evalOk(auth, "A2.create({ name: 'Bob', ttl: Time.now() })")

      // = false.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection A3 {
             |  document_ttls false
             |}""".stripMargin
      )
      evalOk(auth, "A3.definition.document_ttls") shouldBe Value.False
      evalErr(auth, "A3.create({ ttl: Time.now() })").errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Type `Time` is not a subtype of `Null`
           |at *query*:1:11
           |  |
           |1 | A3.create({ ttl: Time.now() })
           |  |           ^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "allows seeing and setting ttl when true" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  name: String
             |
             |  document_ttls true
             |}""".stripMargin
      )
      evalOk(auth, "User.definition.document_ttls") shouldBe Value.True

      evalOk(
        auth,
        """|User.create({
           |  id: 0,
           |  name: 'Bob',
           |  ttl: Time.now().add(1, 'days')
           |})""".stripMargin
      )
      evalOk(
        auth,
        "User.byId(0)!.ttl!.difference(Time.now(), 'hours') > 12") shouldBe Value.True
    }

    "allows seeing but not setting ttl when false" in {
      // Allow TTLs and create a doc with one so we can check it later.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  name: String
             |
             |  document_ttls true
             |}""".stripMargin
      )
      evalOk(auth, "User.definition.document_ttls") shouldBe Value.True

      // Create.
      evalOk(
        auth,
        """|User.create({
           |  id: 0,
           |  name: 'Bob',
           |  ttl: Time.now().add(1, 'days')
           |})""".stripMargin
      )
      evalOk(
        auth,
        "User.byId(0)!.ttl!.difference(Time.now(), 'hours') > 12") shouldBe Value.True

      // Update.
      evalOk(
        auth,
        "User.byId(0)!.update({ ttl: Time.now().add(2, 'days') })"
      )
      evalOk(
        auth,
        "User.byId(0)!.ttl!.difference(Time.now(), 'hours') > 24") shouldBe Value.True

      // Replace.
      evalOk(
        auth,
        "User.byId(0)!.replace({ name: 'Gob', ttl: Time.now().add(3, 'days') })"
      )
      evalOk(
        auth,
        "User.byId(0)!.ttl!.difference(Time.now(), 'hours') > 36") shouldBe Value.True

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  name: String
             |
             |  document_ttls false
             |}""".stripMargin
      )
      evalOk(auth, "User.definition.document_ttls") shouldBe Value.False

      // Create.
      evalErr(
        auth,
        "User.create({ name: 'Alice', ttl: Time.now().add(1, 'hours') })"
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ name: "Alice", ttl: Time }` contains extra field `ttl`
           |at *query*:1:13
           |  |
           |1 | User.create({ name: 'Alice', ttl: Time.now().add(1, 'hours') })
           |  |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // Update.
      evalErr(
        auth,
        "User.byId(0)!.update({ ttl: Time.now() })"
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ ttl: Time }` contains extra field `ttl`
           |at *query*:1:22
           |  |
           |1 | User.byId(0)!.update({ ttl: Time.now() })
           |  |                      ^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // Replace.
      evalErr(
        auth,
        "User.byId(0)!.replace({ name: 'Gob', ttl: Time.now() })"
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Type `{ name: "Gob", ttl: Time }` contains extra field `ttl`
           |at *query*:1:23
           |  |
           |1 | User.byId(0)!.replace({ name: 'Gob', ttl: Time.now() })
           |  |                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      )

      // TTL is visible, though.
      evalOk(
        auth,
        "User.byId(0)!.ttl!.difference(Time.now(), 'hours') > 12") shouldBe Value.True
    }

    "still applies ttl_days when document_ttls are disallowed" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  ttl_days 1
             |  document_ttls false
             |}""".stripMargin
      )

      evalOk(auth, "User.create({ id: 0 })")
      evalOk(
        auth,
        "User.byId(0)!.ttl!.difference(Time.now(), 'hours') > 12") shouldBe Value.True
    }
  }

  "check constraints" - {
    "creates" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check is_it_valid (doc => doc.isValid)
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ isValid: true })")
      val err = evalErr(auth, "User.create({ isValid: false })")
      err.code shouldBe "constraint_failure"
    }

    "creates with short lambdas" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check is_it_valid (.isValid)
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ isValid: true })")
      val err = evalErr(auth, "User.create({ isValid: false })")
      err.code shouldBe "constraint_failure"
    }

    "disallows non-identifier names" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check "foo bar" (.isValid)
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Invalid identifier `foo bar`
           |at main.fsl:2:9
           |  |
           |2 |   check "foo bar" (.isValid)
           |  |         ^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "makes a nice error when missing parenthesis" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check bar doc => doc.foo
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Expected lambda check predicate
           |at main.fsl:2:13
           |  |
           |2 |   check bar doc => doc.foo
           |  |             ^^^^^^^^^^^^^^ Check constraints require parenthesis around the lambda
           |  |""".stripMargin
      )
    }

    "typechecking runs on check constraints" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check foo_bar (_ => 3.foo)
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Type `Int` does not have field `foo`
           |at main.fsl:2:25
           |  |
           |2 |   check foo_bar (_ => 3.foo)
           |  |                         ^^^
           |  |""".stripMargin
      )

      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check foo_bar (_ => if (true) 3 else true)
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Type `Int` is not a subtype of `Null | Boolean`
           |at main.fsl:2:33
           |  |
           |2 |   check foo_bar (_ => if (true) 3 else true)
           |  |                                 ^
           |  |""".stripMargin
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check foo_bar (_ => if (true) true else null)
             |}
             |""".stripMargin
      )
    }

    "translates short lambdas from FQL to FSL" in {
      updateSchemaOk(auth, "main.fsl" -> "")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "User",
           |  constraints: [{
           |    check: {
           |      name: "foo",
           |      body: "(.bar > 3)"
           |    }
           |  }]
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {
           |  check foo (.bar > 3)
           |}
           |""".stripMargin
      )
    }

    "updates don't blow up ValidIdentifierValidator" in {
      updateSchemaOk(
        auth,
        "main.fsl" -> "collection User {}"
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  check foo (.bar > 3)
             |}
             |""".stripMargin
      )
    }
  }

  "computed fields" - {
    "creates" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  compute foo = (doc => doc.bar + 3)
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ bar: 2 }).foo") shouldBe Value.Int(5)
    }

    "parenthesis are optional" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  compute foo = doc => doc.baz + 3
             |  compute bar = (doc => doc.baz + 4)
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ baz: 2 }) { foo, bar }") shouldBe Value.Struct(
        "foo" -> Value.Int(5),
        "bar" -> Value.Int(6))
    }

    "parenthesis are required for short lambdas" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  compute foo = .bar
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Invalid anonymous field access
           |at main.fsl:2:17
           |  |
           |2 |   compute foo = .bar
           |  |                 ^^^^
           |  |""".stripMargin
      )
    }

    "disallows non-identifier names" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  compute "foo bar" = doc => doc.bar
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Invalid identifier `foo bar`
           |at main.fsl:2:11
           |  |
           |2 |   compute "foo bar" = doc => doc.bar
           |  |           ^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "disallows reserved names" in {
      Tokens.SpecialFieldNames foreach { bad =>
        updateSchemaErr(
          auth,
          "main.fsl" ->
            s"""|collection User {
              |  compute $bad = (.count + 1)
              |}""".stripMargin
        ) should include(s"error: Invalid reserved name `$bad`")
      }
    }

    "creates with short lambdas" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  compute foo = (.bar + 3)
             |}
             |""".stripMargin
      )
      evalOk(auth, "User.create({ bar: 2 }).foo") shouldBe Value.Int(5)
    }

    "translates short lambdas from FQL to FSL" in {
      updateSchemaOk(auth, "main.fsl" -> "")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "User",
           |  computed_fields: {
           |    foo: { body: "(.bar + 3)" }
           |  }
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {
           |  compute foo = (.bar + 3)
           |}
           |""".stripMargin
      )
    }

    "converts signature from FSL to FQL" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  compute foo: Number = _ => 2
             |  index byFoo { terms [.foo] }
             |}
             |collection Bar {
             |  compute foo = _ => 2
             |  index byFoo { terms [.foo] }
             |}
             |""".stripMargin
      )

      // Foo has an explicit signature which should widen the type.
      evalRes(auth, "Foo.create({}).foo").typeStr shouldBe "Number"
      evalRes(auth, "(x) => Foo.byFoo(x)").typeStr shouldBe "Number => Set<Foo>"

      // Bar does not, so the inferred type should be used instead.
      evalRes(auth, "Bar.create({}).foo").typeStr shouldBe "2"
      evalRes(auth, "(x) => Bar.byFoo(x)").typeStr shouldBe "2 => Set<Bar>"
    }

    "converts signature from FQL to FSL" in {
      updateSchemaOk(auth, "main.fsl" -> "")
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  computed_fields: {
           |    foo: {
           |      body: "_ => 2",
           |      signature: "Number"
           |    }
           |  }
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection Foo {
           |  compute foo: Number = (_) => 2
           |}
           |""".stripMargin
      )
    }

    "error includes computed field name" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  compute foo = (_ => abort('hi'))
             |}
             |""".stripMargin
      )
      val err = evalErr(auth, "User.create({}).foo")
      err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Query aborted.
           |at *computed_field:User:foo*:1:13
           |  |
           |1 | (_) => abort("hi")
           |  |             ^^^^^^
           |  |
           |at *query*:1:17
           |  |
           |1 | User.create({}).foo
           |  |                 ^^^
           |  |""".stripMargin
      )
    }

    "updates body" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  compute a = _ => 0
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldEqual Value.Int(0)

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  compute a = _ => "0"
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldEqual Value.Str("0")
    }

    "changes signature" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  x: Int
             |  compute a: Number = _ => 0
             |}""".stripMargin
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  x: Int
             |  compute a: Int = _ => 0
             |}""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection Foo {
           |  x: Int
           |  compute a: Int = _ => 0
           |}""".stripMargin
      )
    }

    "adds signature" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  x: Int
             |  compute a = _ => 0
             |}""".stripMargin
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  x: Int
             |  compute a: Number = _ => 0
             |}""".stripMargin
      )

      // Addition is reflected in FSL...
      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection Foo {
           |  x: Int
           |  compute a: Number = _ => 0
           |}""".stripMargin
      )

      // but not the model.
      pendingUntilFixed {
        evalOk(
          auth,
          "Foo.definition.computed_fields.a.signature"
        ) shouldEqual Value.Str("Number")
      }
    }

    "removes signature" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  x: Int = 0
             |  compute a: Number = _ => 0
             |}""".stripMargin
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  x: Int = 0
             |  compute a = _ => 0
             |}""".stripMargin
      )

      // Removal is reflected in FSL...
      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection Foo {
           |  x: Int = 0
           |  compute a = _ => 0
           |}""".stripMargin
      )

      // ... but not the model.
      pendingUntilFixed {
        evalOk(
          auth,
          "Foo.definition.computed_fields.a.signature"
        ).isNull shouldBe true
      }
    }
  }

  "defined fields" - {
    "creates" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: Number
             |
             |  migrations {
             |    add .foo
             |    backfill .foo = 0
             |  }
             |}
             |""".stripMargin
      )

      evalOk(auth, "User.definition.fields") shouldBe Value.Struct(
        "foo" -> Value.Struct("signature" -> Value.Str("Number")))
      evalOk(auth, "User.create({ foo: 2 }).foo") shouldBe Value.Int(2)
      evalErr(auth, "User.create({ foo: 'hi' })")
      evalErr(auth, "User.create({ foo: '' })")
    }

    "disallows non-identifier names" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection User {
             |  "foo bar": String
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Invalid identifier `foo bar`
           |at main.fsl:2:3
           |  |
           |2 |   "foo bar": String
           |  |   ^^^^^^^^^
           |  |""".stripMargin
      )
    }

    "disallows reserved names" in {
      Tokens.SpecialFieldNames foreach { bad =>
        updateSchemaErr(
          auth,
          "main.fsl" ->
            s"""|collection User {
              |  $bad: Number
              |}""".stripMargin
        ) should include(s"error: Invalid reserved name `$bad`")
      }
    }

    "translates from FQL to FSL" in {
      updateSchemaOk(auth, "main.fsl" -> "")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "User",
           |  fields: {
           |    foo: { signature: "Number" }
           |  }
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {
           |  foo: Number
           |}
           |""".stripMargin
      )
    }

    "translates multiple defined and computed fields from FQL to FSL" in {
      updateSchemaOk(auth, "main.fsl" -> "")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "User",
           |  fields: {
           |    foo: { signature: "Number" },
           |    bar: { signature: "Number" },
           |    with_default: { signature: "Number", default: "2 + 3" }
           |  },
           |  computed_fields: {
           |    aaa: { body: "(.foo + .bar ?? 0)" },
           |    bbb: { body: "(.foo * .bar ?? 0)" }
           |  }
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {
           |  foo: Number
           |  bar: Number
           |  with_default: Number = 2 + 3
           |  compute aaa = (.foo + .bar ?? 0)
           |  compute bbb = (.foo * .bar ?? 0)
           |}
           |""".stripMargin
      )
    }

    "works with defaults" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Number = 0
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldEqual Value.Int(0)
    }

    "require signatures" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a = 10
             |}""".stripMargin
      ) shouldBe (
        """|error: Expected a type signature
           |at main.fsl:2:3
           |  |
           |2 |   a = 10
           |  |   ^^^^^^
           |  |""".stripMargin
      )
    }

    "can change signatures" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Number
             |
             |  migrations {
             |    add .a
             |    backfill .a = 0
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({ a: 0 })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: String
             |
             |  migrations {
             |    add .a
             |    backfill .a = 0
             |    split .a -> .X, .Y
             |    drop .X
             |    drop .Y
             |    add .a
             |    backfill .a = ""
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({ a: '0' })")
    }

    "can change defaults" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Number = 3
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldBe Value.Int(3)

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Number = 5
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({}).a") shouldBe Value.Int(5)
    }

    "typechecks defaults" in {
      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: String = 0
             |}""".stripMargin
      ) shouldBe (
        """|error: Type `Int` is not a subtype of `String`
           |at main.fsl:2:15
           |  |
           |2 |   a: String = 0
           |  |               ^
           |  |""".stripMargin
      )
    }
  }

  "indexes" - {
    "updating an index works as expected" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  index byFoo { terms [.foo] }
             |}
             |""".stripMargin
      )

      evalOk(
        auth,
        """|Collection.byName('User')!.update({
           |  indexes: {
           |    byFoo: {
           |      terms: [{ field: "foo" }, { field: "bar" }]
           |    }
           |  }
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|collection User {
           |  index byFoo { terms [.foo, .bar] }
           |}
           |""".stripMargin
      )
    }
  }

  "migrations" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  _no_wildcard: Null
           |}""".stripMargin
    )
    // Make a doc, so that migrations are validated correctly.
    evalOk(auth, "User.create({})")

    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: Int
           |  _no_wildcard: Null
           |}
           |""".stripMargin
    ) shouldBe (
      """|error: Field `.foo` is not present in the live schema
         |at main.fsl:2:3
         |  |
         |2 |   foo: Int
         |  |   ^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     add .foo
         |4 | |     backfill .foo = <expr>
         |5 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |  |
         |2 |   foo: Int = <expr>
         |  |           +++++++++
         |  |
         |hint: Make the field nullable
         |  |
         |2 |   foo: Int?
         |  |           +
         |  |""".stripMargin
    )

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: Int | String | Null
           |
           |  migrations {
           |    add .foo
           |  }
           |
           |  _no_wildcard: Null
           |}
           |""".stripMargin
    )

    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection User {
           |  _no_wildcard: Null
           |}""".stripMargin
    ) shouldBe (
      """|error: Field `.foo` is missing from the submitted schema
         |at main.fsl:1:1
         |  |
         |1 |   collection User {
         |  |  _^
         |2 | |   _no_wildcard: Null
         |3 | | }
         |  | |_^
         |  |
         |hint: Drop the field and its data with a `drop` migration
         |at main.fsl:3:1
         |  |
         |3 |   migrations {
         |  |  _+
         |4 | |     drop .foo
         |5 | |   }
         |  | |____^
         |  |""".stripMargin
    )

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  migrations {
           |    drop .foo
           |  }
           |
           |  _no_wildcard: Null
           |}
           |""".stripMargin
    )
  }

  "fields can be changed with an empty collection" in {
    updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Number
           |}""".stripMargin
    )

    // Empty collections should still validate migration block integrity.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  b: Number
           |
           |  migrations {
           |    add .b
           |    add .b
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Cannot declare non-nullable field `.b`: it has no default and no backfill value
         |at main.fsl:6:9
         |  |
         |6 |     add .b
         |  |         ^^
         |  |
         |hint: Make the field nullable
         |at main.fsl:2:12
         |  |
         |2 |   b: Number?
         |  |            +
         |  |
         |hint: Add a default value to this field
         |at main.fsl:2:12
         |  |
         |2 |   b: Number = <expr>
         |  |            +++++++++
         |  |
         |hint: Add a `backfill` migration
         |at main.fsl:7:3
         |  |
         |7 |     backfill .b = <expr>
         |  |   +++++++++++++++++++++++
         |  |
         |error: Cannot add field `.b` twice
         |at main.fsl:6:9
         |  |
         |6 |     add .b
         |  |         ^^
         |  |
         |hint: Field added here
         |at main.fsl:5:9
         |  |
         |5 |     add .b
         |  |         ^^
         |  |
         |hint: Remove the extra add
         |  |
         |6 |     add .b
         |  |     ------
         |  |""".stripMargin
    )

    // And the TypeEnvValidator should mirror the above behavior.
    evalOk(
      auth,
      """|Collection.byName("Foo")!.update({
         |  fields: {
         |    c: { signature: "String" }
         |  }
         |})""".stripMargin
    )
    renderErr(
      auth,
      """|Collection.byName("Foo")!.update({
         |  fields: {
         |    d: { signature: "String" }
         |  },
         |  migrations: [
         |    { add: { field: ".d" } },
         |    { add: { field: ".d" } }
         |  ]
         |})""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |    error: Cannot declare non-nullable field `.d`: it has no default and no backfill value
         |    at <no source>
         |    hint: Make the field nullable
         |    at *field:Foo:d*:1:7
         |      |
         |    1 | String?
         |      |       +
         |      |
         |    hint: Add a default value to this field
         |    at *field:Foo:d*:1:7
         |      |
         |    1 | String = <expr>
         |      |       +++++++++
         |      |
         |    hint: Add a `backfill` migration
         |    at <no source>
         |    error: Cannot add field `.d` twice
         |    at <no source>
         |    hint: Field added here
         |    at <no source>
         |    hint: Remove the extra add
         |    at <no source>
         |at *query*:1:33
         |  |
         |1 |   Collection.byName("Foo")!.update({
         |  |  _________________________________^
         |2 | |   fields: {
         |3 | |     d: { signature: "String" }
         |4 | |   },
         |5 | |   migrations: [
         |6 | |     { add: { field: ".d" } },
         |7 | |     { add: { field: ".d" } }
         |8 | |   ]
         |9 | | })
         |  | |__^
         |  |""".stripMargin
    )
  }

  // FIXME: The TypeEnvValidator doesn't check backfills and defaults for empty
  // collections.
  "applies runtime type checks to backfill and default values" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |}""".stripMargin
    )

    // Don't insert a document. Backfills and defaults should be checked whether or
    // not the collection is empty.

    // Checks backfills values.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |  b: Int
           |
           |  migrations {
           |    add .b
           |    backfill .b = { let x: Any = '0'; x }
           |  }
           |}""".stripMargin
    )

    // Checks defaults.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |  b: Int = { let x: Any = '0'; x }
           |
           |  migrations {
           |    add .b
           |  }
           |}""".stripMargin
    )

    renderErr(auth, "Foo.create({ a: 3 })") shouldBe (
      """|error: Failed to create document in collection `Foo`.
         |constraint failures:
         |  b: Expected Int, provided String
         |at *query*:1:11
         |  |
         |1 | Foo.create({ a: 3 })
         |  |           ^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "applies runtime type checks to backfill values with non-empty collections" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ a: 3 })")

    // Checks backfills values.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |  b: Int
           |
           |  migrations {
           |    add .b
           |    backfill .b = { let x: Any = '0'; x }
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |    error: Failed to convert backfill value.
         |    constraint failures:
         |      b: Expected Int, provided String
         |    at <no source>
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   a: Int
         |3 | |   b: Int
         |4 | |
         |5 | |   migrations {
         |6 | |     add .b
         |7 | |     backfill .b = { let x: Any = '0'; x }
         |8 | |   }
         |9 | | }
         |  | |_^
         |  |""".stripMargin
    )
  }

  "applies runtime type checks to default values with non-empty collections" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ a: 3 })")

    // Checks defaults.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |  b: Int = { let x: Any = '0'; x }
           |
           |  migrations {
           |    add .b
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |    error: Failed to convert backfill value.
         |    constraint failures:
         |      b: Expected Int, provided String
         |    at foo:1:1
         |      |
         |    1 |   {
         |      |  _^
         |    2 | |   let x: Any = "0"
         |    3 | |   x
         |    4 | | }
         |      | |_^
         |      |
         |at main.fsl:1:1
         |  |
         |1 |   collection Foo {
         |  |  _^
         |2 | |   a: Int
         |3 | |   b: Int = { let x: Any = '0'; x }
         |4 | |
         |5 | |   migrations {
         |6 | |     add .b
         |7 | |   }
         |8 | | }
         |  | |_^
         |  |""".stripMargin
    )
  }

  "apply computed field signature changes correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  compute a: Number = _ => 1
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Collection.byName('Foo')!.update({
         |  computed_fields: {
         |    a: {
         |      body: "_ => 1",
         |      signature: "Int"
         |    }
         |  }
         |})""".stripMargin
    )

    schemaContent(auth, "main.fsl") shouldBe Some(
      """|collection Foo {
         |  compute a: Int = _ => 1
         |}""".stripMargin
    )
  }

  "apply field default changes correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Number
           |
           |  migrations {
           |    add .a
           |    backfill .a = 0
           |  }
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Collection.byName('Foo')!.update({
         |  fields: {
         |    a: {
         |      default: "0"
         |    }
         |  }
         |})""".stripMargin
    )

    schemaContent(auth, "main.fsl") shouldBe Some(
      """|collection Foo {
         |  a: Number = 0
         |
         |  migrations {
         |    add .a
         |    backfill .a = 0
         |  }
         |}""".stripMargin
    )
  }

  "handles ref types correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  foo: Ref<Foo>?
           |}
           |
           |collection Bar {}
           |""".stripMargin
    )

    evalOk(auth, "Foo.create({ id: 0, foo: null })")

    // a live ref.
    evalOk(auth, "Foo.create({ foo: Foo(0) })")

    // a non-existent ref.
    evalOk(auth, "Foo.create({ foo: Foo(1234) })")

    // a ref to another collection.
    renderErr(auth, "Foo.create({ foo: Bar(0) })") shouldBe (
      """|error: Type `{ foo: Ref<Bar> }` is not a subtype of `{ id: ID | Null, foo: Ref<Foo> | Null }`
         |at *query*:1:12
         |  |
         |1 | Foo.create({ foo: Bar(0) })
         |  |            ^^^^^^^^^^^^^^^
         |  |
         |cause: Type `{ id: ID, ts: Time, ttl: Time | Null, *: Any }` cannot have a wildcard
         |  |
         |1 | Foo.create({ foo: Bar(0) })
         |  |            ^^^^^^^^^^^^^^^
         |  |
         |cause: Type `{ id: ID, ts: Time, ttl: Time | Null, *: Any }` is not a subtype of `Null`
         |  |
         |1 | Foo.create({ foo: Bar(0) })
         |  |            ^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )

    // a ref to another collection.
    renderErr(auth, "Foo.create({ foo: Bar(0) })", typecheck = false) shouldBe (
      """|error: Failed to create document in collection `Foo`.
         |constraint failures:
         |  foo: Expected Foo | NullFoo | Null, provided Document
         |at *query*:1:11
         |  |
         |1 | Foo.create({ foo: Bar(0) })
         |  |           ^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "apply computed field signature add correctly" in pendingUntilFixed {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  compute a = _ => 1
           |}""".stripMargin
    )

    // FIXME: Adding a signature isn't picked up by the diff.
    evalOk(
      auth,
      """|Collection.byName('Foo')!.update({
         |  computed_fields: {
         |    a: {
         |      body: "_ => 1",
         |      signature: "Int"
         |    }
         |  }
         |})""".stripMargin
    )

    schemaContent(auth, "main.fsl") shouldBe Some(
      """|collection Foo {
         |  compute a: Int = _ => 1
         |}""".stripMargin
    )
  }

  "translates finite history_days correctly" in {
    evalOk(
      auth,
      """|Collection.create({
         |  name: "Foo",
         |  history_days: 3
         |})""".stripMargin
    )

    schemaContent(auth, "main.fsl") shouldBe Some(
      """|// The following schema is auto-generated.
         |// This file contains FSL for FQL10-compatible schema items.
         |
         |collection Foo {
         |  history_days 3
         |}
         |""".stripMargin
    )
  }

  "translates infinite history_days correctly" in {
    // Create with null won't work, this will set history_days to zero.
    // Update works, and this will make history infinite.
    evalOk(
      auth,
      """|let c = Collection.create({ name: "Foo" })
         |c.update({ history_days: null })
         |""".stripMargin
    )

    schemaContent(auth, "main.fsl") shouldBe Some(
      """|// The following schema is auto-generated.
         |// This file contains FSL for FQL10-compatible schema items.
         |
         |collection Foo {
         |  history_days 9223372036854775807
         |}
         |""".stripMargin
    )
  }

  "translate missing history_days to FQL correctly" in {
    evalOk(auth, "Collection.create({ name: 'Foo' })")

    // Perform an update to force `SchemaManager` to update the model definition.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |""".stripMargin
    )

    evalOk(auth, "Collection.byName('Foo')!.history_days") shouldBe Value.Long(0)
  }

  // Partial values pointing to nested refs shouldn't
  // cause a 500.
  //
  // FIXME: Move elsewhere, once `SchemaSpec` and `FQL2Spec` are merged.
  "partials in nested refs should not blow up" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Bar {}
           |collection Foo {
           |  index byBar {
           |    values [.bar.created_at]
           |  }
           |}
           |""".stripMargin
    )

    evalOk(auth, "Bar.create({ id: 0, created_at: Time('2022-02-02T22:22:22Z') })")
    evalOk(auth, "Foo.create({ bar: Bar(0) })")

    // `bar` missing should work.
    evalOk(auth, "Foo.create({})")

    // `bar` as a non-ref should work.
    evalOk(auth, "Foo.create({ bar: '' })")

    // Object.assign materializes the value, the same way rendering a query response
    // would.
    evalOk(
      auth,
      """|Object.assign(
         |  {},
         |  Foo.byBar().map(doc => doc.bar).paginate(16)
         |).data!.length""".stripMargin) shouldBe Value.Int(3)
  }

  "partials in nested refs should not return incorrect values" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Bar {}
           |collection Foo {
           |  index byBar {
           |    values [.bar.created_at]
           |  }
           |}
           |""".stripMargin
    )

    evalOk(auth, "Bar.create({ id: 0, created_at: Time('2022-02-02T22:22:22Z') })")
    evalOk(auth, "Foo.create({ bar: Bar(0) })")

    evalOk(
      auth,
      """|Object.assign(
         |  {},
         |  Foo.byBar().map(doc => doc.bar.created_at).paginate(16)
         |).data[0]""".stripMargin
    ) shouldBe evalOk(auth, "Time('2022-02-02T22:22:22Z')")
  }

  "schema is not modified when it shouldn't be" in {
    val schema =
      """|collection Foo {
         |  x: Int
         |  history_days 0
         |}
         |""".stripMargin

    updateSchemaOk(auth, "main.fsl" -> schema)
    schemaContent(auth, "main.fsl") shouldBe Some(schema)
  }

  "does not try to re-apply migrations because of weird parsing" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Bar {
           |  b: String
           |
           |  migrations {
           |    add .b
           |    backfill .b = ''
           |  }
           |}""".stripMargin
    )

    // Document for the document gods.
    evalOk(auth, "Bar.create({ b: '0' })")

    // Modify the schema in a way unrelated to migrations. This should be a migration
    // no-op as the old migrations are recognized as done, but previously it failed
    // trying to re-apply the migrations again.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Bar {
           |  b: String
           |
           |  // Migration-unrelated change.
           |  check ok (_ => true)
           |
           |  migrations {
           |    add .b
           |    backfill .b = ''
           |  }
           |}""".stripMargin
    )
  }

  "does not allow unique constraints with no terms" in {
    // FQL.
    evalErr(
      auth,
      """|Collection.create({
         |  name: "Foo",
         |  constraints: [
         |    { unique: [] }
         |  ]
         |})""".stripMargin
    ).errors.head.renderWithSource(Map.empty) shouldBe (
      """|error: Failed to create Collection.
         |constraint failures:
         |  constraints: Unique constraints must have at least one term
         |at *query*:1:18
         |  |
         |1 |   Collection.create({
         |  |  __________________^
         |2 | |   name: "Foo",
         |3 | |   constraints: [
         |4 | |     { unique: [] }
         |5 | |   ]
         |6 | | })
         |  | |__^
         |  |""".stripMargin
    )

    // FSL.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection Bar {
             |  unique []
             |}""".stripMargin
    ) shouldBe (
      """|error: Expected non-empty terms for a unique constraint
         |at main.fsl:2:10
         |  |
         |2 |   unique []
         |  |          ^^
         |  |""".stripMargin
    )
  }

  "doesn't fail the runtime typecheck for defaults of phantom fields" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  _no_wc: Int = 0
           |}""".stripMargin
    )

    // There's no default for `x` because it's never declared and it isn't
    // backfilled. Null will be used to signal the field is unset, and this shouldn't
    // blow up the update because the rewind type of `x` is `Int | String | Null`.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  _no_wc: Int = 0
           |  y: Int = 0
           |  z: String = ""
           |
           |  migrations {
           |    add .x
           |    split .x -> .y, .z
           |  }
           |}""".stripMargin
    )

    // For completeness, verify that the default is typechecked at runtime if the
    // phantom field has a backfill.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  _no_wc: Int = 0
           |  y: Int = 0
           |  z: String = ""
           |
           |  migrations {
           |    add .x
           |    backfill .x = true
           |    split .x -> .y, .z
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Type `Boolean` is not a subtype of `Null | String | Int`
         |at main.fsl:8:19
         |  |
         |8 |     backfill .x = true
         |  |                   ^^^^
         |  |
         |hint: Field type defined here
         |at <no source>""".stripMargin
    )
  }

  "backfill the default if split source is unset (nullable field)" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int | String? | Boolean
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ id: 0 })")

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int = 1
           |  b: String
           |  c: Boolean?
           |
           |  migrations {
           |    split .a -> .a, .b, .c
           |    backfill .b = "-"
           |  }
           |}""".stripMargin
    )

    // `a` is unset so all split targets (including `a`) should be the default.
    // For nullable `c`, the default is null (unset).
    evalOk(auth, "Foo.byId(0)! { a, b }") shouldBe
      Value.Struct.Full(SeqMap("a" -> Value.Int(1), "b" -> Value.Str("-")))
    evalOk(auth, "Foo.byId(0)!.c").isNull shouldBe true // Easier separately.
  }

  "backfill the default if split source is unset (phantom field)" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  _no_wc: Int = 0
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ id: 0 })")

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |   _no_wc: Int = 0
           |  b: Int? = 1
           |  c: String = "-"
           |
           |  migrations {
           |    add .a
           |    split .a -> .b, .c
           |  }
           |}""".stripMargin
    )

    // `a` is unset (null) because it's a phantom, and `b` is nullable, so `b` could
    // get the value `null` in the split, but we want it to get the default instead.
    evalOk(auth, "Foo.byId(0)! { b, c }") shouldBe Value.Struct.Full(
      SeqMap("b" -> Value.Int(1), "c" -> Value.Str("-")))
  }

  "don't explode if a term is computed and the uniqueness flag is changed" in {
    // Create an index and a unique constraint that use the same backing index, which
    // will have the uniqueness flag toggled on.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |
           |  compute aa = (doc) => doc.a + 1
           |
           |  index byAA {
           |    terms [.aa]
           |  }
           |
           |  unique [.aa]
           |}""".stripMargin
    )

    evalOk(auth, "Foo.create({ a: 0 })")
    evalErr(auth, "Foo.create({ a: 0 })")

    // Remove the unique constraint, which should toggle off the backing index's
    // uniqueness flag, and it shouldn't error.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |
           |  compute aa = (doc) => doc.a + 1
           |
           |  index byAA {
           |    terms [.aa]
           |  }
           |}""".stripMargin
    )

    // There should be no more uniqueness requirement.
    ctx.cacheContext.invalidateAll() // In case of stale cache.
    evalOk(auth, "Foo.create({ a: 0 })")
  }

  "should allow accessing `id` and `ts`" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  a: Int
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ a: 0 }).id")
    evalOk(auth, "User.create({ a: 0 }).ts")
  }

  "deleting a collection from v4 should correctly regenerate sources on the next update" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  name: String
           |}
           |
           |collection Bar {
           |  name: String
           }""".stripMargin
    )

    evalV4Ok(auth, DeleteF(ClsRefV("Bar")))

    // The FSL files on disk still contain `Bar`, so this update will "remove" bar a
    // second time. There was a bug where this would attempt to delete the
    // collection, as it would not regenerate schema sources from v4 changes.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {
           |  name: String
           |}""".stripMargin
    )
  }

  "add a collection and ref in the same update" in {
    validateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Project {
           |  name: String
           |}
           |
           |collection ActivityMapping {
           |  project: Ref<Project>
           |
           |  migrations {
           |    add .project
           |    backfill .project = Project(0)
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|* Adding collection `ActivityMapping` to main.fsl:5:1:
         |  No semantic changes.
         |
         |* Adding collection `Project` to main.fsl:1:1:
         |  No semantic changes.
         |
         |""".stripMargin
    )

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Project {
           |  name: String
           |}
           |
           |collection ActivityMapping {
           |  project: Ref<Project>
           |
           |  migrations {
           |    add .project
           |    backfill .project = Project(0)
           |  }
           |}""".stripMargin
    )
  }
}
