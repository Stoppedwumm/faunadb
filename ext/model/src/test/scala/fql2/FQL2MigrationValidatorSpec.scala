package fauna.model.test

import fauna.ast.{ PageL, RefL }
import fauna.atoms._
import fauna.auth.AdminPermissions
import fauna.auth.Auth
import fauna.model.schema.fsl.SourceGenerator
import fauna.repo.values.Value
import fql.ast.Span
import fql.migration.SchemaMigration

class FQL2MigrationValidatorSpec extends FQL2WithV4Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "conversion" - {
    "model converts to schema" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    foo: { signature: "Int | Null" },
           |    baz: { signature: "Boolean" },
           |    qux: { signature: "Time" },
           |    dump: { signature: "{ *: Any }?" }
           |  },
           |  migrations: [
           |        { drop: { field: "tmp" } },
           |        { move: { field: "zzz", to: "bar" } },
           |        { split: { field: "bar", to: ["baz", "qux"] } },
           |        { backfill: { field: "baz", value: "true" } },
           |        { backfill: { field: "qux", value: "Time.now()" } },
           |        { add: { field: "dump" } },
           |        { add_wildcard: {} },
           |        { move_conflicts: { into: "dump" } },
           |        { move_wildcard: { into: "dump" } },
           |  ]
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        s"""|${SourceGenerator.Preamble}
            |collection Foo {
            |  foo: Int | Null
            |  baz: Boolean
            |  qux: Time
            |  dump: { *: Any }?
            |  migrations {
            |    drop .tmp
            |    move .zzz -> .bar
            |    split .bar -> .baz, .qux
            |    backfill .baz = true
            |    backfill .qux = Time.now()
            |    add .dump
            |    add_wildcard
            |    move_conflicts .dump
            |    move_wildcard .dump
            |  }
            |}
            |""".stripMargin
      )
    }

    "schema converts to model" in {
      updateSchemaOk(auth, "main.fsl" -> "")
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  tmp: Any
             |  zzz: String | Boolean | Time
             |}""".stripMargin
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  foo: Int | Null
             |  bar: String
             |  baz: Boolean
             |  qux: Time
             |  dump: { *: Any }?
             |  migrations {
             |    add .foo
             |    backfill .foo = 3
             |    drop .tmp
             |    move .zzz -> .yyy
             |    split .yyy -> .bar, .baz, .qux
             |    backfill .bar = ""
             |    backfill .baz = true
             |    backfill .qux = Time.now()
             |    add .dump
             |    add_wildcard
             |    move_conflicts .dump
             |    move_wildcard .dump
             |  }
             |}
             |""".stripMargin
      )

      // This has better error output than `shouldBe` :P
      evalOk(
        auth,
        """|let assert_eq = (a, b) => if (a != b) abort("not equal:\n#{a}\n#{b}")
           |
           |assert_eq(
           |  Collection.byName('Foo') { fields, migrations },
           |  {
           |    fields: {
           |      foo: { signature: "Int | Null" },
           |      bar: { signature: "String" },
           |      baz: { signature: "Boolean" },
           |      qux: { signature: "Time" },
           |      dump: { signature: "{ *: Any }?"}
           |    },
           |    migrations: [
           |      { add: { field: ".foo" } },
           |      { backfill: { field: ".foo", value: "3" } },
           |      { drop: { field: ".tmp" } },
           |      { move: { field: ".zzz", to: ".yyy" } },
           |      { split: { field: ".yyy", to: [".bar", ".baz", ".qux"] } },
           |      { backfill: { field: ".bar", value: "\"\"" } },
           |      { backfill: { field: ".baz", value: "true" } },
           |      { backfill: { field: ".qux", value: "Time.now()" } },
           |      { add: { field: ".dump" } },
           |      { add_wildcard: {} },
           |      { move_conflicts: { into: ".dump" } },
           |      { move_wildcard: { into: ".dump" } }
           |    ]
           |  }
           |)""".stripMargin
      )
    }

    "it handles invalid exprs" in {
      renderErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  migrations: [
           |    { backfill: { field: "foo", value: "####" } },
           |  ]
           |})""".stripMargin
      ) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  migrations[0].backfill.value: Unable to parse FQL source code.
           |      error: Expected expression
           |      at *value*:1:1
           |        |
           |      1 | ####
           |        | ^
           |        |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   migrations: [
           |4 | |     { backfill: { field: "foo", value: "####" } },
           |5 | |   ]
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }
  }

  "migrations apply on read" - {
    "can create a collection" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    foo: { signature: "Int" },
           |  },
           |  migrations: [
           |    { add : {
           |      field: "foo"
           |    } },
           |    { backfill: {
           |      field: "foo",
           |      value: "3",
           |    } }
           |  ]
           |})""".stripMargin
      )
    }

    "can split a field" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    foo: { signature: "Int | String" },
           |  },
           |  migrations: [
           |    { add: {
           |      field: "foo"
           |    } },
           |    { backfill: {
           |      field: "foo",
           |      value: "3",
           |    } }
           |  ]
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 1, foo: 3 })")
      evalOk(auth, "Foo.create({ id: 2, foo: 5 })")
      evalOk(auth, "Foo.create({ id: 3, foo: 'hello' })")
      evalOk(auth, "Foo.create({ id: 4, foo: 'world' })")

      evalOk(
        auth,
        """|Collection.byName("Foo")!.replace({
           |  name: "Foo",
           |  fields: {
           |    bar: { signature: "Int" },
           |    baz: { signature: "String" },
           |  },
           |  migrations: [
           |    { split: { field: "foo", to: ["bar", "baz"] } },
           |    { backfill: { field: "bar", value: "0" } },
           |    { backfill: { field: "baz", value: "''" } }
           |  ]
           |})""".stripMargin
      )

      evalOk(auth, "Foo.byId(1) { bar, baz }") shouldBe Value.Struct(
        "bar" -> Value.Int(3),
        "baz" -> Value.Str(""))
      evalOk(auth, "Foo.byId(2) { bar, baz }") shouldBe Value.Struct(
        "bar" -> Value.Int(5),
        "baz" -> Value.Str(""))
      evalOk(auth, "Foo.byId(3) { bar, baz }") shouldBe Value.Struct(
        "bar" -> Value.Int(0),
        "baz" -> Value.Str("hello"))
      evalOk(auth, "Foo.byId(4) { bar, baz }") shouldBe Value.Struct(
        "bar" -> Value.Int(0),
        "baz" -> Value.Str("world"))
    }

    "can migrate wildcards" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  *: Any
             |}
             |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 1, foo: 3, bar: 'non_conforming' })")
      evalOk(auth, "Foo.create({ id: 2, bar: 5, baz: 3 })")
      evalOk(auth, "Foo.create({ id: 3, baz: 'hi' })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  foo: Int
             |  bar: Int
             |  baz: String
             |  dump: { *: Any }?
             |  *: Any
             |
             |  migrations {
             |    add .dump
             |    add .foo
             |    add .bar
             |    add .baz
             |    backfill .foo = 0
             |    backfill .bar = 0
             |    backfill .baz = 'defaut'
             |    move_conflicts .dump
             |  }
             |}
             |""".stripMargin
      )

      evalOk(auth, "Foo(1)!.data") shouldBe Value.Struct(
        "foo" -> Value.Int(3),
        "bar" -> Value.Int(0),
        "baz" -> Value.Str("defaut"),
        "dump" -> Value.Struct("bar" -> Value.Str("non_conforming"))
      )
      evalOk(auth, "Foo(2)!.data") shouldBe Value.Struct(
        "foo" -> Value.Int(0),
        "bar" -> Value.Int(5),
        "baz" -> Value.Str("defaut"),
        "dump" -> Value.Struct("baz" -> Value.Int(3))
      )
      evalOk(auth, "Foo(3)!.data") shouldBe Value.Struct(
        "foo" -> Value.Int(0),
        "bar" -> Value.Int(0),
        "baz" -> Value.Str("hi")
      )
    }

    "can move wildcards" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  *: Any
             |}
             |""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 1, foo: 3 })")
      evalOk(auth, "Foo.create({ id: 2, foo: 'non_conforming' })")
      evalOk(auth, "Foo.create({ id: 3, baz: 'hi' })")
      evalOk(auth, "Foo.create({ id: 4, dump: 5 })")
      evalOk(auth, "Foo.create({ id: 5, dump: { a: 2 } })")
      evalOk(auth, "Foo.create({ id: 6, dump: { a: 2 }, bar: 5 })")
      evalOk(auth, "Foo.create({ id: 7, dump: { a: 2 }, a: 5 })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  foo: Int?
             |  dump: { *: Any }?
             |
             |  migrations {
             |    add .dump
             |    add .foo
             |    move_conflicts .dump
             |    move_wildcard .dump
             |  }
             |}
             |""".stripMargin
      )

      evalOk(auth, "let d: Any = Foo(1)!; d.data") shouldBe Value.Struct(
        "foo" -> Value.Int(3)
      )
      evalOk(auth, "let d: Any = Foo(2)!; d.data") shouldBe Value.Struct(
        "dump" -> Value.Struct("foo" -> Value.Str("non_conforming"))
      )
      evalOk(auth, "let d: Any = Foo(3)!; d.data") shouldBe Value.Struct(
        "dump" -> Value.Struct("baz" -> Value.Str("hi"))
      )
      evalOk(auth, "let d: Any = Foo(4)!; d.data") shouldBe Value.Struct(
        "dump" -> Value.Struct("dump" -> Value.Int(5))
      )
      // The `dump` struct is left alone here, as it already matches schema.
      evalOk(auth, "let d: Any = Foo(5)!; d.data") shouldBe Value.Struct(
        "dump" -> Value.Struct("a" -> Value.Int(2))
      )
      // Values can be merged into the pre-existing `dump` struct.
      evalOk(auth, "let d: Any = Foo(6)!; d.data") shouldBe Value.Struct(
        "dump" -> Value.Struct("a" -> Value.Int(2), "bar" -> Value.Int(5))
      )
      // And names will be munged.
      evalOk(auth, "let d: Any = Foo(7)!; d.data") shouldBe Value.Struct(
        "dump" -> Value.Struct("a" -> Value.Int(2), "_a" -> Value.Int(5))
      )
    }

    "backfill values are checked against field types" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  _no_wildcard: Null
             |}
             |""".stripMargin
      )
      evalOk(auth, "Foo.create({})")

      updateSchemaErr(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  foo: Int
             |  _no_wildcard: Null
             |
             |  migrations {
             |    add .foo
             |    backfill .foo = {
             |      let a: Any = "hi"
             |      a
             |    }
             |  }
             |}
             |""".stripMargin
      ) shouldBe (
        """|error: Invalid database schema update.
           |    error: Failed to convert backfill value.
           |    constraint failures:
           |      foo: Expected Int, provided String
           |    at <no source>
           |at main.fsl:1:1
           |   |
           | 1 |   collection Foo {
           |   |  _^
           | 2 | |   foo: Int
           | 3 | |   _no_wildcard: Null
           | 4 | |
           | 5 | |   migrations {
           | 6 | |     add .foo
           | 7 | |     backfill .foo = {
           | 8 | |       let a: Any = "hi"
           | 9 | |       a
           |10 | |     }
           |11 | |   }
           |12 | | }
           |   | |_^
           |   |""".stripMargin
      )
    }
  }

  "refs" - {
    "don't get waxed by a split out of Any" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection A {}
             |
             |collection Foo {
             |  a: Any
             |}""".stripMargin
      )

      evalOk(
        auth,
        "A.create({ id: 0 })"
      )
      evalOk(
        auth,
        "Foo.create({ id: 0, a: 0 })"
      )
      evalOk(
        auth,
        "Foo.create({ id: 1, a: A.byId(0) })"
      )
      evalOk(
        auth,
        "Foo.create({ id: 2, a: A.byId(1) })"
      )

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection A {}
             |
             |collection Foo {
             |  a: Ref<A>?
             |
             |  migrations {
             |    split .a -> .a, .tmp
             |    drop .tmp
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "Foo.byId(0)!.a").isNull shouldBe true
      evalOk(auth, "Foo.byId(1)!.a").as[DocID].subID.toLong shouldBe 0
      evalOk(auth, "Foo.byId(2)!.a").as[DocID].subID.toLong shouldBe 1
    }
  }

  "null isn't a valid backfill for ref" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection A {}
           |
           |collection Foo {
           |  _no_wildcard: Null
           |}""".stripMargin
    )

    evalOk(
      auth,
      "Foo.create({})"
    )

    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection A {}
           |
           |collection Foo {
           |  _no_wildcard: Null
           |  a: Ref<A>
           |
           |  migrations {
           |    add .a
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |    error: Failed to convert backfill value.
         |    constraint failures:
         |      a: Expected A | NullA, provided Null
         |    at *field:Foo:a*:1:1
         |      |
         |    1 | Ref<A>
         |      | ^^^^^^
         |      |
         |at main.fsl:3:1
         |   |
         | 3 |   collection Foo {
         |   |  _^
         | 4 | |   _no_wildcard: Null
         | 5 | |   a: Ref<A>
         | 6 | |
         | 7 | |   migrations {
         | 8 | |     add .a
         | 9 | |   }
         |10 | | }
         |   | |_^
         |   |""".stripMargin
    )
  }

  "don't miss new migrations" - {
    // NOTE: When adding a new `SchemaMigration`, make sure to add it to this match,
    // and to add a test! This match should always be exhaustive.
    SchemaMigration.AddWildcard.asInstanceOf[SchemaMigration] match {
      case SchemaMigration.Add(_, _, _)             =>
      case SchemaMigration.Drop(_)                  =>
      case SchemaMigration.Move(_, _)               =>
      case SchemaMigration.Split(_, _)              =>
      case SchemaMigration.MoveWildcardConflicts(_) =>
      case SchemaMigration.MoveWildcard(_, _)       =>
      case SchemaMigration.AddWildcard              =>
    }

    "add" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  _no_wildcard: Null
             |}""".stripMargin)

      evalOk(auth, "User.create({})")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  _no_wildcard: Null
             |  foo: Int
             |
             |  migrations {
             |    add .foo
             |    backfill .foo = 3
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.all().first()!.foo") shouldBe Value.Int(3)
    }

    "drop" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  _no_wildcard: Null
             |
             |  foo: Int
             |}""".stripMargin)

      evalOk(auth, "User.create({ foo: 3 })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  _no_wildcard: Null
             |
             |  migrations {
             |    drop .foo
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.all().first()!.foo", typecheck = false) shouldBe Value.Null(
        Span.Null)

    }

    "move" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: Int
             |}""".stripMargin)

      evalOk(auth, "User.create({ foo: 3 })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  bar: Int
             |
             |  migrations {
             |    move .foo -> .bar
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.all().first()!.foo", typecheck = false) shouldBe Value.Null(
        Span.Null)
      evalOk(auth, "User.all().first()!.bar") shouldBe Value.Int(3)
    }

    "split" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: Int | String
             |}""".stripMargin)

      evalOk(auth, "User.create({ foo: 3 })")
      evalOk(auth, "User.create({ foo: 'hi' })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: String
             |  bar: Int
             |
             |  migrations {
             |    split .foo -> .foo, .bar
             |    backfill .foo = "default"
             |    backfill .bar = 0
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.all().first()!.foo") shouldBe Value.Str("default")
      evalOk(auth, "User.all().first()!.bar") shouldBe Value.Int(3)

      evalOk(auth, "User.all().last()!.foo") shouldBe Value.Str("hi")
      evalOk(auth, "User.all().last()!.bar") shouldBe Value.Int(0)
    }

    "move_conflicts" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  *: Any
             |}""".stripMargin)

      evalOk(auth, "User.create({ foo: 3, bar: 10 })")
      evalOk(auth, "User.create({ foo: 'hi', bar: 10 })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: Int
             |  conflicts: { *: Any }?
             |  *: Any
             |
             |  migrations {
             |    add .foo
             |    backfill .foo = 0
             |
             |    add .conflicts
             |    move_conflicts .conflicts
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.all().first()!.foo") shouldBe Value.Int(3)
      evalOk(auth, "User.all().first()!.bar") shouldBe Value.Int(10)

      evalOk(auth, "User.all().last()!.foo") shouldBe Value.Int(0)
      evalOk(auth, "User.all().last()!.conflicts?.foo") shouldBe Value.Str("hi")
      evalOk(auth, "User.all().last()!.bar") shouldBe Value.Int(10)
    }

    "move_wildcard" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  *: Any
             |}""".stripMargin)

      evalOk(auth, "User.create({ foo: 3, bar: 10 })")
      evalOk(auth, "User.create({ foo: 'hi' })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: Int
             |  conflicts: { *: Any }?
             |
             |  migrations {
             |    add .foo
             |    backfill .foo = 0
             |
             |    add .conflicts
             |    move_conflicts .conflicts
             |    move_wildcard .conflicts
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.all().first()!.foo") shouldBe Value.Int(3)
      evalOk(auth, "User.all().first()!.conflicts?.bar") shouldBe Value.Int(10)

      evalOk(auth, "User.all().last()!.foo") shouldBe Value.Int(0)
      evalOk(auth, "User.all().last()!.conflicts?.foo") shouldBe Value.Str("hi")
    }

    "add_wildcard" in {
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  foo: Int
             |}""".stripMargin)

      evalOk(auth, "User.create({ foo: 3 })")
      evalErr(auth, "User.create({ foo: 3, bar: 5 })")

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection User {
             |  *: Any
             |  foo: Int
             |
             |  migrations {
             |    add_wildcard
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "User.create({ foo: 3 })")
      evalOk(auth, "User.create({ foo: 3, bar: 5 })")
    }
  }

  "disallows adding migrations with v4 indexes present" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: Int
           |}""".stripMargin)

    evalV4Ok(
      auth,
      CreateIndex(MkObject("name" -> "my_index", "source" -> ClsRefV("User"))))

    // This is fine because there are no docs, so there are no index entries to worry
    // about. Crucially, no docs means no internal migrations will be added, so a v4
    // index is perfectly valid here.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  bar: Int
           |
           |  migrations {
           |    move .foo -> .bar
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ bar: 3 })")

    // Now we can't allow any migrations, because there are v4 indexes about.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection User {
           |  baz: Int
           |
           |  migrations {
           |    move .bar -> .baz
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |
         |constraint failures:
         |  Cannot create migrations on a collection with v4 indexes
         |at main.fsl:1:1
         |  |
         |1 |   collection User {
         |  |  _^
         |2 | |   baz: Int
         |3 | |
         |4 | |   migrations {
         |5 | |     move .bar -> .baz
         |6 | |   }
         |7 | | }
         |  | |_^
         |  |""".stripMargin
    )
  }

  "v4 indexes can be added even when there are migrations" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  foo: Int
           |}""".stripMargin)

    evalOk(auth, "User.create({ id: 0, foo: 3 })")

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  bar: Int
           |
           |  migrations {
           |    move .foo -> .bar
           |  }
           |}""".stripMargin
    )

    evalV4Ok(
      auth,
      CreateIndex(
        MkObject(
          "name" -> "my_index",
          "source" -> ClsRefV("User"),
          "terms" -> MkObject("field" -> Seq("data", "bar")))))

    // The index should work.
    evalV4Ok(auth, Paginate(Match(IndexRef("my_index"), Seq(3))))
      .asInstanceOf[PageL]
      .elems shouldBe Seq(RefL(auth.scopeID, DocID(SubID(0), CollectionID(1024))))

    // Additional migrations cannot be performed.
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection User {
           |  baz: Int
           |
           |  migrations {
           |    move .bar -> .baz
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |
         |constraint failures:
         |  Cannot create migrations on a collection with v4 indexes
         |at main.fsl:1:1
         |  |
         |1 |   collection User {
         |  |  _^
         |2 | |   baz: Int
         |3 | |
         |4 | |   migrations {
         |5 | |     move .bar -> .baz
         |6 | |   }
         |7 | | }
         |  | |_^
         |  |""".stripMargin
    )
  }

  "fields cannot be moved out of refs" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {}
           |
           |collection User {
           |  foo: Ref<Foo>
           |
           |  index byFoo {
           |    terms [.foo.id]
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ id: 0, foo: Foo.create({ id: 1 }) })")

    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection Foo {}
           |
           |collection User {
           |  foo: Ref<Foo>
           |  bar: String
           |
           |  index byFoo {
           |    terms [.bar]
           |  }
           |
           |
           |  migrations {
           |    move .foo.id -> .bar
           |  }
           |}""".stripMargin
    ) shouldBe (
      """|error: Cannot move field `.foo.id`, as the parent value is not an object.
         |at main.fsl:13:10
         |   |
         |13 |     move .foo.id -> .bar
         |   |          ^^^^^^^
         |   |""".stripMargin
    )
  }
}
