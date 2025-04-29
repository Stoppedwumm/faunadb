package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.Effect
import fauna.repo.values._

class FQL2DocRefSpec extends FQL2Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "EmptyRef<Doc>" - {
    "is not persistable" in {
      evalOk(auth, """Collection.create({ name: "Bar" })""")

      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    a: { signature: "EmptyRef<Bar>" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Field `a` in collection Foo is not persistable
           |    at main.fsl:8:6
           |      |
           |    8 |   a: EmptyRef<Bar>
           |      |      ^^^^^^^^^^^^^
           |      |
           |    cause: Type EmptyRef<Bar> is not persistable
           |      |
           |    8 |   a: EmptyRef<Bar>
           |      |      ^^^^^^^^^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     a: { signature: "EmptyRef<Bar>" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "is synonymous with NullDoc" in {
      evalOk(auth, """Collection.create({ name: "Bar" })""")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar: { signature: "Bar | EmptyRef<Bar>" }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ id: 0, bar: Bar.byId(0) })")

      // ID and exists work.
      evalOk(auth, "Foo.byId(0)!.bar.id") shouldBe Value.ID(0)
      evalOk(auth, "Foo.byId(0)!.bar.exists()") shouldBe Value.False

      // Data fields don't.
      evalErr(auth, "Foo.byId(0)!.bar.a").errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Type `Null` does not have field `a`
           |at *query*:1:18
           |  |
           |1 | Foo.byId(0)!.bar.a
           |  |                  ^
           |  |
           |hint: Use the ! or ?. operator to handle the null case
           |  |
           |1 | Foo.byId(0)!.bar!.a
           |  |                 +
           |  |""".stripMargin
      )
    }
  }

  "Ref<Doc>" - {
    "works as a field type" in {
      evalOk(auth, """Collection.create({ name: "Bar" })""")

      evalOk(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar: { signature: "Ref<Bar>" }
           |  }
           |})""".stripMargin
      )

      evalOk(auth, "Bar.create({ id: 0, x: 0 })")
      evalOk(auth, "Foo.create({ id: 0, bar: Bar.byId(0) })")
      evalOk(auth, "Foo.create({ id: 1, bar: Bar.byId(1) })")

      // !
      evalOk(auth, "Foo.byId(0)!.bar!.x") shouldEqual Value.Int(0)
      evalErr(auth, "Foo.byId(1)!.bar!.x").errors.head
        .renderWithSource(Map.empty) shouldBe (
        """|error: Collection `Bar` does not contain document with id 1.
           |at *query*:1:14
           |  |
           |1 | Foo.byId(1)!.bar!.x
           |  |              ^^^^
           |  |""".stripMargin
      )

      // ?
      evalOk(auth, "Foo.byId(0)?.bar?.x") shouldEqual Value.Int(0)
      evalOk(auth, "Foo.byId(1)?.bar?.x").isNull shouldBe true
    }

    "must be used with a document type (scalar)" in {
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar: { signature: "Ref<Int>" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Reference type must refer to a doc type
           |    at main.fsl:5:8
           |      |
           |    5 |   bar: Ref<Int>
           |      |        ^^^^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     bar: { signature: "Ref<Int>" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "must be used with a document type (unbound)" in {
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar: { signature: "Ref<Boo>" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Unknown type `Boo`
           |    at main.fsl:5:12
           |      |
           |    5 |   bar: Ref<Boo>
           |      |            ^^^
           |      |
           |    error: Reference type must refer to a doc type
           |    at main.fsl:5:8
           |      |
           |    5 |   bar: Ref<Boo>
           |      |        ^^^^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     bar: { signature: "Ref<Boo>" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "must be used with a document type (ref)" in {
      evalOk(auth, """Collection.create({ name: "Bar" })""")
      evalErr(
        auth,
        """|Collection.create({
           |  name: "Foo3",
           |  fields: {
           |    bar: { signature: "Ref<Ref<Bar>>" }
           |  }
           |})""".stripMargin
      ).errors.head.renderWithSource(Map.empty) shouldBe (
        """|error: Invalid database schema update.
           |    error: Reference type must refer to a doc type
           |    at main.fsl:8:8
           |      |
           |    8 |   bar: Ref<Ref<Bar>>
           |      |        ^^^^^^^^^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo3",
           |3 | |   fields: {
           |4 | |     bar: { signature: "Ref<Ref<Bar>>" }
           |5 | |   }
           |6 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }

    "is synonymous with Doc | NullDoc" in {
      evalOk(auth, """Collection.create({ name: "Bar" })""")
      evalOk(
        auth,
        """|Function.create({
           |  name: "MyFunc",
           |  signature: "Ref<Bar> => Ref<Bar>",
           |  body: "x => x"
           |})""".stripMargin
      )

      evalRes(
        auth,
        "let x: Bar | NullBar = MyFunc(Bar.byId(0)); x").typeStr shouldBe "Bar | NullBar"
    }

    "is synonymous with Doc | EmptyRef<Doc>" in {
      evalOk(auth, """Collection.create({ name: "Bar" })""")
      evalOk(
        auth,
        """|Function.create({
           |  name: "MyFunc",
           |  signature: "Ref<Bar> => Bar | EmptyRef<Bar>",
           |  body: "x => x"
           |})""".stripMargin
      )

      evalOk(auth, "let x: Bar | EmptyRef<Bar> = MyFunc(Bar.byId(0)); x")
    }

    "works with aliases" in {
      val auth = newDB
      evalOk(
        auth,
        """|Collection.create({ name: "Bar", alias: "Aliased" })
           |Collection.create({ name: "Baz" })
           |
           |Collection.create({
           |  name: "Foo",
           |  fields: {
           |    bar_ref: { signature: "Ref<Aliased>" }
           |  },
           |})""".stripMargin
      )

      evalOk(auth, "Foo.create({ bar_ref: Bar.create({}) })")
      evalOk(auth, "Foo.create({ bar_ref: Aliased.create({}) })")
    }
  }

  "Collection apply" - {
    "works" in {
      evalOk(auth, """Collection.create({ name: "Foo" })""")

      // The apply method just builds the reference. No reads allowed.
      evalRes(auth, "Foo(0)", effect = Effect.Pure).typeStr shouldBe "Ref<Foo>"
      evalRes(auth, """Foo("0")""").typeStr shouldBe "Ref<Foo>"

      evalOk(auth, "Foo.create({ id: 0, a: 0 })")
      evalOk(auth, "Foo(0)?.a") shouldBe Value.Int(0)
      evalOk(auth, "Foo(1)?.a").isNull shouldBe true
    }
  }

  "NamedRef" - {
    "works" in {
      evalOk(
        auth,
        """|Function.create({
           |  name: "plus1",
           |  "body": "x => x + 1"
           |})""".stripMargin
      )

      evalOk(
        auth,
        "let x: NamedRef<FunctionDef> = Function.byName('plus1'); x.name"
      ) shouldBe Value.Str("plus1")

      evalOk(
        auth,
        "let x: NamedRef<FunctionDef> = Function.byName('plus2'); x.name"
      ) shouldBe Value.Str("plus2")
    }

    "is not persistable (and neither is EmptyNamedRef)" in {
      renderErr(
        auth,
        """|Collection.create({
           |  name: "Foo",
           |  fields: {
           |    fn: { signature: "NamedRef<FunctionDef>" },
           |    nofn: { signature: "EmptyNamedRef<FunctionDef>"}
           |}
           |})""".stripMargin
      ) shouldBe (
        """|error: Invalid database schema update.
           |    error: Field `fn` in collection Foo is not persistable
           |    at main.fsl:5:7
           |      |
           |    5 |   fn: NamedRef<FunctionDef>
           |      |       ^^^^^^^^^^^^^^^^^^^^^
           |      |
           |    cause: Type NamedRef<FunctionDef> is not persistable
           |      |
           |    5 |   fn: NamedRef<FunctionDef>
           |      |       ^^^^^^^^^^^^^^^^^^^^^
           |      |
           |    error: Field `nofn` in collection Foo is not persistable
           |    at main.fsl:6:9
           |      |
           |    6 |   nofn: EmptyNamedRef<FunctionDef>
           |      |         ^^^^^^^^^^^^^^^^^^^^^^^^^^
           |      |
           |    cause: Type EmptyNamedRef<FunctionDef> is not persistable
           |      |
           |    6 |   nofn: EmptyNamedRef<FunctionDef>
           |      |         ^^^^^^^^^^^^^^^^^^^^^^^^^^
           |      |
           |at *query*:1:18
           |  |
           |1 |   Collection.create({
           |  |  __________________^
           |2 | |   name: "Foo",
           |3 | |   fields: {
           |4 | |     fn: { signature: "NamedRef<FunctionDef>" },
           |5 | |     nofn: { signature: "EmptyNamedRef<FunctionDef>"}
           |6 | | }
           |7 | | })
           |  | |__^
           |  |""".stripMargin
      )
    }
  }
}
