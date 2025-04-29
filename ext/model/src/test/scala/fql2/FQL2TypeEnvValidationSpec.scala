package fauna.model.test

import fauna.auth.AdminPermissions

class FQL2TypeEnvValidationSpec extends FQL2Spec {

  "type env validation" - {
    Seq(true, false) foreach { typechecked =>
      "always checks defined fields'" - {
        s"signatures (typechecked=$typechecked)" in {
          val auth = newDB(typechecked)
          renderErr(
            auth,
            """|Collection.create({
               |  name: "Foo",
               |  fields: {
               |    badsig: { signature: "Ref<Bar>" }
               |  }
               |})""".stripMargin
          ) should include(
            "error: Unknown type `Bar`"
          )
        }

        s"persistability (typechecked=$typechecked)" in {
          val auth = newDB(typechecked)
          renderErr(
            auth,
            """|Collection.create({
               |  name: "Foo",
               |  fields: {
               |    badsig: { signature: "Int => Int" }
               |  }
               |})""".stripMargin
          ) should include(
            "error: Field `badsig` in collection Foo is not persistable"
          )
        }
      }

      s"always checks for type env name conflicts (typechecked=$typechecked)" in {
        val auth = newDB(typechecked)

        evalOk(auth, "Collection.create({ name: 'Foo' })")
        renderErr(auth, "Collection.create({ name: 'NullFoo' })") should include(
          "The collection name `NullFoo` generates types that conflict with the following existing types: NullFoo"
        )
      }
    }

    "when typechecking is off, doesn't typecheck or validate other things" - {
      "like check constraints" in {
        val auth = newDB(typeChecked = false)

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  constraints: [
             |    { check: { name: "bad", "body": "_ => 0" } }
             |  ]
             |})""".stripMargin
        )
      }

      "like computed fields" in {
        val auth = newDB(typeChecked = false)

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    bad: { signature: "Int", body: "_ => '0'" }
             |  }
             |})""".stripMargin
        )
      }

      "like UDFs" in {
        val auth = newDB(typeChecked = false)

        evalOk(
          auth,
          """|Function.create({
             |  name: "Foo",
             |  signature: "Int => Int",
             |  body: "_ => '0'"
             |})""".stripMargin
        )
      }

      "like index fields" in {
        val auth = newDB(typeChecked = false)

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  fields: {
             |    a: { signature: "Int" }
             |  },
             |  indexes: {
             |    byB: { terms: [{ field: ".b" }] }
             |  }
             |})""".stripMargin
        )
      }

      "like indexed computed fields" in {
        val auth = newDB(typeChecked = false)

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  computed_fields: {
             |    a: { body: "_ => 2" },
             |    b: { body: "doc => doc.a + 2" }
             |  },
             |  indexes: {
             |    byB: { terms: [{ field: ".b" }] }
             |  }
             |})""".stripMargin
        )
      }

      "like role predicates" in {
        val auth = newDB(typeChecked = false)
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          admin,
          """|Collection.create({ name: 'Users' })
             |
             |Role.create({
             |  name: "Foo",
             |  membership: {
             |    resource: "Users",
             |    predicate: '_ => 0'
             |  }
             |})""".stripMargin
        )
      }
    }
  }

}
