package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.fql2.QueryCheckFailure
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.repo.values.Value
import fql.error.TypeError
import scala.concurrent.duration._

class FQL2CacheSpec extends FQL2Spec {

  var auth: Auth = _

  before {
    auth = newAuth
  }

  "FQL2Cache" - {
    "works" - {
      "collections" in testTopLevel { name =>
        s"""Collection.create({ name: "$name" })"""
      }

      "functions" in testTopLevel { name =>
        s"""Function.create({ name: "$name", body: "_ => null" })"""
      }

      "roles" in {
        evalOk(auth, "Collection.create({ name: 'Bar' })")

        val admin = auth.withPermissions(AdminPermissions)

        evalOk(
          admin,
          s"""|Role.create({
              |  name: "aRole",
              |  privileges: {
              |    resource: "Bar",
              |    actions: {}
              |  }
              |})""".stripMargin
        )

        noException shouldBe thrownBy {
          evalOk(admin, """Role.byName("aRole")!""")
        }

        evalOk(
          admin,
          """Role.byName("aRole")!.update({name: "newName"})"""
        )

        // renamed items is immediately available
        noException shouldBe thrownBy {
          evalOk(admin, """Role.byName("newName")!""")
        }

        // eventually old item is no longer available
        eventually(timeout(5.seconds)) {
          evalErr(admin, """Role.byName("aRole")!""") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "document_not_found",
                  "Role `aRole` not found.",
                  _,
                  Seq()) =>
          }
        }
      }

      "indexes" in {
        evalOk(
          auth,
          """|Collection.create({
             |  name: "Foo",
             |  indexes: {
             |    byName: { terms: [{ field: "name" }] }
             |  }
             |})""".stripMargin
        )

        // new index is immediately available
        noException shouldBe thrownBy { evalOk(auth, "Foo.byName") }

        evalOk(
          auth,
          """|Foo.definition.update({
             |  indexes: {
             |    byName0: { terms: [{ field: "name" }] },
             |    byName: null
             |  }
             |})""".stripMargin
        )

        // new index name is immediately available
        evalOk(auth, "Foo.byName0") shouldNot
          matchPattern { case Value.Null(_) => }

        // eventually old index name is no longer available
        eventually(timeout(5.seconds)) {
          inside(evalErr(auth, "Foo.byName")) {
            case QueryCheckFailure(Seq(TypeError(msg, _, _, _))) =>
              msg shouldEqual "Type `FooCollection` does not have field `byName`"
          }
        }
      }
    }
  }

  private def testTopLevel(create: String => String) = {
    evalOk(auth, create("Foo"))
    // new items are immediately available
    noException shouldBe thrownBy { evalOk(auth, "Foo") }

    evalOk(auth, """Foo.definition.update({ name: "Foo0" })""")
    // renamed items is immediately available
    noException shouldBe thrownBy { evalOk(auth, "Foo0") }
    // eventually old item is no longer available
    eventually(timeout(5.seconds)) {
      evalErr(auth, "Foo") should matchPattern {
        case QueryCheckFailure(
              Seq(
                TypeError(
                  "Unbound variable `Foo`",
                  _,
                  _,
                  _
                ))) =>
      }
    }
  }
}
