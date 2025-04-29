package fauna.model.test

import fauna.atoms.SchemaVersion
import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.schema.SchemaStatus
import fauna.repo.schema.ConstraintFailure._
import fauna.repo.values.Value

class FQL2FunctionsSpec extends FQL2Spec {
  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "abort" - {
    "aborts a query and cancels its effects" in {
      evalOk(auth, "Collection.create({ name: 'Insect' })")

      evalErr(
        auth,
        """|Insect.create({ name: 'bee' })
           |abort(0)""".stripMargin
      ).code shouldBe "abort"

      pendingUntilFixed { // Model tests don't fail queries correctly.
        evalOk(auth, "Insect.all.count()") shouldBe (Value.Int(0))
      }
    }
  }

  "isa" - {
    "works with user-defined functions" in {
      evalOk(
        auth,
        """|let sheepIt = Function.create({
           |  name: "sheepIt",
           |  body: "x => x"
           |})
           |sheepIt isa Function""".stripMargin
      ) shouldBe (Value.True)
    }

    "works with lambdas" in {
      evalOk(
        auth,
        """|let sheepOut = (s) => "baaa"
           |sheepOut isa Function""".stripMargin) shouldBe (Value.True)

      evalOk(auth, "abort isa Function") shouldBe (Value.True)
    }

    "works with native functions" in {
      evalOk(auth, "abort isa Function") shouldBe (Value.True)
    }
  }

  "rejects writes to customer tenant root db" in {
    val tenantRoot = newCustomerTenantRoot(auth)

    val err = evalErr(
      tenantRoot,
      "Function.create({ name: 'rootColl', body: 'x => true' })"
    )

    err.code shouldBe "constraint_failure"
    err.failureMessage shouldBe "Failed to create Function."
    err.asInstanceOf[QueryRuntimeFailure].constraintFailures shouldBe Seq(
      TenantRootWriteFailure("Function")
    )
  }

  "allows writes to internal tenant root db" in {
    val tenantRoot = newInternalTenantRoot(auth)

    evalOk(
      tenantRoot,
      "Function.create({ name: 'rootColl', body: 'x => true' })"
    )
  }

  "allows staged deletes" in {
    evalOk(auth, "Function.create({ name: 'fooFn', body: 'x => true' })")

    ctx ! SchemaStatus.pinActiveSchema(auth.scopeID, SchemaVersion.Min)

    evalOk(auth, "Function.byName('fooFn')!.delete()")
  }
}
