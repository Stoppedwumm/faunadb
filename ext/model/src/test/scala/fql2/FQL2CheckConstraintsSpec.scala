package fauna.model.test

import fauna.ast.{ CheckConstraintEvalError, TransactionAbort }
import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.fql2.{ QueryCheckFailure, QueryRuntimeFailure }
import fauna.repo.values.Value

class FQL2CheckConstraintsSpec extends FQL2WithV4Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "check constraints" - {
    "can be declared" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'a', body: 'doc => doc.x == 0' } },
           |    { check: { name: 'b', body: 'doc => doc.y != 0' } }
           |  ]
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.definition.constraints.length") shouldEqual Value
        .Int(2)
      evalOk(
        auth,
        "Foo.definition.constraints.first()!.check.body",
        typecheck = false)
        .as[String] shouldEqual "doc => doc.x == 0"
      evalOk(
        auth,
        "Foo.definition.constraints.last()!.check.body",
        typecheck = false)
        .as[String] shouldEqual "doc => doc.y != 0"
    }

    "cannot be declared with an invalid name" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'foo bar', body: 'doc => doc.x == 0' } },
           |  ]
           |})
           |""".stripMargin
      )
      err.code shouldEqual "constraint_failure"
      err.errors.size shouldEqual 1
      err.errors.head.message shouldEqual
        """|Failed to create Collection.
           |constraint failures:
           |  constraints[0].check.name: Invalid identifier.""".stripMargin
    }

    "can be declared with short lambda" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'a', body: '(.x == 0)' } }
           |  ]
           |})
           |""".stripMargin
      )

      evalOk(auth, "Foo.create({ x: 0 })")
      val err = evalErr(auth, "Foo.create({ x: 1 })")
      err.code shouldEqual "constraint_failure"
      err.errors.size shouldEqual 1
      err.errors.head.message shouldEqual
        """|Failed to create document in collection `Foo`.
           |constraint failures:
           |  Document failed check constraint `a`""".stripMargin
    }

    "reject checks of the wrong arity" in {
      val err = evalErr(
        auth,
        """Collection.create({ name: "Foo", constraints: [{ check: { name: "nullary", body: "() => true" } }] })""")

      err.code shouldBe "constraint_failure"
      err.errors.head.message should include(
        "Expected exactly 1 argument, but the function was defined with 0 arguments")
    }

    "forbid duplicate check names" in {
      val err = evalErr(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'ok', body: 'doc => doc.ok' } },
           |    { check: { name: 'ok', body: 'doc => doc.ok' } },
           |    { check: { name: 'ko', body: 'doc => doc.ko' } },
           |    { check: { name: 'ko', body: 'doc => doc.ko' } }
           |  ]
           |})
           |""".stripMargin
      )
      err.code shouldBe "constraint_failure"
      err.errors.head.message shouldEqual
        """|Failed to create Collection.
           |constraint failures:
           |  constraints: Duplicate check constraint name 'ko'
           |  constraints: Duplicate check constraint name 'ok'""".stripMargin
    }

    "accept passing docs and reject failing docs" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'ok', body: 'doc => doc.ok' } },
           |    { check: { name: 'cool', body: 'doc => doc.cool ?? true' } }
           |  ]
           |})
           |""".stripMargin
      )

      // Create.
      // Passes both constraints.
      evalOk(auth, "Foo.create({ ok: true })")
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(1)

      // Fails the first constraint.
      val err1 = evalErr(auth, "Foo.create({ ok: false })")
      err1.code shouldEqual "constraint_failure"
      err1.errors.size shouldEqual 1
      err1.errors.head.message shouldEqual
        """|Failed to create document in collection `Foo`.
           |constraint failures:
           |  Document failed check constraint `ok`""".stripMargin
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(1)

      // Fails the second constraint.
      val err2 = evalErr(auth, "Foo.create({ ok: true, cool: false })")
      err2.code shouldEqual "constraint_failure"
      err2.errors.size shouldEqual 1
      err2.errors.head.message shouldEqual
        """|Failed to create document in collection `Foo`.
           |constraint failures:
           |  Document failed check constraint `cool`""".stripMargin
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(1)

      // Fails both constraints (doc.ok is null, treated like false).
      val errBoth = evalErr(auth, "Foo.create({ cool: false })")
      errBoth.code shouldEqual "constraint_failure"
      errBoth.errors.size shouldEqual 1
      errBoth.errors.head.message shouldEqual
        """|Failed to create document in collection `Foo`.
           |constraint failures:
           |  Document failed check constraint `ok`
           |  Document failed check constraint `cool`""".stripMargin
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(1)

      // Update.
      val errUpdate = evalErr(auth, "Foo.all().first()!.update({ cool: false })")
      errUpdate.errors.size shouldEqual 1
      errUpdate.errors.head.message should endWith(
        """|constraint failures:
           |  Document failed check constraint `cool`""".stripMargin
      )
      evalOk(auth, "Foo.all().first()!.update({ cool: true })")

      // Replace.
      val errReplace = evalErr(auth, "Foo.all().first()!.replace({ ok: false })")
      errReplace.errors.size shouldEqual 1
      errReplace.errors.head.message should endWith(
        """|constraint failures:
           |  Document failed check constraint `ok`""".stripMargin
      )
      evalOk(auth, "Foo.all().first()!.replace({ ok: true })")
    }

    "accept passing docs and reject failing docs: V4 edition" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'FooV4',
           |  constraints: [
           |    { check: { name: 'ok', body: 'doc => doc.ok' } },
           |    { check: { name: 'cool', body: 'doc => doc.cool ?? true' } }
           |  ]
           |})
           |""".stripMargin
      )

      // Passes both constraints.
      evalV4Ok(auth, CreateF("FooV4", MkObject("data" -> MkObject("ok" -> true))))
      evalOk(auth, "FooV4.all().count()") shouldEqual Value.Int(1)

      // Fails one constraint.
      val errs0 = evalV4Err(
        auth,
        CreateF("FooV4", MkObject("data" -> MkObject("ok" -> false))))
      errs0.size shouldBe 1
      errs0.head match {
        case CheckConstraintEvalError(inner, _) =>
          inner.msg shouldBe "Document failed check constraint `ok`"
        case _ => fail("expected a different error")
      }
      evalOk(auth, "FooV4.all().count()") shouldEqual Value.Int(1)

      // Fails both constraints.
      val errs1 = evalV4Err(
        auth,
        CreateF(
          "FooV4",
          MkObject("data" -> MkObject("ok" -> false, "cool" -> false))))
      errs1.size shouldBe 2
      errs1(0) match {
        case CheckConstraintEvalError(inner, _) =>
          inner.msg shouldBe "Document failed check constraint `ok`"
        case _ => fail("expected a different error")
      }
      errs1(1) match {
        case CheckConstraintEvalError(inner, _) =>
          inner.msg shouldBe "Document failed check constraint `cool`"
        case _ => fail("expected a different error")
      }
      evalOk(auth, "FooV4.all().count()") shouldEqual Value.Int(1)
    }

    "aren't clobbered by V4 collection changes" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'ok', body: 'doc => doc.ok' } }
           |  ]
           |})
           |""".stripMargin
      )

      evalV4Ok(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "byX",
            "source" -> ClassRef("Foo"),
            "active" -> true,
            "terms" -> List(MkObject("field" -> List("data", "x"))),
            "unique" -> true
          ))
      )

      // Adding a unique constraint in V4 preserves the check constraint.
      val err = evalErr(auth, "Foo.create({ x: 0, ok: false })")
      err.errors.size shouldEqual 1
      err.errors.head.message should endWith(
        """|constraint failures:
           |  Document failed check constraint `ok`""".stripMargin
      )
    }

    "use computed fields" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  computed_fields: {
           |    fpb: { body: "doc => doc.foo + doc.bar" },
           |    fmb: { body: "doc => doc.foo - doc.bar" }
           |  },
           |  constraints: [
           |    { check: { name: 'fpbpfmpgt0', body: 'doc => doc.fpb + doc.fmb > 0' } }
           |  ]
           |})
           |""".stripMargin
      )

      // Passes.
      evalOk(auth, "Foo.create({ foo: 1, bar: -1 })")
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(1)

      // Fails.
      val err = evalErr(auth, "Foo.create({ foo: -1, bar: 1 })")
      err.code shouldEqual "constraint_failure"
      err.errors.size shouldEqual 1
      err.errors.head.message shouldEqual
        """|Failed to create document in collection `Foo`.
           |constraint failures:
           |  Document failed check constraint `fpbpfmpgt0`""".stripMargin
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(1)
    }

    "handle evaluation errors" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  constraints: [
           |    { check: { name: 'ok', body: 'doc => doc.ok' } },
           |    { check: { name: 'plus1', body: 'doc => doc.foo + 1 > 0' } },
           |  ]
           |})
           |""".stripMargin
      )

      val err = evalErr(auth, "Foo.create({ ok: 'true' })")
      err.code shouldEqual "invalid_function_invocation"
      err.errors.size shouldEqual 1
      err.errors.head.message shouldEqual
        "The function `+` does not exist on `Null`"
      // Check that the stack and span look correct.
      err.errors.head.renderWithSource(Map.empty) shouldEqual
        """|error: The function `+` does not exist on `Null`
           |at *check_constraint:plus1*:1:16
           |  |
           |1 | doc => doc.foo + 1 > 0
           |  |                ^
           |  |
           |hint: Null value created here
           |  |
           |1 | doc => doc.foo + 1 > 0
           |  |            ^^^
           |  |
           |trace:
           |at *query*:1:11
           |  |
           |1 | Foo.create({ ok: 'true' })
           |  |           ^^^^^^^^^^^^^^^^
           |  |""".stripMargin
      evalOk(auth, "Foo.all().count()") shouldEqual Value.Int(0)
    }

    "handle evaluation errors: V4 edition" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'BarV4',
           |  constraints: [
           |    { check: { name: 'ok', body: 'doc => doc.ok' } },
           |    { check: { name: 'plus1', body: 'doc => doc.foo + 1 > 0' } },
           |  ]
           |})
           |""".stripMargin
      )

      val errs = evalV4Err(
        auth,
        CreateF("BarV4", MkObject("data" -> MkObject("ok" -> false))))
      errs.size shouldBe 1
      errs.head match {
        case TransactionAbort(msg, _) =>
          msg shouldBe "The function `+` does not exist on `Null`"
        case _ => fail("expected a different error")
      }
      evalOk(auth, "BarV4.all().count()") shouldEqual Value.Int(0)
    }

    "can read" in {
      // This test implements "at most 2" behavior using a check constraint,
      // showing how check constraints and indexes combined can generalize
      // unique constraints.
      //
      // NB: Count < 3 because the check constraint evaluates after the write.
      //
      // The byAnimal index is a side-check that the writes don't end up in a
      // user-defined index either.
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Ark',
           |  indexes: {
           |    byAnimal: {
           |      terms: [{ field: "animal" }]
           |    }
           |  },
           |  constraints: [
           |    { check: {
           |      name: 'atMostTwoOfMe',
           |      body: 'doc => Ark.byAnimal(doc.animal).count() < 3'
           |    } }
           |  ]
           |})
           |""".stripMargin
      )

      evalOk(auth, "Ark.create({ animal: 'turtle' })")
      evalOk(auth, "Ark.create({ animal: 'turtle' })")
      evalErr(
        auth,
        "Ark.create({ animal: 'turtle' })").code shouldEqual "constraint_failure"
      evalOk(auth, "Ark.byAnimal('turtle').count()") shouldEqual Value.Int(2)
    }

    "cannot write" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Animal',
           |  constraints: [
           |    { check: {
           |      name: 'cloneMe',
           |      body: 'doc => { Animal.create({ animal: doc.animal }); true }'
           |    } }
           |  ]
           |})
           |""".stripMargin
      )

      val err = evalErr(auth, "Animal.create({ name: 'Dolly', animal: 'sheep' })")
      err.code shouldEqual "invalid_effect"
      err.errors.size shouldEqual 1
      err.errors.head.message shouldEqual "`create` performs a write, which is not allowed in check constraints."
      evalOk(auth, "Animal.all().count()") shouldEqual Value.Int(0)
    }

    "allow miscreants to transmit information via abort" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Felon',
           |  constraints: [
           |    { check: {
           |      name: 'isGuilty',
           |      body: '_ => abort(0)'
           |    } }
           |  ]
           |})
           |""".stripMargin
      )

      val err = evalErr(auth, "Felon.create({ name: 'DX' })")
      err.code shouldEqual "abort"
      err.errors.size shouldEqual 1
      err.errors.head.message shouldEqual
        "Query aborted."
      err.asInstanceOf[QueryRuntimeFailure].abortReturn.value.value shouldBe Value
        .Int(0)
      evalOk(auth, "Felon.all().count()") shouldEqual Value.Int(0)
    }
  }

  "byId allows integers and strings" in {
    evalOk(auth, "Collection.create({ name: 'Foo' })")
    evalRes(auth, "Foo.byId(ID(3))").typeStr shouldBe "Ref<Foo>"
    evalRes(auth, "Foo.byId(ID('3'))").typeStr shouldBe "Ref<Foo>"
    evalRes(auth, "Foo.byId(3)").typeStr shouldBe "Ref<Foo>"
    evalRes(auth, "Foo.byId('3')").typeStr shouldBe "Ref<Foo>"
    evalErr(auth, "Foo.byId(true)")
      .asInstanceOf[QueryCheckFailure]
      .errors(0)
      .message shouldBe
      "Type `Boolean` is not a subtype of `ID`"
  }

  "work correctly with at-expression" in {
    evalOk(
      auth,
      """|Collection.create({
         |  name: 'Foo',
         |  constraints: [
         |    { check: {
         |      name: 'increasing',
         |      body: 'doc => {
         |        let prev = at (Time.now().subtract(1, "microsecond")) {
         |          (doc ?? { x: 0 }).x
         |        }
         |        doc.x > prev
         |      }'
         |    } }
         |  ]
         |})
         |""".stripMargin
    )

    evalErr(
      auth,
      "Foo.create({ id: 0, x: 0 })").code shouldEqual "constraint_failure"
    evalOk(auth, "Foo.create({ id: 0, x: 1 })")
    evalErr(
      auth,
      "Foo.byId(0)!.update({ x: 1 })").code shouldEqual "constraint_failure"
    evalOk(auth, "Foo.byId(0)!.update({ x: 3 })")
    evalErr(
      auth,
      "Foo.byId(0)!.update({ x: 2 })").code shouldEqual "constraint_failure"
  }
}
