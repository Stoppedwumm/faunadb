package fauna.model.test

import fauna.ast.{
  ArrayL,
  InvalidFQL4Value,
  LongL,
  RootPosition,
  SchemaValidationError,
  TransactionAbort,
  VersionL
}
import fauna.atoms._
import fauna.auth.{ Auth, ServerPermissions }
import fauna.codex.json._
import fauna.logging.ExceptionLogging
import fauna.model.{ Database, Key, SchemaNames, UserFunction }
import fauna.model.runtime.fql2.{ QueryCheckFailure, QueryRuntimeFailure }
import fauna.model.schema.SchemaCollection
import fauna.repo.schema.{ ConstraintFailure, Path }
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.Diff
import fauna.storage.ir._
import fql.ast.{ Span, Src }
import fql.error.TypeError

class FQL2UserFunctionSpec extends FQL2WithV4Spec {
  import ConstraintFailure._

  var auth: Auth = _

  before {
    val db = newDB
    auth = Auth.adminForScope(db.scopeID)
  }

  "FQL2UserFunctionSpec" - {

    "can delete UDFs" in {
      evalOk(auth, "Function.create({ name: 'AddTwo', body: 'x => x + 2' })")

      evalOk(auth, "Function.byName('AddTwo').exists()") shouldEqual Value.True
      evalOk(auth, "Function.byName('AddTwo') == null") shouldEqual Value.False
      evalOk(auth, "Function.byName('AddTwo').name") shouldEqual Value.Str("AddTwo")
      evalOk(auth, "AddTwo.definition.name") shouldEqual Value.Str("AddTwo")

      evalOk(auth, "AddTwo.definition.delete()")

      evalOk(auth, "Function.byName('AddTwo').exists()") shouldEqual Value.False
      evalOk(auth, "Function.byName('AddTwo') == null") shouldEqual Value.True
      // .name gets the name off the ref, so it should still work even when the
      // function doesn't exist
      evalOk(auth, "Function.byName('AddTwo').name") shouldEqual Value.Str("AddTwo")
      evalErr(auth, "AddTwo.definition.name") should matchPattern {
        case QueryCheckFailure(
              List(TypeError("Unbound variable `AddTwo`", _, _, _))) =>
      }

      evalOk(auth, "Function.byName('Foo').exists()") shouldEqual Value.False
      evalOk(auth, "Function.byName('Foo') == null") shouldEqual Value.True
      evalOk(auth, "Function.byName('Foo').name") shouldEqual Value.Str("Foo")
    }

    "can refer to builtin roles" in {
      val query =
        Key.Role.Builtin.Names.zipWithIndex map { case (role, i) =>
          s"""|Function.create({
              |  name: "Fn$i",
              |  body: "_ => null",
              |  role: "$role"
              |})""".stripMargin
        }
      evalOk(auth, query.mkString("\n"))
    }

    "can refer to user-defined roles by name" in {
      evalOk(
        auth,
        """|Role.create({
           |  name: "aRole",
           |  privileges: {
           |    resource: "Collection",
           |    actions: {
           |      read: true
           |    }
           |  }
           |})
           |
           |Function.create({
           |  name: "aFn",
           |  body: "_ => null",
           |  role: "aRole"
           |})""".stripMargin
      )
    }

    "can create refer its refered role" in {
      evalOk(
        auth,
        """|Function.create({
           |  name: "aFn",
           |  body: "_ => null",
           |  role: "aRole"
           |})
           |
           |Role.create({
           |  name: "aRole",
           |  privileges: {
           |    resource: "Collection",
           |    actions: {
           |      read: true
           |    }
           |  }
           |})""".stripMargin
      )
    }

    "validates role name" in {
      evalErr(
        auth,
        """|Function.create({
           |  name: "aFn",
           |  body: "_ => null",
           |  role: "aRole"
           |})""".stripMargin
      ) should matchPattern {
        case QueryRuntimeFailure.Simple(
              "constraint_failure",
              "Failed to create Function.",
              _,
              Seq(
                ValidatorFailure(
                  Path(Right("role")),
                  "Field refers to an unknown role name `aRole`.")
              )) =>
      }
    }

    "cannot assign admin privileges with the server role" in {
      val sauth = auth.withPermissions(ServerPermissions)

      // Can't create with admin permissions.
      evalErr(
        sauth,
        "Function.create({ name: 'no', body: '_ => 0', role: 'admin' })") should matchPattern {
        case QueryRuntimeFailure.Simple(
              "constraint_failure",
              "Failed to create Function.",
              _,
              Seq(
                ValidatorFailure(
                  Path(Right("role")),
                  "Cannot create a function with admin permissions using the server role."))
            ) =>
      }

      // Can't update to admin permissions.
      evalOk(
        sauth,
        "Function.create({ name: 'yes', body: '_ => 0', role: 'server' })")
      evalErr(
        sauth,
        "yes.definition.update({ role: 'admin' })") should matchPattern {
        case QueryRuntimeFailure.Simple(
              "constraint_failure",
              "Failed to update Function `yes`.",
              _,
              Seq(ValidatorFailure(
                Path(Right("role")),
                "Cannot grant a function admin permissions using the server role."))
            ) =>
      }

      // If the update to admin permissions is a no-op, it's OK.
      evalOk(auth, "Function.create({ name: 'meh', body: '_ => 0', role: 'admin' })")
      evalOk(sauth, "meh.definition.update({ role: 'admin' })")
    }

    "internal signatures are added (via post-eval hook)" in {
      val db = (ctx ! Database.forScope(auth.scopeID)).get
      val parentAuth = Auth.adminForScope(db.parentScopeID)

      // turn off typechecking
      evalOk(
        parentAuth,
        s"Database.byName('${db.name}')!.update({ typechecked: false })")

      evalOk(
        auth,
        """|Function.create({
           |  name: "aFn",
           |  body: "x => x"
           |})""".stripMargin)

      // functions get a signature of "any"
      evalRes(auth, "aFn").typeStr shouldEqual "UserFunction<Any>"

      // this typechecks, but fails at runtime
      evalErr(auth, "aFn()") should matchPattern {
        case QueryRuntimeFailure.Simple(_, _, _, _) =>
      }

      // this typechecks, but fails at runtime
      evalErr(auth, "aFn(3, 4)") should matchPattern {
        case QueryRuntimeFailure.Simple(_, _, _, _) =>
      }

      evalRes(auth, "aFn.definition").typeStr shouldEqual "FunctionDef"

      // turn on typechecking
      evalOk(
        parentAuth,
        s"Database.byName('${db.name}')!.update({ typechecked: true })")

      ctx.cacheContext.invalidateAll()

      // update triggers schema validation
      val ref = evalOk(auth, """Function.byName("aFn")!.update({})""")

      val fn =
        (ctx ! Store.getUnmigrated(auth.scopeID, ref.asInstanceOf[Value.Doc].id)).get
      fn.data(UserFunction.InternalSigField) shouldEqual Some("<A> A => A")

      // function type is now shown
      evalRes(auth, "aFn").typeStr shouldEqual "UserFunction<A => A>"

      // now this fails to typecheck
      evalErr(auth, "aFn()") should matchPattern {
        case QueryCheckFailure(Seq(TypeError(
              "Function was not called with enough arguments. Expected 1, received 0",
              _,
              _,
              _))) =>
      }
      evalErr(auth, "aFn(3, 4)") should matchPattern {
        case QueryCheckFailure(Seq(TypeError(
              "Function was called with too many arguments. Expected 1, received 2",
              _,
              _,
              _))) =>
      }

      evalRes(auth, "aFn.definition").typeStr shouldEqual "FunctionDef"

      evalOk(
        auth,
        """|Function.create({
           |  name: "foo1",
           |  body: "x => foo2(x)"
           |})
           |Function.create({
           |  name: "foo2",
           |  body: "x => 1 + x"
           |})""".stripMargin
      )

      // it...typechecks
      evalErr(auth, "foo1(true)", typecheck = true)
    }

    "internal signature anneals correctly" in {
      evalOk(auth, s"Function.create({ name: 'foo', body: 'x => x' })")

      def sig(udf: String): String = {
        evalOk(auth, s"foo.definition.update({ body: '$udf' })")

        val id = evalOk(auth, "foo.definition").asInstanceOf[Value.Doc].id
        val vers = (ctx ! Store.getUnmigrated(auth.scopeID, id)).get

        vers.data(UserFunction.InternalSigField).get
      }

      sig("x => x * 2") shouldBe "Number => Number"
      sig("x => x + 2") shouldBe "Number => Number"
      sig("x => x + x") shouldBe "(Number => Number) & (String => String)"
    }

    "recursion works (w/o typechecks)" in {
      val db = (ctx ! Database.forScope(auth.scopeID)).get
      val parentAuth = Auth.adminForScope(db.parentScopeID)

      // turn off typechecking
      evalOk(
        parentAuth,
        s"Database.byName('${db.name}')!.update({ typechecked: false })")

      evalOk(
        auth,
        """|Function.create({
           |  name: "fact",
           |  body: "x => if (x == 0) 0 else x + fact(x - 1)"
           |})""".stripMargin
      )

      evalOk(auth, "fact(10)", typecheck = false) shouldEqual Value.Int(55)
    }

    "recursion works (with typechecks)" in {
      evalOk(
        auth,
        """|Function.create({
           |  name: "fact",
           |  signature: "Number => Number",
           |  body: "x => if (x == 0) 0 else x + fact(x - 1)"
           |})""".stripMargin
      )

      evalOk(
        auth,
        """|Function.create({
           |  name: "fact2",
           |  signature: "Number => Number",
           |  body: "x => if (x == 0) 0 else fact(x - 1) + x"
           |})""".stripMargin
      )

      evalOk(auth, "fact(10)") shouldEqual Value.Int(55)
      evalOk(auth, "fact2(10)") shouldEqual Value.Int(55)
    }

    // count(n) recurs to a depth of n.
    def mkCountUDF() =
      evalOk(
        auth,
        """|Function.create({
           |  name: "count",
           |  signature: "Number => Number",
           |  body: "x => if (x == 0) 0 else 1 + count(x - 1)"
           |})""".stripMargin
      )

    "stack frame limits apply to recursion depth" in {
      mkCountUDF()
      val max = TestMaxStackFrames
      evalOk(auth, s"count($max)") shouldEqual Value.Int(max)
      evalErr(auth, s"count(${max + 1})") should matchPattern {
        case QueryRuntimeFailure.Simple("stack_overflow", _, _, _) =>
      }
    }

    "stack frame limits apply to parallel execution" in {
      mkCountUDF()

      // Brushing the frame limit but not exceeding it.
      val m = TestMaxStackFrames
      evalOk(auth, s"[count($m), count($m), count($m), count($m)]")

      // Going just over the limit
      val n = m + 1
      evalErr(
        auth,
        s"[count($n), count($n), count($n), count($n)]") should matchPattern {
        case QueryRuntimeFailure.Simple("stack_overflow", _, _, _) =>
      }
    }

    "user signatures are validated" in {
      // Bad user signature parse.
      val err = evalErr(
        auth,
        """|Function.create({
           |  name: "badParse",
           |  signature: "Int <:-^-< Number",
           |  body: "x => x"
           |})""".stripMargin
      )
      err.code shouldBe "constraint_failure"
      err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: Failed to create Function.
           |constraint failures:
           |  signature: Failed parsing user-provided signature.
           |      error: Expected end-of-input
           |      at *signature*:1:5
           |        |
           |      1 | Int <:-^-< Number
           |        |     ^
           |        |
           |at *query*:1:16
           |  |
           |1 |   Function.create({
           |  |  ________________^
           |2 | |   name: "badParse",
           |3 | |   signature: "Int <:-^-< Number",
           |4 | |   body: "x => x"
           |5 | | })
           |  | |__^
           |  |""".stripMargin
      )

      // Bad user signature
      evalErr(
        auth,
        """|Function.create({
           |  name: "badSig",
           |  signature: "Int => Int",
           |  body: "x => 'hello, '.concat(x)"
           |})""".stripMargin
      ).failureMessage should include(
        "Type `String => String` is not a subtype of `(x: Int) => Int`")
    }

    "user signatures are added" in {
      // More general user signature.
      val ref = evalOk(
        auth,
        """|Function.create({
           |  name: "aFn",
           |  signature: "Number => Number",
           |  body: "x => Math.sign(x)"
           |})""".stripMargin
      )

      val fn =
        (ctx ! Store.getUnmigrated(auth.scopeID, ref.asInstanceOf[Value.Doc].id)).get
      fn.data(UserFunction.InternalSigField) shouldEqual Some(
        "(x: Number) => Number")
      fn.data(UserFunction.UserSigField) shouldEqual Some("Number => Number")

      // Recursive UDF.
      val refR = evalOk(
        auth,
        """|Function.create({
           |  name: "aFnR",
           |  signature: "Number => Number",
           |  body: "x => if (x == 0) Math.sign(x) else aFnR(x - 1)"
           |})""".stripMargin
      )

      val fnR = (ctx ! Store.getUnmigrated(
        auth.scopeID,
        refR.asInstanceOf[Value.Doc].id)).get
      fnR.data(UserFunction.InternalSigField) shouldEqual Some(
        "(x: Number) => Number")
      fnR.data(UserFunction.UserSigField) shouldEqual Some("Number => Number")

      // UDF references another udf.
      val ref2 = evalOk(
        auth,
        """|Function.create({
           |  name: "aFn2",
           |  signature: "Number => Number",
           |  body: "x => Math.sign(aFn(x))"
           |})""".stripMargin
      )

      val fn2 = (ctx ! Store.getUnmigrated(
        auth.scopeID,
        ref2.asInstanceOf[Value.Doc].id)).get
      fn2.data(UserFunction.InternalSigField) shouldEqual Some(
        "(x: Number) => Number")
      fn2.data(UserFunction.UserSigField) shouldEqual Some("Number => Number")

      // Generic signature.
      val refG = evalOk(
        auth,
        """|Function.create({
           |  name: "aFnG",
           |  signature: "A => A",
           |  body: "x => x"
           |})""".stripMargin
      )

      val fnG = (ctx ! Store.getUnmigrated(
        auth.scopeID,
        refG.asInstanceOf[Value.Doc].id)).get
      fnG.data(UserFunction.InternalSigField) shouldEqual Some("<A> (x: A) => A")
      fnG.data(UserFunction.UserSigField) shouldEqual Some("A => A")
    }

    "invalid internal sig falls back to any" in {
      val ref =
        evalOk(auth, "Function.create({ name: 'badInternal', body: 'x => x' })")
      val fn =
        (ctx ! Store.getUnmigrated(auth.scopeID, ref.asInstanceOf[Value.Doc].id)).get

      // break the internal sig
      val coll = (ctx ! SchemaCollection.UserFunction(auth.scopeID))
      val diff = Diff(UserFunction.InternalSigField -> Some("```BAD```"))
      val fn0 = ctx ! Store.internalUpdate(coll.Schema, fn.id, diff)

      fn0.data(UserFunction.InternalSigField) shouldEqual Some("```BAD```")

      // This will log an exception and return a 200 in prod, and the type will
      // be `UserFunction<Any>`.
      val e = the[IllegalStateException] thrownBy evalRes(auth, "badInternal")
      e.getMessage should include("UDF internal sig is invalid on")

      ExceptionLogging.alwaysSquelch {
        val res = evalRes(auth, "badInternal")
        res.typeStr shouldEqual "UserFunction<Any>"
      }
    }

    "disallows short lambdas" in {
      val expected =
        """|Unable to parse FQL source code.
           |error: Short form lambdas are not allowed here
           |at *body*:1:2
           |  |
           |1 | (.x)
           |  |  ^^
           |  |""".stripMargin
      val err =
        evalErr(auth, "Function.create({ name: 'noShortLambdas', body: '(.x)' })")
      err.code shouldEqual "constraint_failure"
      err
        .asInstanceOf[QueryRuntimeFailure]
        .constraintFailures(0)
        .fields(0)
        .elements shouldBe Seq(Right("body"))
      err
        .asInstanceOf[QueryRuntimeFailure]
        .constraintFailures(0)
        .message shouldBe expected
    }

    "validate foreign references" - {
      "roles" - {
        def withRoleFK(isDelete: Boolean, query: => String) = {
          evalOk(
            auth,
            """|Function.create({
               |  name: "aFn",
               |  body: "_ => null"
               |})
               |
               |Role.create({
               |  name: "aRole",
               |  privileges: {
               |    resource: "aFn",
               |    actions: { call: true }
               |  }
               |})""".stripMargin
          )
          if (isDelete) {
            renderErr(auth, query) shouldBe (
              """|error: Invalid database schema update.
                 |    error: Resource `aFn` does not exist
                 |    at main.fsl:5:14
                 |      |
                 |    5 |   privileges aFn {
                 |      |              ^^^
                 |      |
                 |at *query*:1:31
                 |  |
                 |1 | Function.byName('aFn')!.delete()
                 |  |                               ^^
                 |  |""".stripMargin
            )
          } else {
            evalOk(auth, query)
            evalOk(
              auth,
              """|
                 |Role.byName("aRole")!.privileges.resource
                 |""".stripMargin
            ) shouldBe Value.Str("anotherName")
          }
        }
        "rename" in withRoleFK(
          false,
          "Function.byName('aFn')!.update({ name: 'anotherName'})"
        )
        "delete" in withRoleFK(
          true,
          "Function.byName('aFn')!.delete()"
        )
      }
    }

    "fql4" - {
      "preserves fqlx fields on update" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "sheepIt",
             |  alias: "goatIt",
             |  body: "x => x"
             |})
             |""".stripMargin
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(Ref("functions/sheepIt"))
        ).asInstanceOf[VersionL].version

        evalV4Ok(
          auth,
          Update(Ref("functions/sheepIt"), MkObject())
        )

        val legacyReadTwo = evalV4Ok(
          auth,
          Get(Ref("functions/sheepIt"))
        ).asInstanceOf[VersionL].version

        legacyReadTwo.data shouldEqual legacyRead.data
      }
      "preserves fqlx fields on replace" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "sheepIt",
             |  alias: "goatIt",
             |  body: "x => x"
             |})
             |""".stripMargin
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(Ref("functions/sheepIt"))
        ).asInstanceOf[VersionL].version

        evalV4Ok(
          auth,
          Replace(
            Ref("functions/sheepIt"),
            MkObject(
              "name" -> "newName",
              "body" -> QueryF(Lambda(JSArray() -> "newFunc"))
            ))
        )

        val legacyReadTwo = evalV4Ok(
          auth,
          Get(Ref("functions/newName"))
        ).asInstanceOf[VersionL].version

        SchemaNames.findAlias(legacyReadTwo) shouldEqual SchemaNames.findAlias(
          legacyRead)
      }
      "can't update fqlx fields" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "sheepIt",
             |  alias: "goatIt",
             |  body: "x => x"
             |})
             |""".stripMargin
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(Ref("functions/sheepIt"))
        ).asInstanceOf[VersionL].version

        evalV4Ok(
          auth,
          Update(
            Ref("functions/sheepIt"),
            MkObject(
              UserFunction.InternalSigField.path.head -> "hello",
              SchemaNames.AliasField.path.head -> "goodbye"
            ))
        )

        val legacyReadTwo = evalV4Ok(
          auth,
          Get(Ref("functions/sheepIt"))
        ).asInstanceOf[VersionL].version

        legacyReadTwo.data shouldEqual legacyRead.data
      }
      "can update body with fql4 body (should remove internal sig)" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "sheepIt",
             |  alias: "goatIt",
             |  body: "x => x"
             |})
             |""".stripMargin
        )

        evalV4Ok(
          auth,
          Update(
            Ref("functions/sheepIt"),
            MkObject(
              "body" -> QueryF(Lambda(JSArray() -> "newFunc"))
            ))
        )

        val legacyRead = evalV4Ok(
          auth,
          Get(Ref("functions/sheepIt"))
        ).asInstanceOf[VersionL].version

        legacyRead.data.getOpt(UserFunction.BodyField) should contain(
          Right(
            QueryV(
              APIVersion.V4,
              MapV("lambda" -> ArrayV(), "expr" -> StringV("newFunc")))))
        legacyRead.data.fields.contains(
          UserFunction.InternalSigField.path) shouldBe false
      }
      "can't update body with string body" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "sheepIt",
             |  alias: "goatIt",
             |  body: "x => x"
             |})
             |""".stripMargin
        )
        val errs = evalV4Err(
          auth,
          Update(
            Ref("functions/sheepIt"),
            MkObject(
              "body" -> "x => 1"
            ))
        )
        errs.length shouldBe 1
        errs.head shouldBe a[SchemaValidationError]
        errs.head
          .asInstanceOf[SchemaValidationError]
          .msg shouldBe "Invalid type provided for field `body`."
      }

      "can't create UDF with string body" in {
        val errs = evalV4Err(
          auth,
          CreateFunction(MkObject("name" -> "fn", "body" -> "x => x"))
        )
        errs.length shouldBe 1
        errs.head shouldBe a[SchemaValidationError]
        errs.head
          .asInstanceOf[SchemaValidationError]
          .msg shouldBe "Invalid type provided for field `body`."
      }

      "can update v4 functions from v10" in {
        evalV4Ok(
          auth,
          CreateFunction(
            MkObject(
              "name" -> "fn",
              "body" -> JSObject(
                "query" -> JSObject("lambda" -> JSArray(), "expr" -> "newFunc"))))
        )

        evalOk(auth, "Function.byName('fn')!.update({ alias: 'FnV4' })")
      }

      "can call fqlx udf" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "noParams",
             |  body: "() => [1 , 2]"
             |})
             |Function.create({
             |  name: "doubleFunc",
             |  body: "x => x * 2"
             |})
             |Function.create({
             |  name: "twoParams",
             |  body: "(x, y) => [x, y]"
             |})
             |""".stripMargin
        )

        evalV4Ok(auth, Call("noParams")) shouldBe ArrayL(LongL(1), LongL(2))
        evalV4Ok(auth, Call("doubleFunc", 10)) shouldBe LongL(20)
        evalV4Ok(auth, Call("twoParams", JSArray(1, 2))) shouldBe ArrayL(
          LongL(1),
          LongL(2))
      }
      "invalid arity is returned when calling fqlx udf with the wrong number of parameters" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "doubleFunc",
             |  body: "x => x * 2"
             |})
             |""".stripMargin
        )

        evalV4Err(auth, Call("doubleFunc")) shouldBe List(
          TransactionAbort(
            "callee function expects exactly 1 argument, but 0 were provided.",
            RootPosition))

        evalV4Err(auth, Call("doubleFunc", 10, 10)) shouldBe List(
          TransactionAbort(
            "callee function expects exactly 1 argument, but 2 were provided.",
            RootPosition))
      }
      "invalid return from fqlx udf in fql4 renders as an error" in {
        evalOk(
          auth,
          """|Function.create({
             |  name: "invalidReturn",
             |  body: "x => (.name)"
             |})
             |""".stripMargin
        )
        evalV4Err(auth, Call("invalidReturn", "x")) shouldBe List(
          InvalidFQL4Value("Lambda", RootPosition))
      }
    }

    "FQLX values can be passed to FQLX functions" in {
      evalOk(
        auth,
        """|Function.create({
           |  name: "Passthrough",
           |  body: "x => x"
           |})
           |""".stripMargin
      )
      evalOk(auth, "Passthrough(Collection.all)")
    }

    "FQLX values can be passed to FQLX functions, with an FQLX role" in {
      val key = evalOk(
        auth,
        """|Role.create({
           |  name: "MyRoleThatNeedsArgs",
           |  privileges: [{
           |    resource: "PassthroughWithRole",
           |    actions: {
           |      call: "x => true",
           |    },
           |  }],
           |})
           |
           |Function.create({
           |  name: "PassthroughWithRole",
           |  role: "MyRoleThatNeedsArgs",
           |  body: "x => x"
           |})
           |
           |Key.create({ role: "MyRoleThatNeedsArgs" }).secret
           |""".stripMargin
      ).as[String]

      evalOk(key, "PassthroughWithRole(Collection.all)")
    }

    "FQLX values can be passed to FQLX functions, with an FQL4 role" in {
      evalOk(
        auth,
        """Function.create({
           |  name: "PassthroughWithRole",
           |  body: "x => x"
           |})
           |""".stripMargin)

      evalV4Ok(
        auth,
        CreateRole(
          MkObject(
            "name" -> "MyFQL4Role",
            "privileges" -> JSArray(
              MkObject(
                "resource" -> FunctionRef("PassthroughWithRole"),
                "actions" -> MkObject(
                  "call" -> JSObject(
                    "@query" -> JSObject(
                      "lambda" -> JSArray("x"),
                      "expr" -> JSBoolean(true)
                    )
                  )
                )
              )
            )
          ))
      )
      val key = evalOk(auth, "Key.create({ role: 'MyFQL4Role' }).secret").as[String]

      evalOk(key, "PassthroughWithRole(3)") shouldBe Value.Int(3)
      evalErr(
        key,
        "PassthroughWithRole(Collection.all)") shouldBe QueryRuntimeFailure(
        "permission_denied",
        "Insufficient privileges to perform the action.",
        Span(19, 35, Src.Query("")))
    }

    "doesn't give hint for user function that don't exist" in {
      evalErr(
        auth,
        """|Function("Foo")
           |""".stripMargin
      ).errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: invalid argument `function`: No such user function `Foo`.
           |at *query*:1:9
           |  |
           |1 | Function("Foo")
           |  |         ^^^^^^^
           |  |""".stripMargin
      )
    }

    "give pretty error for collections created in this transaction" in {
      evalErr(
        auth,
        """|Function.create({
           |  name: "Foo",
           |  body: "x => x"
           |})
           |Function("Foo")
           |""".stripMargin
      ).errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
        """|error: invalid argument `function`: No such user function `Foo`.
           |at *query*:5:9
           |  |
           |5 | Function("Foo")
           |  |         ^^^^^^^
           |  |
           |hint: A function cannot be created and used in the same query.
           |  |
           |5 | Function("Foo")
           |  |         ^^^^^^^
           |  |""".stripMargin
      )
    }
  }
}
