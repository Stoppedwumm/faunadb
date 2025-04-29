package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.codex.json._
import fauna.model.schema.SchemaSource

class FQL2SchemaV4Spec extends FQL2WithV4Spec {

  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  def evalV4SchemaErr(auth: Auth, expr: JSValue) = {
    val errs = evalV4Err(auth, expr)
    errs.foreach { e => e.code shouldBe "schema validation failed" }
    errs.map(_.asInstanceOf[fauna.ast.SchemaValidationError].msg)
  }

  "v4 enforces schema on create" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin)

    evalV4Ok(
      auth,
      CreateF(ClsRefV("User"), MkObject("data" -> MkObject("name" -> "user-1"))))

    evalV4SchemaErr(auth, CreateF(ClsRefV("User"), MkObject())) shouldBe List(
      "name: Missing required field of type String")

    evalV4SchemaErr(
      auth,
      CreateF(
        ClsRefV("User"),
        MkObject("data" -> MkObject("foo" -> "bar")))) shouldBe List(
      "foo: Unexpected field provided",
      "name: Missing required field of type String")

    evalV4SchemaErr(
      auth,
      CreateF(
        ClsRefV("User"),
        MkObject("data" -> MkObject("name" -> 3)))) shouldBe List(
      "name: Expected String, provided Int")
  }

  "v4 allows setting top-level fields like 'ts' when they're under 'data'" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  *: Any
           |}""".stripMargin)

    evalV4Ok(
      auth,
      CreateF(ClsRefV("User"), MkObject("data" -> MkObject("ts" -> Now()))))
  }

  "v4 enforces update correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  address: String
           |}""".stripMargin
    )

    evalV4Ok(
      auth,
      CreateF(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> "hi", "address" -> "foo"))))

    evalV4Ok(
      auth,
      Update(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> "hello"))))

    evalV4SchemaErr(
      auth,
      Update(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> 3)))) shouldBe List(
      "name: Expected String, provided Int")

    evalV4SchemaErr(
      auth,
      Update(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> JSNull)))) shouldBe List(
      "name: Missing required field of type String")
  }

  "v4 enforces replace correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  address: String
           |}""".stripMargin
    )

    evalV4Ok(
      auth,
      CreateF(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> "hi", "address" -> "foo"))))

    evalV4Ok(
      auth,
      Replace(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> "hello", "address" -> "bar"))))

    evalV4SchemaErr(
      auth,
      Replace(
        RefV(1, ClsRefV("User")),
        MkObject("data" -> MkObject("name" -> "hi")))) shouldBe List(
      "address: Missing required field of type String")

    evalV4SchemaErr(
      auth,
      Replace(
        RefV(1, ClsRefV("User")),
        MkObject(
          "data" -> MkObject("name" -> 3, "address" -> "bar")))) shouldBe List(
      "name: Expected String, provided Int")
  }

  "v4 enforces check constraints" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: Int
           |
           |  check name_positive (.name > 0)
           |}""".stripMargin
    )

    evalV4Ok(
      auth,
      CreateF(ClsRefV("User"), MkObject("data" -> MkObject("name" -> 3))))
    val errs = evalV4Err(
      auth,
      CreateF(ClsRefV("User"), MkObject("data" -> MkObject("name" -> -2))))
    errs.foreach { e => e.code shouldBe "check constraint failure" }
    errs.map {
      _.asInstanceOf[fauna.ast.CheckConstraintEvalError].inner.msg
    } shouldBe List("Document failed check constraint `name_positive`")
  }

  "v4 disallows overwriting computed fields" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  compute fullName = doc => doc.name + "foo"
           |}""".stripMargin
    )

    evalV4Ok(
      auth,
      CreateF(ClsRefV("User"), MkObject("data" -> MkObject("name" -> "hi"))))
    evalV4SchemaErr(
      auth,
      CreateF(
        ClsRefV("User"),
        MkObject(
          "data" -> MkObject("name" -> "hi", "fullName" -> "hello")))) shouldBe List(
      "fullName: Unexpected field provided")
  }

  // Not sure if this will ever work. Can't hurt to test the current behavior though.
  "v4 does not return computed fields" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  compute fullName = doc => doc.name + "foo"
           |}""".stripMargin
    )

    evalV4Err(
      auth,
      Select(
        Seq("fullName"),
        CreateF(ClsRefV("User"), MkObject("data" -> MkObject("name" -> "hi")))
      )).map(_.asInstanceOf[fauna.ast.ValueNotFound].path) shouldBe List(
      List(Right("fullName"))
    )

    evalV4Err(
      auth,
      Select(
        Seq("data", "fullName"),
        CreateF(ClsRefV("User"), MkObject("data" -> MkObject("name" -> "hi")))
      )).map(_.asInstanceOf[fauna.ast.ValueNotFound].path) shouldBe List(
      List(Right("data"), Right("fullName"))
    )
  }

  "getting the active schema should regenerate it" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|function foo() { 0 }
           |""".stripMargin
    )

    evalV4Ok(auth, CreateCollection(MkObject("name" -> "Foo")))

    (ctx ! SchemaSource.getActive(auth.scopeID)).map(_.content) shouldBe Seq(
      """|function foo() { 0 }
         |
         |collection Foo {
         |}
         |""".stripMargin
    )

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection Bar {}
           |""".stripMargin
    )

    (ctx ! SchemaSource.getActive(auth.scopeID)).map(_.content) shouldBe Seq(
      """|function foo() { 0 }
         |
         |collection Foo {
         |}
         |""".stripMargin
    )
  }

  "getting the active schema should regenerate it after staging a schema" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|function foo() { 0 }
           |""".stripMargin
    )

    evalV4Ok(auth, CreateCollection(MkObject("name" -> "Foo")))

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection Bar {}
           |""".stripMargin
    )

    // FIXME: We can't actually regenerate the active schema in `updateSchemaPinOk`,
    // as writing schema in that transaction will write the staged schema.
    (ctx ! SchemaSource.getActive(auth.scopeID)).map(_.content) shouldBe Seq(
      """|function foo() { 0 }
         |""".stripMargin
    )

    pendingUntilFixed {
      (ctx ! SchemaSource.getActive(auth.scopeID)).map(_.content) shouldBe Seq(
        """|function foo() { 0 }
           |
           |collection Foo {
           |}
           |""".stripMargin
      )
    }
  }
}
