package fauna.model.test

import fauna.ast.VersionL
import fauna.atoms.{ AccessProviderID, CollectionID, RoleID, UserFunctionID }
import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.{ AccessProvider, Collection, LambdaWrapper, Role, UserFunction }
import fauna.model.runtime.fql2.{
  QueryFailure,
  QueryRuntimeFailure,
  UserFunction => FQLXUserFunction
}
import fauna.model.runtime.fql2.serialization.MaterializedValue
import fauna.model.runtime.fql2.stdlib.{
  CollectionCompanion,
  UserCollectionCompanion
}
import fauna.repo.schema.ConstraintFailure
import fauna.repo.schema.ConstraintFailure.ValidatorFailure
import fauna.repo.schema.Path
import fauna.repo.values.Value
import fql.parser.Tokens
import scala.concurrent.duration._
import scala.util.Random

class FQL2NameValidationSpec extends FQL2WithV4Spec {
  val globalModules = Seq(
    "Database",
    "FQL",
    "Collection",
    "AccessProvider",
    "Function",
    "Role",
    "Number",
    "Int",
    "Long",
    "Boolean",
    "Bytes",
    "String",
    "Time",
    "Date",
    "UUID",
    "Array",
    "Struct",
    "Doc",
    "Set",
    "Null",
    "asc",
    "desc",
    "log",
    "dbg",
    "newId"
  )

  val reservedTypes = Seq(
    "_",
    "Null",
    "true",
    "false",
    "Int",
    "Boolean",
    "Number",
    "ID",
    "Int",
    "Long",
    "Double",
    "String",
    "Bytes",
    "UUID",
    "Time",
    "Date",
    "Struct",
    "interface",
    "function",
    "Any",
    "Never",
    "type",
    "collection",
    "Float"
  )

  val reservedNames = globalModules ++ Tokens.invalidVariables ++ reservedTypes

  val validNames = Seq("hello", "FooBar", "foo_bar", "admin", "server")
  val invalidNames = Seq(
    "",
    "foo-bar",
    "hello world",
    "$foo",
    "foo.bar",
    "ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็ ด้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็็้้้้้้้้็็็็็้้้้้็็็็",
    "ÅÍÎÏ˝ÓÔÒÚÆ☃",
    "😍",
    "(╯°□°）╯︵ ┻━┻)",
    "𝕿𝖍𝖊 𝖖𝖚𝖎𝖈𝖐 𝖇𝖗𝖔𝖜𝖓 𝖋𝖔𝖝 𝖏𝖚𝖒𝖕𝖘 𝖔𝖛𝖊𝖗 𝖙𝖍𝖊 𝖑𝖆𝖟𝖞 𝖉𝖔𝖌"
  )

  def escape(name: String): String = MaterializedValue.escape(name)

  def validate(auth: Auth)(query: String => String): Unit =
    validate(auth, validNames, invalidNames, reservedNames)(query)

  def validateNonGlobalName(auth: Auth)(query: String => String): Unit =
    validate(auth, validNames, invalidNames, Seq.empty)(query)

  def validate(
    auth: Auth,
    validNames: Seq[String],
    invalidNames: Seq[String],
    reservedNames: Seq[String])(query: String => String): Unit = {
    validNames.foreach { name =>
      evalOk(auth, query(escape(name)))
    }
    invalidNames.foreach { name =>
      val err = evalErr(auth, query(escape(name)))
      err.code shouldEqual "constraint_failure"
      assertMessage(err, s"Invalid identifier.")
    }
    reservedNames.foreach { name =>
      val err = evalErr(auth, query(escape(name)))
      err.code shouldEqual "constraint_failure"
      assertMessage(err, s"The identifier `$name` is reserved.")
    }
  }

  "collections" in {
    val auth = newDB
    validate(auth) { name => s"Collection.create({ name: $name })" }
  }
  "functions" in {
    val auth = newDB
    validate(auth) { name => s"Function.create({ name: $name, body: 'x => x' })" }
  }
  "roles" in {
    val auth = newDB
    val admin = auth.withPermissions(AdminPermissions)
    validate(
      admin,
      // 'admin' and 'server' are reserved, not valid.
      validNames = validNames.filter(v => v != "admin" && v != "server"),
      invalidNames = invalidNames,
      reservedNames = Seq("admin", "server")
    ) { name => s"Role.create({ name: $name })" }
  }
  "keys" in {
    val auth = newDB
    val admin = auth.withPermissions(AdminPermissions)
    validate(
      admin,
      // 'admin' and 'server' are reserved, not valid.
      validNames = validNames.filter(v => v != "admin" && v != "server"),
      invalidNames = invalidNames,
      reservedNames = Seq("admin", "server")
    ) { name =>
      s"""|Role.create({ name: $name })
          |Key.create({ role: $name })""".stripMargin
    }
    evalOk(admin, "Key.create({ role: 'admin' })")
    evalOk(admin, "Key.create({ role: 'server' })")
    // not a valid role name, but we allow it.
    evalOk(admin, "Key.create({ role: 'server-readonly' })")
  }

  "index names" in pendingUntilFixed {
    val auth = newDB
    evalOk(auth, "Collection.create({ name: 'MyColl' })")
    // FIXME: This 500s for an invalid name
    validate(auth) { name =>
      s"""|MyColl.definition.update({
          |  indexes: {
          |    $name: {
          |      terms: [{ field: "my_field" }],
          |    }
          |  }
          |})""".stripMargin
    }
  }

  "access providers" in {
    val auth = newDB
    val admin = auth.withPermissions(AdminPermissions)
    validateNonGlobalName(admin) { name =>
      // issuer and name each need to be unique, so make a random string
      val s = Random.nextInt().toString
      s"""|AccessProvider.create({
          |  name: $name,
          |  issuer: "$s",
          |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
          |})""".stripMargin
    }
  }

  "databases" in {
    val auth = newDB
    val admin = auth.withPermissions(AdminPermissions)
    validateNonGlobalName(admin) { name => s"Database.create({ name: $name })" }
  }

  "static environment conflicts" - {
    "can't create collection name conflicting with top level fauna provided module" in {
      val auth = newAuth

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "$gm",
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")
      }
    }
    "can't create collection alias that conflicts with top level fauna provided module" in {
      val auth = newAuth

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "Hello",
              |  alias: "$gm"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")
      }
    }
    "can't update a collection name to conflict with a top level fauna provided module" in {
      val auth = newAuth
      val existingCollName = "testColl"
      mkColl(auth, existingCollName)

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|$existingCollName.definition.update({
              |  name: "$gm",
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")

        (ctx ! Collection.idByNameActive(auth.scopeID, gm)) shouldBe None
      }
    }
    "can't update a collection alias to conflict with top level fauna provided module" in {
      val auth = newAuth
      val existingCollName = "testColl"
      mkColl(auth, existingCollName)

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|$existingCollName.definition.update({
              |  alias: "$gm",
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")

        (ctx ! Collection.idByAliasActive(auth.scopeID, gm)) shouldBe None
      }
    }
    "can't create a udf conflicting with a top level fauna provided module" in {
      val auth = newAuth

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "$gm",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")
      }
    }
    "can't create a udf alias that conflicts with a top level fauna provided module" in {
      val auth = newAuth

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "Hello",
              |  alias: "$gm",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")
      }
    }
    "can't update a udf to conflict with a top level fauna provided module" in {
      val auth = newAuth
      val existingUDFName = "testUdf"
      createFunction(auth, existingUDFName)

      reservedNames foreach { gm =>
        val err = evalErr(
          auth,
          s"""|$existingUDFName.definition.update({
              |  name: "$gm",
              |})
              |""".stripMargin,
          // FIXME: type checking fails on pulling definition document off of a
          // function
          typecheck = false
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, s"The identifier `$gm` is reserved.")

        (ctx ! UserFunction.idByNameActive(auth.scopeID, gm)) shouldBe None
      }
    }
  }

  "existing collection name conflicts" - {
    "create" - {
      "two collections with the same name can't be created in the same query" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "Hello",
              |})
              |Collection.create({
              |  name: "Hello"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")
      }
      "two collections with the same name and alias can't be created in the same query" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "Hello",
              |})
              |Collection.create({
              |  name: "Hello2",
              |  alias: "Hello"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")
      }
      "can't create a collection with the same name as an existing collection name" in {
        val auth = newAuth

        mkColl(auth, "TestColl")

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "TestColl",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `TestColl`")
      }
      "can't create a UDF with the same name as an existing Collection name" in {
        val auth = newAuth

        mkColl(auth, "Hello")

        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "Hello",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")

        (ctx ! UserFunction.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "can't create a UDF with the same alias as an existing Collection name" in {
        val auth = newAuth

        mkColl(auth, "Hello")

        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "NewFunc",
              |  alias: "Hello",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")

        (ctx ! UserFunction.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "can't create a collection with the same alias as an existing collection name" in {
        val auth = newAuth

        mkColl(auth, "TestColl")

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "KnowName",
              |  alias: "TestColl"
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `TestColl`")
      }
      "conflicting UDF name can not be created in same query as Collection name" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|
              |Collection.create({ name: "Hello" })
              |Function.create({
              |  name: "Hello",
              |  body: '(x) => "Hello #{x}"'
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")
        (ctx ! UserFunction.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
    }
    "update" - {
      "can't update a collection name to conflict with an existing collection name" in {
        val auth = newAuth

        mkColl(auth, "TestColl")
        mkColl(auth, "TestColl2")

        val err = evalErr(
          auth,
          s"""|TestColl2.definition.update({
              |  name: "TestColl",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `TestColl`")
      }
      "can't update a UDF name to conflict with existing collection name" in {
        val auth = newAuth
        mkColl(auth, "Hello")

        createFunction(auth, "Hello2")

        val err = evalErr(
          auth,
          s"""|Hello2.definition.update({
              |  name: "Hello",
              |})
              |""".stripMargin,
          // FIXME: type checking fails on pulling definition document off of a
          // function
          typecheck = false
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")
        (ctx ! UserFunction.idByNameActive(
          auth.scopeID,
          "Hello2")).isDefined shouldBe true
        (ctx ! UserFunction.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "can not update a UDF to conflict with Collection created in the same query" in {
        val auth = newAuth
        createFunction(auth, "TestFunc")
        val err = evalErr(
          auth,
          s"""|
            |Collection.create({ name: "Hello" })
              |TestFunc.definition.update({
              |  name: "Hello",
              |})
              |""".stripMargin,
          // FIXME: type checking fails on pulling definition document off of a
          // function
          typecheck = false
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the name `Hello`")

        (ctx ! UserFunction.idByNameActive(
          auth.scopeID,
          "TestFunc")).isDefined shouldBe true
        (ctx ! UserFunction.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
    }
  }

  "existing collection alias conflicts" - {
    "create" - {
      "can't create a collection with the same name as an existing collection alias" in {
        val auth = newAuth

        createCollectionWithAlias(auth, "TestColl")

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "TestColl",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the alias `TestColl`")
      }
      "can't create a collection with the same alias as an existing collection alias" in {
        val auth = newAuth
        createCollectionWithAlias(auth, "TestAlias")
        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "TestColl",
              |  alias: "TestAlias"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the alias `TestAlias`")
      }
      "can't create a UDF with the same name as an existing collection alias" in {
        val auth = newAuth
        createCollectionWithAlias(auth, "TestAlias")
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "TestAlias",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the alias `TestAlias`")

      }
      "can't create a UDF with an alias that matches an existing collection alias" in {
        val auth = newAuth
        createCollectionWithAlias(auth, "TestAlias")
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "NoName",
              |  alias: "TestAlias",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the alias `TestAlias`")
      }
      "can't create a collection with the same name as an existing collection alias in the same query" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "KnowName",
              |  alias: "AliasName"
              |})
              |Collection.create({
              |  name: "AliasName",
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the alias `AliasName`")
      }
    }
    "update" - {
      "can't update a collection name to conflict with an existing collection alias" in {
        val auth = newAuth

        createCollectionWithAlias(auth, "TestColl")
        mkColl(auth, "TestColl2")

        val err = evalErr(
          auth,
          s"""|TestColl2.definition.update({
              |  alias: "TestColl",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Collection already exists with the alias `TestColl`")
      }
    }
  }

  "existing udf name conflicts" - {
    "create" - {
      "can't create a collection with the same name as an existing udf name" in {
        val auth = newAuth

        createFunction(auth, "Hello")

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "Hello",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")

        (ctx ! Collection.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "can't create a UDF with the same name as an existing UDF name" in {
        val auth = newAuth

        createFunction(auth, "Hello")

        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "Hello",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
      }
      "can't create a UDF with the same alias as an existing udf name" in {
        val auth = newAuth

        createFunction(auth, "Hello")

        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "KnowName",
              |  alias: "Hello",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
      }
      "can't create a collection with the same alias as an existing udf name" in {
        val auth = newAuth

        createFunction(auth, "Hello")

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "KnowName",
              |  alias: "Hello",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
      }
      "conflicting collection name can not be created in same query as udf name" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "Hello",
              |  body: '(x) => "Hello #{x}"'
              |})
              |Collection.create({
              |  name: "Hello"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
        (ctx ! Collection.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "conflicting collection alias with udf name can not be created in same query" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "Hello",
              |  body: '(x) => "Hello #{x}"'
              |})
              |Collection.create({
              |  name: "NewName",
              |  alias: "Hello"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
        (ctx ! Collection.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "two UDFs with the same name can not be created in the same query" in {
        val auth = newAuth
        val err = evalErr(
          auth,
          s"""|
            |Function.create({
              |  name: "Hello",
              |  body: '(x) => x + 1'
              |})
              |Function.create({
              |  name: "Hello",
              |  body: '(x) => "Hello #{x}"'
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
      }
    }
    "update" - {
      "can't update a collection name to conflict with existing udf name" in {
        val auth = newAuth
        createFunction(auth, "Hello")

        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "Hello2",
              |})
              |""".stripMargin
        )

        val err = evalErr(
          auth,
          s"""|Hello2.definition.update({
              |  name: "Hello",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
        (ctx ! Collection.idByNameActive(
          auth.scopeID,
          "Hello2")).isDefined shouldBe true
        (ctx ! Collection.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
      "can't update a UDF name to conflict with an existing UDF name" in {
        val auth = newAuth

        createFunction(auth, "Hello")
        createFunction(auth, "Hello2")

        val err = evalErr(
          auth,
          s"""|Hello2.definition.update({
              |  name: "Hello",
              |})
              |""".stripMargin,
          // FIXME: type checking fails on pulling definition document off of a
          // function
          typecheck = false
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
      }
      "can not update collection name to conflict with a UDF name created in the same query" in {
        val auth = newAuth
        mkColl(auth, "TestColl")
        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "Hello",
              |  body: '(x) => "Hello #{x}"'
              |})
              |TestColl.definition.update({
              |  name: "Hello"
              |})
              |""".stripMargin
        )
        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the name `Hello`")
        (ctx ! Collection.idByNameActive(
          auth.scopeID,
          "TestColl")).isDefined shouldBe true
        (ctx ! Collection.idByNameActive(auth.scopeID, "Hello")) shouldBe None
      }
    }
  }

  "existing udf alias conflicts" - {
    "create" - {
      "can't create a collection with the same name as an existing udf alias" in {
        val auth = newAuth

        createFunction(auth, "Hello", Some("TestAlias"))

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "TestAlias",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the alias `TestAlias`")

        (ctx ! Collection.idByNameActive(auth.scopeID, "TestAlias")) shouldBe None
      }
      "can't create a collection with the same alias as an existing udf alias" in {
        val auth = newAuth

        createFunction(auth, "Hello", Some("TestAlias"))

        val err = evalErr(
          auth,
          s"""|Collection.create({
              |  name: "KnowName",
              |  alias: "TestAlias"
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the alias `TestAlias`")

        (ctx ! Collection.idByAliasActive(auth.scopeID, "TestAlias")) shouldBe None
      }
      "can't create a UDF with the same name as an existing UDF alias" in {
        val auth = newAuth

        createFunction(auth, "Hello", Some("TestAlias"))

        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "TestAlias",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the alias `TestAlias`")

        (ctx ! UserFunction.idByNameActive(auth.scopeID, "TestAlias")) shouldBe None
      }
      "can't create a UDF with the same alias as an existing UDF alias" in {
        val auth = newAuth

        createFunction(auth, "Hello", Some("TestAlias"))

        val err = evalErr(
          auth,
          s"""|Function.create({
              |  name: "KnowName",
              |  alias: "TestAlias",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the alias `TestAlias`")
      }
      "can't create a UDF with the same name as an existing UDF alias in the same query" in {
        val auth = newAuth

        val err = evalErr(
          auth,
          s"""|
              |Function.create({
              |  name: "KnowName",
              |  alias: "TestAlias",
              |  body: 'x => x'
              |})
              |Function.create({
              |  name: "TestAlias",
              |  body: 'x => x'
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the alias `TestAlias`")
      }
    }
    "update" - {
      "can't update a collection name to conflict with existing udf alias" in {
        val auth = newAuth
        createFunction(auth, "Hello", Some("TestAlias"))

        evalOk(
          auth,
          s"""|Collection.create({
              |  name: "Hello2",
              |})
              |""".stripMargin
        )

        val err = evalErr(
          auth,
          s"""|Hello2.definition.update({
              |  alias: "TestAlias",
              |})
              |""".stripMargin
        )

        err.code shouldEqual "constraint_failure"
        assertMessage(err, "A Function already exists with the alias `TestAlias`")
        (ctx ! Collection.idByNameActive(
          auth.scopeID,
          "Hello2")).isDefined shouldBe true
        (ctx ! Collection.idByAliasActive(auth.scopeID, "Hello")) shouldBe None
      }
    }
  }

  "migration use cases - having previously existing conflicting entities" - {
    "existing conflicting collections can still be updated" in {
      val auth = newAuth
      val existingConflictingCollName = "Collection"

      evalV4Ok(
        auth,
        CreateCollection(MkObject("name" -> existingConflictingCollName)))

      val collId = evalOk(
        auth,
        s"""|Collection.byName("$existingConflictingCollName")!.update({
            |  history_days: 20,
            |})
            |""".stripMargin).asInstanceOf[Value.Doc].id.as[CollectionID]

      val updatedColl = ctx ! Collection.getUncached(auth.scopeID, collId)
      updatedColl.value.active.get.config.historyDuration shouldEqual 20.days
    }
    "existing conflicting collections can be used with an alias" in {
      val auth = newAuth
      val existingConflictingCollName = "Collection"

      evalV4Ok(
        auth,
        CreateCollection(MkObject("name" -> existingConflictingCollName)))

      evalOk(
        auth,
        s"""|Collection.byName("$existingConflictingCollName")!.update({
            |  alias: "NewColl",
            |})
            |""".stripMargin
      ).asInstanceOf[Value.Doc].id.as[CollectionID]

      val collId = evalOk(
        auth,
        s"""|NewColl.definition.update({
            |  history_days: 20,
            |})
            |""".stripMargin).asInstanceOf[Value.Doc].id.as[CollectionID]

      val updatedColl = ctx ! Collection.getUncached(auth.scopeID, collId)
      updatedColl.value.active.get.name shouldEqual "Collection"
      updatedColl.value.active.get.config.historyDuration shouldEqual 20.days
    }
    "existing conflicting udfs can still be updated" in {
      val auth = newAuth
      val existingConflictingUDFName = "Function"

      evalV4Ok(
        auth,
        CreateFunction(
          MkObject(
            "name" -> existingConflictingUDFName,
            "body" -> QueryF(Lambda("x" -> AddF(Var("x"), Var("x"))))
          ))
      )

      val udfID = evalOk(
        auth,
        s"""|Function.byName("$existingConflictingUDFName")!.update({
            |  body: 'x => x',
            |})
            |""".stripMargin).asInstanceOf[Value.Doc].id.as[UserFunctionID]

      val updatedUDF =
        (ctx ! UserFunction.getItemUncached(auth.scopeID, udfID)).get.active.get
      updatedUDF.lambda.asInstanceOf[LambdaWrapper.FQLX].src shouldEqual "x => x"
    }
    "existing conflicting udfs can still be used with alias" in {
      val auth = newAuth
      val existingConflictingUDFName = "Function"

      evalV4Ok(
        auth,
        CreateFunction(
          MkObject(
            "name" -> existingConflictingUDFName,
            "body" -> QueryF(Lambda("x" -> AddF(Var("x"), Var("x"))))
          ))
      )

      evalOk(
        auth,
        s"""|Function.byName("$existingConflictingUDFName")!.update({
            |  alias: 'FuncAlias',
            |  body: 'x => x'
            |})
            |""".stripMargin
      )

      // FIXME: type checking fails on pulling definition document off of a function
      evalOk(
        auth,
        s"""|FuncAlias.definition.update({
            |  body: 'x => x + 1'
            |})
            |""".stripMargin,
        typecheck = false
      )

      val res = evalOk(
        auth,
        s"""|FuncAlias(2)
            |""".stripMargin)

      res.asInstanceOf[Value.Int].value shouldEqual 3
    }
    "existing udf conflicting with a collection name can still be updated" in {
      val auth = newAuth
      val existingConflictingCollName = "TestColl"
      mkColl(auth, existingConflictingCollName)

      evalV4Ok(
        auth,
        CreateFunction(
          MkObject(
            "name" -> existingConflictingCollName,
            "body" -> QueryF(Lambda("x" -> AddF(Var("x"), Var("x"))))
          ))
      )

      val udfID = evalOk(
        auth,
        s"""|Function.byName("$existingConflictingCollName")!.update({
            |  body: 'x => x',
            |})
            |""".stripMargin).asInstanceOf[Value.Doc].id.as[UserFunctionID]

      val updatedUDF =
        (ctx ! UserFunction.getItemUncached(auth.scopeID, udfID)).get.active.get
      updatedUDF.lambda.asInstanceOf[LambdaWrapper.FQLX].src shouldEqual "x => x"
    }
    "All global modules are available under FQL prefix" in {
      globalModules
        .filter(_ != "FQL") foreach { gm =>
        val auth = newAuth
        val res = evalOk(
          auth,
          s"""|FQL.$gm
              |""".stripMargin
        )

        res should not be a[Value.Null]
      }
    }
    "Collection, Function, Role are available under FQL prefix" in {
      val auth = newDB
      mkColl(auth, "TestColl")
      createFunction(auth, "TestFunc")
      val collRes = evalOk(
        auth,
        s"""|FQL.Collection.all().paginate()
            |""".stripMargin)
      val docs =
        (collRes / "data")
          .asInstanceOf[Value.Array]
          .elems
          .asInstanceOf[Seq[Value.Doc]]
      val retrievedCollNames = docs map { doc =>
        (ctx ! Collection.get(auth.scopeID, doc.id.as[CollectionID])).value.name
      }
      retrievedCollNames shouldEqual Seq("TestColl")

      val funcRes = evalOk(
        auth,
        s"""|FQL.Function.all().paginate()
            |""".stripMargin)

      val funcDocs =
        (funcRes / "data")
          .asInstanceOf[Value.Array]
          .elems
          .asInstanceOf[Seq[Value.Doc]]
      val retrievedFuncNames = funcDocs map { doc =>
        (ctx ! UserFunction.get(auth.scopeID, doc.id.as[UserFunctionID])).value.name
      }
      retrievedFuncNames shouldEqual Seq("TestFunc")

      val admin = auth.withPermissions(AdminPermissions)
      val roleDoc = evalOk(
        admin,
        s"""|FQL.Role.create({
            |  name: "aRole",
            |  privileges: {
            |    resource: "TestColl",
            |    actions: {}
            |  }
            |})
            |""".stripMargin
      ).asInstanceOf[Value.Doc]

      val role = (ctx ! Role.get(auth.scopeID, roleDoc.id.as[RoleID])).get
      role.name shouldEqual "aRole"

      val accessProviderDoc = evalOk(
        admin,
        """|FQL.AccessProvider.create({
           |  name: "anAccessProvider",
           |  issuer: "https://fauna2.auth0.com",
           |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
           |  roles: [{
           |    role: "aRole",
           |    predicate: 'arg => true'
           |  }],
           |})
           |""".stripMargin
      ).asInstanceOf[Value.Doc]
      val apId =
        (ctx ! AccessProvider.idByNameActive(auth.scopeID, "anAccessProvider")).get
      apId shouldEqual accessProviderDoc.id.as[AccessProviderID]
    }
  }

  "global name conflict precedence" - {
    "Fauna provided modules take precedence over Collections with the same name" in {
      globalModules foreach { gm =>
        // FIXME: Bytes and UUID aren't currently in the top level v10 environment
        // so if you create an fql4 collection with those
        // names the collection will show up. Once those are available at the top
        // level we should remove this.
        if (gm != "Bytes" && gm != "UUID") {
          val auth = newAuth
          evalV4Ok(auth, CreateCollection(MkObject("name" -> gm)))
          val res = evalOk(
            auth,
            s"""|$gm
                |""".stripMargin
          )
          res should not be a[UserCollectionCompanion]
        }
      }

      val auth = newAuth

      evalV4Ok(auth, CreateCollection(MkObject("name" -> "Collection")))

      mkColl(auth, "TestColl")
      mkColl(auth, "TestColl2")

      val res = evalOk(
        auth,
        s"""|Collection
            |""".stripMargin
      )
      res shouldBe a[CollectionCompanion]
      res should not be a[UserCollectionCompanion]

      val res2 = evalOk(
        auth,
        s"""|Collection.all().paginate()
            |""".stripMargin
      )
      val docs =
        (res2 / "data").asInstanceOf[Value.Array].elems.asInstanceOf[Seq[Value.Doc]]
      val retrievedCollNames = docs map { doc =>
        (ctx ! Collection.get(auth.scopeID, doc.id.as[CollectionID])).value.name
      }

      retrievedCollNames shouldEqual Seq(
        "Collection",
        "TestColl",
        "TestColl2"
      )
    }
    "Fauna provided modules take precedence over UDFs with the same name" in {
      globalModules foreach { gm =>
        // FIXME: Bytes and UUID aren't currently in the top level v10 environment
        // so if you create an fql4 collection with those
        // names the collection will show up. Once those are available at the top
        // level we should remove this.
        if (gm != "Bytes" && gm != "UUID") {
          val auth = newAuth
          evalV4Ok(
            auth,
            CreateFunction(
              MkObject(
                "name" -> gm,
                "body" -> QueryF(Lambda("x" -> AddF(Var("x"), Var("x"))))
              ))
          )
          val res = evalOk(
            auth,
            s"""|$gm
                |""".stripMargin
          )
          res should not be a[FQLXUserFunction]
        }
      }
      val auth = newAuth

      evalV4Ok(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "Function",
            "body" -> QueryF(Lambda("x" -> AddF(Var("x"), Var("x"))))
          ))
      )

      createFunction(auth, "TestFunction")
      createFunction(auth, "TestFunction2")

      val res = evalOk(
        auth,
        s"""|Function
            |""".stripMargin
      )
      res shouldBe a[CollectionCompanion]
      res should not be a[FQLXUserFunction]

      val res2 = evalOk(
        auth,
        s"""|Function.all().paginate()
            |""".stripMargin
      )
      val docs =
        (res2 / "data").asInstanceOf[Value.Array].elems.asInstanceOf[Seq[Value.Doc]]
      val retrievedCollNames = docs map { doc =>
        (ctx ! UserFunction.get(auth.scopeID, doc.id.as[UserFunctionID])).value.name
      }

      retrievedCollNames shouldEqual Seq(
        "Function",
        "TestFunction",
        "TestFunction2"
      )
    }
    "Collection names take precedence over function names" in {
      val auth = newAuth

      evalOk(
        auth,
        s"""|Function.create({
            |  name: "CollFunc",
            |  body: 'x => x'
            |})
            |""".stripMargin
      )
      val res = evalOk(
        auth,
        s"""|CollFunc
            |""".stripMargin
      )
      res shouldBe a[FQLXUserFunction]

      evalV4Ok(auth, CreateCollection(MkObject("name" -> "CollFunc")))

      val res2 = evalOk(
        auth,
        s"""|CollFunc
            |""".stripMargin
      )
      res2 shouldBe a[UserCollectionCompanion]
    }
  }

  "invalid identifiers are disallowed" in {
    val auth = newAuth

    evalOk(auth, "Collection.create({ name: 'foobar' })")
    evalOk(auth, "Collection.create({ name: 'FooBar' })")
    evalOk(auth, "Collection.create({ name: 'Foo3' })")
    evalOk(auth, "Collection.create({ name: 'foo_bar' })")

    def checkInvalidIdent(err: QueryFailure) = {
      err.code shouldBe "constraint_failure"
      err
        .asInstanceOf[QueryRuntimeFailure]
        .constraintFailures shouldBe Seq(
        ConstraintFailure
          .ValidatorFailure(Path(Right("name")), "Invalid identifier."))
    }

    checkInvalidIdent(evalErr(auth, "Collection.create({ name: '' })"))
    checkInvalidIdent(evalErr(auth, "Collection.create({ name: '3' })"))
    checkInvalidIdent(evalErr(auth, "Collection.create({ name: 'foo bar' })"))
    checkInvalidIdent(evalErr(auth, "Collection.create({ name: '$blah' })"))
  }

  "type environment" - {
    "disallows creating of collections that cause type name conflicts with derived types of existing collection" in {
      val existingCollQueries = List(
        """Collection.create({name: "Author"})""",
        """Collection.create({name: "TestColl", alias: "Author"})"""
      )
      val errQueries = Map(
        (
          """Collection.create({name: "AuthorCollection"})""",
          """Invalid database schema update.
             |    error: The collection name `AuthorCollection` generates types that conflict with the following existing types: AuthorCollection
             |    at *query*:1:18
             |      |
             |    1 | Collection.create({name: "AuthorCollection"})
             |      |                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |      |""".stripMargin),
        (
          """Collection.create({name: "TestColl2", alias: "AuthorCollection"})""",
          """Invalid database schema update.
             |    error: The collection alias `AuthorCollection` generates types that conflict with the following existing types: AuthorCollection
             |    at *query*:1:18
             |      |
             |    1 | Collection.create({name: "TestColl2", alias: "AuthorCollection"})
             |      |                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |      |""".stripMargin)
      )
      existingCollQueries.foreach { eq =>
        errQueries.foreach { case (errq, expectedMessage) =>
          val authSet = List(newDB(true), newDB(false))
          authSet.foreach { auth =>
            evalOk(auth, eq)

            val err = evalErr(
              auth,
              errq
            )

            err.code shouldEqual "invalid_schema"
            err.failureMessage shouldEqual expectedMessage
          }
        }
      }
    }
    "disallows conflicts with derived types of a new collection" in {
      val existingCollQueries = List(
        """Collection.create({name: "AuthorCollection"})""",
        """Collection.create({name: "TestColl", alias: "AuthorCollection"})"""
      )
      val errQueries = Map(
        (
          """Collection.create({name: "Author"})""",
          """Invalid database schema update.
             |    error: The collection name `Author` generates types that conflict with the following existing types: AuthorCollection
             |    at *query*:1:18
             |      |
             |    1 | Collection.create({name: "Author"})
             |      |                  ^^^^^^^^^^^^^^^^^^
             |      |""".stripMargin),
        (
          """Collection.create({name: "TestColl2", alias: "Author"})""",
          """Invalid database schema update.
             |    error: The collection alias `Author` generates types that conflict with the following existing types: AuthorCollection
             |    at *query*:1:18
             |      |
             |    1 | Collection.create({name: "TestColl2", alias: "Author"})
             |      |                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |      |""".stripMargin)
      )
      existingCollQueries.foreach { eq =>
        errQueries.foreach { case (errq, expectedMessage) =>
          val authSet = List(newDB(true), newDB(false))
          authSet.foreach { auth =>
            evalOk(auth, eq)

            val err = evalErr(
              auth,
              errq
            )

            err.code shouldEqual "invalid_schema"
            err.failureMessage shouldEqual expectedMessage
          }
        }
      }
    }

    "existing collections can not be updated to conflict with the type environment" in {
      val existingCollQueries = List(
        """Collection.create({name: "Author"})""",
        """Collection.create({name: "TestColl", alias: "Author"})"""
      )
      val errQueries = Map(
        (
          """TestColl2.definition.update({name: "AuthorCollection"})""",
          """Invalid database schema update.
            |    error: The collection name `AuthorCollection` generates types that conflict with the following existing types: AuthorCollection
            |    at *query*:1:28
            |      |
            |    1 | TestColl2.definition.update({name: "AuthorCollection"})
            |      |                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            |      |""".stripMargin),
        (
          """TestColl2.definition.update({alias: "AuthorCollection"})""",
          """Invalid database schema update.
             |    error: The collection alias `AuthorCollection` generates types that conflict with the following existing types: AuthorCollection
             |    at *query*:1:28
             |      |
             |    1 | TestColl2.definition.update({alias: "AuthorCollection"})
             |      |                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |      |""".stripMargin)
      )
      existingCollQueries.foreach { eq =>
        errQueries.foreach { case (errq, expectedMessage) =>
          val authSet = List(newDB(true), newDB(false))
          authSet.foreach { auth =>
            mkColl(auth, "TestColl2")
            evalOk(auth, eq)

            val err = evalErr(
              auth,
              errq
            )

            err.code shouldEqual "invalid_schema"
            err.failureMessage shouldEqual expectedMessage
          }
        }
      }
    }

    "collections that create a conflicting type environment cannot be created in the same query" in {
      val authSet = List(newDB(true), newDB(false))
      authSet.foreach { auth =>
        val err = evalErr(
          auth,
          s"""|
              |Collection.create({ name: "Author" })
              |Collection.create({ name: "AuthorCollection" })
              |""".stripMargin
        )

        err.code shouldEqual "invalid_schema"

        err.failureMessage shouldEqual """Invalid database schema update.
                                         |    error: The collection name `Author` generates types that conflict with the following existing types: AuthorCollection
                                         |    at *query*:2:18
                                         |      |
                                         |    2 | Collection.create({ name: "Author" })
                                         |      |                  ^^^^^^^^^^^^^^^^^^^^
                                         |      |
                                         |    error: The collection name `AuthorCollection` generates types that conflict with the following existing types: AuthorCollection
                                         |    at *query*:3:18
                                         |      |
                                         |    3 | Collection.create({ name: "AuthorCollection" })
                                         |      |                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                         |      |""".stripMargin

      }
    }

    "existing collections that conflict with the type environment can be updated" in {
      val authSet = List(newDB(true), newDB(false))
      authSet.foreach { auth =>
        mkColl(auth, "Author")
        val collID = mkCollLegacy(auth, "AuthorCollection")

        evalOk(
          auth,
          """
            |Collection.byName("AuthorCollection")!.update({
            |  alias: "AuthorColl"
            |})
            |""".stripMargin)

        // ensure schema cache is invalidated
        ctx.cacheContext.invalidateAll()

        val coll = (ctx ! Collection.get(auth.scopeID, collID)).get
        coll.name shouldBe "AuthorCollection"
        coll.alias shouldBe Some("AuthorColl")
      }
    }

    "existing collections that conflict with the type environment can be used when typechecking is disabled" in {
      val auth = newAuth
      mkColl(auth, "Author")
      mkCollLegacy(auth, "AuthorCollection")
      val docID = evalOk(
        auth,
        """
          |AuthorCollection.create({})
          |""".stripMargin).asInstanceOf[Value.Doc].id
      val res = evalOk(
        auth,
        """
          |Author
          |AuthorCollection.all().first()!.id
          |""".stripMargin,
        typecheck = false).asInstanceOf[Value.ID]
      res.value shouldEqual docID.subID.toLong
    }
  }

  private def createFunction(
    auth: Auth,
    name: String,
    aliasOpt: Option[String] = None) = {
    aliasOpt map { alias =>
      evalOk(
        auth,
        s"""|Function.create({
            |  name: "$name",
            |  alias: "$alias",
            |  body: '(x) => "Hello #{x}"'
            |})
            |""".stripMargin
      )
    } getOrElse {
      evalOk(
        auth,
        s"""|Function.create({
            |  name: "$name",
            |  body: '(x) => "Hello #{x}"'
            |})
            |""".stripMargin
      )
    }
  }

  private def createCollectionWithAlias(
    auth: Auth,
    alias: String
  ) = {
    evalOk(
      auth,
      s"""|Collection.create({
          |  name: "CollectionNoMatch7556",
          |  alias: "$alias"
          |})
          |""".stripMargin
    )
  }

  private def mkCollLegacy(auth: Auth, name: String): CollectionID = {
    evalV4Ok(auth, CreateCollection(MkObject("name" -> name)))
      .asInstanceOf[VersionL]
      .version
      .docID
      .as[CollectionID]
  }

  private def assertMessage(validationFailure: QueryFailure, msg: String) = {
    val failureMsg = validationFailure
      .asInstanceOf[QueryRuntimeFailure]
      .constraintFailures
      .map(_.asInstanceOf[ValidatorFailure].msg)
    failureMsg shouldEqual List(
      msg
    )
  }
}
