package fauna.model.test

import fauna.atoms.{ DatabaseID, DocID, SchemaSourceID }
import fauna.auth.{ AdminPermissions, Auth }
import fauna.logging.ExceptionLogging
import fauna.model.{ Collection, Database }
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.schema.{ InternalCollection, SchemaError, SchemaManager }
import fauna.model.schema.fsl.SourceFile
import fauna.model.schema.index.CollectionIndex
import fauna.repo.schema.DataMode
import fauna.repo.values.Value
import fauna.repo.Store

class SchemaManagerSpec extends FQL2WithV4Spec {
  var auth: Auth = _

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  val idFn = "@alias(TheID) @role(server) function id(x) { x }"
  val doubleFn = "function double(x) { x * 2 }"

  val myRole = "role MyRole {}"
  val myRolev2 =
    s"""|collection MyCol {}
        |collection YourCol {}
        |
        |role MyRole {
        |  privileges MyCol {
        |    read {
        |      predicate ((x) => true)
        |    }
        |    create_with_id
        |  }
        |
        |  membership YourCol
        |}""".stripMargin

  val myAP =
    s"""|access provider MyAP {
        |  issuer "https://fauna0.auth0.com"
        |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
        |}""".stripMargin
  val myAPNoPredicate =
    s"""|role MyRole {}
        |
        |access provider MyAP {
        |  issuer "https://fauna0.auth0.com"
        |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
        |  role MyRole
        |}""".stripMargin
  val myAPv2 =
    s"""|role MyRole {}
        |
        |access provider MyAP {
        |  issuer "https://fauna0.auth0.com"
        |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
        |  role MyRole {
        |    predicate ((x) => x.foo)
        |  }
        |}""".stripMargin

  val myCol = """|@alias(TheCol)
                 |collection MyCol {}""".stripMargin
  val myColv2 =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [.foo]
        |  }
        |
        |  unique [.bar]
        |  history_days 5
        |  ttl_days 1
        |}""".stripMargin

  val myIndex =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [.foo]
        |    values [asc(.title)]
        |  }
        |}""".stripMargin

  val indexDescMva =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [mva(.foo)]
        |    values [desc(mva(.title))]
        |  }
        |}""".stripMargin

  val indexAscMva =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [mva(.foo)]
        |    values [asc(mva(.title))]
        |  }
        |}""".stripMargin

  val indexMvaAsc =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [mva(.foo)]
        |    values [mva(asc(.title))]
        |  }
        |}""".stripMargin

  val indexMvaDesc =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [mva(.foo)]
        |    values [mva(desc(.title))]
        |  }
        |}""".stripMargin

  val indexMVAOnly =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [mva(.foo), mva(.author)]
        |    values [mva(.title), mva(.category)]
        |  }
        |}""".stripMargin

  val indexNoOrderOrMVA =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [.foo, .author]
        |    values [.title, .category]
        |  }
        |}""".stripMargin

  val indexDescOrder =
    s"""|collection MyCol {
        |  index byFoo {
        |    terms [.foo]
        |    values [desc(.title), .category]
        |  }
        |}""".stripMargin

  val uniqueMVA =
    s"""|collection MyCol {
        |  unique [.title, mva(.category)]
        |}""".stripMargin

  val computedFields =
    s"""|collection MyCol {
        |  compute bar: String = (doc) => doc.foo + "o"
        |  compute baaz: String = doc => doc.bar + "s"
        |}""".stripMargin

  val checkConstrained =
    s"""|collection MyCol {
        |  index byOK { terms [.ok] }
        |  check a ((doc) => doc.ok)
        |  check b ((_) => true)
        |}""".stripMargin

  "SchemaManager" - {

    "validate" - {
      "fails validation for tenant root" in {
        val containerAuth = newCustomerTenantRoot(auth)

        val errRes = ctx ! SchemaManager.validate(
          containerAuth.scopeID,
          Seq(SourceFile("foo.fsl", SourceFile.Ext.FSL, "")))
        errRes.isErr shouldBe true
        errRes.errOption.get shouldEqual List(
          SchemaError.Validation("Schema can not be created in your account root.")
        )
      }
    }
    "files" - {
      "empty files are removed" in {
        updateSchemaOk(auth, "foo.fsl" -> idFn)
        updateSchemaOk(auth, "foo.fsl" -> "")
        schemaContent(auth, "foo.fsl") shouldBe empty
      }

      "allow to override broken fsl" in {
        updateSchemaOk(auth, "main.fsl" -> idFn)
        // Break the main.fsl
        ctx ! InternalCollection.SchemaSource(auth.scopeID).flatMap { config =>
          Store.replace(
            config.Schema,
            SchemaSourceID.MinValue.toDocID,
            DataMode.Default,
            SourceFile.FSL("main.fsl", s"$idFn!broken!").toData
          )
        }
        schemaContent(auth, "main.fsl").value shouldBe s"$idFn!broken!"
        updateSchemaOk(auth, "main.fsl" -> idFn) // fix it!
        schemaContent(auth, "main.fsl").value shouldBe idFn
      }
    }

    "functions" - {

      "creates a new function on an existing file" in {
        updateSchemaOk(auth, "main.fsl" -> idFn)
        schemaContent(auth, "main.fsl").value shouldEqual idFn
        evalOk(auth, "id(10)") shouldBe Value.Int(10)
        evalOk(auth, "id.definition.role!") shouldBe Value.Str("server")
        evalOk(auth, "id.definition.alias!") shouldBe Value.Str("TheID")
      }

      "creates a new function on a new file" in {
        updateSchemaOk(auth, "foo.fsl" -> idFn)
        schemaContent(auth, "foo.fsl").value shouldEqual idFn
        evalOk(auth, "id(10)") shouldBe Value.Int(10)
      }

      "moves a function from one file to another" in {
        updateSchemaOk(
          auth,
          "main.fsl" -> s"""|$idFn
                            |$doubleFn
                            |""".stripMargin
        )

        updateSchemaOk(
          auth,
          "main.fsl" -> idFn,
          "foo.fsl" -> doubleFn
        )

        schemaContent(auth, "main.fsl").value shouldEqual idFn
        schemaContent(auth, "foo.fsl").value shouldEqual doubleFn
        evalOk(auth, "id(10)") shouldBe Value.Int(10)
        evalOk(auth, "double(2)") shouldBe Value.Int(4)
      }

      "modifies a function" in {
        updateSchemaOk(auth, "main.fsl" -> doubleFn)
        updateSchemaOk(auth, "main.fsl" -> "function double(x) { 4 * x }")
        schemaContent(
          auth,
          "main.fsl").value shouldEqual "function double(x) { 4 * x }"
        evalOk(auth, "double(2)") shouldBe Value.Int(8)
      }

      "modifies a function while moving it" in {
        updateSchemaOk(
          auth,
          "main.fsl" -> s"""|$idFn
                            |$doubleFn
                            |""".stripMargin
        )

        updateSchemaOk(
          auth,
          "main.fsl" -> idFn,
          "foo.fsl" -> "function double(x) { 4 * x }"
        )

        schemaContent(auth, "main.fsl").value shouldEqual idFn
        schemaContent(
          auth,
          "foo.fsl").value shouldEqual "function double(x) { 4 * x }"
        evalOk(auth, "double(2)") shouldBe Value.Int(8)
      }

      "removes a function" in {
        updateSchemaOk(
          auth,
          "main.fsl" -> s"""|$idFn
                            |$doubleFn
                            |""".stripMargin
        )

        updateSchemaOk(auth, "main.fsl" -> idFn)
        schemaContent(auth, "main.fsl").value shouldEqual idFn
        evalErr(auth, "double(2)")
      }

      "supports variadic functions" in {
        updateSchemaOk(
          auth,
          "main.fsl" -> "function numargs(x, ...rest) { x + rest.length }")
        evalOk(auth, "numargs(1)") shouldBe Value.Int(1)
        evalOk(auth, "numargs(2, 0)") shouldBe Value.Int(3)
        evalOk(auth, "numargs(3, 0, 0)") shouldBe Value.Int(5)
      }

      "stores the user signature" in {
        updateSchemaOk(
          auth,
          "main.fsl" -> """|function add(x: Number, y: Number): Number { x + y }
                           |function alsoadd(x, y) { x + y }""".stripMargin
        )

        evalOk(auth, "add(1, 1)") shouldEqual Value.Int(2)
        evalOk(auth, "alsoadd(1, 1)") shouldEqual Value.Int(2)

        evalOk(auth, "add.definition.signature") shouldEqual Value.Str(
          "(x: Number, y: Number) => Number")
        evalOk(auth, "alsoadd.definition.signature") should matchPattern {
          case Value.Null(_) =>
        }
      }
    }

    "roles" - {
      "creates a new role" in {
        updateSchemaOk(auth, "main.fsl" -> myRole)
        schemaContent(auth, "main.fsl").value shouldEqual myRole
        evalOk(auth, "Role.byName('MyRole')!")
      }

      "modifies a role" in {
        updateSchemaOk(auth, "main.fsl" -> myRole)
        updateSchemaOk(auth, "main.fsl" -> myRolev2)
        schemaContent(auth, "main.fsl").value shouldEqual myRolev2
        evalOk(auth, "Role.byName('MyRole')!")
        evalOk(
          auth,
          "Role.byName('MyRole')!.privileges[0].actions.create_with_id") shouldBe
          Value.True
        evalOk(auth, "Role.byName('MyRole')!.membership[0].resource") shouldBe
          Value.Str("YourCol")
      }

      "removes a role" in {
        updateSchemaOk(auth, "main.fsl" -> myRolev2)
        updateSchemaOk(auth, "main.fsl" -> idFn)
        schemaContent(auth, "main.fsl").value shouldEqual idFn
        evalErr(auth, "Role.byName('MyRole')!")
      }

      "add predicates correctly when removing privileges" in {
        val fsl =
          """|collection User {}
             |collection Product {}
             |
             |role AuthenticatedRole {
             |  privileges User {
             |    read
             |  }
             |  privileges Product {
             |    write
             |  }
             |}""".stripMargin

        updateSchemaOk(auth, "main.fsl" -> fsl)

        val fql =
          """|Role.byName("AuthenticatedRole")!.update({
             |  privileges: [
             |    {
             |      resource: "Product",
             |      actions: {
             |        write: "(oldData, newData) => Query.identity() == oldData.owner && oldData.owner == newData.owner",
             |      }
             |    }
             |  ]
             |})""".stripMargin

        evalOk(auth, fql)

        schemaContent(auth, "main.fsl").value shouldEqual (
          """|collection User {}
             |collection Product {}
             |
             |role AuthenticatedRole {
             |  privileges Product {
             |    write {
             |      predicate ((oldData, newData) => Query.identity() == oldData.owner && oldData.owner == newData.owner)
             |    }
             |  }
             |}""".stripMargin
        )
      }
    }

    "access providers" - {
      "creates a new AP" in {
        updateSchemaOk(auth, "main.fsl" -> myAP)
        schemaContent(auth, "main.fsl").value shouldEqual myAP
        evalOk(auth, "AccessProvider.byName('MyAP')!")
      }

      "modifies an AP" in {
        updateSchemaOk(auth, "main.fsl" -> myAP)
        updateSchemaOk(auth, "main.fsl" -> myAPv2)
        schemaContent(auth, "main.fsl").value shouldEqual myAPv2
        evalOk(auth, "AccessProvider.byName('MyAP')!")
        evalOk(auth, "AccessProvider.byName('MyAP')!.roles[0].predicate") shouldBe
          Value.Str("(x) => x.foo")
      }

      "removes an AP" in {
        updateSchemaOk(auth, "main.fsl" -> myAPv2)
        updateSchemaOk(auth, "main.fsl" -> idFn)
        schemaContent(auth, "main.fsl").value shouldEqual idFn
        evalErr(auth, "AccessProvider.byName('MyAP')!")
      }

      "allows no predicate for roles" in {
        updateSchemaOk(auth, "main.fsl" -> myAPNoPredicate)
        schemaContent(auth, "main.fsl").value shouldEqual myAPNoPredicate
        evalOk(auth, "AccessProvider.byName('MyAP')!")
        evalOk(auth, "AccessProvider.byName('MyAP')!.roles[0]") shouldBe
          Value.Str("MyRole")
      }

      "handles funky unicode characters" in {
        evalOk(
          auth,
          s"""|AccessProvider.create({
          |  name: "Foo",
          |  issuer: "https://dev-foo\\u200b.us.auth0.com/",
          |  jwks_uri: "https://foo\\u{10fffe}.us.auth0.com/.well-known/jwks.json"
          |})""".stripMargin
        )

        val fsl = schemaContent(auth, "main.fsl").get
        fsl shouldBe (
          s"""|// The following schema is auto-generated.
          |// This file contains FSL for FQL10-compatible schema items.
          |
          |access provider Foo {
          |  issuer "https://dev-foo\\u200b.us.auth0.com/"
          |  jwks_uri "https://foo\\u{10fffe}.us.auth0.com/.well-known/jwks.json"
          |}
          |""".stripMargin
        )

        // No-op update to check the generated FSL parses.
        updateSchemaOk(auth, "main.fsl" -> fsl)
      }
    }

    "collections" - {
      "creates a new collection" in {
        updateSchemaOk(auth, "main.fsl" -> myCol)
        schemaContent(auth, "main.fsl").value shouldEqual myCol
        evalOk(auth, "MyCol.create({ foo: 0 })")
        evalOk(auth, "MyCol.definition.alias!") shouldBe Value.Str("TheCol")
      }

      "modifies a collection" in {
        updateSchemaOk(auth, "main.fsl" -> myCol)
        schemaContent(auth, "main.fsl").value shouldEqual myCol
        evalOk(auth, "MyCol.create({ foo: 0, bar: 0 })")
        updateSchemaOk(auth, "main.fsl" -> myColv2)
        schemaContent(auth, "main.fsl").value shouldEqual myColv2
        evalOk(auth, "MyCol.definition.history_days!") shouldBe Value.Long(5)
        evalOk(auth, "MyCol.definition.ttl_days!") shouldBe Value.Long(1)
        evalOk(auth, "MyCol.byFoo(0)!")
        evalErr(auth, "MyCol.create({ bar: 0 })")
      }

      "correctly creates indexes" in {
        updateSchemaOk(auth, "main.fsl" -> myIndex)
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual Vector(
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".foo"),
            mvaOpt = Some(false)
          )
        )
        res.collIndexes.head.values shouldEqual Vector(
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".title"),
            ascending = true,
            mvaOpt = Some(false)
          )
        )
      }

      "correctly creates indexes with desc mva" in {
        updateSchemaOk(auth, "main.fsl" -> indexDescMva)
        val expectedTerms = Vector(
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".foo"),
            mvaOpt = Some(true)
          )
        )
        val expectValues = Vector(
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".title"),
            ascending = false,
            mvaOpt = Some(true)
          )
        )
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual expectedTerms
        res.collIndexes.head.values shouldEqual expectValues

        updateSchemaOk(auth, "main.fsl" -> indexMvaDesc)
        val res2 = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res2.collIndexes.size shouldEqual 1
        res2.collIndexes.head.terms shouldEqual expectedTerms
        res2.collIndexes.head.values shouldEqual expectValues
      }

      "correctly creates indexes with asc mva" in {
        updateSchemaOk(auth, "main.fsl" -> indexAscMva)
        val expectedTerms = Vector(
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".foo"),
            mvaOpt = Some(true)
          )
        )
        val expectedValues = Vector(
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".title"),
            ascending = true,
            mvaOpt = Some(true)
          )
        )
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual expectedTerms
        res.collIndexes.head.values shouldEqual expectedValues

        updateSchemaOk(auth, "main.fsl" -> indexMvaAsc)
        val res2 = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res2.collIndexes.size shouldEqual 1
        res2.collIndexes.head.terms shouldEqual expectedTerms
        res2.collIndexes.head.values shouldEqual expectedValues
      }

      "correctly creates indexes that only set mva" in {
        updateSchemaOk(auth, "main.fsl" -> indexMVAOnly)
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual Vector(
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".foo"),
            mvaOpt = Some(true)
          ),
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".author"),
            mvaOpt = Some(true)
          )
        )
        res.collIndexes.head.values shouldEqual Vector(
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".title"),
            ascending = true,
            mvaOpt = Some(true)
          ),
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".category"),
            ascending = true,
            mvaOpt = Some(true)
          )
        )
      }

      "correctly creates indexes that don't specify order or mva" in {
        updateSchemaOk(auth, "main.fsl" -> indexNoOrderOrMVA)
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual Vector(
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".foo"),
            mvaOpt = Some(false)
          ),
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".author"),
            mvaOpt = Some(false)
          )
        )
        res.collIndexes.head.values shouldEqual Vector(
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".title"),
            ascending = true,
            mvaOpt = Some(false)
          ),
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".category"),
            ascending = true,
            mvaOpt = Some(false)
          )
        )
      }

      "correctly creates indexes that only specify order as desc" in {
        updateSchemaOk(auth, "main.fsl" -> indexDescOrder)
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual Vector(
          CollectionIndex.Term(
            field = CollectionIndex.Field.Fixed(".foo"),
            mvaOpt = Some(false)
          )
        )
        res.collIndexes.head.values shouldEqual Vector(
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".title"),
            ascending = false,
            mvaOpt = Some(false)
          ),
          CollectionIndex.Value(
            field = CollectionIndex.Field.Fixed(".category"),
            ascending = true,
            mvaOpt = Some(false)
          )
        )
      }

      "correctly creates unique constraints that specify mva" in {
        updateSchemaOk(auth, "main.fsl" -> uniqueMVA)
        val res = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        res.collIndexes.size shouldEqual 1
        res.collIndexes.head.terms shouldEqual Vector(
          CollectionIndex.Term(CollectionIndex.Field.Fixed(".title"), Some(false)),
          CollectionIndex.Term(CollectionIndex.Field.Fixed(".category"), Some(true))
        )
        res.collIndexes.head.values shouldBe empty
      }

      "removes a collection" in {
        updateSchemaOk(auth, "main.fsl" -> myCol)
        schemaContent(auth, "main.fsl").value shouldEqual myCol
        updateSchemaOk(auth, "main.fsl" -> idFn)
        schemaContent(auth, "main.fsl").value shouldEqual idFn
        evalErr(auth, "MyCol.create({ foo: 0 })")
      }

      "correctly create computed fields" in {
        updateSchemaOk(auth, "main.fsl" -> computedFields)
        evalOk(
          auth,
          "let d = MyCol.create({ foo: 'tac' }); d.baaz") shouldEqual Value.Str(
          "tacos")
      }

      "correctly creates check constraints" in {
        // NB: See FQL2CollectionSpec for thorough tests.
        updateSchemaOk(auth, "main.fsl" -> checkConstrained)
        val col = (ctx ! Collection.getAll(auth.scopeID).flattenT).head
        col.config.checkConstraints.size shouldEqual 2
        col.config.checkConstraints(0).name shouldEqual "a"
        col.config.checkConstraints(0).body shouldEqual "(doc) => doc.ok"
        col.config.checkConstraints(1).name shouldEqual "b"
        col.config.checkConstraints(1).body shouldEqual "(_) => true"

        evalOk(auth, "MyCol.create({ ok: true })")
        val err0 = evalErr(auth, "MyCol.create({ ok: false })")
        err0 should matchPattern {
          case QueryRuntimeFailure("constraint_failure", _, _, _, _, _) => // OK.
        }
        val err1 = evalErr(auth, "MyCol.create({ ok: 1 })")
        err1 should matchPattern {
          case QueryRuntimeFailure("constraint_failure", _, _, _, _, _) => // OK.
        }

        evalOk(auth, "MyCol.byOK(true).isEmpty()") shouldEqual Value.False
        evalOk(auth, "MyCol.byOK(false).isEmpty()") shouldEqual Value.True
        evalOk(auth, "MyCol.byOK(1).isEmpty()") shouldEqual Value.True
      }
    }

    "data" - {
      "doesn't get clobbered on update" in {
        evalOk(auth, "Collection.create({ name: 'OK', data: { a: 0 } })")
        evalOk(auth, "OK.definition.data!.a") shouldEqual Value.Int(0)
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|collection OK {
               |  index byStatus {
               |    terms [.status]
               |  }
               |}""".stripMargin
        )
        evalOk(auth, "OK.definition.data!.a") shouldEqual Value.Int(0)
      }
    }

    "conflicting collections and functions" in {
      ExceptionLogging.alwaysSquelch {
        evalOk(auth, "Function.create({ name: 'foo', body: 'x => x' })")

        evalV4Ok(auth, CreateCollection(MkObject("name" -> "foo")))

        schemaContent(auth, "main.fsl") shouldBe Some(
          """|// WARNING: the following auto-generated items contain invalid FSL syntax.
             |// Please fix its syntax and re-submit the file. Contact support if you need
             |// assistance via https://support.fauna.com.
             |
             |
             |// The following schema is auto-generated.
             |// This file contains FSL for FQL10-compatible schema items.
             |@alias(fooFunction)
             |function foo(x) {
             |  x
             |}
             |
             |collection foo {
             |}
             |""".stripMargin
        )
      }
    }

    "conflicting collections and functions when there is a second conflicting function" in {
      ExceptionLogging.alwaysSquelch {
        evalOk(auth, "Function.create({ name: 'foo', body: 'x => x' })")
        evalOk(auth, "Function.create({ name: 'fooFunction', body: 'x => x' })")

        evalV4Ok(auth, CreateCollection(MkObject("name" -> "foo")))

        schemaContent(auth, "main.fsl") shouldBe Some(
          """|// WARNING: the following auto-generated items contain invalid FSL syntax.
             |// Please fix its syntax and re-submit the file. Contact support if you need
             |// assistance via https://support.fauna.com.
             |
             |
             |// The following schema is auto-generated.
             |// This file contains FSL for FQL10-compatible schema items.
             |@alias(fooFunctionFunction)
             |function foo(x) {
             |  x
             |}
             |
             |function fooFunction(x) {
             |  x
             |}
             |
             |collection foo {
             |}
             |""".stripMargin
        )
      }
    }

    "conflicting collections and functions when there is a second conflicting collection" in {
      ExceptionLogging.alwaysSquelch {
        evalOk(auth, "Function.create({ name: 'foo', body: 'x => x' })")
        evalOk(auth, "Collection.create({ name: 'fooFunction' })")

        evalV4Ok(auth, CreateCollection(MkObject("name" -> "foo")))

        schemaContent(auth, "main.fsl") shouldBe Some(
          """|// WARNING: the following auto-generated items contain invalid FSL syntax.
             |// Please fix its syntax and re-submit the file. Contact support if you need
             |// assistance via https://support.fauna.com.
             |
             |
             |// The following schema is auto-generated.
             |// This file contains FSL for FQL10-compatible schema items.
             |@alias(fooFunctionFunction)
             |function foo(x) {
             |  x
             |}
             |
             |collection fooFunction {
             |}
             |
             |collection foo {
             |}
             |""".stripMargin
        )
      }
    }

    "protected mode" - {
      "forbids destructive changes" in {
        val db = {
          val doc =
            evalOk(auth, "Database.create({ name: 'protected', protected: true })")
          val id = doc.as[DocID].as[DatabaseID]
          (ctx ! Database.getUncached(auth.scopeID, id)).get
        }
        val pAuth = Auth.forScope(db.scopeID)

        updateSchemaOk(
          pAuth,
          "main.fsl" ->
            """|collection Foo {
               |  history_days 30
               |  index byX {
               |    terms [.x]
               |  }
               |  index byY {
               |    terms [.y]
               |  }
               |}""".stripMargin
        )

        // FQL10.
        // Can't delete the collection.
        renderErr(pAuth, "Foo.definition.delete()") shouldBe (
          """|error: Failed to delete Collection `Foo`.
             |constraint failures:
             |  Cannot delete collection: destructive change forbidden because database is in protected mode.
             |at *query*:1:22
             |  |
             |1 | Foo.definition.delete()
             |  |                      ^^
             |  |""".stripMargin
        )

        // Can't decrease history_days...
        renderErr(pAuth, "Foo.definition.update({ history_days: 15 })") shouldBe (
          """|error: Failed to update Collection `Foo`.
             |constraint failures:
             |  Cannot decrease `history_days` field: destructive change forbidden because database is in protected mode.
             |at *query*:1:22
             |  |
             |1 | Foo.definition.update({ history_days: 15 })
             |  |                      ^^^^^^^^^^^^^^^^^^^^^^
             |  |""".stripMargin
        )
        // ... but can increase it.
        evalOk(pAuth, "Foo.definition.update({ history_days: 60 })")

        // Can't delete an index because it causes a backing index to be deleted.
        renderErr(
          pAuth,
          "Foo.definition.update({ indexes: { byX: null } })") shouldBe (
          """|error: Invalid database schema update.
             |
             |constraint failures:
             |  Cannot cause deletion of backing index: destructive change forbidden because database is in protected mode.
             |at *query*:1:22
             |  |
             |1 | Foo.definition.update({ indexes: { byX: null } })
             |  |                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |  |""".stripMargin
        )

        // FSL.
        // Can't delete the collection.
        updateSchemaErr(pAuth, "main.fsl" -> "") shouldBe (
          """|error: Failed to delete Collection `Foo`.
             |constraint failures:
             |  Cannot delete collection: destructive change forbidden because database is in protected mode.
             |at main.fsl:1:1
             |  |
             |1 |
             |  | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
             |  |""".stripMargin
        )

        // Can't decrease history_days.
        updateSchemaErr(
          pAuth,
          "main.fsl" ->
            """|collection Foo {
               |  history_days 15
               |  index byX {
               |    terms [.x]
               |  }
               |  index byY {
               |    terms [.y]
               |  }
               |}""".stripMargin
        ) shouldBe (
          """|error: Failed to update Collection `Foo`.
             |constraint failures:
             |  Cannot decrease `history_days` field: destructive change forbidden because database is in protected mode.
             |at main.fsl:1:1
             |  |
             |1 |   collection Foo {
             |  |  _^
             |2 | |   history_days 15
             |3 | |   index byX {
             |4 | |     terms [.x]
             |5 | |   }
             |6 | |   index byY {
             |7 | |     terms [.y]
             |8 | |   }
             |9 | | }
             |  | |_^
             |  |""".stripMargin
        )

        // Can't delete an index because it causes a backing index to be deleted.
        updateSchemaErr(
          pAuth,
          "main.fsl" ->
            """|collection Foo {
               |  history_days 60
               |  index byX {
               |    terms [.x]
               |  }
               |}""".stripMargin
        ) shouldBe (
          """|error: Invalid database schema update.
             |
             |constraint failures:
             |  Cannot cause deletion of backing index: destructive change forbidden because database is in protected mode.
             |at main.fsl:1:1
             |  |
             |1 |   collection Foo {
             |  |  _^
             |2 | |   history_days 60
             |3 | |   index byX {
             |4 | |     terms [.x]
             |5 | |   }
             |6 | | }
             |  | |_^
             |  |""".stripMargin
        )
      }
    }
  }

  "handles updates to a single file when there are multiple files" - {
    "items from all files are included in validation" in {
      updateSchemaOk(
        auth,
        "main.fsl" -> "collection Customer {}",
        "function.fsl" -> "function mkCustomer() { Customer.create({}) }"
      )

      // This basically simulates the single item endpoint.
      updateSchemaNoOverride(
        auth,
        "function.fsl" -> "function mkCustomer() { Customer.create({ x: 0 }) }"
      ).isErr shouldBe false

      evalOk(auth, "mkCustomer().x") shouldBe Value.Number(0)
    }

    "items deleted from a file aren't included in the environment" in {
      updateSchemaOk(
        auth,
        "main.fsl" -> "collection Customer {}; collection Product {}",
        "function.fsl" -> "function mkCustomer() { Customer.create({}) }"
      )

      updateSchemaErr(
        auth,
        "main.fsl" -> "collection Customer {}",
        "function.fsl" -> "function mkCustomer() { Product.create({ x: 0 }) }"
      ) shouldBe (
        """|error: Unbound variable `Product`
           |at function.fsl:1:25
           |  |
           |1 | function mkCustomer() { Product.create({ x: 0 }) }
           |  |                         ^^^^^^^
           |  |""".stripMargin
      )
    }

    "items from a deleted file aren't included in the environment" in {
      updateSchemaOk(
        auth,
        "main.fsl" -> "collection Customer {}",
        "function.fsl" -> "function mkCustomer() { Customer.create({}) }"
      )

      updateSchemaErr(
        auth,
        "function.fsl" -> "function mkCustomer() { Customer.create({}) }"
      ) shouldBe (
        """|error: Unbound variable `Customer`
           |at function.fsl:1:25
           |  |
           |1 | function mkCustomer() { Customer.create({}) }
           |  |                         ^^^^^^^^
           |  |""".stripMargin
      )
    }
  }
}
