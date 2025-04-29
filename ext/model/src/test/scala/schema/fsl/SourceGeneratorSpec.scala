package fauna.model.test

import fauna.atoms.{ SchemaSourceID, SchemaVersion }
import fauna.auth.Auth
import fauna.codex.json._
import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure }
import fauna.model.schema.{ InternalCollection, SchemaManager, SchemaSource }
import fauna.model.schema.fsl.{ SourceFile, SourceGenerator }
import fauna.repo.schema.DataMode
import fauna.repo.values.Value
import fauna.repo.Store
import fql.TextUtil
import org.scalactic.source.Position

class SourceGeneratorSpec extends FQL2WithV4Spec {

  var auth: Auth = _

  before {
    val db = newDB
    auth = Auth.adminForScope(db.scopeID)
  }

  "Basic source generation" - {
    "generates FSL for collections and indexes" in {
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: 'User',
            |  alias: 'TheUsers',
            |  history_days: 3,
            |  ttl_days: 1,
            |  indexes: {
            |    byFirstName: {
            |      terms: [ { field: ".firstName" } ]
            |    },
            |    byLastName: {
            |      terms: [ { field: "lastName" } ],
            |      values: [
            |        { field: "firstName", order: "desc", mva: true },
            |        { field: "lastName", mva: true }
            |      ]
            |    },
            |  },
            |  constraints: [
            |    { unique: ["name"] },
            |    { check: { name: "ok", body: "(doc) => doc.ok ?? true" } }
            |  ],
            |  computed_fields: {
            |    one: { body: "_ => 1" },
            |    two: { body: "_ => 2" }
            |  }
            |})""".stripMargin
      )

      val files = deriveOk(
        "main.fsl" ->
          s"""|${SourceGenerator.Preamble}
              |@alias(TheUsers)
              |collection User {
              |  compute one = (_) => 1
              |  compute two = (_) => 2
              |  index byFirstName {
              |    terms [.firstName]
              |  }
              |  index byLastName {
              |    terms [.lastName]
              |    values [desc(mva(.firstName)), mva(.lastName)]
              |  }
              |  unique [.name]
              |  check ok ((doc) => doc.ok ?? true)
              |  history_days 3
              |  ttl_days 1
              |}
              |""".stripMargin
      )

      // Delete and recreate the collection using the generated FSL.
      evalOk(auth, "User.definition.delete()")
      evalErr(auth, """User.create({ no: "nope" })""")

      // Run post-eval hooks to create indexes for the above schema.
      val intp = new FQLInterpreter(auth)
      ctx ! (for {
        _   <- SchemaManager.update(intp, files)
        res <- intp.runPostEvalHooks()
        _ = res.getOrElse(fail())
      } yield ())

      // Check the collection and indexes work.
      evalOk(
        auth,
        """User.create({ firstName: "Sam", lastName: "I Am", name: "Sam I Am"})""")
      evalOk(
        auth,
        """User.create({ firstName: "Sam", lastName: "I Ain't", name: "Sam I Ain't"})""")
      evalOk(
        auth,
        """User.create({ firstName: "Pam", lastName: "I Am", name: "Pam I Am"})""")

      evalErr(
        auth,
        """|User.create({
           |  firstName: "Sam",
           |  middleName: "Says",
           |  lastName: "I Am",
           |  name: "Sam I Am"})""".stripMargin
      ) should matchPattern {
        case QueryRuntimeFailure(
              "constraint_failure",
              "Failed unique constraint.",
              _,
              _,
              _,
              _) =>
      }

      evalOk(auth, """User.byFirstName("Sam").count()""") shouldEqual Value.Int(2)
    }

    "generates FSL for FQL10 functions" in {
      evalOk(auth, "Function.create({ name: 'AddTwo', body: 'x => x + 2' })")
      evalOk(
        auth,
        """|Function.create({
           |  name: 'Variadic',
           |  body: '(x, ...y) => x + y.length',
           |  signature: '(Number,...Array<Any>) => Number'
           |})""".stripMargin
      )
      evalOk(
        auth,
        """|Function.create({
           |  name: 'MegaMath',
           |  body: 'x => {
           |    let y = 2 * x
           |    let z = y - x
           |    let w = x * (z - x) - (2 * x - y)
           |    w * z * y + x
           |  }',
           |  role: 'server',
           |  alias: 'MM',
           |  signature: 'Number => Number'
           |})""".stripMargin
      )

      // Ensure the generator skips generating legacy JSON.
      evalV4Ok(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "Incr",
            "body" -> QueryF(Lambda("x" -> AddF(Var("x"), 1))))))

      val files = deriveOk(
        "main.fsl" ->
          s"""|${SourceGenerator.Preamble}
              |function AddTwo(x) {
              |  x + 2
              |}
              |
              |function Variadic(x: Number, ...y: Array<Any>): Number {
              |  x + y.length
              |}
              |
              |@alias(MM)
              |@role(server)
              |function MegaMath(x: Number): Number {
              |  let y = 2 * x
              |  let z = y - x
              |  let w = x * (z - x) - (2 * x - y)
              |  w * z * y + x
              |}
              |""".stripMargin
      )

      evalOk(auth, "AddTwo.definition.delete()")
      evalOk(auth, "Variadic.definition.delete()")
      evalOk(auth, "MegaMath.definition.delete()")

      val intrp = new FQLInterpreter(auth)
      ctx ! SchemaManager.update(intrp, files)

      evalOk(auth, "AddTwo(1)") shouldEqual Value.Number(3)
      evalOk(auth, "Variadic(2, 0, 0)") shouldEqual Value.Number(4)
      evalOk(auth, "MegaMath(1)") shouldEqual Value.Number(1)
    }

    "generates FSL for roles" in {
      evalOk(auth, "Collection.create({ name: 'Users' })")
      evalOk(auth, "Collection.create({ name: 'Orders' })")
      evalOk(auth, "Collection.create({ name: 'Products' })")

      evalOk(
        auth,
        """|Role.create({
           |  name: "AdminUsers",
           |  membership: [
           |    {
           |      resource: "Users",
           |      predicate: 'user => user.isAdmin'
           |    },
           |    {
           |      resource: "Orders"
           |    }
           |  ],
           |  privileges: [{
           |    resource: "Users",
           |    actions: {
           |      create: true,
           |      read: 'user => user.canRead'
           |    }
           |  },
           |  {
           |    resource: "Orders",
           |    actions: {
           |      read: 'order => order.foo'
           |    }
           |  },
           |  {
           |    resource: "Products",
           |    actions: {
           |      read: true,
           |      write: true
           |    }
           |  }]
           |})""".stripMargin
      )

      // Ensure the generator skips generating legacy JSON.
      evalV4Ok(
        auth,
        CreateRole(
          MkObject(
            "name" -> "role_a",
            "membership" -> MkObject(
              "resource" -> ClassRef("Users")
            ),
            "privileges" -> MkObject(
              "resource" -> ClassRef("Orders"),
              "actions" -> MkObject("read" -> true)
            )
          ))
      )

      val files = ctx ! SourceGenerator.deriveSchema(auth.scopeID)
      files should have size 1 // No legacy.json.

      // There's no processing for roles right now. Manual check!
      deriveOk(
        "main.fsl" ->
          s"""|${SourceGenerator.Preamble}
              |collection Users {
              |}
              |
              |collection Orders {
              |}
              |
              |collection Products {
              |}
              |
              |role AdminUsers {
              |  privileges Users {
              |    create
              |    read {
              |      predicate ((user) => user.canRead)
              |    }
              |  }
              |  privileges Orders {
              |    read {
              |      predicate ((order) => order.foo)
              |    }
              |  }
              |  privileges Products {
              |    read
              |    write
              |  }
              |  membership Users {
              |    predicate ((user) => user.isAdmin)
              |  }
              |  membership Orders
              |}
              |""".stripMargin
      )
    }

    "generates FSL for access providers" in {
      evalOk(
        auth,
        "Role.create({ name: 'MyRole' })"
      )

      evalOk(
        auth,
        """|AccessProvider.create({
           |  name: "MyAPNoRole",
           |  issuer: "https://fauna.auth0.com",
           |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json"
           |})
           |""".stripMargin
      )

      evalOk(
        auth,
        """|AccessProvider.create({
           |  name: "MyAP",
           |  issuer: "https://fauna0.auth0.com",
           |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
           |  roles: {
           |    role: "MyRole",
           |    predicate: 'arg => true'
           |  },
           |})
           |""".stripMargin
      )

      // Ensure the generator skips legacy.json.
      evalV4Ok(
        auth,
        CreateAccessProvider(
          MkObject(
            "name" -> "YourAP",
            "issuer" -> "https://fauna1.auth0.com",
            "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
            "roles" -> Seq(
              MkObject(
                "role" -> "MyRole",
                "predicate" -> QueryF(Lambda("x" -> true))
              )
            )
          )
        )
      )

      deriveOk(
        "main.fsl" ->
          s"""|${SourceGenerator.Preamble}
              |role MyRole {
              |}
              |
              |access provider MyAPNoRole {
              |  issuer "https://fauna.auth0.com"
              |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
              |}
              |
              |access provider MyAP {
              |  issuer "https://fauna0.auth0.com"
              |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
              |  role MyRole {
              |    predicate ((arg) => true)
              |  }
              |}
              |""".stripMargin
      )
    }

    "quote legacy schema names" in {
      evalV4Ok(auth, CreateCollection(MkObject("name" -> "0")))
      evalV4Ok(auth, CreateRole(MkObject("name" -> "0")))
      evalV4Ok(
        auth,
        CreateAccessProvider(
          MkObject(
            "name" -> "0",
            "issuer" -> "https://fauna.auth0.com",
            "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
            "roles" -> JSArray("0")
          ))
      )

      deriveOk(
        "main.fsl" ->
          s"""|${SourceGenerator.Preamble}
              |collection "0" {
              |}
              |
              |role "0" {
              |}
              |
              |access provider "0" {
              |  issuer "https://fauna.auth0.com"
              |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
              |  role "0"
              |}
              |""".stripMargin
      )
    }
  }

  "generation with existing data field" - {
    "doesn't clobber data" in {
      evalOk(
        auth,
        s"""|Collection.create({
            |  name: 'OK',
            |  data: { a : 0 }
            |})""".stripMargin
      )
      evalOk(auth, "OK.definition.data!.a") shouldEqual Value.Int(0)

      // Sneak in a corrupt schema file.
      val srcs = ctx ! SchemaSource.getStaged(auth.scopeID)
      val invalid = srcs.head.file.append("*&^)*&^(*&^%(*@&#^%@#$@#$")

      ctx ! InternalCollection.SchemaSource(auth.scopeID).flatMap { config =>
        Store.replace(
          config.Schema,
          srcs.head.id.toDocID,
          DataMode.Default,
          invalid.toData
        )
      }

      // Re-retrieving the files will trigger source regeneration, zapping the bad
      // data.
      ctx ! SchemaSource.getStaged(auth.scopeID)

      // The data should still be on the collection.
      evalOk(auth, "OK.definition.data!.a") shouldEqual Value.Int(0)
    }
  }

  "patch existing source files" - {
    "creates a new main.fsl file with new schema items" in {
      val res = evalRes(auth, "Collection.create({ name: 'Bar' })")
      val file =
        SchemaSource(
          auth.scopeID,
          SchemaSourceID(0),
          SchemaVersion(res.ts),
          SourceFile.FSL(
            "foo.fsl",
            "collection Bar {}"
          )
        )

      evalOk(auth, "Collection.create({ name: 'Foo' })") // new schema item

      patchOk(file)(
        "main.fsl" ->
          s"""|${SourceGenerator.Preamble}
              |collection Foo {
              |}
              |""".stripMargin
      )
    }

    "modifies existing source files" in {
      val res =
        evalRes(
          auth,
          s"""|Collection.create({ name: 'Foo' })
              |Collection.create({ name: 'Bar' })
              |Collection.create({ name: 'Baz' })
              |""".stripMargin
        )

      val file =
        SchemaSource(
          auth.scopeID,
          SchemaSourceID(0),
          SchemaVersion(res.ts), // prev version
          SourceFile.FSL(
            "foo.fsl",
            """|collection Foo {}
               |collection Bar {}
               |collection Baz {}""".stripMargin
          )
        )

      evalOk(auth, "Bar.definition.update({ history_days: 10 })") // schema update

      patchOk(file)(
        "foo.fsl" ->
          """|collection Foo {}
             |collection Bar {
             |  history_days 10
             |}
             |collection Baz {}""".stripMargin
      )
    }

    "drops removed items" in {
      val res =
        evalRes(
          auth,
          """|Collection.create({ name: 'Foo' })
             |Collection.create({ name: 'Bar' })
             |Collection.create({ name: 'Baz' })
             |""".stripMargin
        )

      val file =
        SchemaSource(
          auth.scopeID,
          SchemaSourceID(0),
          SchemaVersion(res.ts), // prev version
          SourceFile.FSL(
            "foo.fsl",
            """|collection Foo {
               |}
               |collection Bar {
               |}
               |collection Baz {
               |}""".stripMargin
          )
        )

      evalOk(auth, "Bar.definition.delete()") // schema item removal

      patchOk(file)(
        "foo.fsl" ->
          """|collection Foo {
             |}
             |collection Baz {
             |}""".stripMargin
      )
    }

    "drops, and modifies items" in {
      val res =
        evalRes(
          auth,
          """|Collection.create({ name: 'Foo' })
             |Collection.create({ name: 'Bar' })
             |""".stripMargin
        )

      val file =
        SchemaSource(
          auth.scopeID,
          SchemaSourceID(0),
          SchemaVersion(res.ts), // prev version
          SourceFile.FSL(
            "foo.fsl",
            """|collection Foo {}
               |collection Bar {}""".stripMargin
          )
        )

      // Originally, patching processed deletes first, invalidating spans, so
      // subsequent modifications could cause string index out-of-bounds exceptions.
      evalOk(auth, "Foo.definition.delete()")
      evalOk(auth, "Bar.definition.update({ history_days: 10 })")

      patchOk(file)(
        "foo.fsl" ->
          """|
             |collection Bar {
             |  history_days 10
             |}""".stripMargin
      )
    }

    "regenerate schema when a file doesn't parse" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection ComputedFieldColl {
             |}""".stripMargin
      )

      // This was allowed in the past, so write it without any validation.
      val invalid = (
        """|collection ComputedFieldColl {
           |  unique []
           |}""".stripMargin
      )

      ctx ! InternalCollection
        .SchemaSource(auth.scopeID)
        .replace(
          SchemaSourceID(0),
          DataMode.Default,
          SourceFile.FSL("main.fsl", invalid).toData)

      // Even though the previous schema was invalid, we should still be able to
      // generate and replace it with the correct schema.
      evalOk(
        auth,
        """|Collection.byName('ComputedFieldColl')!.update({
           |  indexes: {
           |    byName: {
           |      terms: [{ field: '.name' }]
           |    }
           |  }
           |})""".stripMargin
      )

      schemaContent(auth, "main.fsl") shouldBe Some(
        """|// The following schema is auto-generated.
           |// This file contains FSL for FQL10-compatible schema items.
           |
           |collection ComputedFieldColl {
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |""".stripMargin
      )
    }
  }

  private def deriveOk(expected: (String, String)*)(implicit pos: Position) = {
    val files = ctx ! SourceGenerator.deriveSchema(auth.scopeID)
    assertPatchedFiles(files, expected.toSeq)
    files
  }

  private def patchOk(files: SchemaSource*)(expected: (String, String)*)(
    implicit pos: Position) = {
    val patched = ctx ! SourceGenerator.patchSchema(auth.scopeID, files.toSeq)
    assertPatchedFiles(patched, expected.toSeq)
  }

  private def assertPatchedFiles(
    patched: Seq[SourceFile.FSL],
    expected: Seq[(String, String)])(implicit pos: Position) = {

    patched foreach { file =>
      assert(file.parse().isOk, s"failed to parse ${file.filename}")
    }

    val patchedMap = patched.map { file => file.filename -> file.content }.toMap
    val expectedMap = expected.toMap

    expectedMap foreachEntry { (filename, expectedContent) =>
      val actualContent = patchedMap(filename)
      if (actualContent != expectedContent) {
        info(
          s"""|
              |** Expected:
              |$expectedContent
              |
              |** Actual:
              |$actualContent
              |
              |**Diff
              |${TextUtil.diff(actualContent, expectedContent)}
              |""".stripMargin
        )
        fail(s"$filename is different than expected")
      }
    }
  }
}
