package fauna.model.test

import fauna.atoms.ScopeID
import fauna.auth.Auth
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.schema.{
  InternalCollection,
  Result,
  SchemaError,
  SchemaManager,
  SchemaSource
}
import fauna.model.schema.fsl.{ SourceFile, SourceGenerator }
import fauna.repo.schema.DataMode
import fauna.repo.Store
import fql.ast._

class SchemaSourceSpec extends FQL2Spec {
  import Result.Err

  var auth: Auth = _
  var scope: ScopeID = _

  before {
    auth = newDB
    scope = auth.scopeID
  }

  "SchemaSource" - {
    "getOrInit initializes the schema" in {
      val sources = ctx ! SchemaSource.getStaged(scope)
      sources should have size 0

      evalOk(auth, "Collection.create({ name: 'Foo' })")
      val sources0 = ctx ! SchemaSource.getStaged(scope)
      sources0 should have size 1

      val file = sources0.head.file
      file.filename shouldBe SourceFile.Builtin.Main.filename
      file.content shouldBe
        s"""|${SourceGenerator.Preamble}
            |collection Foo {
            |}
            |""".stripMargin
    }

    "getOrCreate an empty builtin" in {
      (ctx ! SchemaSource.getOrCreate(
        scope,
        SourceFile.Builtin.Main
      )).content shouldBe empty

      evalOk(auth, "Collection.create({ name: 'Foo' })") // lands in main.fsl
      val main = ctx ! SchemaSource.getOrCreate(scope, SourceFile.Builtin.Main)
      main.content should not be empty // preserve existing content
    }

    "get by id" in {
      evalOk(auth, "Collection.create({ name: 'Foo' })")
      val sources = ctx ! SchemaSource.getStaged(scope)
      val source = ctx ! SchemaSource.get(scope, sources.head.id)
      source.value.id shouldEqual sources.head.id
    }

    "get by filename" in {
      evalOk(auth, "Collection.create({ name: 'Foo' })") // lands in main.fsl
      val source = ctx ! SchemaSource.get(scope, SourceFile.Builtin.Main.filename)
      source.value.filename shouldBe SourceFile.Builtin.Main.filename
    }

    "locate item by kind and name" in {
      val fsl = SourceFile.FSL(
        "main.fsl",
        """|function foo(x) {x}
           |function bar(x) {x*2}
           |""".stripMargin
      )
      val intrp = new FQLInterpreter(auth)
      ctx ! SchemaManager.update(intrp, Seq(fsl))

      val itemRes =
        ctx ! SchemaSource.locateFSLItem(scope, SchemaItem.Kind.Function, "bar")

      val (_, file, span) = itemRes.getOrElse(fail()).value
      span.extract(file.content) shouldBe "function bar(x) {x*2}"
    }

    "fail gracefully on broken schema" in {
      evalOk(auth, "Collection.create({ name: 'Foo' })")
      evalOk(auth, "Collection.create({ name: 'Baz' })")

      // Generate main.fsl
      val sources = ctx ! SchemaSource.getStaged(scope)

      // Break main.fsl
      ctx ! InternalCollection.SchemaSource(scope).flatMap { config =>
        Store.replace(
          config.Schema,
          sources.head.id.toDocID,
          DataMode.Default,
          SourceFile
            .empty(SourceFile.Builtin.Main)
            .append(
              """|collection Foo {}
                 |cccollecctiionn Baz {}
                 |""".stripMargin
            )
            .toData
        )
      }

      val foo =
        ctx ! SchemaSource.locateFSLItem(
          scope,
          SchemaItem.Kind.Collection,
          "Foo"
        )

      val (_, file, span) = foo.getOrElse(fail()).value
      span.extract(file.content) shouldEqual "collection Foo {}"

      val bar =
        ctx ! SchemaSource.locateFSLItem(
          scope,
          SchemaItem.Kind.Collection,
          "Baz"
        )

      bar should matchPattern { case Err(SchemaError.InvalidStoredSchema :: Nil) => }
    }

    "patch schema on FQL updates" in {
      val fsl = SourceFile.FSL(
        "functions.fsl",
        """|function foo(x) {x}
           |function bar(x) {x*2}
           |""".stripMargin
      )
      val intrp = new FQLInterpreter(auth)
      ctx ! SchemaManager.update(intrp, Seq(fsl))

      // Schema update via FQL
      evalOk(auth, "Function.create({ name: 'id', body: '(x) => x' })")
      evalOk(auth, "foo.definition.update({ body: '(x) => x*x' })")
      evalOk(auth, "bar.definition.delete()")

      (ctx ! SchemaSource.getStaged(scope)) should have size 2

      val functions = ctx ! SchemaSource.get(scope, "functions.fsl")
      functions.value.content shouldEqual
        s"""|function foo(x) {
            |  x * x
            |}
            |""".stripMargin

      val main = ctx ! SchemaSource.get(scope, SourceFile.Builtin.Main.filename)
      main.value.content shouldEqual
        s"""|${SourceGenerator.Preamble}
            |function id(x) {
            |  x
            |}
            |""".stripMargin
    }

    "handle server-readonly role for functions" in {
      evalOk(
        auth,
        "Function.create({ name: 'id', role: 'server-readonly', body: '(x) => x' })")

      val itemRes =
        ctx ! SchemaSource.locateFSLItem(scope, SchemaItem.Kind.Function, "id")

      val (_, file, span) = itemRes.getOrElse(fail()).value
      span.extract(file.content) shouldBe
        """|@role("server-readonly")
           |function id(x) {
           |  x
           |}""".stripMargin
    }
  }
}
