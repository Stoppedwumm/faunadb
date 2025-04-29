package fauna.model.test

import fauna.auth.Auth
import fauna.model.runtime.fql2.{ FQLInterpreter, GlobalContext }
import fauna.model.runtime.fql2.stdlib.Global
import fauna.model.schema.fsl.SourceFile
import fauna.model.schema.SchemaSource
import fql.ast.display._
import fql.env.DatabaseSchema
import fql.env.SchemaTypeValidator
import fql.env.TypeEnv
import fql.env.TypecheckedEnvironment
import fql.typer.Type
import fql.Result
import fql.TextUtil

// This test validates the schema environment and the runtime environment produce
// consistent results.
class FQL2SchemaEnvSpec extends FQL2Spec {
  var auth: Auth = _
  before {
    auth = newDB
  }

  // Creates a typer with the standard library.
  def stdlib = Global.StaticEnv.newTyper()

  def lookupEnv: TypeEnv = {
    val intp = new FQLInterpreter(auth)
    (ctx ! GlobalContext.completeEnv(intp)).typeEnv
  }

  def schemaEnv: TypecheckedEnvironment = {
    val intp = new FQLInterpreter(auth)

    def validateEnv0(files: Seq[SourceFile.FSL], stdlib: TypeEnv) = {
      val items = files.flatMap { file =>
        file.parse() match {
          case Result.Ok(it) => it
          case Result.Err(_) => Seq.empty
        }
      }

      // There are no v4 items in these unit tests.
      val schema =
        DatabaseSchema.fromItems(items, v4Funcs = Seq.empty, v4Roles = Seq.empty)

      SchemaTypeValidator.validate(stdlib, schema, isEnvTypechecked = true)
    }

    ctx ! (for {
      files <- SchemaSource.stagedFSLFiles(auth.scopeID)
      // This needs to be filtered based on feature flags.
      stdlib <- GlobalContext.filterDisabledTypeMembers(intp, Global.StaticEnv)
      res = validateEnv0(files, stdlib)
    } yield res match {
      case fql.Result.Ok(env) => env
      case fql.Result.Err(e) =>
        println(e)
        throw new IllegalStateException("Schema validation failed")
    })
  }

  def checkEnv() = {
    val model = lookupEnv
    val schema = schemaEnv.environment

    def envStr(env: TypeEnv) = {
      val sb = new StringBuilder
      sb ++= "globals\n"
      env.globalTypes.toSeq.sortBy(_._1).foreach { case (k, v) =>
        sb ++= "  "
        sb ++= k
        sb ++= " -> "
        sb ++= v.toString
        sb ++= "\n"
      }
      sb ++= "shapes\n"
      env.typeShapes.toSeq.sortBy(_._1).foreach { case (k, v) =>
        sb ++= "  "
        sb ++= k
        sb ++= " -> TypeShape(\n"

        sb ++= "    self: "
        sb ++= v.self.toString
        sb ++= "\n"

        sb ++= "    fields:\n"
        v.fields.toSeq.sortBy(_._1).foreach { case (k, v) =>
          sb ++= "      "
          sb ++= k
          sb ++= " -> "
          sb ++= v.toString
          sb ++= "\n"
        }
        sb ++= "\n"
      }
      sb.toString
    }

    if (model != schema) {
      val diff = TextUtil.diff(envStr(model), envStr(schema))
      fail(s"model and schema are different. diff:\n${diff}")
    }

    schema
  }

  "produces the same env" in {
    evalOk(
      auth,
      """|Collection.create({
         |  name: "Author",
         |  alias: "Author2",
         |  indexes: {
         |    byName: { terms: [{ field: "fullName" }] }
         |  },
         |  fields: {
         |    firstName: { signature: "String" },
         |    lastName: { signature: "String" },
         |  },
         |  computed_fields: {
         |    fullName: {
         |      body: "doc => doc.firstName + ' ' + doc.lastName",
         |    }
         |  }
         |})
         |
         |Function.create({
         |  name: "TimesTwo",
         |  body: "(x) => x * 2",
         |})""".stripMargin
    )

    val env = checkEnv()

    // `checkEnv()` already validates that the schema-based and runtime-based env
    // produce the same results, so below is just for clarity.

    env
      .globalTypes("Author")
      .raw
      .asInstanceOf[Type]
      .display shouldBe "AuthorCollection"

    env
      .globalTypes("Author2")
      .raw
      .asInstanceOf[Type]
      .display shouldBe "Author2Collection"

    env
      .typeShapes("Author")
      .alias
      .get
      .raw
      .asInstanceOf[Type]
      .display shouldBe (
      "{ id: ID, ts: Time, ttl: Time | Null, firstName: String, lastName: String, fullName: String }"
    )

    env
      .typeShapes("AuthorCollection")
      .fields("byName")
      .raw
      .asInstanceOf[Type]
      .display shouldBe (
      "((term1: String) => Set<Author>) & ((term1: String, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Author>)"
    )

    env.globalTypes("TimesTwo").raw.asInstanceOf[Type].display shouldBe (
      "UserFunction<Number => Number>"
    )
  }
}
