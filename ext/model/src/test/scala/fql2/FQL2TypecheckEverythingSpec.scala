package fauna.model.test

import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model.schema.SchemaCollection
import fauna.model.schema.TypecheckEverything
import fauna.model.Database
import fauna.repo.store.CacheStore
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

// This tests the specific validation in `TypecheckEverything`. General
// typechecking tests should be placed in `FQL2TypecheckSpec` or `TyperCoreSpec`.
class FQL2TypecheckEverythingSpec extends FQL2Spec {
  var auth: Auth = _

  before {
    auth = newDB
  }

  def check(): Seq[String] = {
    val messages = new ConcurrentLinkedQueue[String]()
    val checker = new TypecheckEverything()

    ctx.runSynchronously(
      checker.check(auth.scopeID) { msg => messages.add(msg) },
      10.seconds)

    messages.asScala.toSeq
  }

  def setTypechecked(enabled: Boolean) = {
    val db = ctx ! Database.forScope(auth.scopeID).map(_.get)
    val auth0 = Auth.adminForScope(db.parentScopeID)

    evalOk(
      auth0,
      s"Database.byName('${db.name}')!.update({ typechecked: $enabled })")

    ctx ! CacheStore.invalidateScope(auth.scopeID)
  }

  "it works" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
           |
           |function foo(a: Number): Number { a + 2 }
           |""".stripMargin
    )

    check() shouldBe Seq()
  }

  "it finds invalid databases" in {
    setTypechecked(false)

    updateSchemaOk(auth, "main.fsl" -> "function foo(a: Int): Int { a.fooo }")

    // Re-enable typechecking, without validating schema.
    setTypechecked(true)

    check() shouldBe Seq(
      """|Unexpected error:
         |error: Invalid database schema update.
         |    error: Type `Int` does not have field `fooo`
         |    at main.fsl:5:5
         |      |
         |    5 |   a.fooo
         |      |     ^^^^
         |      |
         |    hint: Type `Int` inferred here
         |    at main.fsl:4:17
         |      |
         |    4 | function foo(a: Int): Int {
         |      |                 ^^^
         |      |
         |at <no source>""".stripMargin
    )
  }

  "it finds orphaned indexes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |""".stripMargin
    )

    // Wipe out all the collections, so that the index is orphaned.
    ctx ! SchemaCollection.Collection(auth.scopeID).allIDs().flattenT.flatMap {
      colls =>
        colls.map { coll =>
          SchemaCollection.Collection(auth.scopeID).internalDelete(coll)
        }.sequence
    }

    check() shouldBe Seq(
      "Index IndexID(32768) is orphaned (no collections have it as a backing index)."
    )
  }
}
