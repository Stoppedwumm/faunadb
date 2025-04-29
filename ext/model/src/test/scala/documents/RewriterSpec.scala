package fauna.model.test

import fauna.auth._
import fauna.ast._
import fauna.lang.clocks._
import fauna.model._
import fauna.repo.cassandra.CassandraService
import fauna.repo.test.CassandraHelper

/**
  * This test generates a data set for testing fauna.tools.Rewriter.
  * It should be run alone, like: sbt
  *    "model/testOnly fauna.model.test.RewriterSpec"
  *
  * See fauna.tools.test.RewriterSpec for how the output of this test
  * is used.
  */
class RewriterSpec extends Spec {
  import SocialHelpersV4._

  val ctx = CassandraHelper.context("model")

  // Replace by `"generate" in` to enable this test.
  registerIgnoredTest("generate") {
    def generate(i: Int) = {
      val parentQ = runQuery(RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> s"parent-$i",
            "container" -> true))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val parent = ctx ! parentQ
      val auth = Auth.adminForScope(parent.data(Database.ScopeField))

      val childQ = runQuery(auth,
        Clock.time,
        CreateDatabase(MkObject("name" -> s"child-$i"))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val child = ctx ! childQ

      println(parent)
      println(child)

      val admin = Auth.adminForScope(child.data(Database.ScopeField))

      ctx ! mkCollection(admin, MkObject("name" -> "documents"))

      val docQ = runQuery(admin,
        Clock.time,
        CreateF(ClassRef("documents"),
          MkObject("data" ->
            MkObject("id" -> 1)))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val doc = ctx ! docQ
      val ref = RefV(doc.id.subID.toLong, ClsRefV("documents"))

      ctx ! runQuery(admin,
        Clock.time,
        Update(ref,
          MkObject("data" ->
            MkObject("id" -> 2))))

      ctx ! runQuery(admin, Clock.time, DeleteF(ref))

      ctx ! runQuery(admin, Clock.time,
        CreateAccessProvider(
          MkObject(
            "name" -> "provider",
            "issuer" -> "issuer",
            "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json")))
    }


    generate(0)
    println("-------------")
    generate(1)
    println("-------------")
    generate(2)

    Thread.sleep(5000)
    CassandraService.instance.storage.sync()
  }
}
