package fauna.model.test

import fauna.auth.AdminPermissions
import fauna.model.{ Collection, RuntimeEnv }
import scala.concurrent.duration._

class RuntimeEnvSpec extends FQL2Spec {

  "RuntimeEnvSpec" - {
    "Default" - {
      "returns cached collections" in {
        // retry until success since cache is inherently racy
        eventually(timeout(30.seconds), interval(1.millis)) {
          val auth = newDB.withPermissions(AdminPermissions)

          evalOk(auth, "Collection.create({ name: 'Foo', document_ttls: false })")
          val id = (ctx ! Collection.idByNameActive(auth.scopeID, "Foo")).get
          val cfg = (ctx ! RuntimeEnv.Default.getCollection(auth.scopeID, id)).get

          // use doc ttls to probe state
          cfg.documentTTLs shouldEqual false

          // update the collection
          evalOk(auth, "Collection.byName('Foo')!.update({ document_ttls: true })")
          val cfg2 = (ctx ! RuntimeEnv.Default.getCollection(auth.scopeID, id)).get

          cfg2.documentTTLs shouldEqual false
        }
      }
    }

    "Uncached" - {
      "returns uncached collections" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Foo', document_ttls: false })")
        val id = (ctx ! Collection.idByNameActive(auth.scopeID, "Foo")).get
        val cfg =
          (ctx ! RuntimeEnv.InlineIndexEnv.getCollection(auth.scopeID, id)).get

        // use doc ttls to probe state
        cfg.documentTTLs shouldEqual false

        // update the collection
        evalOk(auth, "Collection.byName('Foo')!.update({ document_ttls: true })")
        val cfg2 =
          (ctx ! RuntimeEnv.InlineIndexEnv.getCollection(auth.scopeID, id)).get

        cfg2.documentTTLs shouldEqual true
      }
    }

    "Static" - {
      "errors for user collections" in {
        val auth = newDB.withPermissions(AdminPermissions)

        evalOk(auth, "Collection.create({ name: 'Foo', document_ttls: false })")
        val id = (ctx ! Collection.idByNameActive(auth.scopeID, "Foo")).get

        val ex = the[IllegalArgumentException] thrownBy {
          (ctx ! RuntimeEnv.Static.getCollection(auth.scopeID, id)).get
        }

        ex.getMessage should include("Invalid NativeCollectionID")
      }
    }
  }
}
