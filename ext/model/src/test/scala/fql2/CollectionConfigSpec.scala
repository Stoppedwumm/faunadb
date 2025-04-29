package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.schema.CollectionConfig
import fauna.model.Collection
import fauna.repo.schema.DataMode
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.Diff

class CollectionConfigSpec extends FQL2Spec {
  var auth: Auth = _
  def scope = auth.scopeID

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  "CollectionConfigSpec" - {
    "lookupIndex flag has no indexes" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  indexes: { bar: { terms: [{ field: '.bar' }] } }
           |})""".stripMargin
      )

      val cfg = {
        val id = (ctx ! Collection.idByNameActive(scope, "Foo")).get
        (ctx ! CollectionConfig.getUncached(scope, id, lookupIndexes = false)).get
      }

      cfg.collIndexes.isEmpty shouldEqual true
      cfg.uniqueIndexes.isEmpty shouldEqual true
    }

    "lookupIndex flag also disallows writes" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'Foo',
           |  indexes: { bar: { terms: [{ field: '.bar' }] } }
           |})""".stripMargin)

      val cfg = {
        val id = (ctx ! Collection.idByNameActive(scope, "Foo")).get
        (ctx ! CollectionConfig.getUncached(scope, id, lookupIndexes = false)).get
      }

      val doc = evalOk(auth, "Foo.create({})").asInstanceOf[Value.Doc]
      val ex = the[IllegalStateException] thrownBy {
        ctx ! Store.externalUpdate(cfg.Schema, doc.id, DataMode.Default, Diff.empty)
      }

      ex.getMessage() should startWith("Invalid collection write")
    }
  }
}
