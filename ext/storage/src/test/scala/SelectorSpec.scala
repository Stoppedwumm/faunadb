package fauna.storage.test

import fauna.atoms._
import fauna.lang.syntax._
import fauna.prop.Prop
import fauna.snowflake.IDSource
import fauna.storage._

class SelectorSpec extends Spec {
  test("handles the root scope") {
    for (id <- 0 until Short.MaxValue) {
      val coll = CollectionID(id)
      val selector =
        Selector.from(ScopeID.RootID, Vector(coll))

      val doc = DocID(SubID(Prop.long.sample), coll)
      val key = Tables.Versions.rowKeyByteBuf(ScopeID.RootID, doc)

      withClue(s"key=${key.toHexString} doc=$doc") {
        selector.keep(Tables.Versions.CFName, key) should be(true)
      }
    }
  }

  test("handles collections in user scopes") {
    val scope = ScopeID(new IDSource(() => 512).getID)
    for (id <- 0 until Short.MaxValue) {
      val coll = CollectionID(id)
      val selector =
        Selector.from(scope, Vector(coll))

      val doc = DocID(SubID(Prop.long.sample), coll)
      val key = Tables.Versions.rowKeyByteBuf(scope, doc)

      withClue(s"key=${key.toHexString} doc=$doc") {
        selector.keep(Tables.Versions.CFName, key) should be(true)
      }
    }
  }
}
