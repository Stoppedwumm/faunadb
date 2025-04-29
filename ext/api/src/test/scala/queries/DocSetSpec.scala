package fauna.api.test.queries

import fauna.api.test._
import fauna.prop.Prop

class DocSetSpec extends SetSpec {

  prop("collection") {
    for {
      db <- aDatabase
      cls <- aCollection(db)
      inst <- aDocument(cls)
      count <- Prop.int(DefaultRange)
    } {
      for (i <- 0 until count) {
        // FIXME: mix in some deletes
        runQuery(Update(inst.refObj, MkObject("data" -> MkObject("i" -> i))), db)
      }

      validateCollection(db, inst.refObj, Seq(Document.fromJS(inst)))
    }
  }

  prop("history") {
    for {
      db <- aDatabase
      cls <- aCollection(db)
      inst <- aDocument(cls)
      count <- Prop.int(DefaultRange)
    } {
      val set = inst.refObj
      val expected = Seq.newBuilder[Event]

      expected += Event(SetRef(Events(set)), inst.refObj, inst.ts, "create")

      for (i <- 0 until count) {
        // FIXME: mix in some deletes
        val res = runQuery(Update(inst.refObj, MkObject("data" -> MkObject("i" -> i))), db)

        expected += Event(SetRef(Events(set)), inst.refObj, res.ts, "update")
      }

      validateEvents(db, Events(set), expected.result(), false)
    }
  }
}
