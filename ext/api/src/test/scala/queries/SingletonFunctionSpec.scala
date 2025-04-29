package fauna.api.test.queries

import fauna.api.test._
import fauna.prop._

class SingletonFunctionSpec extends SetSpec {
  "Singleton" - {
    prop("collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        count <- Prop.int(DefaultRange)
      } {
        // make the history more interesting - this shouldn't have any
        // effect on a collection query
        for (i <- 0 until count) {
          runQuery(Update(inst.refObj, MkObject("data" -> MkObject("i" -> i))), db)
        }

        validateCollection(db, Singleton(inst.refObj), Set(Document.fromJS(inst)))
      }
    }

    prop("history") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        count <- Prop.int(DefaultRange)
      } {
        val set = Singleton(inst.refObj)
        val expected = Seq.newBuilder[Event]

        expected += Event(SetRef(Events(set)), inst.refObj, inst.ts, "add")

        for (i <- 0 until count) {
          // FIXME: mix in some deletes
          runQuery(Update(inst.refObj, MkObject("data" -> MkObject("i" -> i))), db)
        }

        validateEvents(db, Events(set), expected.result())
      }
    }
  }
}
