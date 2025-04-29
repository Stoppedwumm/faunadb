package fauna.api.test.queries

import fauna.api.test._

class DocumentsFunctionSpec extends SetSpec {

  "Documents" - {

    prop("collection") {
      for {
        db <- aDatabase
        (cls, _, docs) <- collP(db)
      } validateCollection(db, Documents(cls.refObj), docs)
    }

    prop("historical") {
      for {
        db <- aDatabase
        (cls, _, events) <- eventsP(db)
      } validateEvents(db, Events(Documents(cls.refObj)), events)
    }

    once("returns a reusable set ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        ins <- aDocument(cls)
      } {
        val set = runQuery(Documents(cls.refObj), db)
        val refs = collection(set, db)
        refs should contain only ins.refObj
      }
    }

    once("set ref can be stored") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val set = runQuery(Documents(coll.refObj), db)
        val doc = runQuery(CreateF(coll.refObj, MkObject("data" -> set)), db)
        qassert(Equals(doc / "data", Documents(coll.refObj)), db)
      }
    }
  }
}
