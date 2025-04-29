package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.prop._
import fauna.prop.api._

trait PaginationTest { self: FQL1APISpec with FQL1QuerySpec =>

  def testPagination(
    name: String,
    action: String,
    refPath: String,
    getRef: JSValue => JSObject) =
    prop(name) {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        count <- Prop.int(1 to 100)
        size <- Prop.int(1 to count)
      } {
        runQuery(CreateIndex(
                   MkObject("name" -> "foo",
                            "source" -> cls.refObj,
                            "active" -> true,
                            "terms" -> JSArray(
                              MkObject("field" -> JSArray("data", "element"))))),
                 db)

        val documents = (0 until count) map { _ =>
          runQuery(CreateF(cls.refObj,
                           MkObject("data" -> MkObject("element" -> "fire"))),
                   db)
        }

        val fire = Match(Ref("indexes/foo"), "fire")
        val refs = documents map getRef
        val history = documents map { inst =>
          JSObject("ts" -> inst.ts, "action" -> action, refPath -> getRef(inst))
        }

        collection(fire, db, size = count) should equal(refs)
        collection(fire, db, size = count, cursor = Before(JSNull)) should equal(
          refs)
        events(fire, db, size = count) should equal(history)
        events(fire, db, size = count, ascending = true) should equal(history)

        val firstPage = JSArray(refs.takeRight(size): _*)
        val lastPage = JSArray(refs.take(size): _*)
        val firstHistoryPage = JSArray(history.take(size): _*)
        val lastHistoryPage = JSArray(history.takeRight(size): _*)

        (runQuery(Paginate(fire, Before(JSNull), size = size), db) / "data") should equal(
          firstPage)
        (runQuery(Paginate(fire, After(JSNull), size = size), db) / "data") should equal(
          JSArray())
        (runQuery(Paginate(fire, Before(0), size = size), db) / "data") should equal(
          JSArray())
        (runQuery(Paginate(fire, After(0), size = size), db) / "data") should equal(
          lastPage)

        (runQuery(Paginate(fire,
                           cursor = Before(MkObject("ts" -> Clock.time.micros)),
                           size = size,
                           events = true),
                  db) / "data") should equal(lastHistoryPage)

        (runQuery(
          Paginate(fire,
                   cursor =
                     Before(MkObject("ts" -> Clock.time.micros, "action" -> action)),
                   size = size,
                   events = true),
          db) / "data") should equal(lastHistoryPage)

        (runQuery(Paginate(fire,
                           cursor = Before(
                             MkObject("ts" -> Clock.time.micros,
                                      "action" -> action,
                                      refPath -> getRef(documents.last))),
                           size = size,
                           events = true),
                  db) / "data") should equal(lastHistoryPage)

        (runQuery(Paginate(fire, Before(Time("now")), size = size, events = true),
                  db) / "data") should equal(lastHistoryPage)

        (runQuery(Paginate(fire, Before(JSNull), size = size, events = true), db) / "data") should equal(
          lastHistoryPage)
        (runQuery(Paginate(fire, After(JSNull), size = size, events = true), db) / "data") should equal(
          JSArray())
        (runQuery(Paginate(fire, Before(0), size = size, events = true), db) / "data") should equal(
          JSArray())
        (runQuery(Paginate(fire, After(0), size = size, events = true), db) / "data") should equal(
          firstHistoryPage)

        // XXX: 2100 seems far enough into the future...
        (runQuery(
          Paginate(fire, Before(DateF("2100-01-01")), size = size, events = true),
          db) / "data") should equal(lastHistoryPage)
      }
    }
}

class ReadFunctions20Spec extends QueryAPI20Spec with PaginationTest {

  def createTestIdx(cls: JSValue, db: Database) =
    runQuery(
      CreateIndex(
        MkObject("name" -> "foo",
                 "source" -> cls,
                 "active" -> true,
                 "terms" -> JSArray(MkObject("field" -> JSArray("data", "foo"))),
                 "values" -> JSArray(MkObject("field" -> JSArray("data", "bar"))))),
      db
    )

  def aMatchSet(db: Database) =
    for {
      cls <- aCollection(db)
    } yield {
      createTestIdx(Ref(cls.ref), db)
      for (i <- 1 to 5)
        runQuery(CreateF(Ref(cls.ref),
                         MkObject("data" -> MkObject("foo" -> 1, "bar" -> i))),
                 db)

      for (i <- 1 to 5)
        runQuery(CreateF(Ref(cls.ref),
                         MkObject("data" -> MkObject("foo" -> 2, "bar" -> i))),
                 db)

      Match(Ref("indexes/foo"), 1)
    }

  "exists" - {
    once(
      "returns true if an document exists at `ts` and the reader has permission to see it.") {
      for {
        db <- aDatabase
        user <- aUser(db)
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        qassert(Exists(Ref(inst.ref)), db)
        qassert(Exists(Ref(inst.ref), Time("now")), db)
        // NB: Read after the collection's MVT.
        val ts = TimeAdd(Time("now"), -10, "seconds")
        qequals(Exists(Ref(inst.ref), ts), JSFalse, db)
        qequals(Exists(Ref(inst.ref)), JSFalse, user)
      }
    }

    once("returns false for invalid ref") {
      for {
        db <- aDatabase
      } {
        qequals(Exists(Ref("classes/foofoo")), JSFalse, db)
        qequals(Exists(Ref("classes/foofoo/123")), JSFalse, db)
        qequals(Exists(Ref("indexes/foofoo")), JSFalse, db)
        qassertErr(Exists(4),
                   "invalid argument",
                   "Ref or Set expected, Integer provided.",
                   JSArray("exists"),
                   db)
      }
    }

    once(
      "in a sub scope, returns true if an document exists at `ts` and the reader has permission to see it.") {
      for {
        db <- aDatabase
        user <- aUser(db)
        subDB <- aDatabase("2.1", db)
        cls <- aCollection(subDB)
        inst <- aDocument(cls)
      } {
        val id = inst.ref.split("/").last.toLong
        val ref = RefV(id, ClsRefV(cls.name, DBRefV(subDB.name)))
        qassert(Exists(ref), db)
        qassert(Exists(ref, Time("now")), db)
        // NB: Read after the collection's MVT.
        val ts = TimeAdd(Time("now"), -10, "seconds")
        qequals(Exists(ref, ts), JSFalse, db)
        qequals(Exists(ref), JSFalse, user)
      }
    }
  }

  "get" - {
    once(
      "returns a document if it exists at `ts`, and the reader has permission to see it.") {
      for {
        db <- aDatabase
        user <- aUser(db)
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        qequals(Select("ref", Get(Ref(inst.ref))), Ref(inst.ref), db)
        qequals(Select("ref", Get(Ref(inst.ref), Time("now"))), Ref(inst.ref), db)
        // NB: Read after the collection's MVT.
        val ts = TimeAdd(Time("now"), -10, "seconds")
        qassertErr(Get(Ref(inst.ref), ts), "instance not found", JSArray(), db)
        qassertErr(Get(Ref(inst.ref)), "permission denied", JSArray(), user)
      }
    }

    once("returns error for invalid ref") {
      for {
        db <- aDatabase
      } {
        qassertErr(Get(Ref("classes/foofoo")),
                   "invalid ref",
                   "Ref refers to undefined class 'foofoo'",
                   JSArray("get"),
                   db)
        qassertErr(Get(Ref("classes/foofoo/123")),
                   "invalid ref",
                   "Ref refers to undefined class 'foofoo'",
                   JSArray("get"),
                   db)
        qassertErr(Get(Ref("foo ref")), "invalid expression", JSArray("get"), db)
      }
    }

    once("returns error for invalid ts") {
      for{
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        qassertErr(Get(Ref(inst.ref), JSArray("blah")),
                   "invalid argument",
                   "Time expected, Array provided.",
                   JSArray("ts"),
                   db)
      }
    }

    once("can read from sub scopes") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.1", db)
        cls <- aCollection(subDB)
        inst <- aDocument(cls)
      } {
        val id = inst.ref.split("/").last.toLong
        val ref = RefV(id, ClsRefV(cls.name, DBRefV(subDB.name)))
        qequals(Select("ref", Get(ref)), ref, db)
        qequals(Select("ref", Get(ref, Time("now"))), ref, db)
      }
    }

    "native" - {
      once("by class name") {
        for {
          rootDB <- aDatabase
          db <- aDatabase("2.0", rootDB)
          childDB <- aDatabase("2.0", db)

          cls <- aCollection(db)
          idx <- anIndex(cls)
          key <- aKey(db, Some(childDB))
        } {

          val classesR = Get(RefV(JSString("classes"), None, Some(DBRefV(db.name))))
          val indexesR = Get(RefV(JSString("indexes"), None, Some(DBRefV(db.name))))
          val dbsR = Get(RefV(JSString("databases"), None, Some(DBRefV(db.name))))
          val keysR = RefV(JSString("keys"), None, Some(DBRefV(db.name)))

          // classes are visible to enclosing scopes but not the scope itself
          (runQuery(classesR, rootDB) / "class") should equal(Ref("classes"))
          (runQuery(classesR, rootDB) / "ref") should equal(
            Ref("classes/" + cls.name))
          qassertErr(classesR, "invalid ref", JSArray("get"), db) //FIXME: position should be ["get", "database"]

          // indexes are visible in outer scope as well as inside one's own scope
          (runQuery(indexesR, rootDB) / "class") should equal(Ref("indexes"))
          (runQuery(indexesR, rootDB) / "ref") should equal(
            Ref("indexes/" + idx.name))
          qassertErr(indexesR, "invalid ref", JSArray("get"), db) //FIXME: position should be ["get", "database"]

          // databases
          // instance key should not have visibility into databases...
          qassertErr(dbsR, "permission denied", JSArray("get"), rootDB.key)
          // ...But the admin key should.
          (runQuery(dbsR, rootDB.adminKey) / "class") should equal(Ref("databases"))
          (runQuery(dbsR, rootDB.adminKey) / "ref") should equal(
            Ref("databases/" + childDB.name))

          // keys
          qassertErr(Get(keysR), "permission denied", JSArray("get"), rootDB.key)
          (runQuery(Get(keysR), rootDB.adminKey) / "class") should equal(Ref("keys"))
          (runQuery(Paginate(keysR), rootDB.adminKey) / "data") should containElem(
            Ref(key.ref))
        }
      }

      prop("with invalid identifiers cannot be accessed") {
        for {
          parentDB <- aDatabase
          db <- aDatabase("2.0", parentDB)
          invalidCls <- Prop.alphaString()
        } {
          val invalidID = Get(RefV(JSString(invalidCls),
                                   None,
                                   Some(DBRefV(db.name)))) //Invalid id; valid scope
          qassertErr(invalidID,
                     "invalid expression",
                     s"Valid native class name expected, '${invalidCls}' provided.",
                     JSArray("get", "id"),
                     parentDB)

          val invalidScope = Get(RefV(
            JSString("classes"),
            None,
            Some(DBRefV(invalidCls)))) //Valid id; invalid scope
          qassertErr(invalidScope,
                     "invalid ref",
                     s"Ref refers to undefined database '${invalidCls}'",
                     JSArray("get"),
                     parentDB) //FIXME: position should be ["get", "database"]
        }
      }
    }
  }

  "get, exists" - {
    once("returns the first element of a set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
                       termProp = Prop.const(Seq(JSArray("data", "foo"))),
                       valueProp = Prop.const(Seq((JSArray("data", "bar"), false))))
        inst1 <- aDocument(cls,
                            dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 2)))
        _ <- aDocument(cls,
                            dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 3)))
      } {
        val set = Match(Ref(idx.ref), 1)
        collection(set, db) should equal(List(2, 3) map { JSLong(_) })
        runQuery(Get(set), db) should equal(inst1)
        runQuery(Exists(set), db) should equal(JSTrue)

        val empty_set = Match(Ref(idx.ref), 0)
        qassertErr(Get(empty_set), "instance not found", JSArray(), db)
      }
    }

    once("returns the first element of a union of sets") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
                       termProp = Prop.const(Seq(JSArray("data", "foo"))),
                       valueProp = Prop.const(Seq((JSArray("data", "bar"), false))))
        inst1 <- aDocument(cls,
                            dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 2)))
        _ <- aDocument(cls,
                            dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 3)))
        _ <- aDocument(cls,
                            dataProp = Prop.const(JSObject("foo" -> 2, "bar" -> 4)))
      } {
        val union = Union(Match(Ref(idx.ref), 1), Match(Ref(idx.ref), 2))
        collection(union, db) should equal(List(2, 3, 4) map { JSLong(_) })
        runQuery(Get(union), db) should equal(inst1)
        runQuery(Exists(union), db) should equal(JSTrue)
      }
    }

    once("get returns minimal instance in given time series") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
                       valueProp = Prop.const(Seq((JSArray("data", "bar"), false))))
        doc <- aDocument(cls,
                            dataProp = Prop.const(JSObject("foo" -> 1, "bar" -> 3)))
      } {
        val m1 = Match(Ref(idx.ref), 1)
        val m2 = Match(Ref(idx.ref), 2)
        val union = Union(m1, m2)
        runQuery(Get(Ref(doc.ref), doc / "ts"), db) should equal(doc)
        runQuery(Get(m1, doc / "ts"), db) should equal(doc)
        runQuery(Get(union), db) should equal(doc)
        runQuery(Get(union, doc / "ts"), db) should equal(doc)
      }
    }
  }

  "key_from_secret" - {
    once("returns a key if it exists in scope") {
      for {
        parent <- aDatabase
        db <- aDatabase(apiVers, parent)
        db2 <- aDatabase(apiVers, db)
        db3 <- aDatabase(apiVers, parent)
        key <- aKey(parent, Some(db))
        key2 <- aKey(db, Some(db2))
        key3 <- aKey(parent, Some(db3))
      } {
        qassert(Equals(KeyFromSecret(key.secret), Get(Ref(key.ref))),
                parent.adminKey)
        qassertErr(KeyFromSecret(key.secret),
                   "instance not found",
                   JSArray(),
                   parent.key)
        qassertErr(KeyFromSecret(key.secret),
                   "instance not found",
                   JSArray(),
                   db.adminKey)
        qassertErr(KeyFromSecret(key.secret),
                   "instance not found",
                   JSArray(),
                   db.key)
        qassertErr(KeyFromSecret("not a secret"),
                   "instance not found",
                   JSArray(),
                   parent.adminKey)

        // can return a key from a lower scope
        val key2ID = key2.ref.split("/")(1).toLong
        val key2Ref = RefV(key2ID, KeysRef, DBRefV(db.name))
        qassert(Equals(KeyFromSecret(key2.secret), Get(key2Ref)), parent.adminKey)
        qassert(Equals(KeyFromSecret(key2.secret), Get(RefV(key2ID, KeysRef))),
                db.adminKey)
        qassert(Equals(KeyFromSecret(key2.secret), Get(Ref(key2.ref))), db.adminKey)

        // cannot step outside scope
        qassertErr(KeyFromSecret(key3.secret),
                   "instance not found",
                   JSArray(),
                   db.key)
      }
    }

    once("returns a token") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        pass <- Prop.string(1, 40)
        _ <- mkCredentials(db, inst, pass)
      } yield {
        // Create a token
        val token = runQuery(Login(inst.refObj, MkObject("password" -> pass)), db.clientKey)
        val secret = token / "secret"

        // Can't retrieve it using client key
        qassertErr(KeyFromSecret(secret), "instance not found", JSArray(), db.clientKey)

        // Can retrieve it using server key
        qassert(Equals(KeyFromSecret(secret), Get(token.refObj)), db.key)
      }
    }
  }

  "paginate" - {
    testPagination("retrieves pages from the set", "create", "resource", { i =>
      Ref(i.ref)
    })

    prop("default cursor respects index shape") {
      val fieldNames = Seq("a", "b", "c", "d", "e")

      val fields = (fieldNames map { k =>
        jsLong map { k -> _ }
      }).sequence map { ts =>
        JSObject((ts :+ ("term" -> JSLong(1))): _*)
      }

      val covered = (fieldNames map { f =>
        Prop.boolean map { JSArray("data", f) -> _ }
      }).sequence flatMap { vs =>
        Prop.int(1 to vs.size) flatMap { num =>
          Prop.shuffle(vs) map { _ take num }
        }
      }

      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "term"))), covered)
        _ <- aDocument(cls, dataProp = fields)
        _ <- aDocument(cls, dataProp = fields)
        _ <- aDocument(cls, dataProp = fields)
        _ <- aDocument(cls, dataProp = fields)
        _ <- aDocument(cls, dataProp = fields)
      } {
        (runQuery(Paginate(Match(Ref(idx.ref), 1), size = 10), db) / "data")
          .as[Seq[JSValue]]
          .size should equal(5)
      }
    }

    once("paginate with cursor object") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, termProp = Prop.const(Nil))
        refs <- (aDocument(cls) * 4) mapT { _.refObj }
      } {
        val set = Match(idx.refObj)
        val p1 = runQuery(Paginate(set, Cursor(NoCursor), size = 2), db)
        val p2 = runQuery(Paginate(set, Cursor(After(p1 / "after")), size = 2), db)
        val p3 = runQuery(Paginate(set, Cursor(Before(p2 / "before")), size = 2), db)

        (p1 / "data").as[Seq[JSValue]] shouldBe refs.take(2)
        (p2 / "data").as[Seq[JSValue]] shouldBe refs.drop(2)
        (p3 / "data") shouldBe (p1 / "data")
      }
    }

    once("fail on invalid cursor object") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Paginate(DatabaseNativeClassRef,
                   Cursor(
                     MkObject(
                       "invalid" -> "cursor"
                     ))),
          "invalid argument",
          JSArray("cursor"),
          db
        )

        qassertErr(
          Paginate(DatabaseNativeClassRef,
                   Cursor(
                     MkObject(
                       "before" -> "something",
                       "more" -> "keys"
                     ))),
          "invalid argument",
          JSArray("cursor"),
          db
        )
      }
    }
  }
}

class ReadFunctions21Spec extends QueryAPI21Spec with PaginationTest {
  "paginate" - {
    testPagination("retrieves pages from the set", "add", "instance", _.refObj)
  }
}

class ReadFunctions3Spec extends QueryAPI3Spec {
}

class ReadFunctions40Spec extends QueryAPI4Spec {

  "first/last" - {
    once("returns the first/last element of a set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, termProp = Prop.const(Seq.empty))
      } {
        val set1 = Documents(cls.refObj)
        val set2 = Match(idx.refObj)
        (1 to 100) map { x =>
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> x))), db)
        }
        val q1 = Select(JSArray("data", "foo"), Get(First(set1)))
        val q2 = Select(JSArray("data", "foo"), Get(First(set2)))
        val q3 = Select(JSArray("data", "foo"), Get(Last(set1)))
        val q4 = Select(JSArray("data", "foo"), Get(Last(set2)))

        runQuery(q1, db) should equal(JSLong(1))
        runQuery(q2, db) should equal(JSLong(1))
        runQuery(q3, db) should equal(JSLong(100))
        runQuery(q4, db) should equal(JSLong(100))
      }
    }

    once("fails on empty set") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, termProp = Prop.const(Seq.empty))
      } {
        val empty_set = Documents(cls.refObj)
        qassertErr(First(empty_set),
                  "invalid argument",
                  "Non-empty set expected.",
                  JSArray("first"),
                  db)
        qassertErr(Last(empty_set),
                  "invalid argument",
                  "Non-empty set expected.",
                  JSArray("last"),
                  db)
        qassertErr(Last(Match(idx.refObj)),
                  "invalid argument",
                  "Non-empty set expected.",
                  JSArray("last"),
                  db)
      }
    }

    once("works with arrays") {
      for {
        db <- aDatabase
      } {
        val array = (0 to 10).toList
        runQuery(First(array), db) should equal(JSLong(0))
        runQuery(Last(array), db) should equal(JSLong(10))
      }
    }

    once("fails on empty array") {
      for {
        db <- aDatabase
      } {
        val empty = JSArray.empty
        qassertErr(First(empty),
                  "invalid argument",
                  "Non-empty array expected.",
                  JSArray("first"),
                  db)
        qassertErr(Last(empty),
                  "invalid argument",
                  "Non-empty array expected.",
                  JSArray("last"),
                  db)
      }
    }

    once("doesn't accept pages") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val page = Paginate(Documents(cls.refObj), size=1)
        qassertErr(First(page),
                  "invalid argument",
                  "Set or Array expected, Page provided.",
                  JSArray("first"), db)
        qassertErr(Last(page),
                  "invalid argument",
                  "Set or Array expected, Page provided.",
                  JSArray("last"), db)
      }
    }

  }
}
