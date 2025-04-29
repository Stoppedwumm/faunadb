package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.model.Index
import fauna.prop.api.{ Collection, Database }
import fauna.prop.Prop
import fauna.util.Base64
import scala.concurrent.duration._

class IndexSpec extends QueryAPI21Spec {

  "create/get works" - {
    once("creates an index") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val idx = runQuery(
          CreateIndex(
            MkObject(
              "name" -> "spells",
              "source" -> cls.refObj,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "element"))),
              "values" -> JSArray(MkObject("field" -> JSArray("data", "name")))
            )),
          db
        )

        runQuery(Get(idx.refObj), db)

        val obj = JSObject(
          "ref" -> idx.refObj,
          "ts" -> JSLong(idx.ts),
          "active" -> true,
          "serialized" -> true,
          "name" -> "spells",
          "source" -> cls.refObj,
          "terms" -> JSArray(JSObject("field" -> JSArray("data", "element"))),
          "values" -> JSArray(JSObject("field" -> JSArray("data", "name"))),
          "partitions" -> 1
        )

        idx should equal(obj)

        // readable fields
        runQuery(Contains(JSArray("active"), Get(idx.refObj)), db) should equal(JSTrue)
        runQuery(Select(JSArray("active"), Get(idx.refObj)), db) should equal(JSTrue)
      }
    }
  }

  "query works" - {
    once("queries an index") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        cls2 <- aCollection(db)
      } {

        runQuery(
          CreateIndex(
            MkObject(
              "name" -> "all_heroes_with_element",
              "source" -> cls.refObj,
              "active" -> true,
              "terms" -> JSArray(),
              "values" -> JSArray(MkObject("field" -> JSArray("data", "element"))))),
          db
        )

        runQuery(CreateIndex(
                   MkObject("name" -> "effect_name_by_element",
                            "source" -> cls2.refObj,
                            "active" -> true,
                            "terms" -> JSArray(
                              MkObject("field" -> JSArray("data", "element"))))),
                 db)

        runQuery(CreateF(cls.refObj,
                         MkObject(
                           "data" -> MkObject("element" -> JSArray("fire", "water"),
                                              "name" -> "Zeus",
                                              "bar" -> 5))),
                 db)

        runQuery(CreateF(cls.refObj,
                         MkObject(
                           "data" -> MkObject("element" -> JSArray("water", "wind"),
                                              "name" -> "Trident",
                                              "bar" -> 7))),
                 db)

        val fireballInst = runQuery(
          CreateF(cls2.refObj,
                  MkObject("data" -> MkObject("element" -> "fire", "bar" -> 7))),
          db)

        val stormInst = runQuery(
          CreateF(cls2.refObj,
                  MkObject("data" -> MkObject("element" -> "water", "bar" -> 9))),
          db)

        val galeInst = runQuery(
          CreateF(cls2.refObj,
                  MkObject("data" -> MkObject("element" -> "wind", "bar" -> 8))),
          db)

        val elemInst = List(fireballInst, stormInst, galeInst)

        val zeusElems = elemInst.slice(0, 2) map { inst =>
          inst / "data" / "element"
        }

        val tridentElems = elemInst.slice(1, 3) map { inst =>
          inst / "data" / "element"
        }

        val heroTuples =
          JSArray(zeusElems(0), zeusElems(1), tridentElems(0), tridentElems(1))
        val heroElements = Match(Ref("indexes/all_heroes_with_element"), JSArray())
        val water = Match(Ref("indexes/effect_name_by_element"), "water")

        (runQuery(Paginate(water), db) / "data" / 0) should equal(stormInst.refObj)
        (runQuery(Paginate(heroElements), db) / "data") should equal(heroTuples)
      }
    }

    once("Term's field param works") {
      for {
        db <- aDatabase
        person <- aCollection(db)
      } {
        val nameIdx = runQuery(
          CreateIndex(
            MkObject(
              "name" -> "person_and_place_by_name",
              "source" -> JSArray(person.refObj),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "name"))))),
          db
        ).refObj
        val personRef = runQuery(
          CreateF(
            person.refObj,
            MkObject("data" -> MkObject("name" -> "Ray", "last_name" -> "Johnson"))),
          db).refObj

        (runQuery(Paginate(Match(nameIdx, "Ray")), db) / "data") should equal(
          JSArray(personRef))
      }
    }

  }

  "rename/get works" - {
    once("renames an index takes effect soon") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val params = MkObject(
          "name" -> "spells",
          "source" -> cls.refObj,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "element"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "name")))
        )

        val ref = runQuery(CreateIndex(params), db).refObj
        val ref2 = runQuery(Update(ref, MkObject("name" -> "novels")), db).refObj

        beforeTTLCacheExpiration {
          qassertErr(Get(ref), "invalid ref", "Ref refers to undefined index 'spells'", JSArray("get"), db)
        }
        runQuery(Get(ref2), db)
        runQuery(Select("name", Get(ref2)), db) should equal(JSString("novels"))
      }
    }
  }

  "delete works" - {
    once("deletes an index") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val ref =
          runQuery(CreateIndex(MkObject("name" -> "spells", "source" -> cls.refObj)),
                   db).refObj

        runQuery(DeleteF(ref), db)
        beforeTTLCacheExpiration {
          qassertErr(Get(ref), "invalid ref", "Ref refers to undefined index 'spells'", JSArray("get"), db)
        }
      }
    }
  }

  "paginate works" - {
    once("paginates all indexes") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        cls2 <- aCollection(db)
      } {

        val spellsIdx =
          runQuery(CreateIndex(MkObject("name" -> "spells", "source" -> cls.refObj)),
                   db).refObj

        val novelsIdx =
          runQuery(CreateIndex(MkObject("name" -> "novels", "source" -> cls.refObj)),
                   db).refObj

        val poemsIdx =
          runQuery(CreateIndex(MkObject("name" -> "poems", "source" -> cls2.refObj)),
                   db).refObj

        val indexes = JSObject("data" -> JSArray(spellsIdx, novelsIdx, poemsIdx))

        (runQuery(Paginate(Ref("indexes")), db)) should equal(indexes)
      }
    }

    once("multiple modifications to single doc doesn't mess with index events") {
      for {
        db <- aDatabase
        col <- aCollection(db)
      } {

        val idx = runQuery(
          CreateIndex(
            MkObject(
              "name" -> "my_index",
              "source" -> JSArray(col.refObj),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "t"))),
              "values" -> JSArray(MkObject("field" -> JSArray("data", "v"), "reverse" -> true)),
            )
          ),
          db
        )

        val doc = runQuery(CreateF(col.refObj, MkObject("data" -> MkObject("t" -> "t1", "v" -> "v1"))), db)
        runQuery(Update(doc.refObj, MkObject("data" -> MkObject("v" -> "v2"))), db)

        val indexEvents1 = runQuery(Paginate(Events(Match(idx.refObj, "t1")), size = 10), db)

        // expecting 3 events: add v1, remove v1, add v2
        val events1 = (indexEvents1 / "data").as[JSArray].value
        events1.size should equal (3)

        runQuery(
          Do(
            Update(doc.refObj, MkObject("data" -> MkObject("v" -> "v3"))),
            Update(doc.refObj, MkObject("data" -> MkObject("v" -> "v2")))
          ),
          db
        )

        val indexEvents2 = runQuery(Paginate(Events(Match(idx.refObj, "t1")), size = 10), db)
        // still expecting same 3 events
        (indexEvents2 / "data").as[JSArray].value.size should equal (3)
        indexEvents2 should equal (indexEvents1)

        runQuery(
          Do(
            Update(doc.refObj, MkObject("data" -> MkObject("v" -> "v3"))),
            Update(doc.refObj, MkObject("data" -> MkObject("v" -> "v4")))
          ),
          db
        )
        val indexEvents3 = runQuery(Paginate(Events(Match(idx.refObj, "t1")), size = 10), db)
        // expecting 5 events: add v1, remove v1, add v2, remove v2, add v4
        val events3 = (indexEvents3 / "data").as[JSArray].value
        events3.size should equal (5)
        events3.take(3) should equal(events1)
      }
    }
  }

  "multiple sources work" - {
    once("multiple source index") {
      for {
        db <- aDatabase
        person <- aCollection(db)
        place <- aCollection(db)
      } {
        val nameIdx = runQuery(
          CreateIndex(
            MkObject(
              "name" -> "person_and_place_by_name",
              "source" -> JSArray(person.refObj, place.refObj),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "name"))))),
          db
        ).refObj

        (runQuery(Get(nameIdx), db) / "source") shouldBe JSArray(person.refObj, place.refObj)

        val personOnlyIdx =
          runQuery(CreateIndex(
                     MkObject("name" -> "person_by_name",
                              "source" -> JSArray(person.refObj),
                              "active" -> true,
                              "terms" -> JSArray(
                                MkObject("field" -> JSArray("data", "name"))))),
                   db).refObj
        val placeRef = runQuery(
          CreateF(place.refObj,
                  MkObject("data" -> MkObject("name" -> "Ray", "state" -> "TX"))),
          db).refObj
        val personRef = runQuery(
          CreateF(
            person.refObj,
            MkObject("data" -> MkObject("name" -> "Ray", "last_name" -> "Johnson"))),
          db).refObj

        (runQuery(Paginate(Match(nameIdx, "Ray")), db) / "data") should equal(
          JSArray(placeRef, personRef))
        (runQuery(Paginate(Match(personOnlyIdx, "Ray")), db) / "data") should equal(
          JSArray(personRef))
      }
    }
  }

  "multiple sources: when class is deleted, " - {
    once("index matches are filtered, index is deleted for last source") {
      for {
        db <- aDatabase
        person <- aCollection(db)
        place <- aCollection(db)
      } {
        val nameIdx = runQuery(
          CreateIndex(
            MkObject(
              "name" -> "person_and_place_by_name",
              "source" -> JSArray(person.refObj, place.refObj),
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "name"))))),
          db
        ).refObj
        val placeInstRef = runQuery(
          CreateF(place.refObj,
                  MkObject("data" -> MkObject("name" -> "Ray", "state" -> "TX"))),
          db).refObj
        val personInstRef = runQuery(
          CreateF(
            person.refObj,
            MkObject("data" -> MkObject("name" -> "Ray", "last_name" -> "Johnson"))),
          db).refObj

        val q = Paginate(Match(nameIdx, "Ray"))
        (runQuery(q, db) / "data") should equal(JSArray(placeInstRef, personInstRef))

        runQuery(DeleteF(place.refObj), db)

        beforeTTLCacheExpiration {
          (runQuery(q, db) / "data") should equal(JSArray(personInstRef))
        }
      }
    }
  }

  "wildcard sources work" - {
    once("global attribute index") {
      for {
        db <- aDatabase
        person <- aCollection(db)
        place <- aCollection(db)
      } {
        val nameIdx =
          runQuery(CreateIndex(
                     MkObject("name" -> "entity_by_name",
                              "source" -> "_",
                              "active" -> true,
                              "terms" -> JSArray(
                                MkObject("field" -> JSArray("data", "name"))))),
                   db).refObj
        val personOnlyIdx =
          runQuery(CreateIndex(
                     MkObject("name" -> "person_by_name",
                              "source" -> JSArray(person.refObj),
                              "active" -> true,
                              "terms" -> JSArray(
                                MkObject("field" -> JSArray("data", "name"))))),
                   db).refObj
        val placeRef = runQuery(
          CreateF(place.refObj,
                  MkObject("data" -> MkObject("name" -> "Ray", "state" -> "TX"))),
          db).refObj
        val personRef = runQuery(
          CreateF(
            person.refObj,
            MkObject("data" -> MkObject("name" -> "Ray", "last_name" -> "Johnson"))),
          db).refObj

        (runQuery(Paginate(Match(nameIdx, "Ray")), db) / "data") should equal(
          JSArray(placeRef, personRef))
        (runQuery(Paginate(Match(nameIdx, "foo")), db) / "data") should equal(
          JSArray())
        (runQuery(Paginate(Match(personOnlyIdx, "Ray")), db) / "data") should equal(
          JSArray(personRef))
      }
    }
  }

  "enforces uniqueness" - {
    once("globally unique attribute") {
      for {
        db <- aDatabase
        person <- aCollection(db)
        place <- aCollection(db)
      } {
        runQuery(CreateIndex(
                   MkObject("name" -> "entity_by_name",
                            "source" -> "_",
                            "terms" -> JSArray(
                              MkObject("field" -> JSArray("data", "name"))),
                            "unique" -> true)),
                 db).refObj
        val placeQ =
          CreateF(place.refObj,
                  MkObject("data" -> MkObject("name" -> "Ray", "state" -> "TX")))
        val personQ = CreateF(
          person.refObj,
          MkObject("data" -> MkObject("name" -> "Ray", "last_name" -> "Johnson")))
        runQuery(placeQ, db)

        val err = runRawQuery(personQ, db.key)
        err should respond(BadRequest)
        (err.json / "errors" / 0 / "code").as[String] should equal(
          "instance not unique")
      }
    }

    once("create, delete and create of a unique doc in the same transaction works") {
      for {
        db <- aDatabase
        col <- aCollection(db)
      } {
        val idx = runQuery(
          CreateIndex(MkObject(
            "name" -> "unique_index_on_f1",
            "source" -> col.refObj,
            "terms" -> JSArray(MkObject("field" -> JSArray("data", "f1"))),
            "active" -> true,
            "unique" -> true
          )),
          db.adminKey
        )

        val f1Value = "1"

        val createDoc = CreateF(
          col.refObj, MkObject("data" -> MkObject("f1" -> f1Value, "otherField" -> "x"))
        )

        val res = runQuery(
          Let("doc" -> createDoc)(
            Do(
              DeleteF(Select("ref", Var("doc"))),
              createDoc
            )
          ),
          db.adminKey)

        collection(Match(idx.refObj, f1Value), db) should equal (List(res.refObj))
      }
    }

    once("don't fail to check uniqueness on very large index tuples") {
      for {
        db  <- aDatabase
        col <- aCollection(db)
      } {
        val idx =
          runQuery(
            CreateIndex(
              MkObject(
                "name" -> "an_index",
                "source" -> col.refObj,
                "values" -> JSArray(MkObject("field" -> JSArray("data", "str"))),
                "active" -> true,
                "unique" -> true
              )),
            db.adminKey
          )

        val createDoc =
          CreateF(
            col.refObj,
            MkObject(
              "data" -> MkObject(
                "str" -> RepeatString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1024)
              )
            ))

        runQuery(createDoc, db) // should succeed
        collection(Match(idx.refObj), db) shouldBe empty // set write was dropped
      }
    }
  }

  "extracts terms correctly" - {
    once("MVAs are flattened in order to pick terms and splat into values") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
                       Prop.const(Seq(JSArray("data", "foo"))),
                       Prop.const(Seq((JSArray("data", "bar"), false))))
        _ <- aDocument(cls,
                        dataProp =
                          Prop.const(JSObject("foo" -> JSArray(1, 42), "bar" -> 2)))
        _ <- aDocument(cls,
                        dataProp =
                          Prop.const(JSObject("foo" -> 1, "bar" -> JSArray(3, 42))))
        _ <- aDocument(
          cls,
          dataProp =
            Prop.const(JSObject("foo" -> 1, "bar" -> JSArray(JSArray(4, 5), 6))))
        _ <- aDocument(
          cls,
          dataProp = Prop.const(
            JSObject("foo" -> JSArray("foo", "bar"), "bar" -> JSArray("3", "42"))))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("bar" -> 56)))
      } {
        val set1 = Match(idx.refObj, 1)
        collection(set1, db) should equal(List(2, 3, 4, 5, 6, 42) map { JSLong(_) })
        val set2 = Match(idx.refObj, 42)
        collection(set2, db) should equal(List(2) map { JSLong(_) })
        val set3 = Match(idx.refObj, "foo")
        collection(set3, db) should equal(List("3", "42") map { JSString(_) })
      }
    }

    once("indexes paths into multiple objects in an array") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
                       Prop.const(Seq(JSArray("data", "foo", "baz"))),
                       Prop.const(Seq((JSArray("data", "bar"), false))))
        _ <- aDocument(
          cls,
          dataProp = Prop.const(
            JSObject("foo" -> JSArray(JSObject("baz" -> 1), JSObject("baz" -> 2)),
                     "bar" -> "yay")))
      } {
        val set1 = Match(idx.refObj, 1)
        val set2 = Match(idx.refObj, 2)
        collection(set1, db) should equal(List(JSString("yay")))
        collection(set2, db) should equal(List(JSString("yay")))
      }
    }

    once(
      "transforms should apply to each value in a MVA and pass through other types") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val idx = runQuery(
          CreateIndex(
            MkObject(
              "name" -> "foo",
              "source" -> cls.refObj,
              "active" -> true,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "foo"),
                                          "transform" -> "casefold")),
              "values" -> JSArray(MkObject("field" -> JSArray("data", "bar")))
            )),
          db
        )

        runQuery(CreateF(cls.refObj,
                         MkObject("data" -> MkObject("foo" -> 1, "bar" -> 2))),
                 db)

        runQuery(
          CreateF(cls.refObj,
                  MkObject("data" -> MkObject("foo" -> "Snow", "bar" -> "flakes"))),
          db)

        runQuery(CreateF(cls.refObj,
                         MkObject(
                           "data" -> MkObject("foo" -> JSArray("Snow", "RAIN"),
                                              "bar" -> "FALL"))),
                 db)

        collection(Match(idx.refObj, "snow"), db) should equal(
          List("FALL", "flakes") map (JSString(_)))
        collection(Match(idx.refObj, "rain"), db) should equal(
          List("FALL") map (JSString(_)))
        collection(Match(idx.refObj, "RAIN"), db) should equal(Nil)

        collection(Match(idx.refObj, 1), db) should equal(List(2) map { JSLong(_) })
      }
    }

    once("transaction time can be a covered value") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx1",
          "source" -> cls.refObj,
          "active" -> true,
          "values" -> MkObject("field" -> "ts"))), db)

        val bindingIdx = runQuery(CreateIndex(MkObject(
          "name" -> "idx2",
          "source" -> MkObject(
            "class" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        val inst = runQuery(CreateF(cls.refObj, MkObject()), db)

        collection(Match(idx.refObj), db) should equal (List(inst / "ts"))
        collection(Match(bindingIdx.refObj), db) should equal (List(inst / "ts"))
      }
    }

    once("transaction time is nulled out as term") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        // FIXME: these indexes should fail to create.
        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx1",
          "source" -> cls.refObj,
          "active" -> true,
          "terms" -> MkObject("field" -> "ts"))), db)

        val bindingIdx = runQuery(CreateIndex(MkObject(
          "name" -> "idx2",
          "source" -> MkObject(
            "class" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "terms" -> MkObject("binding" -> "the_time"))), db)

        val inst = runQuery(CreateF(cls.refObj, MkObject()), db)

        collection(Match(idx.refObj, JSNull), db) should equal (List(inst / "ref"))
        collection(Match(bindingIdx.refObj, JSNull), db) should equal (List(inst / "ref"))
        collection(Match(idx.refObj, inst / "ts"), db) should equal (Nil)
        collection(Match(bindingIdx.refObj, inst / "ts"), db) should equal (Nil)
      }
    }

    once("valid time is nulled ast term on Insert()") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        pendingUntilFixed {
          // FIXME: these indexes should fail to create.
          val idx = runQuery(CreateIndex(MkObject(
            "name" -> "idx1",
            "source" -> cls.refObj,
            "active" -> true,
            "terms" -> MkObject("field" -> "ts"))), db)

          val inst = runQuery(InsertVers(MkRef(cls.refObj, 123), 123, "create", MkObject()), db)

          collection(Match(idx.refObj, JSNull), db) should equal (List(inst / "resource"))
          collection(Match(idx.refObj, inst / "ts"), db) should equal (Nil)
        }
      }
    }
  }

  "sort numbers correctly" - {
    once("ascending") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
          Prop.const(Seq(JSArray("data", "foo"))),
          Prop.const(Seq((JSArray("data", "bar"), false))))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> JSNull)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> 2)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> 1.1)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> 1)))
      } {
        val set1 = Match(idx.refObj, "foo")
        collection(set1, db) should equal(List(JSLong(1), JSDouble(1.1), JSLong(2)))
      }
    }

    once("descending") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
          Prop.const(Seq(JSArray("data", "foo"))),
          Prop.const(Seq((JSArray("data", "bar"), true))))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> JSNull)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> 2)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> 1.1)))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> 1)))
      } {
        val set1 = Match(idx.refObj, "foo")
        collection(set1, db) should equal(List(JSLong(2), JSDouble(1.1), JSLong(1)))
      }
    }

    once("sort array") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls,
          Prop.const(Seq(JSArray("data", "foo"))),
          Prop.const(Seq((JSArray("data", "bar"), false))))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "foo", "bar" -> JSArray(JSNull, 2, 1.1, 1))))
      } {
        val set1 = Match(idx.refObj, "foo")
        collection(set1, db) should equal(List(JSLong(1), JSDouble(1.1), JSLong(2)))
      }
    }

    once("mix different types") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx0 <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))), Prop.const(Some(8)))
        idx1 <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), true))), Prop.const(Some(8)))
      } {
        val bytes = Array[Byte](0x1, 0x2, 0x3, 0x4)
        val base64 = Base64.encodeStandard(bytes)

        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 1))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 1))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 2))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 2))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 4))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 4))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> "str"))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> "str"))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3.14))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> 3.14))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Epoch(0, "seconds")))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Epoch(0, "seconds")))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Date("1970-01-01")))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Date("1970-01-01")))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> coll.refObj))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> coll.refObj))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Bytes(bytes)))), db)
        runQuery(CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Bytes(bytes)))), db)

        val ts = JSObject("@ts" -> "1970-01-01T00:00:00Z")
        val dt = JSObject("@date" -> "1970-01-01")
        val b = JSObject("@bytes" -> base64)

        runQuery(Paginate(Match(idx0.refObj)), db) shouldBe JSObject("data" -> JSArray(1, 1, 2, 2, 3, 3, 3.14, 3.14, 4, 4, b, b, "str", "str", coll.refObj, coll.refObj, ts, ts, dt, dt))
        runQuery(Paginate(Match(idx1.refObj)), db) shouldBe JSObject("data" -> JSArray(dt, dt, ts, ts, coll.refObj, coll.refObj, "str", "str", b, b, 4, 4, 3.14, 3.14, 3, 3, 2, 2, 1, 1))
      }
    }
  }

  "index bombs" - {
    def createIndex(db: Database, col: Collection, shouldSucceed: Boolean, waitActive: Boolean = true): Unit = {
      val res = runRawQuery(
        CreateIndex(MkObject(
          "name" -> "col_by_f1",
          "source" -> col.refObj,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "f1"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "f2")), MkObject("field" -> JSArray("data", "f3")))
        )),
        db.adminKey
      )

      if (shouldSucceed) {
        res should respond(200, 201)

        val idx = res.json / "resource"

        if (waitActive) {
          eventually(timeout(2.minutes), interval(1.second)) {
            runQuery(Get(idx.refObj), db).get("active") should equal(JSTrue)
          }
        }
      } else {
        res should respond(400)
      }
    }

    def createDocWithLargeArrays(db: Database, col: Collection, shouldSucceed: Boolean): Unit = {
      val res = runRawQuery(
        CreateF(
          col.refObj,
          MkObject(
            "data" -> MkObject(
              "f1" -> JSArray(1 to 100 map { i => JSString(s"$i fdsj ivxcozz887890vuxc90 2314p],jklfsaoz") }:_*),
              "f2" -> JSArray(1 to 75 map { i => JSString(s"jfksdl;a fjskdlfjds fids fsdof sdo fsd $i") }:_*),
              "f3" -> JSArray(1 to 25 map { i => JSString(s"jfksdl;a fjskdlfjds fids fsdof sdo fsd $i") }:_*),
            )
          )
        ),
        db.adminKey
      )
      if (shouldSucceed) {
        res should respond(200, 201)
      } else {
        res should respond(400)
      }
    }

    once("return 400 when inserted") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        _ = createIndex(db, col, shouldSucceed = true)
      } {
        createDocWithLargeArrays(db, col, shouldSucceed = false)
      }
    }

    once("return 400 when caught during synchronous indexing") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        _ = createDocWithLargeArrays(db, col, shouldSucceed = true)
      } {
        createIndex(db, col, shouldSucceed = false)
      }
    }
  }

  "class is used instead of collection in api version 2.1" - {
    once("CreateIndex accepts 'class' as a source") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> MkObject(
            "class" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        val inst = runQuery(CreateF(cls.refObj, MkObject()), db)

        collection(Match(idx.refObj), db) should equal(List(inst / "ts"))
      }
    }

    once("CreateIndex accepts an array of 'class's as sources") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db)
        cls2 <- aCollection(db)
      } {

        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> JSArray(
            MkObject(
              "class" -> cls.refObj, "fields" -> MkObject(
                "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
            MkObject(
              "class" -> cls2.refObj, "fields2" -> MkObject(
                "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          ),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        val inst = runQuery(CreateF(cls.refObj, MkObject()), db)

        collection(Match(idx.refObj), db) should equal(List(inst / "ts"))
      }
    }

    once("CreateIndex accepts 'collection' as a source but reads back as 'class'") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> MkObject(
            "collection" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        (runQuery(Get(idx.refObj), db) / "source" / "class" match {
          case _: JSNotFound => false
          case _: JSInvalid  => false
          case _             => true
        }) shouldBe true
      }
    }

    once("CreateIndex accepts an array of 'collection's as sources but read them all back as 'class'") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db)
        cls2 <- aCollection(db)
      } {

        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> JSArray(
            MkObject(
            "collection" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
            MkObject(
              "collection" -> cls2.refObj, "fields2" -> MkObject(
                "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          ),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        JSArray.unapplySeq(runQuery(Get(idx.refObj), db) / "source") match {
          case None      => fail()
          case Some(seq) => seq.forall {
            case o: JSObject => o / "class" match {
              case _: JSNotFound => false
              case _: JSInvalid  => false
              case _             => true
            }
            case _           => false
          } shouldBe true
        }
      }
    }
  }
}

class IndexSpec27 extends QueryAPI27Spec {
  "create doesn't work for containers" - {
    once("can't create an index in a container") {
      for {
        cdb <- aContainerDB
      } {
        val createIndex =
          CreateIndex(
            MkObject(
              "name" -> "spells",
              "source" -> MkObject("class" -> "_"),
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "element"))),
              "values" -> JSArray(MkObject("field" -> JSArray("data", "name")))
            ))

        qassertErr(createIndex, "invalid object in container", JSArray("create_index"), cdb)
      }
    }
  }

  "collection is used instead of class in 2.7 api" - {
    once("CreateIndex accepts 'collection' as a source") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> MkObject(
            "collection" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        (runQuery(Get(idx.refObj), db) / "source" / "collection" match {
          case _: JSNotFound => false
          case _: JSInvalid  => false
          case _             => true
        }) shouldBe true
      }
    }

    once("CreateIndex accepts an array of 'collection's as sources") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db)
        cls2 <- aCollection(db)
      } {

        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> JSArray(
            MkObject(
              "collection" -> cls.refObj, "fields" -> MkObject(
                "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
            MkObject(
              "collection" -> cls2.refObj, "fields2" -> MkObject(
                "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          ),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        JSArray.unapplySeq(runQuery(Get(idx.refObj), db) / "source") match {
          case None      => fail()
          case Some(seq) => seq.forall {
            case o: JSObject => o / "collection" match {
              case _: JSNotFound => false
              case _             => true
            }
            case _           => false
          } shouldBe true
        }
      }
    }

    once("v21 CreateIndex with source 'class' and read with v27 with source 'collection'") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        val createIndex = CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> MkObject(
            "class" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time")))

        // write with 2.1
        val idx = client.withVersion("2.1").api.query(createIndex, db.key).json / "resource"

        // read with 2.7
        (runQuery(Get(idx.refObj), db) / "source" / "collection" match {
          case _: JSNotFound => false
          case _: JSInvalid  => false
          case _             => true
        }) shouldBe true
      }
    }

    once("v27 CreateIndex with source 'collection' and read with v21 with source 'class'") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        //write with 2.7
        val idx = runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> MkObject(
            "collection" -> cls.refObj, "fields" -> MkObject(
              "the_time" -> QueryF(Lambda("x" -> Select("ts", Var("x")))))),
          "active" -> true,
          "values" -> MkObject("binding" -> "the_time"))), db)

        // read with 2.1
        val resource = client.withVersion("2.1").api.query(Get(idx.refObj), db.key).json / "resource"
        (resource / "source" / "class" match {
          case _: JSNotFound => false
          case _: JSInvalid  => false
          case _             => true
        }) shouldBe true
      }
    }
  }

  "large component scenarios" - {
    def addIndexOnBigField(db: Database, col: Collection): Unit = {
      val idx = runQuery(
        CreateIndex(MkObject(
          "name" -> "col_by_f1",
          "source" -> col.refObj,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "f1"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "f2")))
        )),
        db.adminKey
      )

      eventually(timeout(2.minutes), interval(1.second)) {
        runQuery(Get(idx.refObj), db).get("active") should equal(JSTrue)
      }
    }

    def writeDocWithBigField(db: Database, col: Collection): Prop[Unit] =
      for {
        b <- Prop.boolean
      } {
        val (f1, f2) = {
          val big: JSValue = Bytes(new Array[Byte](Short.MaxValue * 10))
          val small: JSValue = "small"
          if (b) (big, small) else (small, big)
        }
        runQuery(
          CreateF(
            col.refObj,
            MkObject(
              "data" -> MkObject(
                "f1" -> f1,
                "f2" -> f2
              )
            )
          ),
          db.adminKey
        )
      }


    once("adding 'bad' doc after creating index") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        _ = addIndexOnBigField(db, col)
        _ <- writeDocWithBigField(db, col)
      } yield ()
    }

    once("adding index after creating small number of 'bad' docs works") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        _ <- writeDocWithBigField(db, col)
      } {
        addIndexOnBigField(db, col)
      }
    }

    once("index eventually becomes active after adding it to large 'bad' docs collection") {
      pending // FIXME

      for {
        db <- aDatabase
        col <- aCollection(db)
        _ <- writeDocWithBigField(db, col).times(2 * Index.BuildSyncSize)
      } {
        addIndexOnBigField(db, col)
      }
    }
  }

  "synchronous builds" - {
    once("succeeds with bindings") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        data = Prop.const(JSObject("foo" -> 1))
        _ <- someDocuments(10, col, dataProp = data)
      } {
        runQuery(CreateIndex(MkObject(
          "name" -> "idx",
          "source" -> MkObject(
            "collection" -> col.refObj,
            "fields" -> MkObject(
              "b1" -> QueryF(Lambda("doc" ->
                Multiply(2, Select(JSArray("data", "foo"), Var("doc"))))))),
          "terms" -> JSArray(MkObject("binding" -> "b1"))
        )), db)

        val set = runQuery(Paginate(Match(IndexRef("idx"), 2)), db)

        (set / "data").as[Seq[JSValue]].size should equal (10)
      }
    }
  }

}

class IndexSpec3 extends QueryAPI3Spec {
  "ttl on read" - {
    once("remove index entries on read - single entry per doc") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        data = Prop.const(JSObject("foo" -> 1))
        idx <- anIndex(col, termProp = Prop.const(Seq.empty), valueProp = Prop.const(Seq((JSArray("ref"), false), (JSArray("data", "foo"), false))))
        docs <- someDocuments(10, col, dataProp = data)
      } {
        runQuery(Update(docs(5).refObj, MkObject("ttl" -> TimeAdd(Now(), 5, "seconds"))), db)

        eventually(timeout(10.seconds), interval(200.millis)) {
          //use custom index
          val set0 = runQuery(Paginate(Match(idx.refObj)), db)

          (set0 / "data").as[Seq[JSValue]] should contain.only(
            JSArray(docs(0).refObj, JSLong(1)),
            JSArray(docs(1).refObj, JSLong(1)),
            JSArray(docs(2).refObj, JSLong(1)),
            JSArray(docs(3).refObj, JSLong(1)),
            JSArray(docs(4).refObj, JSLong(1)),
            //JSArray(docs(5).refObj, JSLong(1)) <-- ttl'ed
            JSArray(docs(6).refObj, JSLong(1)),
            JSArray(docs(7).refObj, JSLong(1)),
            JSArray(docs(8).refObj, JSLong(1)),
            JSArray(docs(9).refObj, JSLong(1))
          )

          //use builtin documents index
          val set1 = runQuery(Paginate(Documents(col.refObj)), db)

          (set1 / "data").as[Seq[JSValue]] should contain.only(
            docs(0).refObj,
            docs(1).refObj,
            docs(2).refObj,
            docs(3).refObj,
            docs(4).refObj,
            //docs(5).refObj <-- ttl'ed
            docs(6).refObj,
            docs(7).refObj,
            docs(8).refObj,
            docs(9).refObj
          )
        }
      }
    }

    once("remove index entries on read - multiple entries per doc") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        data = Prop.const(JSObject("foo" -> JSArray(1, 2)))
        idx <- anIndex(col, termProp = Prop.const(Seq.empty), valueProp = Prop.const(Seq((JSArray("ref"), false), (JSArray("data", "foo"), false))))
        docs <- someDocuments(5, col, dataProp = data)
      } {
        runQuery(Update(docs(3).refObj, MkObject("ttl" -> TimeAdd(Now(), 5, "seconds"))), db)

        eventually(timeout(10.seconds), interval(200.millis)) {
          //use custom index
          val set0 = runQuery(Paginate(Match(idx.refObj)), db)

          (set0 / "data").as[Seq[JSValue]] should contain.only(
            JSArray(docs(0).refObj, JSLong(1)),
            JSArray(docs(0).refObj, JSLong(2)),
            JSArray(docs(1).refObj, JSLong(1)),
            JSArray(docs(1).refObj, JSLong(2)),
            JSArray(docs(2).refObj, JSLong(1)),
            JSArray(docs(2).refObj, JSLong(2)),
            //JSArray(docs(3).refObj, JSLong(1)),
            //JSArray(docs(3).refObj, JSLong(2)),
            JSArray(docs(4).refObj, JSLong(1)),
            JSArray(docs(4).refObj, JSLong(2))
          )

          //use builtin documents index
          val set1 = runQuery(Paginate(Documents(col.refObj)), db)

          (set1 / "data").as[Seq[JSValue]] should contain.only(
            docs(0).refObj,
            docs(1).refObj,
            docs(2).refObj,
            //docs(3).refObj,
            docs(4).refObj
          )
        }
      }
    }
  }
}
