package fauna.api.test.queries

import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.Instant
import fauna.api.test._
import fauna.codex.json._
import fauna.exec.FaunaExecutionContext
import fauna.lang.clocks.Clock
import fauna.model.Parsing
import fauna.prop.api.Privilege
import fauna.prop.Prop
import scala.concurrent.Future
import scala.concurrent.duration._

class WriteFunctionsSpec extends QueryAPI21Spec {

  "create" - {
    once(
      "create an instance of the class referred to by class_ref, using params_object") {
      for {
        db <- aDatabase
        user <- aUser(db)
        cls <- aCollection(db)
      } {
        runQuery(CreateF(cls.refObj,
                         MkObject(
                           "data" -> MkObject("name" -> "Thunder",
                                              "element" -> "air",
                                              "cost" -> 15))),
                 db)

        runQuery(
          CreateF(cls.refObj,
                  MkObject("data" -> MkObject("name" -> "Thunder",
                                              "element" -> "air",
                                              "cost" -> 15),
                           "credentials" -> MkObject("password" -> "sekrit"))),
          db)

        qassertErr(CreateF(cls.refObj,
                           MkObject(
                             "data" -> MkObject("name" -> "Thunder",
                                                "element" -> "air",
                                                "cost" -> 15))),
                   "permission denied",
                   JSArray("create"),
                   user)

        qassertErr(
          CreateF(Ref("classes/spells/104979526888456192"),
                  MkObject(
                    "data" -> MkObject("name" -> "Thunder",
                                       "element" -> "air",
                                       "cost" -> 15))),
          "invalid ref",
          JSArray("create"),
          db
        )
      }
    }

    once("create an instance and paginate over its events") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val create =
          CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> 1, "bar" -> 4)))
        val query =
          Let("inst" -> Select("ref", create))(Paginate(Events(Var("inst"))))
        val result = runQuery(query, db)
        val ref = result / "data" / 0 / "instance"
        runQuery(Paginate(Events(ref)), db) / "data" should equal(result / "data")
      }
    }

    once("shorthand create") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val createClass = CreateClass(MkObject("name" -> "fooclass"))
        val createIndex =
          CreateIndex(MkObject("name" -> "fooindex", "source" -> cls.refObj))
        val createDatabase = CreateDatabase(MkObject("name" -> "foodb"))
        val createKey =
          CreateKey(MkObject("database" -> db.refObj, "role" -> "server"))
        val createFunction = CreateFunction(
          MkObject("name" -> "foofunc", "body" -> QueryF(Lambda("x" -> Var("x")))))
        val createRole = CreateRole(
          MkObject("name" -> "foorole",
                   "privileges" -> JSArray(
                     MkObject("resource" -> ClassRef("fooclass"),
                              "actions" -> MkObject("read" -> JSTrue)))))

        runQuery(createClass, db) / "ref" should equal(ClsRefV("fooclass"))
        runQuery(createIndex, db) / "ref" should equal(IdxRefV("fooindex"))
        runQuery(createDatabase, db.adminKey) / "ref" should equal(DBRefV("foodb"))
        (runQuery(createKey, rootKey) / "secret").isEmpty should be(false)
        runQuery(createFunction, db) / "ref" should equal(FnRefV("foofunc"))
        runQuery(createRole, db.adminKey) / "ref" should equal(RoleRefV("foorole"))
      }
    }

    once("using 'events' as field name returns better error") {
      for {
        db <- aDatabase
      } {
        Parsing.ReservedNames foreach (reservedName => {
          val createClass =
            CreateClass(MkObject("name" -> JSString(reservedName.toString)))
          val err = qassertErr(createClass, "validation failed", JSArray("create_class"), db)
          (err / "failures" / 0 / "code").as[String] should equal("reserved name")
        })
      }
    }

    once("can read own inserts within transaction") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        // NB: Perform all operations after the collection's MVT.
        val cTS = TimeAdd(Time("now"), -10, "seconds")
        val dTS = TimeAdd(Time("now"), -9, "seconds")
        val create = Let("id" -> MkRef(cls.refObj, NewID)) {
          Do(InsertVers(Var("id"),
                        cTS,
                        "create",
                        MkObject("data" -> MkObject("foo" -> 1))),
             InsertVers(Var("id"), dTS, "delete"))
        }
        val ref = runQuery(create, db) / "instance"
        (runQuery(Paginate(Match(idx.refObj, 1), ts = cTS), db) / "data" / 0) should equal(
          ref)
        (runQuery(Paginate(Match(idx.refObj, 1)), db) / "data").isEmpty should be(true)
      }
    }

    once("can read own create/update within transaction") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val create = Let(
          "inst" -> CreateF(cls.refObj, MkObject("data" -> MkObject("a" -> "a"))),
          "ref" -> Select("ref", Var("inst"))
        ) {
          Do(
            Update(Var("ref"), MkObject("data" -> MkObject("b" -> "b"))),
            Update(Var("ref"), MkObject("data" -> MkObject("c" -> "c"))),
            Update(Var("ref"), MkObject("data" -> MkObject("d" -> "d"))),
            Get(Var("ref")))
        }
        val obj = runQuery(create, db)
        val obj2 = runQuery(Get(obj / "ref"), db)

        (obj / "data" / "a") should equal (JSString("a"))
        (obj / "data" / "b") should equal (JSString("b"))
        (obj / "data" / "c") should equal (JSString("c"))
        (obj / "data" / "d") should equal (JSString("d"))

        (obj2 / "data" / "a") should equal (JSString("a"))
        (obj2 / "data" / "b") should equal (JSString("b"))
        (obj2 / "data" / "c") should equal (JSString("c"))
        (obj2 / "data" / "d") should equal (JSString("d"))
      }
    }

    once("can read own create/update within transaction (indexes)") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        val create = Let(
          "inst" -> CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> 1))),
          "ref" -> Select("ref", Var("inst"))
        ) {
          Do(
            Get(Match(idx.refObj, 1)), // will throw not found if wrong
            Update(Var("ref"), MkObject("data" -> MkObject("foo" -> 2))),
            Get(Match(idx.refObj, 2)), // will throw not found if wrong
            Update(Var("ref"), MkObject("data" -> MkObject("foo" -> 1))),
            Get(Match(idx.refObj, 1)), // will throw not found if wrong
            Update(Var("ref"), MkObject("data" -> MkObject("foo" -> 2))),
            Get(Match(idx.refObj, 2)), // will throw not found if wrong
            Get(Var("ref")))
        }
        val obj = runQuery(create, db)
        val obj2 = runQuery(Get(obj / "ref"), db)
        val page = runQuery(Paginate(Match(idx.refObj, 2)), db)
        val page2 = runQuery(Paginate(Match(idx.refObj, 1)), db)

        (obj / "data" / "foo") should equal (JSLong(2))
        (obj2 / "data" / "foo") should equal (JSLong(2))
        (page / "data") should equal (JSArray(obj / "ref"))
        (page2 / "data") should equal (JSArray())
      }
    }

    once("can read own update within transaction") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val obj = runQuery(CreateF(cls.refObj, MkObject()), db)

        val obj1 = runQuery(Do(
          Update(obj / "ref", MkObject("data" -> MkObject("a" -> 1))),
          Update(obj / "ref", MkObject("data" -> MkObject("b" -> 2)))), db)

        val obj2 = runQuery(Get(obj / "ref"), db)

        (obj1 / "data") should equal (JSObject("a" -> 1, "b" -> 2))
        (obj2 / "data") should equal (JSObject("a" -> 1, "b" -> 2))
      }
    }

    once("threads through transaction time") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val res = runQuery(Let(
          "i" -> CreateF(cls.refObj, MkObject()),
          "j" -> CreateF(cls.refObj, MkObject("data" -> MkObject("itime" -> Select("ts", Var("i")))))
        ) {
          If(Not(Equals(Select("ts", Var("i")), Select(JSArray("data", "itime"), Var("j")))),
            Abort("failed"),
            JSArray(Var("i"), Var("j")))
        }, db)

        (res / 0 / "ts") should equal (res / 1 / "ts")
        (res / 0 / "ts") should equal (res / 1 / "data" / "itime")
      }
    }

    once("validates references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val create =
          CreateF(cls.refObj,
                  MkObject(
                    "data" ->
                      MkObject("foo" -> 1, "bar" -> ClassRef("foofoo"))))
        val err = qassertErr(create, "validation failed", JSArray(), db)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("data")
        (err / "failures" / 0 / "field" / 1).as[String] should equal("bar")
      }
    }

    once("errors with arrays should indicate indexes correctly") {
      for {
        db <- aDatabase
        role <- aName
        coll0 <- aName
        coll1 <- aName
      } {
        val create =
          CreateRole(MkObject(
            "name" -> role,
            "privileges" -> JSArray(
              MkObject("resource" -> ClassRef(coll0)),
              MkObject("resource" -> ClassRef(coll1))
            )))

        val ret = runRawQuery(create, db.adminKey)
        ret should respond (400)

        val errors = ret.json / "errors"

        (errors / 0 / "code").as[String] shouldBe "validation failed"
        (errors / 0 / "description").as[String] shouldBe "instance data is not valid."

        (errors / 0 / "failures" / 0 / "code").as[String] shouldBe "invalid reference"
        (errors / 0 / "failures" / 0 / "description").as[String] shouldBe "Cannot read reference."
        (errors / 0 / "failures" / 0 / "field").as[Seq[String]] shouldBe Seq("privileges", "0", "resource")

        (errors / 0 / "failures" / 1 / "code").as[String] shouldBe "invalid reference"
        (errors / 0 / "failures" / 1 / "description").as[String] shouldBe "Cannot read reference."
        (errors / 0 / "failures" / 1 / "field").as[Seq[String]] shouldBe Seq("privileges", "1", "resource")
      }
    }

    once("rejects scoped refs") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.0", db)
        cls <- aFaunaClass(subDB)
        subDB2 <- aDatabase("2.0", subDB)
      } {
        val subRef = ClsRefV(cls.name, DBRefV(subDB.name))
        val sub2Ref = RefV(123, ClsRefV(cls.name, DBRefV(subDB2.name)))
        qassertErr(CreateF(subRef, MkObject("data" -> MkObject("foo" -> "bar"))),
                   "invalid argument",
                   JSArray("create"),
                   db)
        runQuery(CreateF(ClassRef(cls.name),
                         MkObject("data" -> MkObject("foo" -> "bar"))),
                 subDB)

        val create =
          CreateF(ClassRef(cls.name),
                  MkObject("data" -> MkObject("foo" -> 1, "bar" -> sub2Ref)))
        val err = qassertErr(create, "validation failed", JSArray(), subDB)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("data")
        (err / "failures" / 0 / "field" / 1).as[String] should equal("bar")
      }
    }

    once("rejects duplicate instances") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.1", db)
        cls <- aCollection(db)
        inst <- aDocument(cls)
        idx <- anIndex(cls, Prop.const(List.empty))
        role <- aRole(db, Privilege.open(cls.refObj))
        func <- aFunc(db)
      } {
        qassertErr(CreateDatabase(MkObject("name" -> subDB.name)),
          "instance already exists", "Database already exists.", JSArray("create_database"), db.adminKey)

        qassertErr(CreateClass(MkObject("name" -> cls.name)),
          "instance already exists", "Class already exists.", JSArray("create_class"), db)

        qassertErr(CreateIndex(MkObject("name" -> idx.name, "source" -> ClassRef(cls.name))),
          "instance already exists", "Index already exists.", JSArray("create_index"), db)

        qassertErr(CreateRole(MkObject(
          "name" -> role.name,
          "membership" -> JSArray(),
          "privileges" -> MkObject(
            "resource" -> Ref(cls.refObj),
            "actions" -> MkObject("read" -> true)
          )
        )), "instance already exists", "Role already exists.", JSArray("create_role"), db.adminKey)

        qassertErr(CreateFunction(MkObject(
          "name" -> func.name,
          "body" -> QueryF(Lambda("x" -> Var("x")))
        )), "instance already exists", "Function already exists.", JSArray("create_function"), db)

        qassertErr(CreateF(Ref(inst.refObj), MkObject()),
          "instance already exists", "Instance already exists.", JSArray("create"), db)
      }
    }
  }

  "update" - {
    once("Updates are partial, and only modify values that are specified.") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        blankInst <- aDocument(cls)
      } {

        val inst = runQuery(Update(blankInst.refObj,
                                   MkObject(
                                     "data" -> MkObject("name" -> "Hen Wen",
                                                        "element" -> "air",
                                                        "cost" -> 15))),
                            db)

        inst.refObj should equal(blankInst.refObj)

        (inst / "data" / "name") should equal(JSString("Hen Wen"))
        (inst / "data" / "element") should equal(JSString("air"))
        (inst / "data" / "cost") should equal(JSLong(15))

        val update1 =
          runQuery(Update(inst.refObj,
                          MkObject("data" -> MkObject("element" -> "fire"))),
                   db)

        update1.refObj should equal(inst.refObj)
        (update1 / "data" / "name") should equal(JSString("Hen Wen"))
        (update1 / "data" / "element") should equal(JSString("fire"))
        (update1 / "data" / "cost") should equal(JSLong(15))

        val update2 = runQuery(
          Update(inst.refObj, MkObject("data" -> MkObject("cost" -> JSNull))),
          db)

        update2.refObj should equal(inst.refObj)
        (update2 / "data" / "name") should equal(JSString("Hen Wen"))
        (update2 / "data" / "element") should equal(JSString("fire"))
        (update2 / "data" / "cost").asOpt[Long] should equal(None)

        qassertErr(Update(Ref("classes/spells/104979526888456192"),
                          MkObject("data" -> MkObject("name" -> "Goji"))),
                   "invalid ref",
                   JSArray("update"),
                   db)
      }
    }

    once("are serial") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val inst =
          runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("count" -> 0))),
                   db)

        val incr =
          Let("count" -> Select(JSArray("data", "count"), Get(inst / "ref")))(
            Update(inst / "ref",
                   MkObject("data" -> MkObject("count" -> AddF(Var("count"), 1)))))

        1 to 50 map { _ =>
          implicit val ec = FaunaExecutionContext.Implicits.global
          Future {
            eventually(timeout(20.seconds)) {
              runRawQuery(incr, db.key) should respond(200)
            }
          }
        }

        eventually(timeout(30.seconds)) {
          (runQuery(Get(inst / "ref"), db) / "data" / "count").as[Int] should equal(50)
        }
      }
    }

    once("validates references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val update =
          Update(inst.refObj,
                 MkObject(
                   "data" ->
                     MkObject("foo" -> 1, "bar" -> Ref("indexes/foofoo"))))
        val err = qassertErr(update, "validation failed", JSArray(), db)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("data")
        (err / "failures" / 0 / "field" / 1).as[String] should equal("bar")
      }
    }

    once("rejects scoped refs") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.0", db)
        cls <- aFaunaClass(subDB)
        subDB2 <- aDatabase("2.0", subDB)
        doc <- aDocument(cls)
      } {
        val id = doc.refObj / "@ref" / "id"
        val subRef = RefV(id, ClsRefV(cls.name, DBRefV(subDB.name)))
        val sub2Ref = RefV(123, ClsRefV(cls.name, DBRefV(subDB2.name)))
        qassertErr(Update(subRef, MkObject("data" -> MkObject("foo" -> "bar"))),
                   "invalid argument",
                   JSArray("update"),
                   db)

        runQuery(Update(MkRef(ClassRef(cls.name), id),
                        MkObject("data" -> MkObject("foo" -> "baz"))),
                 subDB)

        val update =
          Update(MkRef(ClassRef(cls.name), id),
                 MkObject("data" -> MkObject("foo" -> 1, "bar" -> sub2Ref)))
        val err = qassertErr(update, "validation failed", JSArray(), subDB)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("data")
        (err / "failures" / 0 / "field" / 1).as[String] should equal("bar")
      }
    }

  }

  "replace" - {
    once("replace the resource ref. Values not specified are removed.") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        blankInst <- aDocument(cls)
      } {
        val inst = runQuery(Replace(blankInst.refObj,
                                    MkObject(
                                      "data" -> MkObject("name" -> "Hen Wen",
                                                         "element" -> "air",
                                                         "cost" -> 15))),
                            db)

        inst.refObj should equal(blankInst.refObj)

        (inst / "data" / "name") should equal(JSString("Hen Wen"))
        (inst / "data" / "element") should equal(JSString("air"))
        (inst / "data" / "cost") should equal(JSLong(15))

        val replaced = runQuery(
          Replace(
            inst.refObj,
            MkObject("data" -> MkObject("name" -> "Hen Wen", "element" -> "fire"))),
          db)

        replaced.refObj should equal(inst.refObj)
        (replaced / "data" / "name") should equal(JSString("Hen Wen"))
        (replaced / "data" / "element") should equal(JSString("fire"))
        (replaced / "data" / "cost").asOpt[Long] should equal(None)

        qassertErr(Replace(Ref("classes/spells/104979526888456192"),
                           MkObject("data" -> MkObject("name" -> "Goji"))),
                   "invalid ref",
                   JSArray("replace"),
                   db)
      }
    }

    once("keys with a bad secret") {
      for {
        db <- aDatabase
      } {
        val key =
          runQuery(CreateF(Ref("keys"),
                           MkObject("role" -> "server", "database" -> db.refObj)),
                   rootKey)

        qassertErr(Replace(key.refObj,
                           MkObject("role" -> "server",
                                    "database" -> db.refObj,
                                    "secret" -> "badsecret")),
                   "validation failed",
                   JSArray(),
                   rootKey)
      }
    }

    once("validates references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val replace = Replace(inst.refObj,
                              MkObject(
                                "data" ->
                                  MkObject("foo" -> 1, "bar" -> db.refObj)))
        val err = qassertErr(replace, "validation failed", JSArray(), db)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("data")
        (err / "failures" / 0 / "field" / 1).as[String] should equal("bar")
      }
    }

  }

  "insert" - {
    prop("inserts events") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db)
        inst <- aDocument(cls)
        instTS = (inst / "ts").as[Long]
        // NB: All reads occur after MVT, and before inst ts.
        deltaTS <- Prop.int(2 until 5.minutes.toMicros.toInt)
        ts = instTS - deltaTS
      } {
        // assert document has no history
        qassertErr(Get(inst.refObj, instTS - 1), "instance not found", JSArray(), db)
        runQuery(InsertVers(inst.refObj, ts, "create"), db.key)
        runQuery(InsertVers(inst.refObj, ts + 1, "delete"), db.key)
        qassertErr(Get(inst.refObj, ts - 1), "instance not found", JSArray(), db)
        runQuery(Get(inst.refObj, ts), db.key)
        qassertErr(Get(inst.refObj, ts + 1), "instance not found", JSArray(), db)

      }
    }

    prop("inserts events using @ts values") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db)
        inst <- aDocument(cls)
        instTS = (inst / "ts").as[Long]
        // NB: All reads occur after MVT, and before inst ts.
        deltaTS <- Prop.int(2 until 5.minutes.toMicros.toInt)
        ts = instTS - deltaTS
      } {
        val tsStr = microsToISOString(ts)
        val afterTsStr = microsToISOString(ts + deltaTS)
        val beforeTsStr = microsToISOString(ts - deltaTS)

        val create = runQuery(InsertVers(inst.refObj, Time(tsStr), "create"), db.key)
        (create / "instance") should equal(inst.refObj)
        (create / "ts") should equal(JSLong(ts))

        val delete =
          runQuery(InsertVers(inst.refObj, Time(afterTsStr), "delete"), db.key)
        (delete / "instance") should equal(inst.refObj)
        (delete / "ts") should equal(JSLong(ts + deltaTS))

        qassertErr(Get(inst.refObj, Time(beforeTsStr)),
                   "instance not found",
                   JSArray(),
                   db)
        runQuery(Get(inst.refObj, Time(tsStr)), db.key)
        qassertErr(Get(inst.refObj, Time(afterTsStr)),
                   "instance not found",
                   JSArray(),
                   db)

      }
    }

    prop("rejects natives") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        ts <- Prop.int(0 until Int.MaxValue)
      } {
        qassertErr(InsertVers(Ref("databases"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(InsertVers(db.refObj, ts, "create"),
                   "invalid argument",
                   JSArray(),
                   rootKey)
        qassertErr(InsertVers(cls.refObj, ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(InsertVers(idx.refObj, ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(InsertVers(Ref("indexes"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(InsertVers(Ref("tokens"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(InsertVers(Ref("credentials"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
      }
    }

    prop("should not sneak in credentials") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        ts <- Prop.int(1 until Int.MaxValue)
      } {
        val q =
          InsertVers(inst.refObj,
                     ts,
                     "create",
                     MkObject("data" -> MkObject("name" -> "Hen Wen"),
                              "credentials" -> MkObject("password" -> "sekrit")))
        runQuery(q, db) should excludeKeys("credentials")
      }
    }

    prop("keys") {
      for {
        db <- aDatabase
        ts <- Prop.int(1 to Int.MaxValue)
      } {
        val data = MkObject("role" -> "server", "database" -> db.refObj)
        runQuery(InsertVers(Ref("keys/1"), ts, "create", data), rootKey)
        qassertErr(InsertVers(Ref("keys"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
      }
    }

    prop("invalid refs") {
      for {
        db <- aDatabase
        ts <- Prop.int(0 until Int.MaxValue)
      } {
        qassertErr(InsertVers(db.refObj, ts, "create"),
                   "invalid ref",
                   JSArray("insert"),
                   db)

        val data = MkObject("role" -> "server", "database" -> db.refObj)
        val err = qassertErr(InsertVers(Ref("keys/1"), ts, "create", data),
                             "validation failed",
                             JSArray(),
                             db)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("database")
      }
    }

    prop("rejects scoped refs") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.0", db)
        cls <- aFaunaClass(subDB)
        subDB2 <- aDatabase("2.0", subDB)
        doc <- aDocument(cls)
        ts <- Prop.int(1 to Int.MaxValue)
      } {
        val id = doc.refObj / "@ref" / "id"
        val subRef = RefV(id, ClsRefV(cls.name, DBRefV(subDB.name)))
        val sub2Ref = RefV(123, ClsRefV(cls.name, DBRefV(subDB2.name)))
        val insert = InsertVers(subRef,
                                ts,
                                "create",
                                MkObject("data" -> MkObject("foo" -> "bar")))
        qassertErr(insert, "invalid argument", JSArray("insert"), db)

        runQuery(InsertVers(MkRef(ClassRef(cls.name), id),
                            ts,
                            "create",
                            MkObject("data" -> MkObject("foo" -> "baz"))),
                 subDB)

        val insert2 =
          InsertVers(MkRef(ClassRef(cls.name), id),
                     ts,
                     "create",
                     MkObject("data" -> MkObject("foo" -> 1, "bar" -> sub2Ref)))
        val err = qassertErr(insert2, "validation failed", JSArray(), subDB)
        (err / "failures" / 0 / "code").as[String] should equal("invalid reference")
        (err / "failures" / 0 / "field" / 0).as[String] should equal("data")
        (err / "failures" / 0 / "field" / 1).as[String] should equal("bar")
      }
    }

    prop("cannot insert into the future") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val future = ToTime(AddF(ToMillis(Time("now")), 100))
        qassertErr(InsertVers(inst.refObj, future, "create"), "invalid write time", JSArray(), db)
        runQuery(InsertVers(inst.refObj, Time("now"), "create"), db)
      }
    }

    once("event insert for update should maintain correct index view") {
      testInsertWithDocActionIndexView("update")
    }
    once("event insert for create should maintain correct index view") {
      testInsertWithDocActionIndexView("create")
    }
    def testInsertWithDocActionIndexView(action: String) = {
      for {
        db  <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject(
              "name" -> "testIndex",
              "source" -> ClassRef(cls.name),
              "terms" -> JSArray(
                MkObject("field" -> JSArray("data", "foo"))
              ),
              "active" -> true
            )
          ),
          db.key
        )

        val d0 = runQuery(
          CreateF(
            ClassRef(cls.name),
            MkObject(
              "data" -> MkObject("foo" -> "firstVal")
            )),
          db.key
        )
        val page1 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page1 / "data").as[JSArray].length shouldEqual 1

        val d1 = runQuery(
          Update(
            d0 / "ref",
            MkObject("data" -> MkObject("foo" -> "secondVal"))
          ),
          db.key
        )

        val page2 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page2 / "data").as[JSArray].length shouldEqual 0
        val page3 = runQuery(Paginate(Match(IndexRef("testIndex"), "secondVal")), db)
        (page3 / "data").as[JSArray].length shouldEqual 1

        // insert event
        runQuery(
          InsertVers(
            d1 / "ref",
            d1 / "ts",
            action,
            MkObject(
              "data" -> MkObject("foo" -> "thirdVal")
            )
          ),
          db.key
        )
        val page4 = runQuery(Paginate(Match(IndexRef("testIndex"), "thirdVal")), db)
        (page4 / "data").as[JSArray].length shouldEqual 1
        val page5 = runQuery(Paginate(Match(IndexRef("testIndex"), "secondVal")), db)
        (page5 / "data").as[JSArray].length shouldEqual 0
        val page6 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page6 / "data").as[JSArray].length shouldEqual 0
      }
    }
  }

  "remove" - {
    prop("removes events") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db, historyDays = Some(Long.MaxValue))
        inst <- aDocument(cls)
        ts <- Prop.int(1 until Int.MaxValue)
      } {
        runQuery(InsertVers(inst.refObj, ts, "create"), db.key)
        runQuery(InsertVers(inst.refObj, ts + 1, "delete"), db.key)
        runQuery(Get(inst.refObj, ts), db.key)
        qassertErr(Get(inst.refObj, ts + 1), "instance not found", JSArray(), db)

        runQuery(RemoveVers(inst.refObj, ts + 1, "delete"), db.key) should equal(
          JSNull)
        runQuery(Get(inst.refObj, ts + 1), db.key)
        runQuery(RemoveVers(inst.refObj, ts, "create"), db.key) should equal(JSNull)
        qassertErr(Get(inst.refObj, ts), "instance not found", JSArray(), db)
      }
    }

    prop("removes events using @ts values") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        // NB: Set so all reads occur after the collection's MVT.
        deltaTS <- Prop.int(1 until 5.minutes.toMicros.toInt)
        ts = Clock.time.micros - deltaTS
      } {
        val tsStr = microsToISOString(ts)
        val afterTsStr = microsToISOString(ts + deltaTS)

        runQuery(InsertVers(inst.refObj, Time(tsStr), "create"), db.key)
        runQuery(InsertVers(inst.refObj, Time(afterTsStr), "delete"), db.key)
        runQuery(Get(inst.refObj, Time(tsStr)), db.key)
        qassertErr(Get(inst.refObj, Time(afterTsStr)),
                   "instance not found",
                   JSArray(),
                   db)

        runQuery(RemoveVers(inst.refObj, Time(afterTsStr), "delete"), db.key) should equal(
          JSNull)
        runQuery(Get(inst.refObj, Time(afterTsStr)), db.key)
        runQuery(RemoveVers(inst.refObj, Time(tsStr), "create"), db.key) should equal(
          JSNull)
        qassertErr(Get(inst.refObj, Time(tsStr)),
                   "instance not found",
                   JSArray(),
                   db)
      }
    }
    once(
      "event remove with following update should only show one index match result") {
      for {
        db  <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject(
              "name" -> "testIndex",
              "source" -> ClassRef(cls.name),
              "terms" -> JSArray(
                MkObject("field" -> JSArray("data", "foo"))
              ),
              // this issue was only visible when the original and new version of the
              // document had different values
              "values" -> JSArray(
                MkObject("field" -> JSArray("data", "bar"))
              ),
              "active" -> true
            )
          ),
          db.key
        )

        val d0 = runQuery(
          CreateF(
            ClassRef(cls.name),
            MkObject(
              "data" -> MkObject("foo" -> "A", "bar" -> "barA")
            )),
          db.key
        )
        val d1 = runQuery(
          Update(
            d0 / "ref",
            MkObject(
              "data" -> MkObject("foo" -> "B")
            )),
          db.key
        )

        runQuery(
          Update(
            d0 / "ref",
            MkObject("data" -> MkObject("foo" -> "A", "bar" -> "barB"))
          ),
          db.key
        )

        runQuery(
          RemoveVers(
            d0 / "ref",
            d1 / "ts",
            "update"
          ),
          db.key
        )

        val page = runQuery(Paginate(Match(IndexRef("testIndex"), "A")), db)
        (page / "data").as[JSArray].length shouldEqual 1
        (page / "data").as[JSArray].get(0).as[String] shouldEqual "barB"
      }
    }
    once("event remove for update should maintain correct index view") {
      testRemoveWithDocActionIndexView("update")
    }
    once("event remove for create should maintain correct index view") {
      testRemoveWithDocActionIndexView("create")
    }
    def testRemoveWithDocActionIndexView(action: String) = {
      for {
        db  <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(
          CreateIndex(
            MkObject(
              "name" -> "testIndex",
              "source" -> ClassRef(cls.name),
              "terms" -> JSArray(
                MkObject("field" -> JSArray("data", "foo"))
              ),
              "active" -> true
            )
          ),
          db.key
        )

        val d0 = runQuery(
          CreateF(
            ClassRef(cls.name),
            MkObject(
              "data" -> MkObject("bar" -> "bar")
            )),
          db.key
        )
        val d1 = runQuery(
          Update(
            d0 / "ref",
            MkObject(
              "data" -> MkObject("foo" -> "firstVal")
            )),
          db.key
        )
        val page1 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page1 / "data").as[JSArray].length shouldEqual 1

        val d2 = runQuery(
          Update(
            d1 / "ref",
            MkObject("data" -> MkObject("foo" -> "secondVal"))
          ),
          db.key
        )

        val page2 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page2 / "data").as[JSArray].length shouldEqual 0
        val page3 = runQuery(Paginate(Match(IndexRef("testIndex"), "secondVal")), db)
        (page3 / "data").as[JSArray].length shouldEqual 1

        runQuery(
          Update(
            d1 / "ref",
            MkObject("data" -> MkObject("foo" -> "thirdVal"))
          ),
          db.key
        )
        val page4 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page4 / "data").as[JSArray].length shouldEqual 0
        val page5 = runQuery(Paginate(Match(IndexRef("testIndex"), "secondVal")), db)
        (page5 / "data").as[JSArray].length shouldEqual 0
        val page6 = runQuery(Paginate(Match(IndexRef("testIndex"), "thirdVal")), db)
        (page6 / "data").as[JSArray].length shouldEqual 1

        // remove middle version
        runQuery(
          RemoveVers(
            d2 / "ref",
            d2 / "ts",
            action
          ),
          db.key
        )
        val page7 = runQuery(Paginate(Match(IndexRef("testIndex"), "thirdVal")), db)
        (page7 / "data").as[JSArray].length shouldEqual 1
        val page8 = runQuery(Paginate(Match(IndexRef("testIndex"), "secondVal")), db)
        (page8 / "data").as[JSArray].length shouldEqual 0
        val page9 = runQuery(Paginate(Match(IndexRef("testIndex"), "firstVal")), db)
        (page9 / "data").as[JSArray].length shouldEqual 0
      }
    }
    prop("rejects natives") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        ts <- Prop.int(0 until Int.MaxValue)
      } {
        qassertErr(RemoveVers(Ref("databases"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(db.refObj, ts, "create"),
                   "invalid argument",
                   JSArray(),
                   rootKey)
        qassertErr(RemoveVers(cls.refObj, ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(idx.refObj, ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(Ref("indexes"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(Ref("tokens"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(Ref("credentials"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(Ref("keys"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
        qassertErr(RemoveVers(Ref("keys/1"), ts, "create"),
                   "invalid argument",
                   JSArray(),
                   db)
      }
    }

    prop("invalid refs") {
      for {
        db <- aDatabase
        ts <- Prop.int(0 until Int.MaxValue)
      } {
        qassertErr(RemoveVers(db.refObj, ts, "create"),
                   "invalid ref",
                   JSArray("remove"),
                   db)
      }
    }

    prop("rejects scoped refs") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.0", db)
        cls <- aFaunaClass(subDB)
        ts <- Prop.int(0 until Int.MaxValue)
      } {
        val ref = RefV(123, ClsRefV(cls.name, DBRefV(subDB.name)))
        qassertErr(RemoveVers(ref, ts, "create"),
                   "invalid argument",
                   JSArray("remove"),
                   db)
      }
    }
  }

  "delete" - {
    once("delete removes a resource") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        runQuery(DeleteF(inst.refObj), db)
        qassertErr(Get(inst.refObj), "instance not found", JSArray(), db)
        qassertErr(DeleteF(inst.refObj), "instance not found", JSArray(), db)
      }
    }

    once("rejects scoped refs") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.0", db)
        cls <- aFaunaClass(subDB)
      } {
        val ref = RefV(123, ClsRefV(cls.name, DBRefV(subDB.name)))
        qassertErr(DeleteF(ref), "invalid argument", JSArray("delete"), db)
      }
    }

    once("removes a pending write (with indexes)") {
      for {
        db  <- aDatabase
        col <- aCollection(db)
        idx <- anIndex(col, termProp = Prop.const(Nil))
        doc <- aDocument(col)

        updateAndDelete =
          runQuery(
            Do(
              Update(doc.refObj, MkObject("data" -> MkObject("foo" -> "bar"))),
              DeleteF(doc.refObj)
            ),
            db
          )

        paginateAfterDelete =
          runQuery(
            Paginate(
              Match(idx.refObj)
            ),
            db
          )
      } yield {
        (updateAndDelete / "data") should containKeys("foo")
        (paginateAfterDelete / "data") shouldBe empty
      }
    }
  }

  "actions" - {
    once("insert/remove should accept actions") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db, historyDays = Some(Long.MaxValue))
        inst <- aDocument(cls)
        ts <- Prop.int(1 until Int.MaxValue)
      } {
        runQuery(
          Let(
            "action" -> Select("action", InsertVers(inst.refObj, ts, "create"))
          ) {
            InsertVers(inst.refObj, ts+1, Var("action"))
          },
          db.key
        )

        runQuery(
          MapF(
            Lambda("event" -> Concat(JSArray(ToString(Select("action", Var("event"))), "foo"), "/")),
            Paginate(Events(inst.refObj))
          ),
          db.key
        ) should equal (JSObject("data" -> JSArray("create/foo", "update/foo", "update/foo")))
      }
    }

    once("doc actions should convert from/to string") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        ts <- Prop.int(1 until Int.MaxValue)
      } {
        runQuery(
          InsertVers(inst.refObj, ts, Concat(JSArray("cre", "ate"))),
          db.key
        )

        runQuery(
          RemoveVers(inst.refObj, ts+1, Concat(JSArray("up", "date"))),
          db.key
        ) should equal(JSNull)

        runQuery(
          DeleteF(inst.refObj),
          db.key
        )

        runQuery(
          MapF(
            Lambda("event" -> Concat(JSArray(ToString(Select("action", Var("event"))), "foo"), "/")),
            Paginate(Events(inst.refObj))
          ),
          db.key
        ) should equal (JSObject("data" -> JSArray("create/foo", "update/foo", "delete/foo")))
      }
    }

    once("set actions should convert to string") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty))
      } {
        val inst = runQuery(
          CreateF(cls.refObj, MkObject("data" -> MkObject())),
          db.key
        )

        runQuery(
          DeleteF(inst / "ref"),
          db.key
        )

        runQuery(
          MapF(
            Lambda("event" -> Concat(JSArray(ToString(Select("action", Var("event"))), "foo"), "/")),
            Paginate(Events(Match(idx.refObj)))
          ),
          db.key
        ) should equal (JSObject("data" -> JSArray("add/foo", "remove/foo")))
      }
    }
  }

  "move_database" - {
    once("should work") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB2 <- aDatabase("2.1", db)
      } {
        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data") shouldBe JSArray(subDB1.refObj, subDB2.refObj)
        (runQuery(Paginate(DatabasesRef), subDB2.adminKey) / "data") shouldBe JSArray.empty

        val ret = runQuery(MoveDatabase(subDB1.refObj, subDB2.refObj), db.adminKey)
        (ret / "name").as[String] shouldBe subDB1.name
        (ret / "global_id").as[String] shouldBe subDB1.globalID

        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data") shouldBe JSArray(subDB2.refObj)
        (runQuery(Paginate(DatabasesRef), subDB2.adminKey) / "data") shouldBe JSArray(subDB1.refObj)
      }
    }

    once("should move to parent's sibling") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB1_2 <- aDatabase("2.1", subDB1)
        subDB2 <- aDatabase("2.1", db)
      } {
        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data") shouldBe JSArray(subDB1.refObj, subDB2.refObj)
        (runQuery(Paginate(DatabasesRef), subDB1.adminKey) / "data") shouldBe JSArray(subDB1_2.refObj)
        (runQuery(Paginate(DatabasesRef), subDB2.adminKey) / "data") shouldBe JSArray.empty

        runQuery(MoveDatabase(DatabaseRef(subDB1_2.name, subDB1.refObj), subDB2.refObj), db.adminKey)

        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data") shouldBe JSArray(subDB1.refObj, subDB2.refObj)
        (runQuery(Paginate(DatabasesRef), subDB1.adminKey) / "data") shouldBe JSArray.empty
        (runQuery(Paginate(DatabasesRef), subDB2.adminKey) / "data") shouldBe JSArray(subDB1_2.refObj)
      }
    }

    once("should move to parent's parent") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB1_1 <- aDatabase("2.1", subDB1)
        subDB1_1_1 <- aDatabase("2.1", subDB1_1)
      } {
        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data") shouldBe JSArray(subDB1.refObj)
        (runQuery(Paginate(DatabasesRef), subDB1.adminKey) / "data") shouldBe JSArray(subDB1_1.refObj)
        (runQuery(Paginate(DatabasesRef), subDB1_1.adminKey) / "data") shouldBe JSArray(subDB1_1_1.refObj)

        runQuery(MoveDatabase(DatabaseRef(subDB1_1_1.name, DatabaseRef(subDB1_1.name, subDB1.refObj)), subDB1.refObj), db.adminKey)

        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data") shouldBe JSArray(subDB1.refObj)
        (runQuery(Paginate(DatabasesRef), subDB1.adminKey) / "data") shouldBe JSArray(subDB1_1.refObj, subDB1_1_1.refObj)
        (runQuery(Paginate(DatabasesRef), subDB1_1.adminKey) / "data") shouldBe JSArray.empty
      }
    }

    once("should have admin privileges to move") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB2 <- aDatabase("2.1", db)
      } {
        qassertErr(
          MoveDatabase(subDB1.refObj, subDB2.refObj),
          "permission denied",
          "Insufficient privileges to perform the action.",
          JSArray.empty,
          db.key
        )

        qassertErr(
          MoveDatabase(subDB1.refObj, subDB2.refObj),
          "permission denied",
          "Insufficient privileges to perform the action.",
          JSArray.empty,
          db.clientKey
        )
      }
    }

    once("should not move if name already exists") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB2 <- aDatabase("2.1", db)
      } {
        runQuery(CreateDatabase(MkObject("name" -> subDB1.name)), subDB2.adminKey)

        qassertErr(
          MoveDatabase(subDB1.refObj, subDB2.refObj),
          "instance already exists",
          "Database already exists.",
          JSArray.empty,
          db.adminKey
        )
      }
    }

    once("should not move to itself") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
      } {
        qassertErr(
          MoveDatabase(subDB1.refObj, subDB1.refObj),
          "move database error",
          s"Cannot move database '${subDB1.name}' into parent '${subDB1.name}': Cannot move database from ancestor to descendant.",
          JSArray.empty,
          db.adminKey
        )
      }
    }

    once("should not move to its child") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB1_1 <- aDatabase("2.1", subDB1)
      } {
        qassertErr(
          MoveDatabase(subDB1.refObj, DatabaseRef(subDB1_1.name, subDB1.refObj)),
          "move database error",
          s"Cannot move database '${subDB1.name}' into parent '${subDB1_1.name}': Cannot move database from ancestor to descendant.",
          JSArray.empty,
          db.adminKey
        )
      }
    }

    once("should fail if databases doesn't exists") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        dbName <- aUniqueDBName
      } {
        qassertErr(
          MoveDatabase(DatabaseRef(dbName), subDB1.refObj),
          "invalid ref",
          s"Ref refers to undefined database '$dbName'",
          JSArray("move_database"),
          db.adminKey
        )

        qassertErr(
          MoveDatabase(subDB1.refObj, DatabaseRef(dbName)),
          "invalid ref",
          s"Ref refers to undefined database '$dbName'",
          JSArray("to"),
          db.adminKey
        )
      }
    }

    once("should keep data and keys after move") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB2 <- aDatabase("2.1", db)
        cls <- aCollection(subDB1)
        idx <- anIndex(cls, Prop.const(List.empty))
        inst <- aDocument(cls)
      } {
        runQuery(MoveDatabase(subDB1.refObj, subDB2.refObj), db.adminKey)

        (runQuery(Get(cls.refObj), subDB1.key) / "ref") shouldBe cls.refObj
        (runQuery(Get(idx.refObj), subDB1.key) / "ref") shouldBe idx.refObj
        (runQuery(Get(inst.refObj), subDB1.key) / "ref") shouldBe inst.refObj
        (runQuery(Paginate(Match(idx.refObj)), subDB1.key) / "data") shouldBe JSArray(inst.refObj)
      }
    }

    once("should keep tokens after move") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB2 <- aDatabase("2.1", db)
        user <- aUser(subDB1)
      } {
        runQuery(Identity(), user.token) shouldBe user.refObj

        runQuery(MoveDatabase(subDB1.refObj, subDB2.refObj), db.adminKey)

        runQuery(Identity(), user.token) shouldBe user.refObj
      }
    }

    once("should preserve metadata") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        dbName <- aUniqueDBName
      } {
        runQuery(CreateDatabase(MkObject("name" -> dbName, "data" -> MkObject("xxx" -> "yyy"))), db.adminKey)

        runQuery(MoveDatabase(DatabaseRef(dbName), subDB1.refObj), db.adminKey)

        (runQuery(Get(DatabaseRef(dbName, subDB1.refObj)), db.adminKey) / "data") shouldBe JSObject("xxx" -> "yyy")
      }
    }

    once("create more data after move") {
      for {
        db <- aDatabase
        subDB1 <- aDatabase("2.1", db)
        subDB2 <- aDatabase("2.1", db)
        cls <- aCollection(subDB1)
        idx <- anIndex(cls, Prop.const(List.empty), Prop.const(List((JSArray("ref"), false), (JSArray("data", "foo"), false))))
        inst1 <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1)))
        inst2 <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 2)))
      } {
        runQuery(MoveDatabase(subDB1.refObj, subDB2.refObj), db.adminKey)

        val inst3 = runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> 3))), subDB1)
        (runQuery(Paginate(Match(idx.refObj)), subDB1) / "data") shouldBe JSArray(
          JSArray(inst1.refObj, 1),
          JSArray(inst2.refObj, 2),
          JSArray(inst3.refObj, 3)
        )
      }
    }
  }

  private def microsToISOString(micros: Long) =
    ISO_INSTANT.format(
      Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000))
}

class WriteFunctionsV27Spec extends QueryAPI27Spec {

  "credentials sidecar" - {
    once("create + update with admin key allows creds") {
      for {
        db  <- aDatabase
        cls <- aCollection(db)
      } {
        val res1 = runQuery(
          CreateF(
            cls.refObj,
            MkObject("credentials" -> MkObject("password" -> "sec1"))),
          db.adminKey)

        (res1 / "credentials") should matchPattern { case JSNotFound(_) => }

        runQuery(Login(res1.refObj, MkObject("password" -> "sec1")), db)

        val res2 = runQuery(
          Update(
            res1.refObj,
            MkObject("credentials" -> MkObject("password" -> "sec2"))),
          db.adminKey)

        (res2 / "credentials") should matchPattern { case JSNotFound(_) => }

        runQuery(Login(res1.refObj, MkObject("password" -> "sec2")), db)
      }
    }
  }

  "create" - {
    once(
      "create a document of the collection referred to by collection_ref, using params_object") {
      for {
        db <- aDatabase
        user <- aUser(db)
        cls <- aCollection(db)
      } {
        runQuery(CreateF(cls.refObj,
          MkObject(
            "data" -> MkObject("name" -> "Thunder",
              "element" -> "air",
              "cost" -> 15))),
          db)

        runQuery(
          CreateF(cls.refObj,
            MkObject("data" -> MkObject("name" -> "Thunder",
              "element" -> "air",
              "cost" -> 15),
              "credentials" -> MkObject("password" -> "sekrit"))),
          db)

        qassertErr(CreateF(cls.refObj,
          MkObject(
            "data" -> MkObject("name" -> "Thunder",
              "element" -> "air",
              "cost" -> 15))),
          "permission denied",
          JSArray("create"),
          user)

        qassertErr(
          CreateF(Ref("classes/spells/104979526888456192"),
            MkObject(
              "data" -> MkObject("name" -> "Thunder",
                "element" -> "air",
                "cost" -> 15))),
          "invalid ref",
          JSArray("create"),
          db
        )
      }
    }

    once("create an documents and paginate over its events") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val create =
          CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> 1, "bar" -> 4)))
        val query =
          Let("inst" -> Select("ref", create))(Paginate(Events(Var("inst"))))
        val result = runQuery(query, db)
        val ref = result / "data" / 0 / "document"
        runQuery(Paginate(Events(ref)), db) / "data" should equal(result / "data")
      }
    }

    once("shorthand create collection") {
      for {
        db <- aDatabase
      } {
        val createCollection = CreateCollection(MkObject("name" -> "fooCollection"))
        runQuery(createCollection, db) / "ref" should equal(ClsRefV("fooCollection"))
      }
    }

    once("rejects duplicate instances") {
      for {
        db <- aDatabase
        subDB <- aDatabase("2.1", db)
        cls <- aCollection(db)
        inst <- aDocument(cls)
        idx <- anIndex(cls, Prop.const(List.empty))
        role <- aRole(db, Privilege.open(cls.refObj))
        func <- aFunc(db)
      } {
        qassertErr(CreateDatabase(MkObject("name" -> subDB.name)),
          "instance already exists", "Database already exists.", JSArray("create_database"), db.adminKey)

        qassertErr(CreateCollection(MkObject("name" -> cls.name)),
          "instance already exists", "Collection already exists.", JSArray("create_collection"), db)

        qassertErr(CreateIndex(MkObject("name" -> idx.name, "source" -> ClassRef(cls.name))),
          "instance already exists", "Index already exists.", JSArray("create_index"), db)

        qassertErr(CreateRole(MkObject(
          "name" -> role.name,
          "membership" -> JSArray(),
          "privileges" -> MkObject(
            "resource" -> Ref(cls.refObj),
            "actions" -> MkObject("read" -> true)
          )
        )), "instance already exists", "Role already exists.", JSArray("create_role"), db.adminKey)

        qassertErr(CreateFunction(MkObject(
          "name" -> func.name,
          "body" -> QueryF(Lambda("x" -> Var("x")))
        )), "instance already exists", "Function already exists.", JSArray("create_function"), db)

        qassertErr(CreateF(Ref(inst.refObj), MkObject()),
          "instance already exists", "Document already exists.", JSArray("create"), db)
      }
    }
  }
}
