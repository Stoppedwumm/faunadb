package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop
import fauna.prop.api._

class BasicForms20Spec extends QueryAPI20Spec {
  "@ref" - {
    once("self ref behaviors") {
      for {
        db <- aDatabase
      } {

        // self-refs that should never work
        qassertErr(Ref("databases/self"), "invalid expression", JSArray(), db)
        qassertErr(Ref("index/self"), "invalid expression", JSArray(), db)
        qassertErr(Ref("classes/self"), "invalid expression", JSArray(), db)

        // impossible, for now
        qassertErr(Ref("keys/self"), "invalid expression", JSArray(), db)

        // self refs not associated with instances
        qassertErr(Ref("tokens/self"), "invalid expression", JSArray(), db)
        qassertErr(Ref("credentials/self"), "invalid expression", JSArray(), db)
        qassertErr(Ref("classes/123/self"), "invalid expression", JSArray(), db)

        // self refs with instances
        val cls = aFaunaClass(db).sample
        val inst = aDocument(cls).sample
        val creds = mkCredentials(db, inst, "1234").sample
        val token =
          runQuery(Login(inst.refObj, MkObject("password" -> "1234")), db)
        val tknSec = (token / "secret").as[String]
        val srvKey =
          (runQuery(CreateKey(MkObject("database" -> db.refObj, "role" -> "server")),
                    rootKey) / "secret").as[String]
        runQuery(Ref("tokens/self"), tknSec) should equal(token.refObj)
        runQuery(Ref("credentials/self"), tknSec) should equal(creds.refObj)

        //token auth
        runQuery(Ref(s"classes/${cls.name}/self"), tknSec) should equal(inst.refObj)
        //document auth
        runQuery(Ref(s"classes/${cls.name}/self"), s"${db.adminKey}:@doc/${cls.name}/${inst.id}") should equal(inst.refObj)

        qassertErr(Ref("classes/self"), "invalid expression", JSArray(), tknSec)
        qassertErr(Ref("tokens/self"),
                   "invalid expression",
                   JSArray(),
                   s"$srvKey:${inst.ref}")

        val inst2 = aDocument(cls).sample
        val tkn2 =
          runQuery(CreateF(Ref("tokens"), MkObject("instance" -> inst2.refObj)), db)
        val tknSec2 = (tkn2 / "secret").as[String]
        runQuery(Ref("tokens/self"), tknSec2) should equal(tkn2.refObj)
        qassertErr(Ref("credentials/self"), "invalid expression", JSArray(), tknSec2)
      }
    }
  }
}

class BasicForms21Spec extends QueryAPI21Spec {

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
      createTestIdx(cls.refObj, db)
      for (i <- 1 to 5)
        runQuery(
          CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> 1, "bar" -> i))),
          db)

      JSObject("match" -> 1, "index" -> Ref("indexes/foo"))
    }

  "at" - {
    once("sets the valid time of a query") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        qequals(Select("ref", Get(inst.refObj)), inst.refObj, db)
        qequals(At(Time("now"), Select("ref", Get(inst.refObj))), inst.refObj, db)
        // NB: Query after the collection's MVT.
        qassertErr(At(TimeAdd(Time("now"), -10, "seconds"), Get(inst.refObj)),
                   "instance not found",
                   JSArray("expr"),
                   db)
      }
    }

    once("does not allow writes if the time has changed") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        runQuery(At(Time("now"), Update(inst.refObj, MkObject("data" -> MkObject("foo" -> "bar")))), db)
        runQuery(At(123, At(Time("now"), Update(inst.refObj, MkObject("data" -> MkObject("foo" -> "bar"))))), db)

        qassertErr(At(123, Update(inst.refObj, MkObject(
          "data" -> MkObject("foo" -> "bar")))), "invalid write time", JSArray("expr"), db)

        qassertErr(At(Time("now"), At(0, Update(inst.refObj, MkObject(
          "data" -> MkObject("foo" -> "bar"))))), "invalid write time", JSArray("expr", "expr"), db)
      }
    }

    once("transaction time reads to present") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val res = runQuery(Let(
          "i" -> CreateF(cls.refObj, MkObject())) {
          JSArray(Var("i"), At(Select("ts", Var("i")), Get(Select("ref", Var("i")))))
        }, db)

        res / "0" should equal (res / "1")
      }
    }

    once("composes with paginate") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        instA <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1)))
        instB <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> 1)))
      } {
        val tsA = runQuery(Select("ts", Get(instA.refObj)), db)
        val tsB = runQuery(Select("ts", Get(instB.refObj)), db)

        qequals(Select("data", At(tsA, Paginate(Match(idx.refObj, 1)))), JSArray(instA.refObj), db)
        qequals(Select("data", At(tsB, Paginate(Match(idx.refObj, 1)))), JSArray(instA.refObj, instB.refObj), db)

        qequals(MapF(Lambda("x" -> Select("instance", Var("x"))),
          At(tsA, Select("data", Paginate(Events(Match(idx.refObj, 1)))))), JSArray(instA.refObj), db)
        qequals(MapF(Lambda("x" -> Select("instance", Var("x"))),
          At(tsB, Select("data", Paginate(Events(Match(idx.refObj, 1)))))), JSArray(instA.refObj, instB.refObj), db)
        qequals(MapF(Lambda("x" -> Select("instance", Var("x"))),
          At(Time("now"), Select("data", Paginate(Events(Match(idx.refObj, 1)))))), JSArray(instA.refObj, instB.refObj), db)
      }
    }

    once("disallows reading past the transaction snapshot time") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        // FIXME: eventually reject future valid times.
        pendingUntilFixed {
          qassertErr(At(Time("2045-10-21T00:00:00Z"), Get(cls.refObj)),
            "invalid argument",
            "Cannot read at valid time 2045-10-21T00:00:00Z, as it is greater than the transaction snapshot time.",
            JSArray("at"),
            db)
        }
      }
    }
  }

  "let/var" - {
    once("evaluates to its body") {
      for (db <- aDatabase) {
        qequals(3, Let("x" -> 3, "y" -> 4)(Var("x")), db)
        qequals(4, Let("x" -> 3, "y" -> 4)(Var("y")), db)
        qequals("x", Let("x" -> 3, "y" -> "x")(Var("y")), db)
        qequals(3, Let()(AddF(1, 2)), db)
      }
    }

    once("allows backrefs, disallows forward refs") {
      for (db <- aDatabase) {
        qequals(3,
                Let("x" -> 3, "y" -> Var("x"))(Var("y")),
                db)

        qassertErr(Let("y" -> Var("x"), "x" -> 3)(Var("y")),
                   "invalid expression",
                   JSArray("let", "y"),
                   db)
      }
    }

    once("rejects invalid var exprs") {
      for (db <- aDatabase) {
        qassertErr(Let("x" -> 4)(Var(4)),
                   "invalid expression",
                   JSArray("in", "var"),
                   db)
      }
    }

    once("rejects unbound free variables") {
      for (db <- aDatabase) {
        qassertErr(Let()(Var("bad")),
                   "invalid expression",
                   JSArray("in"),
                   db)

        qassertErr(Let()(If(true, "was true", Var("bad"))),
                   "invalid expression",
                   JSArray("in", "else"),
                   db)

        qassertErr(Let()(AddF(2, Var("bad"))),
                   "invalid expression",
                   JSArray("in", "add", 1),
                   db)

        qassertErr(Let()(MkObject("x" -> Var("bad"))),
                   "invalid expression",
                   JSArray("in", "object", "x"),
                   db)

        qassertErr(At(Time("now"), Var("bad")),
                   "invalid expression",
                   JSArray("expr"),
                   db)

        qassertErr(At(Var("bad"), 3),
                   "invalid expression",
                   JSArray("at"),
                   db)

        qassertErr(Do(Var("bad")),
                   "invalid expression",
                   JSArray("do", 0),
                   db)

        qassertErr(MapF(Lambda("x" -> AddF(Var("x"), 2)), JSArray(1, Var("bad"))),
                   "invalid expression",
                   JSArray("collection", 1),
                   db)

        qassertErr(MapF(Lambda("x" -> AddF(Var("x"), Var("bad"))), JSArray(1, 2)),
                   "invalid expression",
                   JSArray("map", "expr", "add", 1),
                   db)
      }
    }
  }

  "lambda" - {
    once("captures closures") {
      for (db <- aDatabase) {
        qequals(Let("x" -> 2)(MapF(Lambda("y" -> AddF(Var("x"), Var("y"))), JSArray(1, 2))),
                JSArray(3, 4),
                db)
      }
    }

    once("defers evaluation") {
      for (db <- aDatabase) {
        // FIXME: Reenable.
        pendingUntilFixed {
          qequals(Let("x" -> 2, "l" -> Lambda("y" -> AddF(Var("x"), Var("y"))))(MapF(Var("l"), JSArray(1, 2))),
                  JSArray(3, 4),
                  db)
        }
      }
    }

    once("round-trips") {
      for (db <- aDatabase) {
        // FIXME: Reenable.
        pendingUntilFixed {
          val addOne = runQuery(Lambda("y" -> AddF(1, Var("y"))), db)
          val addX = runQuery(Let("x" -> 1)(Lambda("y" -> AddF(Var("x"), Var("y")))), db)

          qequals(MapF(addOne, JSArray(1, 2)),
                  JSArray(2, 3),
                  db)

          qequals(MapF(addX, JSArray(1, 2)),
                  JSArray(2, 3),
                  db)
        }
      }
    }

    once("may be stored") {
      for {
        db <- aDatabase
        c <- aCollection(db)
      } {
        // FIXME: Reenable.
        pendingUntilFixed {
          val lambdas = MkObject(
            "addOne" -> Lambda("y" -> AddF(1, Var("y"))),
            "addX" -> Let("x" -> 1)(Lambda("y" -> AddF(Var("x"), Var("y")))))

          val doc = runQuery(CreateF(c.refObj, MkObject("data" -> lambdas)), db)

          qequals(MapF(Select(JSArray("data", "addOne"), Get(doc / "ref")), JSArray(1, 2)),
                  JSArray(2, 3),
                  db)

          qequals(MapF(Select(JSArray("data", "addX"), Get(doc / "ref")), JSArray(1, 2)),
                  JSArray(2, 3),
                  db)
        }
      }
    }

    once("may be called") {
      for (db <- aDatabase) {
        // FIXME: Reenable.
        pendingUntilFixed {
          qequals(Let("incr" -> Lambda("x" -> AddF(Var("x"), 1)))(Call(Var("incr"), 1)),
                  2,
                  db)
        }
      }
    }

    once("overflows its stack ") {
      for {
        db <- aDatabase
      } {
        // FIXME: Reenable.
        pendingUntilFixed {
          val bomb = Lambda("x" -> Call(Var("x"), Var("x")))

          qassertErr(Let("bomb" -> bomb)(Call(Var("bomb"), Var("bomb"))),
                     "stack overflow",
                     "Call stack reached the maximum limit of 200.",
                     JSArray("let", "bomb", "expr"),
                     db)
        }
      }
    }
  }

  "if then else" - {
    once(
      "if evaluates and returns true_expr or false_expr depending on the value of cond") {
      for {
        db <- aDatabase
      } {
        qequals("was true", If(true, "was true", "was false"), db)
        qequals("was true", If(Equals(1, 1), "was true", "was false"), db)
        qequals("was false", If(false, "was true", "was false"), db)
        qassertErr(If("someString", "was true", "was false"),
                   "invalid argument",
                   JSArray("if"),
                   db)
      }
    }
  }

  "do" - {
    once("cannot be empty") {
      for {
        db <- aDatabase
      } {
        qassertErr(Do(), "invalid expression", JSArray("do"), db)
      }
    }

    once("can read its own version writes") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
      } {
        val name = "Hen Wen"
        qequals(name,
                Do(Update(inst.refObj, MkObject("data" -> MkObject("name" -> name))),
                   Select(JSArray("data", "name"), Get(inst.refObj))),
                db)
      }
    }

    once("can read its own index writes") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "name"))))
      } {
        val name = "Hen Wen"
        runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("name" -> name))),
                 db)

        val doQ = Do(MapF(Lambda("ref" -> DeleteF(Var("ref"))),
                          Paginate(Match(idx.refObj, name))),
                     MapF(Lambda("ref" -> Get(Var("ref"))),
                          Paginate(Match(idx.refObj, name))))

        qequals(JSArray.empty, Select("data", doQ), db)
      }
    }

    once("has sequential semantics") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {

        runQuery(
          CreateIndex(
            MkObject(
              "name" -> "customers_by_id",
              "source" -> cls.refObj,
              "terms" -> JSArray(MkObject("field" -> JSArray("data", "id"))),
              "unique" -> true
            )),
          db
        )
        runQuery(
          CreateF(
            cls.refObj,
            MkObject("data" -> MkObject("id" -> "1", "companyName" -> "F"))
          ), db)

        val query = Do(
          MapF(
            Lambda("x" -> DeleteF(Var("x"))),
            Paginate(Match(Ref("indexes/customers_by_id"),"1"))
          ),
          MapF(
            Lambda("customers_data" -> CreateF(cls.refObj, MkObject("data" -> Var("customers_data")))),
            JSArray(
              MkObject("id" -> "1", "companyName" -> "F*")
            ),
          )
        )

        (1 to 10) foreach { _ =>
          runQuery(query, db)
        }
      }
    }

    once("can delete and create a single ref") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "name"))), uniqueProp = Prop.const(true))
        _ <- aDocument(cls, dataProp = Prop.const(JSObject("name" -> "Hen Wen")))
      } {
        val doQ = Do(
          DeleteF(Select("ref", Get(Match(Ref(idx.refObj), "Hen Wen")))),
          CreateF(cls.refObj, MkObject("data" -> MkObject("name" -> "Hen Wen"))))

        qequals("Hen Wen", Select(JSArray("data", "name"), doQ), db)
      }
    }
  }

  "quote" - {
    once("returns the unevaluted form of expr") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        set <- aMatchSet(db)
        instance <- aDocument(cls)
      } {
        qequals(JSArray(-1, -1, 5), Quote(JSArray(-1, -1, 5)), db)
        qequals(Quote(JSObject("age" -> JSObject("add" -> JSArray(-1, -1, 5)))),
                Quote(JSObject("age" -> JSObject("add" -> JSArray(-1, -1, 5)))),
                db)

        qequals(instance.refObj, Quote(instance.refObj), db)
        qequals(SetRef(set), Quote(SetRef(set)), db)
      }
    }
  }

  "object" - {
    once("objects evaluate their contents") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        set <- aMatchSet(db)
        instance <- aDocument(cls)
      } {
        qequals(MkObject("age" -> 3),
                MkObject("age" -> JSObject("add" -> JSArray(-1, -1, 5))),
                db)
        qequals(MkObject("add" -> JSArray(-1, -1, 5)),
                MkObject("add" -> JSArray(-1, -1, 5)),
                db)
        qequals(MkObject("" -> instance.refObj), MkObject("" -> instance.refObj), db)
        qequals(MkObject("" -> SetRef(set)), MkObject("" -> SetRef(set)), db)
      }
    }
  }

  "@ref" - {
    once("returns itself") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        instance <- aDocument(cls)
      } {
        runQuery(instance.refObj, db) should equal(instance.refObj)
      }
    }

    once("unresolved refs are rendered as is") {
      for {
        db <- aDatabase
        name <- aName
      } {
        Seq(
          JSObject("id" -> name, "class" -> Ref(JSObject("id" -> "databases"))),
          JSObject("id" -> name, "class" -> Ref(JSObject("id" -> "indexes"))),
          JSObject("id" -> name, "class" -> Ref(JSObject("id" -> "classes"))),
          JSObject(
            "id" -> "123",
            "class" -> Ref(
              JSObject("id" -> name, "class" -> Ref(JSObject("id" -> "classes")))))
        ) foreach { ref =>
          runQuery(Ref(ref), db) should equal(Ref(ref))
        }
      }
    }
  }

  "@set" - {
    prop("returns itself") {
      for {
        db <- aDatabase
        set <- aMatchSet(db)
      } {
        qequals(SetRef(set), SetRef(set), db)
      }
    }
  }

  "@ts" - {
    once("returns itself") {
      for {
        db <- aDatabase
      } {
        runQuery(TS("1970-01-01T00:00:00Z"), db) should equal(
          TS("1970-01-01T00:00:00Z"))
        runQuery(TS("1970-01-01T00:00:00+00:00"), db) should equal(
          TS("1970-01-01T00:00:00Z"))
        qassertErr(TS("Thu, 1 Jan 1970 00:00:00 GMT"),
                   "invalid expression",
                   JSArray(),
                   db)
      }
    }

    once("range extremes") {
      for {
        db <- aDatabase
      } {
        val min = "-999999999-01-01T00:00:00Z"
        val max = "9999-12-31T23:59:59.999999999Z"
        runQuery(TS(min), db) should equal(TS(min))
        runQuery(TS(max), db) should equal(TS(max))

        qassertErr(TS("-1000000000-01-01T00:00:00Z"),
                   "invalid expression",
                   JSArray(),
                   db)
        qassertErr(TS("99999-12-31T23:59:59.999999999Z"),
                   "invalid expression",
                   JSArray(),
                   db)
      }
    }
  }

  "@date" - {
    once("returns itself") {
      for {
        db <- aDatabase
      } {
        runQuery(Date("1970-01-01"), db) should equal(Date("1970-01-01"))
        qassertErr(Date("19700101"), "invalid expression", JSArray(), db)
      }
    }
  }
}

class BasicForms27Spec extends QueryAPI27Spec {

  "@ref" - {
    once("returns itself") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        instance <- aDocument(cls)
      } {
        runQuery(instance.refObj, db) should equal(instance.refObj)
      }
    }

    once("unresolved refs are rendered as is") {
      for {
        db <- aDatabase
        name <- aName
      } {
        Seq(
          JSObject("id" -> name, "collection" -> Ref(JSObject("id" -> "databases"))),
          JSObject("id" -> name, "collection" -> Ref(JSObject("id" -> "indexes"))),
          JSObject("id" -> name, "collection" -> Ref(JSObject("id" -> "collections"))),
          JSObject(
            "id" -> "123",
            "collection" -> Ref(
              JSObject("id" -> name, "collection" -> Ref(JSObject("id" -> "collections")))))
        ) foreach { ref => {
          runQuery(Ref(ref), db) should equal(Ref(ref))
          }
        }
      }
    }
  }
}

class BasicForms4Spec extends QueryAPI4Spec {
  "select" - {
    once("handle optional arguments") {
      for {
        db <- aDatabase
      } {
        val query0 = runQuery(QueryF(Lambda("x" -> Select("x", Var("x")))), db)
        (query0 / "@query" / "expr") shouldBe JSObject("select" -> "x", "from" -> JSObject("var" -> "x"))

        val query1 = runQuery(QueryF(Lambda("x" -> Select("x", Var("x"), "default"))), db)
        (query1 / "@query" / "expr") shouldBe JSObject("select" -> "x", "from" -> JSObject("var" -> "x"), "default" -> "default")

        val query2 = runQuery(QueryF(Lambda("x" -> Select("x", Var("x"), "default", true))), db)
        (query2 / "@query" / "expr") shouldBe JSObject("select" -> "x", "from" -> JSObject("var" -> "x"), "all" -> true, "default" -> "default")
      }
    }
  }
}
