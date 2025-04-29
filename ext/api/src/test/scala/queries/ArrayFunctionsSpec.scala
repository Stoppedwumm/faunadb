package fauna.api.test.queries

import fauna.api.test._
import fauna.ast.ReadAdaptor
import fauna.codex.json._
import fauna.prop.api.Database
import fauna.prop.Prop

class ArrayFunctionsSpec extends QueryAPI21Spec {

  "foreach" - {
    once("applies a function to an array") {
      for {
        db <- aDatabase
      } {
        val add = Foreach(Lambda("elem" -> AddF(Var("elem"), 2)), JSArray(1, 2, 3))
        qequals(JSArray(1, 2, 3), add, db)

        val add2 = Foreach(Lambda("elem" -> AddF(Var("elem"), 2)), JSArray())
        qequals(JSArray(), add2, db)
      }
    }

    prop("applies a function to a page") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val q = Foreach(Lambda("elem" -> Select(0, Var("elem"))), Paginate(set))
        qequals(Paginate(set), q, db)
      }
    }
  }

  "map" - {
    once("applies a function to an array") {
      for {
        db <- aDatabase
      } {
        val add = MapF(Lambda("elem" -> AddF(Var("elem"), 1)), JSArray(1, 2, 3))
        qequals(JSArray(2, 3, 4), add, db)

        val add2 = MapF(Lambda("elem" -> AddF(Var("elem"), 1)), JSArray())
        qequals(JSArray(), add2, db)
      }
    }

    prop("applies a function to a page") {
      for {
        db <- aDatabase
        (_, _, count, set) <- aMatchSet(db)
      } {
        val result = for (_ <- 0 until count) yield JSLong(1)
        val q =
          Select("data",
                 MapF(Lambda("elem" ->
                        Select(JSArray("data", "foo"), Get(Select(1, Var("elem"))))),
                      Paginate(set)))
        qequals(JSArray(result: _*), q, db)
      }
    }
  }

  "prepend" - {
    once("prepends to an AbstractArray") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(1, 2), Prepend(JSArray(1), JSArray(2)), db)
        qequals(JSArray(1, 2), Prepend(JSArray(), JSArray(1, 2)), db)
        qequals(JSArray(1, 2), Prepend(1, JSArray(2)), db)
        qassertErr(Prepend(JSArray(1), 2),
                   "invalid argument",
                   JSArray("collection"),
                   db)
      }
    }
  }

  "append" - {
    once("appends to an AbstractArray") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(2, 1), Append(JSArray(1), JSArray(2)), db)
        qequals(JSArray(2, 1), Append(JSArray(), JSArray(2, 1)), db)
        qequals(JSArray(2, 1), Append(1, JSArray(2)), db)
        qassertErr(Append(JSArray(1), 2),
                   "invalid argument",
                   JSArray("collection"),
                   db)
      }
    }
  }

  "take" - {
    once("takes from the head of an array") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(1, 2), Take(2, JSArray(1, 2, 3)), db)
        qequals(JSArray(), Take(-1, JSArray(1, 2, 3)), db)
        qassertErr(Take(1, 1), "invalid argument", "Array or Page expected, Integer provided.", JSArray("collection"), db)
      }
    }

    once("doesn't support sets") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Take(1, DatabaseNativeClassRef),
          "invalid argument",
          "Array or Page expected, Collection Ref provided.",
          JSArray("collection"),
          db
        )
      }
    }

    once("should not break bindings") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        name <- aName
      } {
        runQuery(
          CreateIndex(MkObject(
            "name" -> name,
            "source" -> MkObject(
              "collection" -> coll.refObj,
              "fields" -> MkObject(
                "a-field" -> QueryF(Lambda("i" -> Take(2, JSArray(1, 2, 3))))
              )
            ),
            "values" -> JSArray(
              MkObject("field" -> "ref"),
              MkObject("binding" -> "a-field")
            )
          )),
          db
        )

        val doc = runQuery(CreateF(coll.refObj), db)

        val page = runQuery(Paginate(Match(name)), db)

        (page / "data") shouldBe JSArray(JSArray(doc.refObj, 1), JSArray(doc.refObj, 2))
      }
    }
  }

  "drop" - {
    once("drops from the head of an array") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(3), Drop(2, JSArray(1, 2, 3)), db)
        qequals(JSArray(1, 2, 3), Drop(-1, JSArray(1, 2, 3)), db)
        qassertErr(Drop(1, 1), "invalid argument", JSArray("collection"), db)
      }
    }
  }

  "is_empty" - {
    once("works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))))
      } {
        runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db)

        //array
        qequals(true, IsEmpty(JSArray.empty), db)
        qequals(false, IsEmpty(JSArray(1)), db)

        //page
        qequals(true, IsEmpty(Paginate(Match(idx.refObj, "baz"))), db)
        qequals(false, IsEmpty(Paginate(Match(idx.refObj, "bar"))), db)

        //set
        qequals(true, IsEmpty(Match(idx.refObj, "baz")), db)
        qequals(false, IsEmpty(Match(idx.refObj, "bar")), db)

        qassertErr(
          IsEmpty(1),
          "invalid argument",
          "Array, Set, or Page expected, Integer provided.",
          JSArray("is_empty"),
          db
        )
      }
    }
  }

  "is_nonempty" - {
    once("works") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))))
      } {
        runQuery(CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db)

        //array
        qequals(false, IsNonEmpty(JSArray.empty), db)
        qequals(true, IsNonEmpty(JSArray(1)), db)

        //page
        qequals(false, IsNonEmpty(Paginate(Match(idx.refObj, "baz"))), db)
        qequals(true, IsNonEmpty(Paginate(Match(idx.refObj, "bar"))), db)

        //set
        qequals(false, IsNonEmpty(Match(idx.refObj, "baz")), db)
        qequals(true, IsNonEmpty(Match(idx.refObj, "bar")), db)

        qassertErr(
          IsNonEmpty(1),
          "invalid argument",
          "Array, Set, or Page expected, Integer provided.",
          JSArray("is_nonempty"),
          db
        )
      }
    }
  }

  once("takes and drops from a page") {
    for {
      db <- aDatabase
      cls <- aCollection(db)
    } {
      runQuery(
        CreateIndex(
          MkObject(
            "name" -> "takeindex",
            "source" -> cls.refObj,
            "active" -> true,
            "terms" -> JSArray(MkObject("field" -> JSArray("data", "foo"))),
            "values" -> JSArray(MkObject("field" -> JSArray("data", "bar")))
          )),
        db
      )

      waitForTaskExecution()

      (0 until 15) foreach { i =>
        runQuery(CreateF(cls.refObj,
                         MkObject("data" -> MkObject("foo" -> "x", "bar" -> i))),
                 db)
      }

      val p = Paginate(Match(Ref("indexes/takeindex"), "x"), After(5), size = 5)
      // Map to drop the tuples from data, initial drop(0) to make "before" into a tuple for consistency
      assertPage(p, db, 5, 10, JSArray(5, 6, 7, 8, 9))

      // Take some
      assertPage(Take(3, p), db, 5, 8, JSArray(5, 6, 7))
      // Take all
      assertPage(Take(5, p), db, 5, 10, JSArray(5, 6, 7, 8, 9))
      // Take too many. Should be same as take all
      assertPage(Take(8, p), db, 5, 10, JSArray(5, 6, 7, 8, 9))
      // Take none. after becomes equal to before
      assertPage(Take(0, p), db, 5, 5, JSArray.empty)
      // Take less than none; same as taking none.
      assertPage(Take(-3, p), db, 5, 5, JSArray.empty)

      // Drop some
      assertPage(Drop(2, p), db, 7, 10, JSArray(7, 8, 9))
      // Drop all. Before becomes equal to after
      assertPage(Drop(5, p), db, 10, 10, JSArray.empty)
      // Drop too many. Should be same as drop all
      assertPage(Drop(8, p), db, 10, 10, JSArray.empty)
      // Drop none. Unchanged.
      assertPage(Drop(0, p), db, 5, 10, JSArray(5, 6, 7, 8, 9))
      // Drop less than none. Unchanged.
      assertPage(Drop(-3, p), db, 5, 10, JSArray(5, 6, 7, 8, 9))

      // Map page values to x+10,...
      val m = MapF(Lambda("x" -> AddF(Var("x"), 10)), p)
      assertPage(m, db, 5, 10, JSArray(15, 16, 17, 18, 19))

      // ... then test take/drop on the mapped page. Cursors should still take underlying values.
      // Take some
      assertPage(Take(3, m), db, 5, 8, JSArray(15, 16, 17))
      // Take all
      assertPage(Take(5, m), db, 5, 10, JSArray(15, 16, 17, 18, 19))
      // Take too many. Should be same as take all
      assertPage(Take(8, m), db, 5, 10, JSArray(15, 16, 17, 18, 19))
      // Take none. after becomes equal to before
      assertPage(Take(0, m), db, 5, 5, JSArray.empty)
      // Take less than none; same as taking none.
      assertPage(Take(-3, m), db, 5, 5, JSArray.empty)

      // Drop some
      assertPage(Drop(2, m), db, 7, 10, JSArray(17, 18, 19))
      // Drop all. Before becomes equal to after
      assertPage(Drop(5, m), db, 10, 10, JSArray.empty)
      // Drop too many. Should be same as drop all
      assertPage(Drop(8, m), db, 10, 10, JSArray.empty)
      // Drop none. Unchanged.
      assertPage(Drop(0, m), db, 5, 10, JSArray(15, 16, 17, 18, 19))
      // Drop less than none. Unchanged.
      assertPage(Drop(-3, m), db, 5, 10, JSArray(15, 16, 17, 18, 19))

      // Filter the mapped page leaving only the even values, ...
      val f = Filter(Lambda("i" -> Equals(0, Modulo(Var("i"), 2))), m)
      assertPage(f, db, 5, 10, JSArray(16, 18))

      // ... then test take/drop on the filtered page. Cursors should still take underlying values.
      // Take some
      assertPage(Take(1, f), db, 5, 8, JSArray(16))
      // Take all
      assertPage(Take(2, f), db, 5, 10, JSArray(16, 18))
      // Take too many. Should be same as take all
      assertPage(Take(5, f), db, 5, 10, JSArray(16, 18))
      // Take none. NOTE: "after" becomes equal to unmappedData[0] which is not the same as "before"!
      assertPage(Take(0, f), db, 5, 6, JSArray.empty)
      // Take less than none. NOTE: "after" becomes equal to "before".
      assertPage(Take(-3, f), db, 5, 5, JSArray.empty)

      // Drop some. Before becomes equal to new unmappedData[0]
      assertPage(Drop(1, f), db, 8, 10, JSArray(18))
      // Drop all. Before becomes equal to after
      assertPage(Drop(2, f), db, 10, 10, JSArray.empty)
      // Drop too many. Should be same as drop all
      assertPage(Drop(5, f), db, 10, 10, JSArray.empty)
      // Drop none. NOTE: "before" becomes unmappedData[0] which is not the same as "before".
      assertPage(Drop(0, f), db, 6, 10, JSArray(16, 18))
      // Drop less than none. Unchanged.
      assertPage(Drop(-3, f), db, 5, 10, JSArray(16, 18))

      // Filter the mapped page to not have any elements
      val empty = Filter(Lambda("i" -> JSFalse), m)
      assertPage(empty, db, 5, 10, JSArray.empty)

      assertPage(Take(-1, empty), db, 5, 5, JSArray.empty)
      assertPage(Take(0, empty), db, 5, 10, JSArray.empty)
      assertPage(Take(1, empty), db, 5, 10, JSArray.empty)

      assertPage(Drop(-1, empty), db, 5, 10, JSArray.empty)
      assertPage(Drop(0, empty), db, 10, 10, JSArray.empty)
      assertPage(Drop(1, empty), db, 10, 10, JSArray.empty)
    }
  }

  def assertPage(
    p: JSValue,
    db: Database,
    before: Int,
    after: Int,
    data: JSArray): Unit = {
    qequals(before, Select(JSArray("before", 0), p), db)
    qequals(after, Select(JSArray("after", 0), p), db)
    qequals(data, Select("data", p), db)
  }
}

class ArrayFunctionsUnstableSpec extends QueryAPIUnstableSpec {
  "take" - {
    once("takes from the head of an array") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(1, 2), Take(2, JSArray(1, 2, 3)), db)
        qequals(JSArray(1, 2, 3), Take(10, JSArray(1, 2, 3)), db)
        qassertErr(
          Take(1, 1),
          "invalid argument",
          "Array, Set, or Page expected, Integer provided.",
          JSArray("collection"),
          db
        )
      }
    }

    once("takes from the tail of an array") {
      for {
        db <- aDatabase
      } {
        qequals(JSArray(2, 3), Take(-2, JSArray(1, 2, 3)), db)
        qequals(JSArray(1, 2, 3), Take(-10, JSArray(1, 2, 3)), db)
        qassertErr(
          Take(1, 1),
          "invalid argument",
          "Array, Set, or Page expected, Integer provided.",
          JSArray("collection"),
          db
        )
      }
    }

    once("takes from the head of pages") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(Foreach(Lambda("i" -> CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 100), db)

        val result0 = runQuery(Take(10, Paginate(Match(idx.refObj))), db)
        (result0 / "data") shouldBe JSArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        (result0 / "after" / 0) shouldBe JSLong(11)

        val result1 = runQuery(Take(10, Paginate(Match(idx.refObj), cursor = After(result0 / "after"))), db)
        (result1 / "data") shouldBe JSArray(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
        (result1 / "after" / 0) shouldBe JSLong(21)
      }
    }

    once("takes from the tail of pages") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(Foreach(Lambda("i" -> CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 100), db)

        val result0 = runQuery(Take(-10, Paginate(Match(idx.refObj))), db)
        (result0 / "data") shouldBe JSArray(55, 56, 57, 58, 59, 60, 61, 62, 63, 64)
        (result0 / "after" / 0) shouldBe JSLong(65)

        val result1 = runQuery(Take(-10, Paginate(Match(idx.refObj), cursor = After(result0 / "after"))), db)
        (result1 / "data") shouldBe JSArray(91, 92, 93, 94, 95, 96, 97, 98, 99, 100)
        (result1 / "before" / 0) shouldBe JSLong(65)
      }
    }

    once("takes from the head of sets") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty))
        docs <- someDocuments(100, coll)
      } {
        val refs = docs sortBy { _.id } map { _.refObj }

        val result0 = runQuery(Take(10, Match(idx.refObj)), db)
        val expected0 = refs take 10
        result0 shouldBe JSArray(expected0: _*)

        val result1 = runQuery(Take(10, Reverse(Match(idx.refObj))), db)
        val expected1 = refs.reverse take 10
        result1 shouldBe JSArray(expected1: _*)
      }
    }

    once("takes from the head of sets / covered values") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(Foreach(Lambda("i" -> CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 100), db)

        val result0 = runQuery(Take(10, Match(idx.refObj)), db)
        result0 shouldBe JSArray(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

        val result1 = runQuery(Take(10, Reverse(Match(idx.refObj))), db)
        result1 shouldBe JSArray(100, 99, 98, 97, 96, 95, 94, 93, 92, 91)
      }
    }

    once("takes from the tail of sets") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty))
        docs <- someDocuments(100, coll)
      } {
        val refs = docs sortBy { _.id } map { _.refObj }

        val result0 = runQuery(Take(-10, Match(idx.refObj)), db)
        val expected0 = refs drop 90
        result0 shouldBe JSArray(expected0: _*)

        val result1 = runQuery(Take(-10, Reverse(Match(idx.refObj))), db)
        val expected1 = refs.reverse drop 90
        result1 shouldBe JSArray(expected1: _*)
      }
    }

    once("takes from the tail of sets / covered values") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(Foreach(Lambda("i" -> CreateF(coll.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 100), db)

        val result0 = runQuery(Take(-10, Match(idx.refObj)), db)
        result0 shouldBe JSArray(91, 92, 93, 94, 95, 96, 97, 98, 99, 100)

        val result1 = runQuery(Take(-10, Reverse(Match(idx.refObj))), db)
        result1 shouldBe JSArray(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
      }
    }

    once("should break bindings") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        name <- aName
      } {
        val error = qassertErr(
          CreateIndex(MkObject(
            "name" -> name,
            "source" -> MkObject(
              "collection" -> coll.refObj,
              "fields" -> MkObject(
                "a-field" -> QueryF(Lambda("i" -> Take(2, JSArray(1, 2, 3))))
              )
            ),
            "values" -> JSArray(
              MkObject("field" -> "ref"),
              MkObject("binding" -> "a-field")
            )
          )),
          "validation failed",
          "document data is not valid.",
          JSArray("create_index"),
          db
        )

        (error / "failures" / 0 / "field") shouldBe JSArray("source", "0", "fields", "0", "a-field")
        (error / "failures" / 0 / "code") shouldBe JSString("invalid binding")
        (error / "failures" / 0 / "description") shouldBe JSString("Field binding must be a pure unary function.")
      }
    }

    once("errors") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        qassertErr(
          Take(ReadAdaptor.MaxPageSize + 1, Documents(coll.refObj)),
          "invalid argument",
          "Number of elements must be <= 100000",
          JSArray.empty,
          db
        )

        qassertErr(
          Take(-(ReadAdaptor.MaxPageSize + 1), Documents(coll.refObj)),
          "invalid argument",
          "Number of elements must be <= 100000",
          JSArray.empty,
          db
        )
      }
    }
  }
}
