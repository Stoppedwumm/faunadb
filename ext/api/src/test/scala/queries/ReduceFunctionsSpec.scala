package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.api.{ Membership, Privilege, RoleAction }
import fauna.prop.Prop
import scala.util.Random

class ReduceFunctionsSpec extends QueryAPI3Spec {
  "Reduce" - {
    prop("arrays") {
      for {
        db <- aDatabase
        size <- Prop.int(10000)
      } {
        val values = (1 to size).toList
        val initial = 10
        val reduced = values.foldLeft(initial) { _ + _ }

        //reduce to number
        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            values
          ), db
        ) shouldBe JSLong(reduced)

        //reduce to string
        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> Concat(JSArray(Var("acc"), Var("v")))),
            "",
            Seq("Fauna", "DB", " ", "rocks")
          ), db
        ) shouldBe JSString("FaunaDB rocks")

        //reduce to object
        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> Merge(Var("acc"), MkObject("str" -> Concat(JSArray(Select("str", Var("acc")), Select(0, Var("v")))), "int" -> AddF(Select("int", Var("acc")), Select(1, Var("v")))))),
            MkObject("str" -> "", "int" -> 10),
            Seq(JSArray("Fauna", 1), JSArray("DB", 2), JSArray(" ", 3), JSArray("rocks", 4))
          ), db
        ) shouldBe JSObject("str" -> "FaunaDB rocks", "int" -> 20)
      }
    }

    prop("sets") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
        size <- Prop.int(1000 to 5000)
      } {
        val values = (1 to size).toList
        val initial = 10
        val reduced = values.foldLeft(initial) { _ + _ }

        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), values),
          db
        )

        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            Match(idx.refObj)
          ),
          db
        ) shouldBe JSLong(reduced)
      }
    }

    once("pages") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 1000),
          db
        )

        val lambda = Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v")))

        val result0 = runQuery(
          Reduce(
            lambda,
            10,
            Paginate(Match(idx.refObj), size = 500)
          ),
          db
        )

        (result0 / "data") shouldBe JSArray(125260)
        (result0 / "after" / 0) shouldBe JSLong(501)

        val result1 = runQuery(
          Reduce(
            lambda,
            125260,
            Paginate(Match(idx.refObj), size = 500, cursor = After(501))
          ),
          db
        )

        (result1 / "data") shouldBe JSArray(500510)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        val lambda = Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v")))

        //array
        runQuery(
          Reduce(
            lambda,
            10,
            JSArray()
          ),
          db
        ) shouldBe JSLong(10)

        //page
        (runQuery(
          Reduce(
            lambda,
            10,
            Paginate(Match(idx.refObj, "baz"))
          ),
          db
        ) / "data") shouldBe JSArray(10)

        //set
        runQuery(
          Reduce(
            lambda,
            10,
            Match(idx.refObj, "baz")
          ),
          db
        ) shouldBe JSLong(10)
      }
    }

    once("union") {
      for {
        db <- aDatabase
        fooCls <- aCollection(db)
        fooIdx <- anIndex(fooCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "foo"), false))))
        barCls <- aCollection(db)
        barIdx <- anIndex(barCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "bar"), false))))
      } {
        val initial = 10
        val reduced = (1 to 1000).foldLeft(initial) { _ + _ }

        runQuery(
          Foreach(Lambda("i" -> CreateF(fooCls.refObj, MkObject("data" -> MkObject("foo" -> Var("i"))))), 1 to 499),
          db
        )

        runQuery(
          Foreach(Lambda("i" -> CreateF(barCls.refObj, MkObject("data" -> MkObject("bar" -> Var("i"))))), 500 to 1000),
          db
        )

        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            Union(
              Match(fooIdx.refObj),
              Match(barIdx.refObj)
            )
          ),
          db
        ) shouldBe JSLong(reduced)
      }
    }

    once("intersection") {
      for {
        db <- aDatabase
        fooCls <- aCollection(db)
        fooIdx <- anIndex(fooCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "foo"), false))))
        barCls <- aCollection(db)
        barIdx <- anIndex(barCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "bar"), false))))
      } {
        val initial = 10
        val reduced = (250 to 500).foldLeft(initial) { _ + _ }

        runQuery(
          Foreach(Lambda("i" -> CreateF(fooCls.refObj, MkObject("data" -> MkObject("foo" -> Var("i"))))), 1 to 1000),
          db
        )

        runQuery(
          Foreach(Lambda("i" -> CreateF(barCls.refObj, MkObject("data" -> MkObject("bar" -> Var("i"))))), 250 to 500),
          db
        )

        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            Intersection(
              Match(fooIdx.refObj),
              Match(barIdx.refObj)
            )
          ),
          db
        ) shouldBe JSLong(reduced)
      }
    }

    once("difference") {
      for {
        db <- aDatabase
        fooCls <- aCollection(db)
        fooIdx <- anIndex(fooCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "foo"), false))))
        barCls <- aCollection(db)
        barIdx <- anIndex(barCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "bar"), false))))
      } {
        val initial = 10
        val reduced = ((1 to 1000) diff (250 to 500)).foldLeft(initial) { _ + _ }

        runQuery(
          Foreach(Lambda("i" -> CreateF(fooCls.refObj, MkObject("data" -> MkObject("foo" -> Var("i"))))), 1 to 1000),
          db
        )

        runQuery(
          Foreach(Lambda("i" -> CreateF(barCls.refObj, MkObject("data" -> MkObject("bar" -> Var("i"))))), 250 to 500),
          db
        )

        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            Difference(
              Match(fooIdx.refObj),
              Match(barIdx.refObj)
            )
          ),
          db
        ) shouldBe JSLong(reduced)
      }
    }

    once("distinct") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        val initial = 10
        val reduced = (1 to 2000).foldLeft(initial) { _ + _ }

        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 1000),
          db
        )

        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 2000),
          db
        )

        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            Distinct(Match(idx.refObj))
          ),
          db
        ) shouldBe JSLong(reduced)
      }
    }

    once("join") {
      for {
        db <- aDatabase
        fooCls <- aCollection(db)
        fooIdx <- anIndex(fooCls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "foo"), false))))
        barCls <- aCollection(db)
        barByFooIdx <- anIndex(barCls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "bar"), false))))
      } {
        val initial = 10
        val reduced = (1 to 1000).map(_ * 2).foldLeft(initial) { _ + _ }

        runQuery(
          Foreach(Lambda("i" -> CreateF(fooCls.refObj, MkObject("data" -> MkObject("foo" -> Var("i"))))), 1 to 1000),
          db
        )

        runQuery(
          Foreach(Lambda("i" -> CreateF(barCls.refObj, MkObject("data" -> MkObject("foo" -> Var("i"), "bar" -> Multiply(Var("i"), 2))))), 1 to 1000),
          db
        )

        runQuery(
          Reduce(
            Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v"))),
            initial,
            Join(
              Match(fooIdx.refObj),
              barByFooIdx.refObj
            )
          ),
          db
        ) shouldBe JSLong(reduced)
      }
    }

    prop("document events") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        size <- Prop.int(10 to 100)
      } {
        val initial = 10
        val reduced = (1 to size).foldLeft(initial) { _ + _ }

        for (i <-  1 to size) {
          runQuery(
            Update(inst.refObj, MkObject("data" -> MkObject("value" -> i))),
            db
          )
        }

        val lambda = Lambda(JSArray("acc", "event") -> AddF(Var("acc"), Select(JSArray("data", "value"), Var("event"), 0)))

        //set
        runQuery(
          Reduce(
            lambda,
            initial,
            Events(inst.refObj)
          ),
          db
        ) shouldBe JSLong(reduced)

        //page
        (runQuery(
          Reduce(
            lambda,
            initial,
            Paginate(Events(inst.refObj), size = size + 1)
          ),
          db
        ) / "data") shouldBe JSArray(reduced)
      }
    }

    prop("set events") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
        size <- Prop.int(10 to 100)
      } {
        val initial = 10
        val reduced = (1 to size).foldLeft(initial) { _ + _ }

        for (i <-  1 to size) {
          runQuery(
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> i))),
            db
          )
        }

        val lambda = Lambda(JSArray("acc", "event") -> AddF(Var("acc"), Select(JSArray("data"), Var("event"))))

        //set
        runQuery(
          Reduce(
            lambda,
            initial,
            Events(Match(idx.refObj))
          ),
          db
        ) shouldBe JSLong(reduced)

        //page
        (runQuery(
          Reduce(
            lambda,
            initial,
            Paginate(Events(Match(idx.refObj)), size = size + 1)
          ),
          db
        ) / "data") shouldBe JSArray(reduced)
      }
    }

    prop("check read permissions") {
      for {
        db <- aDatabase
        user <- aUser(db)
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
        size <- Prop.int(10 to 1000)
      } {
        val initial = 10
        val reduced = (1 to size).foldLeft(initial) { _ + _ }

        val instances = runQuery(
          MapF(
            Lambda(
              "i" ->
                CreateF(
                  cls.refObj,
                  MkObject(
                    "data" -> MkObject("value" -> Var("i"))
                  ))
            ),
            1 to size
          ),
          db
        )

        val index = Random.nextInt(size)
        val removedInstance = instances / index
        val removedValue = (removedInstance / "data" / "value").as[Long]

        aRole(
          db,
          Membership(user.collection),
          Privilege(idx.refObj, read = RoleAction.Granted),
          Privilege(
            cls.refObj,
            read = RoleAction(
              Lambda("ref" ->
                Not(Equals(Var("ref"), removedInstance / "ref")))))
        ).sample

        val lambda = Lambda(JSArray("acc", "v") -> AddF(Var("acc"), Var("v")))

        //set
        runQuery(
          Reduce(
            lambda,
            initial,
            Match(idx.refObj)
          ),
          user.token
        ) shouldBe JSLong(reduced - removedValue)

        //page
        (runQuery(
          Reduce(
            lambda,
            initial,
            Paginate(Match(idx.refObj), size = size)
          ),
          user.token
        ) / "data") shouldBe JSArray(reduced - removedValue)
      }
    }

    once("errors") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Reduce(10, 0, 0),
          "invalid expression",
          "Lambda expected, Integer provided.",
          JSArray("reduce"),
          db
        )

        qassertErr(
          Reduce(Lambda("x" -> Var("x")), 0, "string"),
          "invalid argument",
          "Array, Set, or Page expected, String provided.",
          JSArray("collection"),
          db
        )

        qassertErr(
          Reduce(Lambda(JSArray("x", "y", "z") -> Var("x")), 0, JSArray(1, 2, 3)),
          "invalid argument",
          "Lambda expects an array with 3 elements. Array contains 2.",
          JSArray("reduce", "lambda"),
          db
        )

        qassertErr(
          Reduce(Lambda(JSArray("acc", "value") -> AddF(Var("acc"), Var("value"))), 0, JSArray(1, 2, "str", 3)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("reduce", "expr", "add", 1),
          db
        )
      }
    }

    once("error on index binding") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        _ <- aDocument(cls)
      } {
        val lambda = Lambda(JSArray("acc", "int") -> AddF(Var("acc"), Var("int")))
        def buildIndex() =
          CreateIndex(
            MkObject(
              "name" -> "test_binding",
              "source" -> MkObject(
                "collection" -> cls.refObj,
                "fields" -> MkObject(
                  "something_reduced" -> QueryF(
                    Lambda(
                      "x" -> Reduce(
                        lambda,
                        0,
                        JSArray(1, 2, 3)
                      ))))),
              "active" -> true,
              "values" -> MkObject("binding" -> "something_reduced")
            ))

        qassertErr(
          buildIndex(),
          "validation failed",
          "document data is not valid.",
          JSArray("create_index"),
          db
        )

      }
    }
  }

  "Sum" - {
    once("check sum with longs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        val values = 1 to 1000

        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), values),
          db
        )

        //array
        runQuery(
          Sum(values),
          db
        ) shouldBe JSLong(500500)

        //set
        runQuery(
          Sum(Match(idx.refObj)),
          db
        ) shouldBe JSLong(500500)

        //page
        (runQuery(
          Sum(Paginate(Match(idx.refObj), size = 1000)),
          db
        ) / "data") shouldBe JSArray(500500)
      }
    }

    once("check sum with doubles") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        val values = (1 to 1000).map { _.toDouble }

        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), values),
          db
        )

        //array
        runQuery(
          Sum(values),
          db
        ) shouldBe JSDouble(500500)

        //set
        runQuery(
          Sum(Match(idx.refObj)),
          db
        ) shouldBe JSDouble(500500)

        //page
        (runQuery(
          Sum(Paginate(Match(idx.refObj), size = 1000)),
          db
        ) / "data") shouldBe JSArray(500500d)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        runQuery(
          Sum(Seq.empty[Long]),
          db
        ) shouldBe JSLong(0)

        //set
        runQuery(
          Sum(Match(idx.refObj, "baz")),
          db
        ) shouldBe JSLong(0)

        //page
        (runQuery(
          Sum(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          db
        ) / "data") shouldBe JSArray(0)
      }
    }

    once("cast errors") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> 10))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> "str"))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> 20)))
          ),
          db
        )

        //array
        qassertErr(
          Sum(JSArray(10, "str", 20)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("sum"),
          db
        )

        //set
        qassertErr(
          Sum(Match(idx.refObj)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("sum"),
          db
        )

        //page
        qassertErr(
          Sum(Paginate(Match(idx.refObj), size = 1000)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("sum"),
          db
        )
      }
    }
  }

  "Mean" - {
    once("check mean with longs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        runQuery(
          Mean(1 to 1000),
          db
        ) shouldBe JSDouble(500.5)

        //set
        runQuery(
          Mean(Match(idx.refObj)),
          db
        ) shouldBe JSDouble(500.5)

        //page
        (runQuery(
          Mean(Paginate(Match(idx.refObj), size = 1000)),
          db
        ) / "data") shouldBe JSArray(500.5)
      }
    }

    once("check mean with doubles") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        val values = (1 to 1000).map { _.toDouble }

        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), values),
          db
        )

        //array
        runQuery(
          Mean(values),
          db
        ) shouldBe JSDouble(500.5)

        //set
        runQuery(
          Mean(Match(idx.refObj)),
          db
        ) shouldBe JSDouble(500.5)

        //page
        (runQuery(
          Mean(Paginate(Match(idx.refObj), size = 1000)),
          db
        ) / "data") shouldBe JSArray(500.5)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        qassertErr(
          Mean(Seq.empty[Long]),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("mean"),
          db
        )

        //set
        qassertErr(
          Mean(Match(idx.refObj, "baz")),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("mean"),
          db
        )

        //page
        qassertErr(
          Mean(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("mean"),
          db
        )
      }
    }

    once("cast errors") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> 10))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> "str"))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> 20)))
          ),
          db
        )

        //array
        qassertErr(
          Mean(JSArray(10, "str", 20)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("mean"),
          db
        )

        //set
        qassertErr(
          Mean(Match(idx.refObj)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("mean"),
          db
        )

        //page
        qassertErr(
          Mean(Paginate(Match(idx.refObj), size = 1000)),
          "invalid argument",
          "Number expected, String provided.",
          JSArray("mean"),
          db
        )
      }
    }
  }

  "Count" - {
    once("check count") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        runQuery(
          Count(1 to 1000),
          db
        ) shouldBe JSLong(1000)

        //set
        runQuery(
          Count(Match(idx.refObj)),
          db
        ) shouldBe JSLong(1000)

        //page
        (runQuery(
          Count(Paginate(Match(idx.refObj), size = 1000)),
          db
        ) / "data") shouldBe JSArray(1000)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        runQuery(
          Count(Seq.empty[Long]),
          db
        ) shouldBe JSLong(0)

        //set
        runQuery(
          Count(Match(idx.refObj, "baz")),
          db
        ) shouldBe JSLong(0)

        //page
        (runQuery(
          Count(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          db
        ) / "data") shouldBe JSArray(0)
      }
    }
  }

  "Any" - {
    once("check any") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "true", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "true", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "true", "value" -> true))),

            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "false", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "false", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "false", "value" -> false))),

            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "mixed", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "mixed", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "mixed", "value" -> true)))
          ),
          db
        )

        //array
        runQuery(
          JSArray(
            Any(JSArray(true,  true,  true)),
            Any(JSArray(false, false, false)),
            Any(JSArray(true,  false, true))
          ),
          db
        ) shouldBe JSArray(true, false, true)

        //set
        runQuery(
          JSArray(
            Any(Match(idx.refObj, "true")),
            Any(Match(idx.refObj, "false")),
            Any(Match(idx.refObj, "mixed"))
          ),
          db
        ) shouldBe JSArray(true, false, true)

        //page
        runQuery(
          SelectAll(
            "data",
            JSArray(
              Any(Paginate(Match(idx.refObj, "true"), size = 1000)),
              Any(Paginate(Match(idx.refObj, "false"), size = 1000)),
              Any(Paginate(Match(idx.refObj, "mixed"), size = 1000))
            )
          ),
          db
        ) shouldBe JSArray(true, false, true)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> true)))
          ),
          db
        )

        //array
        runQuery(
          Any(Seq.empty[Long]),
          db
        ) shouldBe JSFalse

        //set
        runQuery(
          Any(Match(idx.refObj, "baz")),
          db
        ) shouldBe JSFalse

        //page
        (runQuery(
          Any(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          db
        ) / "data") shouldBe JSArray(false)
      }
    }

    once("cast errors") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> "str"))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> false)))
          ),
          db
        )

        //array
        qassertErr(
          Any(JSArray(true, "str", false)),
          "invalid argument",
          "Boolean expected, String provided.",
          JSArray("any"),
          db
        )

        //set
        qassertErr(
          Any(Match(idx.refObj)),
          "invalid argument",
          "Boolean expected, String provided.",
          JSArray("any"),
          db
        )

        //page
        qassertErr(
          Any(Paginate(Match(idx.refObj), size = 1000)),
          "invalid argument",
          "Boolean expected, String provided.",
          JSArray("any"),
          db
        )
      }
    }
  }

  "All" - {
    once("check all") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "true", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "true", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "true", "value" -> true))),

            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "false", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "false", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "false", "value" -> false))),

            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "mixed", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "mixed", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "mixed", "value" -> true)))
          ),
          db
        )

        //array
        runQuery(
          JSArray(
            All(JSArray(true,  true,  true)),
            All(JSArray(false, false, false)),
            All(JSArray(true,  false, true))
          ),
          db
        ) shouldBe JSArray(true, false, false)

        //set
        runQuery(
          JSArray(
            All(Match(idx.refObj, "true")),
            All(Match(idx.refObj, "false")),
            All(Match(idx.refObj, "mixed"))
          ),
          db
        ) shouldBe JSArray(true, false, false)

        //page
        runQuery(
          SelectAll(
            "data",
            JSArray(
              All(Paginate(Match(idx.refObj, "true"), size = 1000)),
              All(Paginate(Match(idx.refObj, "false"), size = 1000)),
              All(Paginate(Match(idx.refObj, "mixed"), size = 1000))
            )
          ),
          db
        ) shouldBe JSArray(true, false, false)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> false))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> true)))
          ),
          db
        )

        //array
        runQuery(
          All(Seq.empty[Long]),
          db
        ) shouldBe JSTrue

        //set
        runQuery(
          All(Match(idx.refObj, "baz")),
          db
        ) shouldBe JSTrue

        //page
        (runQuery(
          All(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          db
        ) / "data") shouldBe JSArray(true)
      }
    }

    once("cast errors") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Do(
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> true))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> "str"))),
            CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> false)))
          ),
          db
        )

        //array
        qassertErr(
          All(JSArray(true, "str", false)),
          "invalid argument",
          "Boolean expected, String provided.",
          JSArray("all"),
          db
        )

        //set
        qassertErr(
          All(Match(idx.refObj)),
          "invalid argument",
          "Boolean expected, String provided.",
          JSArray("all"),
          db
        )

        //page
        qassertErr(
          All(Paginate(Match(idx.refObj), size = 1000)),
          "invalid argument",
          "Boolean expected, String provided.",
          JSArray("all"),
          db
        )
      }
    }
  }

  "Max" - {
    once("check the max function") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        qequals(8, Max(1, 2, 4, 8), db)
        qequals("D", Max("A", "B", "C", "D"), db)
        qequals(Time("1980-01-01T00:00:00Z"), Max(Time("1970-01-01T00:00:00Z"), Time("1980-01-01T00:00:00Z")), db)
        qequals(Date("1970-01-01"), Max(Date("1970-01-01"), Date("1930-01-01")), db)
        qequals("A", Max("A", 1), db)
        qequals(1, Max(1, 0, 1, 1, 0, 1), db)
        qequals(16, Max(1, 0, 16), db)
        qequals(0, Max(0, 0, 0), db)
        qequals(10, Max(-10, 0, 10), db)
        qequals(0, Max(0), db)
        qequals(1, Max(1), db)
        qequals(100.0, Max(-1.2, 12, 100), db)

        //set
        qequals(1000, Max(Match(idx.refObj)), db)

        //page
        qequals(1000, Select(JSArray("data", 0), Max(Paginate(Match(idx.refObj), size = 1000))), db)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> Var("i"))))), 1 to 1000),
          db
        )

        qassertErr(
          Max(),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("max"),
          db
        )

        //set
        qassertErr(
          Max(Match(idx.refObj, "baz")),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("max"),
          db
        )

        //page
        qassertErr(
          Max(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("max"),
          db
        )
      }
    }

    once("invalid refs") {
      for {
        db <- aDatabase
      } {
        val ret = runRawQuery(Max(ClassRef("coll"), IndexRef("idx")), db.key)
        ret should respond (400)

        (ret.errors / 0 / "code") shouldBe JSString("invalid ref")
        (ret.errors / 0 / "description") shouldBe JSString("Ref refers to undefined collection 'coll'")
        (ret.errors / 0 / "position") shouldBe JSArray("max")

        (ret.errors / 1 / "code") shouldBe JSString("invalid ref")
        (ret.errors / 1 / "description") shouldBe JSString("Ref refers to undefined index 'idx'")
        (ret.errors / 1 / "position") shouldBe JSArray("max")
      }
    }
  }

  "Min" - {
    once("check the min function") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "value"), true))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("value" -> Var("i"))))), 1 to 1000),
          db
        )

        //array
        qequals(1, Min(1, 2, 4, 8), db)
        qequals("A", Min("A", "B", "C", "D"), db)
        qequals(Time("1970-01-01T00:00:00Z"), Min(Time("1970-01-01T00:00:00Z"), Time("1980-01-01T00:00:00Z")), db)
        qequals(Date("1930-01-01"), Min(Date("1970-01-01"), Date("1930-01-01")), db)
        qequals(1, Min("A", 1), db)
        qequals(0, Min(1, 0, 1, 1, 0, 1), db)
        qequals(-10, Min(1, -10, 16), db)
        qequals(0, Min(0, 0, 0), db)
        qequals(-10, Min(-10, 0, 10), db)
        qequals(0, Min(0), db)
        qequals(1, Min(1), db)
        qequals(-1, Min(-1, 12, 100), db)

        //set
        qequals(1, Min(Match(idx.refObj)), db)

        //page
        qequals(1, Select(JSArray("data", 0), Min(Paginate(Match(idx.refObj), size = 1000))), db)
      }
    }

    once("empty collection") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq(JSArray("data", "foo"))), Prop.const(Seq((JSArray("data", "value"), false))))
      } {
        runQuery(
          Foreach(Lambda("i" -> CreateF(cls.refObj, MkObject("data" -> MkObject("foo" -> "bar", "value" -> Var("i"))))), 1 to 1000),
          db
        )

        qassertErr(
          Min(),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("min"),
          db
        )

        //set
        qassertErr(
          Min(Match(idx.refObj, "baz")),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("min"),
          db
        )

        //page
        qassertErr(
          Min(Paginate(Match(idx.refObj, "baz"), size = 1000)),
          "invalid argument",
          "Non-empty array expected.",
          JSArray("min"),
          db
        )
      }
    }

    once("invalid refs") {
      for {
        db <- aDatabase
      } {
        val ret = runRawQuery(Min(ClassRef("coll"), IndexRef("idx")), db.key)
        ret should respond (400)

        (ret.errors / 0 / "code") shouldBe JSString("invalid ref")
        (ret.errors / 0 / "description") shouldBe JSString("Ref refers to undefined collection 'coll'")
        (ret.errors / 0 / "position") shouldBe JSArray("min")

        (ret.errors / 1 / "code") shouldBe JSString("invalid ref")
        (ret.errors / 1 / "description") shouldBe JSString("Ref refers to undefined index 'idx'")
        (ret.errors / 1 / "position") shouldBe JSArray("min")
      }
    }
  }
}
