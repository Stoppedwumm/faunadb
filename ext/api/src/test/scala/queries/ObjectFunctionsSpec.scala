package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop.Prop

class ObjectFunctionsSpec extends QueryAPI27Spec {
  "merge objects" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        //empty object
        qequals(
          MkObject("x" -> 10, "y" -> 20),
          Merge(
            MkObject(),
            MkObject("x" -> 10, "y" -> 20)
          ),
          db)

        //adds field
        qequals(
          MkObject("x" -> 10, "y" -> 20, "z" -> 30),
          Merge(
            MkObject("x" -> 10, "y" -> 20),
            MkObject("z" -> 30)
          ),
          db)

        //replace field
        qequals(
          MkObject("x" -> 10, "y" -> 20, "z" -> 30),
          Merge(
            MkObject("x" -> 10, "y" -> 20, "z" -> -1),
            MkObject("z" -> 30)
          ),
          db)

        //remove field
        qequals(
          MkObject("x" -> 10, "y" -> 20),
          Merge(
            MkObject("x" -> 10, "y" -> 20, "z" -> -1),
            MkObject("z" -> JSNull)
          ),
          db)

        //merge multiple objects
        qequals(
          MkObject("x" -> 10, "y" -> 20, "z" -> 30),
          Merge(
            MkObject(),
            JSArray(MkObject("x" -> 10), MkObject("y" -> 20), MkObject("z" -> 30))
          ),
          db)
      }
    }

    once("works with version") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        inst <- aDocument(col, dataProp = Prop.const(JSObject("x" -> 10, "y" -> 20)))
      } {
        qequals(
          MkObject(
            "ref" -> inst.refObj,
            "ts" -> inst.ts,
            "data" -> MkObject("z" -> 30)
          ),
          Merge(
            Get(inst.refObj),
            MkObject("data" -> MkObject("z" -> 30))
          ),
          db)

        qequals(
          MkObject(
            "ref" -> inst.refObj,
            "ts" -> inst.ts,
            "data" -> MkObject("x" -> 10, "y" -> 20)
          ),
          Merge(
            MkObject("data" -> MkObject("z" -> 30)),
            Get(inst.refObj)
          ),
          db)

        qequals(
          MkObject(
            "ref" -> inst.refObj,
            "ts" -> inst.ts,
            "data" -> MkObject("x" -> 10, "y" -> 20)
          ),
          Merge(
            Get(inst.refObj),
            Get(inst.refObj)
          ),
          db)

        qequals(
          MkObject(
            "ref" -> inst.refObj,
            "ts" -> inst.ts,
            "data" -> MkObject("x" -> 10, "y" -> 20)
          ),
          Merge(
            MkObject(),
            JSArray(Get(inst.refObj), Get(inst.refObj))
          ),
          db)

        qequals(
          MkObject(
            "ref" -> inst.refObj,
            "ts" -> inst.ts,
            "data" -> MkObject("x" -> 10, "y" -> 20)
          ),
          Merge(
            Get(inst.refObj),
            JSArray(Get(inst.refObj), Get(inst.refObj))
          ),
          db)
      }
    }

    once("with merge lambda") {
      for {
        db <- aDatabase
      } {
        //ignore right value
        qequals(
          MkObject("x" -> 10, "y" -> 20),
          Merge(
            MkObject("x" -> 10, "y" -> 20),
            MkObject("x" -> 100, "y" -> 200),
            Lambda(JSArray("key", "left", "right") -> Var("left"))
          ),
          db)

        //ignore left value
        qequals(
          MkObject("x" -> 100, "y" -> 200),
          Merge(
            MkObject("x" -> 10, "y" -> 20),
            MkObject("x" -> 100, "y" -> 200),
            Lambda(JSArray("key", "left", "right") -> Var("right"))
          ),
          db)

        //lambda 1-arity -> return [key, leftValue, rightValue]
        qequals(
          MkObject("x" -> JSArray("x", 10, 100), "y" -> JSArray("y", 20, 200)),
          Merge(
            MkObject("x" -> 10, "y" -> 20),
            MkObject("x" -> 100, "y" -> 200),
            Lambda("args" -> Var("args"))
          ),
          db)
      }
    }

    once("errors") {
      for {
        db <- aDatabase
      } {
        //first argument wrong
        runRawQuery(Merge("x", MkObject()), db.key).json shouldBe JSObject(
          "errors" -> JSArray(
            JSObject("position" -> JSArray("merge"), "code" -> "invalid argument", "description" -> "Object expected, String provided.")
          )
        )

        //second argument wrong
        runRawQuery(Merge(MkObject(), "y"), db.key).json shouldBe JSObject(
          "errors" -> JSArray(
            JSObject("position" -> JSArray("with"), "code" -> "invalid argument", "description" -> "Object expected, String provided.")
          )
        )

        //both arguments wrong
        runRawQuery(Merge("x", "y"), db.key).json shouldBe JSObject(
          "errors" -> JSArray(
            JSObject("position" -> JSArray("merge"), "code" -> "invalid argument", "description" -> "Object expected, String provided."),
            JSObject("position" -> JSArray("with"), "code" -> "invalid argument", "description" -> "Object expected, String provided.")
          )
        )

        runRawQuery(
          Merge(MkObject(), JSArray(MkObject(), "str", 10)),
          db.key
        ).json shouldBe JSObject(
          "errors" -> JSArray(
            JSObject("position" -> JSArray("with", 1), "code" -> "invalid argument", "description" -> "Object expected, String provided."),
            JSObject("position" -> JSArray("with", 2), "code" -> "invalid argument", "description" -> "Object expected, Integer provided.")
          )
        )

        qassertErr(
          Merge(MkObject(), MkObject(), "z"),
          "invalid expression",
          "Lambda expected, String provided.",
          JSArray("lambda"), db)

        qassertErr(
          Merge(MkObject(), MkObject(), JSObject("lambda" -> "invalid lambda")),
          "invalid expression",
          "No form/function found, or invalid argument keys: { lambda }.",
          JSArray("lambda"), db)
      }
    }
  }
}
