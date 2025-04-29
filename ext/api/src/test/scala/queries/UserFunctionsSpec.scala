package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class UserFunctionsSpec extends QueryAPI21Spec {
  "create/get" - {
    once("creates a user function") {
      for {
        db <- aDatabase
      } {
        val ref = runQuery(CreateF(Ref("functions"), MkObject(
          "name" -> "foo",
          "body" -> QueryF(Lambda("arg" -> Var("arg"))))), db).refObj

        val fndoc = runQuery(Get(ref), db)

        (fndoc / "name") should equal (JSString("foo"))
        (fndoc / "body") should equal (
          JSObject(
            "@query" -> JSObject(
              "lambda" -> "arg",
              "expr" -> JSObject("var" -> "arg"))))
      }
    }
  }

  "call" - {
    once("can call a user function") {
      for {
        db <- aDatabase
        obj <- jsObject
      } {

        val ref = runQuery(CreateF(Ref("functions"), MkObject(
          "name" -> "foo",
          "body" -> QueryF(Lambda("arg" -> Var("arg"))))), db).refObj

        runQuery(Call(ref, Quote(obj)), db) should equal (obj)
        runQuery(Call("foo", Quote(obj)), db) should equal (obj)
        runQuery(Call(ref, Quote(obj), Quote(obj)), db) should equal (JSArray(obj, obj))
      }
    }

    once("function failures are wrapped") {
      for {
        db <- aDatabase
      } {
        val ref = runQuery(CreateF(Ref("functions"), MkObject(
          "name" -> "double",
          "body" -> QueryF(Lambda("arg" -> AddF(Var("arg"), Var("arg")))))), db).refObj

        qequals(Call(ref, 1), 2, db)

        val res = runRawQuery(Call(ref, "oops"), db.key)

        res should respond (400)

        val err = res.json / "errors" / 0

        (err / "code") should equal (JSString("call error"))
        (err / "position") should equal (JSArray())

        val cause = err / "cause" / 0

        (cause / "code") should equal (JSString("invalid argument"))
        (cause / "position") should equal (JSArray("expr", "add", 0))
      }
    }

    once("applies role") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        user <- aUser(db)
      } {
        qassertErr(CreateF(cls.refObj, MkObject()), "permission denied", JSArray("create"), user.token)

        val fun = runQuery(
          CreateF(
            Ref("functions"),
            MkObject(
              "name" -> "createcls",
              "body" -> QueryF(Lambda(JSArray() -> CreateF(cls.refObj, MkObject())))
            )
          ),
          db
        ).refObj

        runQuery(
          CreateF(
            Ref("roles"),
            MkObject(
              "name" -> "callfn",
              "membership" -> JSArray(
                MkObject("resource" -> user.collection.refObj)),
              "privileges" -> JSArray(
                MkObject("resource" -> fun, "actions" -> MkObject("call" -> JSTrue)))
            )
          ),
          db.adminKey
        )

        qassertErr(Call(fun), "call error", JSArray(), user.token)

        runQuery(Update(fun, MkObject("role" -> "server")), db)

        // create should succeed
        beforeTTLCacheExpiration {
          runQuery(Call(fun), user.token)
        }
      }
    }
  }

  "delete" - {
    once("deletes a function") {
      for {
        db <- aDatabase
      } {
        val ref = runQuery(CreateF(Ref("functions"), MkObject(
          "name" -> "foo",
          "body" -> QueryF(Lambda("_" -> "bar")))), db).refObj

        runQuery(DeleteF(ref), db)

        beforeTTLCacheExpiration {
          qassertErr(Call(ref), "invalid ref", JSArray("call"), db)
        }
      }
    }
  }
}

class UserFunctionsSpec27 extends QueryAPI27Spec {
  "create" - {
    once("cannot create a user function in a container") {
      for {
        db <- aContainerDB
      } {
        val create = CreateF(Ref("functions"), MkObject(
          "name" -> "foo",
          "body" -> QueryF(Lambda("arg" -> Var("arg")))))

        qassertErr(create, "invalid object in container", JSArray("create"), db)
      }
    }

    once("can create a function from another function") {
      for {
        db <- aDatabase
      } {
        val createCons = CreateFunction(MkObject(
          "name" -> "mk_fun",
          "body" -> QueryF(Lambda("name" ->
            CreateFunction(MkObject(
              "name" -> Var("name"),
              "body" -> QueryF(Lambda("x" -> AddF(Var("x"), 2)))
            ))
          ))
        ))

        runQuery(createCons, db)

        runQuery(Call(FunctionRef("mk_fun"), "func"), db)
        qequals(Call(FunctionRef("func"), 2), 4, db)
      }
    }
  }
}
