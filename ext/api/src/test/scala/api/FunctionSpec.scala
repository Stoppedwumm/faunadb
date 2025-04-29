package fauna.api.test

import fauna.codex.json._

class FunctionSpec extends API21Spec {
  "functions" / {
    once("GET works") {
      for {
        db <- aDatabase
        user <- aUser(db)
      } {
        val fun = api.post("/functions", JSObject(
          "name" -> "add2",
          "body" ->
            JSObject("@query" ->
              JSObject("lambda" -> "v",
                "expr" -> JSObject("add" -> JSArray(JSObject("var" -> "v"), 2))))), db.key).resource

        val funs = api.get("/functions", db.key)
        funs should respond (OK)

        (funs.resource / "data") should containElem (fun.refObj)

        api.get("/functions", user.token) should respond (Forbidden)
      }
    }

    once("new functions cause cache refresh") {
      for {
        db <- aDatabase
      } {
        api.query(
          CreateFunction(
            MkObject(
              "name" -> "foo",
              "body" -> QueryF(Lambda("x" -> AddF(Var("x"), 1)))
            )),
          db.key) should respond(Created)

        api.query(Call("foo", 2), db.key).json / "resource" shouldEqual JSLong(3)

        api.query(
          Update(
            FunctionRef("foo"),
            MkObject(
              "body" -> QueryF(Lambda("x" -> AddF(Var("x"), 2)))
            )),
          db.key) should respond(OK)

        api.query(
          CreateFunction(
            MkObject(
              "name" -> "fooWithTwo",
              "body" -> QueryF(Lambda(JSArray() -> Call("foo", 2)))
            )),
          db.key) should respond(Created)

        // calling through a new function is guaranteed to see the state it was
        // created within.
        api.query(Call("fooWithTwo"), db.key).json / "resource" shouldEqual JSLong(4)

        // cache has been updated
        api.query(Call("foo", 2), db.key).json / "resource" shouldEqual JSLong(4)
      }
    }

    prop("functions which write are always consistent") {
      for {
        db <- aDatabase
      } {
        api.query(
          CreateCollection(MkObject("name" -> "Foo")),
          db.key) should respond(Created)

        api.query(
          CreateFunction(
            MkObject(
              "name" -> "newFoo",
              "body" -> QueryF(Lambda(JSArray() ->
                CreateF("Foo", MkObject("data" -> MkObject("name" -> "Alice"))))))),
          db.key) should respond(Created)

        api
          .query(Call("newFoo"), db.key)
          .resource / "data" / "name" shouldEqual JSString("Alice")

        api.query(
          Update(
            FunctionRef("newFoo"),
            MkObject(
              "body" -> QueryF(Lambda(JSArray() ->
                CreateF("Foo", MkObject("data" -> MkObject("name" -> "Bob"))))))),
          db.key) should respond(OK)

        // This call can hit the cached version which sets name to Alice, but in
        // that case, the schema OCC check will fail, resulting in the query
        // being re-ran with the updated function def.
        api
          .query(Call("newFoo"), db.key)
          .resource / "data" / "name" shouldEqual JSString("Bob")
      }
    }
  }
}
