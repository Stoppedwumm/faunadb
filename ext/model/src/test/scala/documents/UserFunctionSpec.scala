package fauna.model.test

import fauna.ast._
import fauna.auth._
import fauna.codex.json._
import fauna.exec.FaunaExecutionContext
import fauna.lang.clocks.Clock
import fauna.model.Key.UserRoles
import fauna.repo.test.CassandraHelper
import scala.concurrent.duration._
import scala.concurrent.Await

class UserFunctionSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "UserFunctionSpec" - {
    val scope = ctx ! newScope
    val auth = Auth.adminForScope(scope)

    "allow recursion" in {
      ctx ! runQuery(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "foldLeft",
            "body" -> QueryF(Lambda(JSArray("lastResult", "array") -> If(
              IsEmpty(Var("array")),
              Var("lastResult"),
              Let(
                "elem" -> Select(0, Take(1, Var("array"))),
                "tail" -> Drop(1, Var("array"))) {
                Call("foldLeft", AddF(Var("lastResult"), Var("elem")), Var("tail"))
              }
            )))
          ))
      )

      (ctx ! runQuery(
        auth,
        Call("foldLeft", 0, JSArray(1, 2, 3, 4, 5, 6)))) shouldBe LongL(21L)
      (ctx ! runQuery(auth, Call("foldLeft", 0, 1L to 199L))) shouldBe LongL(19900L)

      val res = (ctx ! evalQuery(
        auth,
        Clock.time,
        Call("foldLeft", 0, 1L to 200L))).left.value
      res should have size 1
      res.head shouldBe a[StackOverflowError]
    }

    "allows adding multiple in the same scope without suffering from contention" in {
      val f1 = ctx.runNow(
        runQuery(
          auth,
          CreateFunction(
            MkObject(
              "name" -> "name1",
              "body" -> QueryF(Lambda(JSArray() -> Now()))
            ))))
      val f2 = ctx.runNow(
        runQuery(
          auth,
          CreateFunction(
            MkObject(
              "name" -> "name2",
              "body" -> QueryF(Lambda(JSArray() -> Now()))
            ))))

      implicit val ec = FaunaExecutionContext.Implicits.global
      // creating these 2 functions concurrently should not fail (specifically it
      // should not fail due to contention)
      Await.result(f1 flatMap { _ => f2 }, 3.seconds)
    }

    "allow indirect recursion" in {
      ctx ! runQuery(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "foldLeft0",
            "body" -> QueryF(Lambda(JSArray("lastResult", "array") -> If(
              IsEmpty(Var("array")),
              Var("lastResult"),
              Let(
                "elem" -> Select(0, Take(1, Var("array"))),
                "tail" -> Drop(1, Var("array"))) {
                Call("foldLeft1", AddF(Var("lastResult"), Var("elem")), Var("tail"))
              }
            )))
          ))
      )

      ctx ! runQuery(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "foldLeft1",
            "body" -> QueryF(Lambda(JSArray("lastResult", "array") ->
              Call("foldLeft0", Var("lastResult"), Var("array"))))
          ))
      )

      (ctx ! runQuery(
        auth,
        Call("foldLeft0", 0, JSArray(1, 2, 3, 4, 5, 6)))) shouldBe LongL(21L)
      (ctx ! runQuery(auth, Call("foldLeft0", 0, 1L to 99L))) shouldBe LongL(4950L)

      val res = (ctx ! evalQuery(
        auth,
        Clock.time,
        Call("foldLeft0", 0, 1L to 100L))).left.value
      res should have size 1
      res.head shouldBe a[StackOverflowError]
    }

    "binary tree" in {
      ctx ! mkCollection(auth, MkObject("name" -> "nodes"))

      ctx ! runQuery(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "insert_node",
            "body" -> QueryF(
              Lambda(JSArray("root_ref", "value") ->
                Select(
                  "ref",
                  If(
                    Equals(Var("root_ref"), JSNull),
                    CreateF(
                      ClsRefV("nodes"),
                      MkObject("data" -> MkObject("value" -> Var("value")))),
                    Let("root" -> Get(Var("root_ref"))) {
                      If(
                        LessThanOrEquals(
                          Var("value"),
                          Select(JSArray("data", "value"), Var("root"))),
                        Let(
                          "left_ref" -> Call(
                            "insert_node",
                            Select(JSArray("data", "left"), Var("root"), JSNull),
                            Var("value"))) {
                          Update(
                            Var("root_ref"),
                            MkObject("data" -> MkObject("left" -> Var("left_ref"))))
                        },
                        Let(
                          "right_ref" -> Call(
                            "insert_node",
                            Select(JSArray("data", "right"), Var("root"), JSNull),
                            Var("value"))) {
                          Update(
                            Var("root_ref"),
                            MkObject(
                              "data" -> MkObject("right" -> Var("right_ref"))))
                        }
                      )
                    }
                  )
                )))
          ))
      )

      ctx ! runQuery(
        auth,
        CreateFunction(
          MkObject(
            "name" -> "print_tree",
            "body" -> QueryF(
              Lambda(JSArray("node_ref", "output") ->
                If(
                  Equals(Var("node_ref"), JSNull),
                  Var("output"),
                  Let(
                    "node" -> Get(Var("node_ref")),
                    "left" -> Call(
                      "print_tree",
                      Select(JSArray("data", "left"), Var("node"), JSNull),
                      JSArray()),
                    "right" -> Call(
                      "print_tree",
                      Select(JSArray("data", "right"), Var("node"), JSNull),
                      JSArray())
                  ) {
                    Append(
                      Var("right"),
                      Append(
                        Select(JSArray("data", "value"), Var("node")),
                        Append(Var("left"), Var("output"))))
                  }
                )))
          ))
      )

      val root =
        (ctx ! runQuery(auth, Call("insert_node", JSNull, 100))).asInstanceOf[RefL]
      ctx ! runQuery(
        auth,
        Call("insert_node", RefV(root.id.subID.toLong, ClsRefV("nodes")), 200))
      ctx ! runQuery(
        auth,
        Call("insert_node", RefV(root.id.subID.toLong, ClsRefV("nodes")), 50))
      ctx ! runQuery(
        auth,
        Call("insert_node", RefV(root.id.subID.toLong, ClsRefV("nodes")), 400))
      ctx ! runQuery(
        auth,
        Call("insert_node", RefV(root.id.subID.toLong, ClsRefV("nodes")), 0))

      val values = ctx ! runQuery(
        auth,
        Call("print_tree", RefV(root.id.subID.toLong, ClsRefV("nodes")), JSArray()))
      values shouldBe ArrayL(
        List(LongL(0), LongL(50), LongL(100), LongL(200), LongL(400)))
    }

    "can escalate privileges" in {
      ctx ! mkCollection(auth, MkObject("name" -> "user-fun-foo"))
      ctx ! runQuery(
        auth,
        Clock.time,
        CreateFunction(
          MkObject(
            "name" -> "creator-fun",
            "body" -> QueryF(Lambda("arg" ->
              CreateF(ClassRef("user-fun-foo"), MkObject("data" -> Var("arg")))))))
      )

      val funRef1 = Ref("functions/creator-fun")

      ctx ! runQuery(
        auth,
        Clock.time,
        CreateFunction(
          MkObject(
            "name" -> "creator-fun-2",
            "role" -> "server",
            "body" -> QueryF(Lambda("arg" ->
              CreateF(ClassRef("user-fun-foo"), MkObject("data" -> Var("arg")))))
          ))
      )

      val funRef2 = Ref("functions/creator-fun-2")

      ctx ! runQuery(
        auth,
        Clock.time,
        CreateRole(
          MkObject(
            "name" -> "callfn",
            "privileges" -> JSArray(
              MkObject(
                "resource" -> funRef2,
                "actions" -> MkObject("call" -> JSTrue)))))
      )

      ctx ! runQuery(auth, Clock.time, Call(funRef1, MkObject("foo" -> "bar")))

      val auth2 = ctx ! Auth.changeRole(auth, UserRoles("callfn"))
      val res =
        (ctx ! evalQuery(auth2, Clock.time, Call(funRef1, MkObject()))).left.value
      res should have size (1)
      res(0) shouldBe a[PermissionDenied]

      ctx ! runQuery(auth, Clock.time, Call(funRef2, MkObject("foo" -> "bar")))
      ctx ! runQuery(auth2, Clock.time, Call(funRef2, MkObject("foo" -> "bar")))
    }

    "udf already exists" in {
      val create = CreateFunction(
        MkObject(
          "name" -> "somefunc",
          "body" -> QueryF(Lambda("x" -> Var("x")))
        ))

      val ref = (ctx ! runQuery(auth, Select("ref", create))).asInstanceOf[RefL]
      (ctx ! evalQuery(auth, create)) match {
        case Left(errors) =>
          errors shouldBe List(
            InstanceAlreadyExists(ref.id, RootPosition at "create_function"))
        case Right(_) => fail()
      }
    }
  }
}
