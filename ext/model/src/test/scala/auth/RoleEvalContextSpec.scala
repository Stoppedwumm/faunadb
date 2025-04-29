package fauna.model.auth.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.lang.clocks._
import fauna.model._
import fauna.model.test._
import fauna.repo.test.CassandraHelper

class RoleEvalContextSpec extends Spec {
  import SocialHelpers._

  private val ctx = CassandraHelper.context("model")
  private val scope = ctx ! newScope
  private val auth = Auth.adminForScope(scope)

  ctx ! mkCollection(auth, MkObject("name" -> "a_class"))

  private val foo = createInstance("foo")
  private val bar = createInstance("bar")
  private val baz = createInstance("baz")

  private def createInstance(name: String) =
    (ctx ! mkDoc(auth,
                      "a_class",
                      params = MkObject("data" -> MkObject(name -> 42)))).id

  private def createRole(config: JSValue): RoleID = {
    ctx ! runQuery(auth, CreateRole(config)) match {
      case VersionL(v, _) => v.id.as[RoleID]
      case _              => throw new RuntimeException()
    }
  }

  private def canRead(name: String) =
    QueryF(
      Lambda(
        "ref" ->
          Contains(JSArray("data", name), Get(Var("ref")))))

  private def eval(action: RoleEvalAction, roles: RoleID*): Boolean = {
    val evalContext = ctx ! RoleEvalContext.lookup(action.scope, Set(roles: _*))
    ctx ! evalContext.eval(action, None, auth.source)
  }

  "RoleEvalContext" - {
    "is consistent with changes in the roles" in {
      val roleA = createRole(
        MkObject(
          "name" -> "role_a",
          "privileges" -> MkObject(
            "resource" -> ClassRef("a_class"),
            "actions" -> MkObject(
              "read" -> canRead("foo")
            )
          )
        ))

      val roleB = createRole(
        MkObject(
          "name" -> "role_b",
          "privileges" -> MkObject(
            "resource" -> ClassRef("a_class"),
            "actions" -> MkObject(
              "read" -> canRead("bar")
            )
          )
        ))

      eval(ReadInstance(scope, foo), roleA, roleB) should be(true)
      eval(ReadInstance(scope, bar), roleA, roleB) should be(true)
      eval(ReadInstance(scope, baz), roleA, roleB) should be(false)

      ctx ! runQuery(
        auth,
        Update(
          RoleRef("role_a"),
          MkObject(
            "privileges" -> MkObject(
              "resource" -> ClassRef("a_class"),
              "actions" -> MkObject(
                "read" -> canRead("baz")
              )
            ))
        )
      )

      // manually force cache invalidation
      ctx ! Cache.invalidate(scope, roleA.toDocID, Clock.time)

      eval(ReadInstance(scope, foo), roleA, roleB) should be(false)
      eval(ReadInstance(scope, bar), roleA, roleB) should be(true)
      eval(ReadInstance(scope, baz), roleA, roleB) should be(true)

      ctx ! runQuery(auth, DeleteF(RoleRef("role_a")))
      ctx ! Cache.invalidate(scope, roleA.toDocID, Clock.time)

      eval(ReadInstance(scope, foo), roleA, roleB) should be(false)
      eval(ReadInstance(scope, bar), roleA, roleB) should be(true)
      eval(ReadInstance(scope, baz), roleA, roleB) should be(false)
    }
  }
}
