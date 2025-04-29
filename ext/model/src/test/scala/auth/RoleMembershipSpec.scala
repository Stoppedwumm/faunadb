package fauna.model.auth.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.lang.clocks._
import fauna.model._
import fauna.model.test._
import fauna.repo.test.CassandraHelper

class RoleMembershipSpec extends Spec {
  import SocialHelpers._

  private val ctx = CassandraHelper.context("model")
  private val scope = ctx ! newScope
  private val auth = Auth.adminForScope(scope)

  ctx ! mkCollection(auth, MkObject("name" -> "a_class"))

  private val anInstance =
    (ctx ! mkDoc(auth, "a_class")).id

  private def rolesFor(id: DocID): Set[RoleID] =
    ctx ! RoleMembership.lookupRoles(scope, id, auth.source)

  private def createRole(config: JSValue): RoleID = {
    ctx ! runQuery(auth, CreateRole(config)) match {
      case VersionL(v, _) => v.id.as[RoleID]
      case _              => throw new RuntimeException()
    }
  }

  "RoleMembership" - {
    "is consistent with changes in the roles" in {
      val roleA = createRole(
        MkObject(
          "name" -> "role_a",
          "membership" -> MkObject(
            "resource" -> ClassRef("a_class")
          ),
          "privileges" -> MkObject(
            "resource" -> ClassRef("a_class"),
            "actions" -> MkObject("read" -> true)
          )
        )
      )

      rolesFor(anInstance) should contain only roleA

      val roleB = createRole(
        MkObject(
          "name" -> "role_b",
          "membership" -> MkObject(
            "resource" -> ClassRef("a_class")
          ),
          "privileges" -> MkObject(
            "resource" -> ClassRef("a_class"),
            "actions" -> MkObject("read" -> true)
          )
        )
      )

      ctx ! Cache.invalidate(scope, roleB.toDocID, Clock.time)
      rolesFor(anInstance) should equal(Set(roleA, roleB))

      ctx ! runQuery(
        auth,
        Update(
          RoleRef("role_a"),
          MkObject("membership" -> JSNull)
        )
      )

      ctx ! Cache.invalidate(scope, roleA.toDocID, Clock.time)
      rolesFor(anInstance) should contain only roleB
    }
  }
}
