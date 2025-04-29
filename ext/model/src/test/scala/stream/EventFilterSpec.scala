package fauna.model.test

import fauna.atoms._
import fauna.auth._
import fauna.lang._
import fauna.model.stream._
import fauna.repo.test.CassandraHelper
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ops._

class EventFilterSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val scope = ctx ! newScope
  val adminAuth = Auth.adminForScope(scope)
  val noAuth = adminAuth.withPermissions(NullPermissions)
  val collID = ctx ! mkCollection(adminAuth, MkObject("name" -> "foo"))
  val docID = DocID(SubID(1), collID)

  "EventFilter" - {

    "should always allow for a protocol events" in {
      (ctx ! EventFilter.check(noAuth, StreamStart(Timestamp.Epoch))) shouldBe true
    }

    "should check document read permission" - {
      val role = ctx ! {
        mkRole(
          adminAuth,
          "read_permissions",
          Seq(
            MkObject(
              "resource" -> ClassRef("foo"),
              "actions" -> MkObject("read" -> true)
            )))
      }

      val auth = ctx ! {
        RolePermissions.lookup(scope, Set(role)) map {
          adminAuth.withPermissions(_)
        }
      }

      "on new versions" in {
        val w = VersionAdd(
          scope,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data.empty,
          diff = None)
        (ctx ! EventFilter.check(auth, NewVersionAdded(w))) shouldBe true
        (ctx ! EventFilter.check(noAuth, NewVersionAdded(w))) shouldBe false
      }
    }

    "should check document history read permission" - {
      val role = ctx ! {
        mkRole(
          adminAuth,
          "history_read",
          Seq(
            MkObject(
              "resource" -> ClassRef("foo"),
              "actions" -> MkObject("history_read" -> true)
            )))
      }

      val auth = ctx ! {
        RolePermissions.lookup(scope, Set(role)) map {
          adminAuth.withPermissions(_)
        }
      }

      "on history rewrites" in {
        val w = VersionAdd(
          scope,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          Data.empty,
          diff = None)
        (ctx ! EventFilter.check(auth, HistoryRewrite(w))) shouldBe true
        (ctx ! EventFilter.check(noAuth, HistoryRewrite(w))) shouldBe false
      }

      "on removed versions" in {
        val w = VersionRemove(scope, docID, Unresolved, Create)
        (ctx ! EventFilter.check(auth, VersionRemoved(w))) shouldBe true
        (ctx ! EventFilter.check(noAuth, VersionRemoved(w))) shouldBe false
      }

      "on removed documents" in {
        val w = DocRemove(scope, docID)
        (ctx ! EventFilter.check(auth, DocumentRemoved(w))) shouldBe true
        (ctx ! EventFilter.check(noAuth, DocumentRemoved(w))) shouldBe false
      }
    }
  }
}
