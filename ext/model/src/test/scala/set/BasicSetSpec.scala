package fauna.model.test

import fauna.ast._
import fauna.auth.Auth
import fauna.repo.test.CassandraHelper

class BasicSetSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  socialSetup(ctx, auth)

  "Basics" - {
    val alice = ctx ! mkPerson(auth)
    val bob = ctx ! mkPerson(auth)
    val charlie = ctx ! mkPerson(auth)

    ctx ! mkFollow(auth, alice, bob)
    ctx ! mkFollow(auth, alice, charlie)

    "works" in {
      val pg = ctx ! collection(auth, followsFor(alice))
      pg.elems should equal (Seq(bob, charlie) map { u => RefL(scope, u.id) })
    }

    "can get page of size 0" in {
      val set = followsFor(alice)

      (ctx ! runQuery(auth, Paginate(set, size = 0))) match {
        case PageL(ids, _, before, after) =>
          ids.size should equal (0)
          before should equal (None)
          after should equal (None)
        case r => sys.error(s"Unexpected: $r")
      }

      (ctx ! runQuery(auth, Paginate(set, After(charlie.refObj), size = 0))) match {
         case PageL(ids, _, Some(before), Some(after)) =>
          ids.size should equal (0)
          before should equal (CursorL(Right(ArrayL(List(RefL(scope, charlie.id))))))
          after should equal (CursorL(Right(ArrayL(List(RefL(scope, (charlie.id)))))))
        case r => sys.error(s"Unexpected: $r")
      }

      (ctx ! runQuery(auth, Paginate(set, Before(bob.refObj), size = 0))) match {
        case PageL(ids, _, Some(before), Some(after)) =>
          ids.size should equal (0)
          before should equal (CursorL(Right(ArrayL(List(RefL(scope, bob.id))))))
          after should equal (CursorL(Right(ArrayL(List(RefL(scope, bob.id))))))
        case r => sys.error(s"Unexpected: $r")
      }
    }
  }
}
