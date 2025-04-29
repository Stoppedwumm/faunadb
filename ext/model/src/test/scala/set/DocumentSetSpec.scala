package fauna.model.test

import fauna.ast._
import fauna.auth._
import fauna.repo._
import fauna.repo.test.CassandraHelper
import fauna.storage.{ Add, AtValid, Remove, SetAction, SetEvent }
import scala.concurrent.duration._

class DocumentSetSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  socialSetup(ctx, auth)

  "DocumentSetSpec" - {
    "works" in {
      val alice = ctx ! mkPerson(auth)
      val post = ctx !! mkPost(auth, alice)
      val del = ctx !! runQuery(auth, DeleteF(post.value.refObj))

      val timeline = Singleton(post.value.refObj)
      val evs = ctx ! events(auth, timeline)
      evs.elems map validTS should equal (Seq(post.transactionTS, del.transactionTS))
      evs.before map validTS should equal (None)
      evs.after map validTS should equal (None)
    }

    "intersections and differences" - {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)
      val postByAlice = ctx !! mkPost(auth, alice)
      val postByBob = ctx !! mkPost(auth, bob)
      val postByCharlie = ctx !! mkPost(auth, charlie)
      val alicesPosts = Match("posts_by_author", alice.refObj)
      val bobsPosts = Match("posts_by_author", bob.refObj)

      def literalForPost(post: RepoContext.Result[Inst]) = RefL(scope, post.value.id)
      def eventForPost(post: RepoContext.Result[Inst], action: SetAction) =
        SetEventL(
          SetEvent(AtValid(post.transactionTS), scope, post.value.id, action))

      "intersections" - {
        "identical sets" in {
          val query = Intersection(alicesPosts, alicesPosts)
          val coll = ctx ! collection(auth, query)
          coll.elems should equal (Seq(literalForPost(postByAlice)))

          val eventsColl = ctx ! events(auth, query)
          eventsColl.elems should equal (Seq(eventForPost(postByAlice, Add)))
        }

        // Intersection of two disjoint sets.
        "disjoint sets" in {
          val coll = ctx ! collection(auth, Intersection(alicesPosts, bobsPosts))
          coll.elems.isEmpty should be (true)
        }

        // Intersection of two sets, losing elements from both sets.
        "partially overlapping sets" in {
          val query = Intersection(Union(Singleton(postByAlice.value.refObj), Singleton(postByBob.value.refObj)),
                                   Union(Singleton(postByBob.value.refObj), Singleton(postByCharlie.value.refObj)))
          val coll = ctx ! collection(auth, query)
          coll.elems should equal (Seq(literalForPost(postByBob)))

          val eventsColl = ctx ! events(auth, query)
          eventsColl.elems should equal (Seq(eventForPost(postByBob, Add)))
        }
      }

      "differences" - {
        "identical sets" in {
          val query = Difference(Singleton(postByAlice.value.refObj), alicesPosts)
          val coll = ctx ! collection(auth, query)
          coll.elems.isEmpty should be (true)
        }

        "disjoint sets" in {
          val query = Difference(alicesPosts, bobsPosts)
          val coll = ctx ! collection(auth, query)
          coll.elems should equal (Seq(literalForPost(postByAlice)))

          val eventsColl = ctx ! events(auth, query)
          eventsColl.elems should equal (Seq(eventForPost(postByAlice, Add)))
        }

        "partially overlapping sets" in {
          val query = Difference(Union(Singleton(postByAlice.value.refObj), Singleton(postByBob.value.refObj)),
                                 Union(Singleton(postByBob.value.refObj), Singleton(postByCharlie.value.refObj)))
          val coll = ctx ! collection(auth, query)
          coll.elems should equal (Seq(literalForPost(postByAlice)))

          val eventsColl = ctx ! events(auth, query)
          eventsColl.elems should equal (Seq(eventForPost(postByAlice, Add), eventForPost(postByBob, Add), eventForPost(postByBob, Remove)))
        }
      }
    }

    // This test demonstrates a workaround: fold
    // functions like Count have their page size reduced so they don't
    // cause over-wide queries.
    "page size" - {
      val limit = 10
      val count = 3 * limit
      val lctx = ctx.copy(queryMaxWidth = limit)

      "intersection" in {
        val alice = lctx ! mkPerson(auth)
        (1 to count) foreach { _ => lctx ! mkPost(auth, alice) }

        val query = Count(Intersection(Match("posts_by_author", alice.refObj), Match("posts_by_author", alice.refObj)))
        (lctx ! runQuery(auth, query)) should be (LongL(count))
      }

      "union" in {
        val alice = lctx ! mkPerson(auth)
        (1 to count) foreach { _ => lctx ! mkPost(auth, alice) }

        val bob = lctx ! mkPerson(auth)
        val bobPost = lctx ! mkPost(auth, bob)

        val query = Count(Union(Singleton(bobPost.refObj), Match("posts_by_author", alice.refObj)))
        (lctx ! runQuery(auth, query)) should be (LongL(count + 1))
      }
    }

    "cursors" in {
      val alice = ctx ! mkPerson(auth)
      val post = ctx !! mkPost(auth, alice)
      val del = ctx !! runQuery(auth, DeleteF(post.value.refObj))
      val ins = ctx ! runQuery(auth, Do(InsertVers(post.value.refObj, Time("now"), "create"), Time("now")))
      val insTS = ins.asInstanceOf[TimeL].value

      val timeline = Singleton(post.value.refObj)

      // start wherever time begins
      val nocursor = ctx ! events(auth, timeline, size = 2)
      nocursor.elems map validTS should equal (Seq(post.transactionTS, del.transactionTS))
      nocursor.before map validTS should equal (None)
      nocursor.after map validTS should equal (Some(insTS))

      val after = After(MkObject(
        "ts" -> insTS.micros,
        "action" -> "create",
        "resource" -> post.value.refObj))

      // follow nocursor.after
      val page2 = ctx ! events(auth, timeline, size = 2, cursor = after)
      page2.elems map validTS should equal (Seq(insTS))
      page2.before map validTS should equal (Some(insTS))
      page2.after map validTS should equal (None)

      val moonTS = insTS + 1.millisecond
      val before = Before(MkObject(
        "ts" -> moonTS.micros,
        "action" -> "delete",
        "resource" -> post.value.refObj))

      // start from the end of time, and moon walk
      val reverse = ctx ! events(auth, timeline, size = 2, cursor = before)
      reverse.elems map validTS should equal (Seq(del.transactionTS, insTS))
      reverse.before map validTS should equal (Some(del.transactionTS))
      reverse.after map validTS should equal (Some(moonTS))

      // follow reverse.before
      val two = Before(MkObject(
        "ts" -> del.transactionTS.micros,
        "action" -> "delete",
        "resource" -> post.value.refObj))

      val origin = ctx ! events(auth, timeline, size = 2, cursor = two)
      origin.elems map validTS should equal (Seq(post.transactionTS))
      origin.before map validTS should equal (None)
      origin.after map validTS should equal (Some(del.transactionTS))

      // check that a more inclusive action remains _exclusive_ in
      // before position
      val twotwo = Before(MkObject(
        "ts" -> del.transactionTS.micros,
        "action" -> "create",
        "resource" -> post.value.refObj))

      val origintwo = ctx ! events(auth, timeline, size = 2, cursor = twotwo)
      origintwo.elems map validTS should equal (Seq(post.transactionTS))
      origintwo.before map validTS should equal (None)
      origintwo.after map validTS should equal (Some(del.transactionTS))
    }

    "synthetic cursors" in {
      val alice = ctx ! mkPerson(auth)
      val post = ctx !! mkPost(auth, alice)
      val del = ctx !! runQuery(auth, DeleteF(post.value.refObj))
      val insTS = ctx ! runQuery(auth, Do(InsertVers(post.value.refObj, Time("now"), "create"), Time("now")))

      val timeline = Singleton(post.value.refObj)

      val delCursor = MkObject("ts" -> del.transactionTS.micros)

      val bef = ctx ! events(auth, timeline, cursor = Before(delCursor))
      bef.elems map validTS should equal (Seq(post.transactionTS))
      bef.before map validTS should equal (None)
      bef.after map validTS should equal (Some(del.transactionTS))

      val fut = ctx ! events(auth, timeline, cursor = After(delCursor))
      fut.elems map validTS should equal (Seq(insTS.asInstanceOf[TimeL].value))
      fut.before map validTS should equal (Some(del.transactionTS))
      fut.after map validTS should equal (None)
    }
  }
}
