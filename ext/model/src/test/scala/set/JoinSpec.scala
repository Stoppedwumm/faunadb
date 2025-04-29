package fauna.model.test

import fauna.ast._
import fauna.auth.Auth
import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.repo.test.CassandraHelper
import fauna.stats.StatsRequestBuffer
import scala.annotation.tailrec

class JoinSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  socialSetup(ctx, auth)

  "Join" - {
    "cursoring works with lambda join" in {
      ctx ! mkCollection(auth, MkObject("name" -> "shots"))
      ctx ! mkCollection(auth, MkObject("name" -> "authors"))
      ctx ! mkCollection(auth, MkObject("name" -> "follows2"))

      ctx ! mkIndex(
        auth,
        "active_followees_by_follower",
        "follows2",
        List(
          List("data", "follower"),
          List("data", "active")),
        List(
          List("data", "followee")))

      ctx ! mkIndex(
        auth,
        "latest_active_shots_by_author",
        "shots",
        List(
          List("data", "author"),
          List("data", "expired")),
        List(
          List("data", "createdAt"),
          List("ref"),
          List("data", "author")))

      val alice = ctx ! mkDoc(auth, s"authors", params =
        MkObject("data" -> MkObject("name" -> "alice")))
      val bob = ctx ! mkDoc(auth, s"authors", params =
        MkObject("data" -> MkObject("name" -> "bob")))

      ctx ! runQuery(auth, Clock.time, CreateF(ClassRef("follows2"), MkObject("data" ->
        MkObject("follower" -> alice.refObj, "active" -> true, "followee" -> bob.refObj))))

      (0 until 100) foreach { i =>
        ctx ! runQuery(auth, Clock.time, CreateF(ClassRef("shots"), MkObject("data" ->
          MkObject("author" -> bob.refObj, "expired" -> false, "createdAt" -> Epoch(i, "second")))))
      }

      @tailrec
      def run(cur: PaginateCursor, count: Int): Unit = {
        assert(count > 0)
        val page = ctx ! collection(auth, size = 10, cursor = cur, set = Join(
          Match(IndexRef("active_followees_by_follower"), JSArray(alice.refObj, true)),
          Lambda("person" ->
            Match(IndexRef("latest_active_shots_by_author"), JSArray(Var("person"), false)))))

        (page.after: @unchecked) match {
          case Some(CursorL(Right(ArrayL(List(TimeL(ts), RefL(_, shotRef1), RefL(_, authRef), RefL(_, _)))))) =>
            run(After(JSArray(
              Time(ts.toInstant.toString),
              Ref(s"classes/shots/${shotRef1.subID.toLong}"),
              Ref(s"classes/shots/${authRef.subID.toLong}"),
              Ref(s"classes/shots/${shotRef1.subID.toLong}"))),
              count - 1)
          case None => ()
        }
      }

      run(NoCursor, 100)
    }

    "basics work" in {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)

      ctx ! mkFollow(auth, alice, bob)
      ctx ! mkPost(auth, bob)

      val timeline = timelineFor(alice)

      val es = ctx ! events(auth, timeline)
      es.elems.size should be (1)

      val ids = ctx ! collection(auth, timeline)
      ids.elems.size should be (1)
    }

    "is historical" in {
      pending

      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)

      ctx ! mkFollow(auth, alice, bob)
      ctx ! mkFollow(auth, alice, charlie)
      ctx ! mkUnfollow(auth, alice, charlie)
      ctx ! mkUnfollow(auth, alice, bob)

      ctx ! mkPost(auth, bob)
      ctx ! mkPost(auth, bob)
      ctx ! mkPost(auth, bob)

      ctx ! mkPost(auth, charlie)
      ctx ! mkPost(auth, charlie)
      ctx ! mkPost(auth, charlie)
      ctx ! mkPost(auth, charlie)
      ctx ! mkPost(auth, charlie)
      ctx ! mkPost(auth, charlie)
      ctx ! mkPost(auth, charlie)

      val expected = Seq(TS(4875))

      val desc = ctx ! events(auth, timelineFor(alice))
      desc.elems map validTS should equal (expected)

      val asc = ctx ! events(auth, timelineFor(alice),
        cursor = After(MkObject("ts" -> 0)))
      asc.elems map validTS should equal (expected)
    }

    "prunes target sets" - {
      "descending" in {
        val alice = ctx ! mkPerson(auth)
        val bob = ctx ! mkPerson(auth)
        val charlie = ctx ! mkPerson(auth)

        ctx ! mkFollow(auth, alice, bob)
        ctx ! mkFollow(auth, alice, charlie)

        ctx ! mkUnfollow(auth, alice, bob)

        for (_ <- 4 to 19) {
          ctx ! mkPost(auth, bob)
          ctx ! mkPost(auth, charlie)
        }

        val posts = ctx ! postsFor(auth, charlie)

        val stats = new StatsRequestBuffer(Set("Storage.Cells.Read"))

        ctx ! events(auth, timelineFor(alice)) // preload cache

        val desc = CassandraHelper.withStats(stats) {
          ctx ! events(auth, timelineFor(alice))
        }

        desc.elems should equal (posts)

        // 3 follow events + 16 post events + 3 row timestamps
        stats.countOrZero("Storage.Cells.Read") should equal (22)
      }

      "ascending" in {
        val alice = ctx ! mkPerson(auth)
        val bob = ctx ! mkPerson(auth)
        val charlie = ctx ! mkPerson(auth)

        ctx ! mkFollow(auth, alice, charlie)

        for (_ <- 1 to 16) {
          ctx ! mkPost(auth, bob)
          ctx ! mkPost(auth, charlie)
        }

        ctx ! mkFollow(auth, alice, bob)

        val posts = ctx ! postsFor(auth, charlie)

        val stats = new StatsRequestBuffer(Set("Storage.Cells.Read"))

        ctx ! events(auth, timelineFor(alice)) // preload cache

        val asc = CassandraHelper.withStats(stats) {
          ctx ! events(auth, timelineFor(alice),
            cursor = After(MkObject("ts" -> 0)))
        }

        asc.elems should equal (posts)

        // 2 follow events + 16 post events + 3 row timestamps
        stats.countOrZero("Storage.Cells.Read") should equal (21)
      }
    }

    "inception join" - {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)

      val posts = List.newBuilder[Inst]
      var snapTS: Timestamp = Timestamp.Epoch
      for (i <- 1 to 30) {
        if (i == 10) ctx ! mkFollow(auth, alice, bob)
        if (i == 20) ctx ! mkUnfollow(auth, alice, bob)
        if (i == 15) ctx ! mkFollow(auth, bob, charlie)
        if (i == 30) ctx ! mkUnfollow(auth, bob, charlie)

        val post = ctx !! mkPost(auth, charlie)
        if (i >= 1 && i < 20) {
          snapTS = post.transactionTS
          posts += post.value
        }
      }
      val expected = posts.result() map { _.id }

      val postsQ =
        Join(followsFor(alice), Lambda("x" ->
          Join(followsFor(Var("x")), Lambda("y" ->
            postsFor(Var("y"))))))

      "collection" in {
        val coll = (ctx ! collection(auth, postsQ, ts = snapTS)).elems collect { case RefL(_, id) => id }
        coll should equal (expected)
      }

      "historical" in {
        pending
        // the period during which both:
        // * alice follows bob
        // * bob follows charlie
        val expected = Seq(TS(15))

        // FIXME: due to a schema order problem, the expected result
        // below does not work. the create event at TS(20) is filtered
        // out because instanceIDs are affecting the on disk event sort.
        // val expected = Seq(TS(20))

        (ctx ! events(auth, postsQ)).elems map validTS should equal (expected)
      }
    }

    "target is filtered by source timestamp" - {
      val alice = ctx ! mkPerson(auth)
      val carol = ctx ! mkPerson(auth)

      //create a post before carol follow alice
      val postBeforeFollow = ctx ! mkPost(auth, alice)

      val follow = ctx ! mkFollow(auth, carol, alice)

      //create a post after carol follow alice
      val postAfterFollow = ctx ! mkPost(auth, alice)

      val postsByAuthorQ = Join(
        followsFor(carol),           //source set
        IndexRef("posts_by_author")  //target set
      )

      "collection" in {
        val page = ctx ! collection(auth, postsByAuthorQ)
        page.elems shouldBe List(RefL(scope, postBeforeFollow.id), RefL(scope, postAfterFollow.id))
      }

      "historical" in {
        val Seq(eventBeforeFollow, eventAfterFollow) = ctx ! postsFor(auth, alice)
        val page = ctx ! events(auth, postsByAuthorQ)
        page.elems shouldBe Seq(eventAfterFollow)

        (ctx ! runQuery(auth, Get(follow.refObj))) match {
          case VersionL(version, _) =>
            eventBeforeFollow.e.ts.validTS should be < version.ts.validTS
            eventAfterFollow.e.ts.validTS should be > version.ts.validTS
          case _ => fail()
        }

      }
    }
  }
}
