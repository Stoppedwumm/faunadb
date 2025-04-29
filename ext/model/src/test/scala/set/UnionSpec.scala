package fauna.model.test

import fauna.auth.Auth
import fauna.repo.test.CassandraHelper
import fauna.stats.StatsRequestBuffer

class UnionSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "Union" - {
    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    socialSetup(ctx, auth)

    "basics work" in {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)

      ctx ! mkFollow(auth, alice, bob)
      ctx ! mkFollow(auth, charlie, bob)
      ctx ! mkPost(auth, bob)

      val alicePosts = timelineFor(alice)
      val charliePosts = timelineFor(charlie)
      val union = Union(alicePosts, charliePosts)

      val es = ctx ! events(auth, union)
      es.elems.size should be (1)

      val ids = ctx ! collection(auth, union)
      ids.elems.size should be (1)
    }

    "minimizes work" in {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)

      for (_ <- 1 to 16) {
        ctx ! mkPost(auth, bob)
        ctx ! mkPost(auth, charlie)
      }
      ctx ! mkFollow(auth, alice, bob)
      ctx ! mkFollow(auth, alice, charlie)

      // FIXME: This just tests that union does nothing if passed a
      // single set. This should probably also test correct behavior
      // of an actual union calculation.
      val union = Union(timelineFor(alice))

      val stats = new StatsRequestBuffer(Set("Storage.Cells.Read"))

      ctx ! events(auth, union) // preload cache

      CassandraHelper.withStats(stats) {
        ctx ! events(auth, union)
      }

      // 2 follow events + 3 row timestamps
      stats.countOrZero("Storage.Cells.Read") should equal (5)

      val stats2 = new StatsRequestBuffer(Set("Storage.Cells.Read"))

      ctx ! collection(auth, union) // preload cache

      CassandraHelper.withStats(stats2) {
        ctx ! collection(auth, union)
      }

      // 2 follow events + 32 post events + 3 row timestamps
      stats2.countOrZero("Storage.Cells.Read") should equal (37)
    }
  }
}
