package fauna.model.test

import fauna.ast.RefL
import fauna.auth.Auth
import fauna.repo.test.CassandraHelper
import fauna.stats.StatsRequestBuffer

class FilterSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  socialSetup(ctx, auth)

  "Intersection" - {
    "basics work" in {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)

      ctx ! mkFollow(auth, alice, bob)
      ctx ! mkFollow(auth, alice, charlie)

      ctx ! mkFollow(auth, charlie, bob)

      val set = Intersection(followsFor(alice), followsFor(charlie))

      val es = ctx ! events(auth, set)
      es.elems.size should be (1)

      val ids = ctx ! collection(auth, set)
      ids.elems.size should be (1)
    }

    "mix value and value-less indexes" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "supplier"))
      ctx ! mkCollection(auth, MkObject("name" -> "part"))
      ctx ! mkCollection(auth, MkObject("name" -> "supplier_parts"))

      ctx ! mkIndex(auth, "all_parts", "part", List.empty)
      ctx ! mkIndex(auth, "parts_by_supplier", "supplier_parts", List(List("data", "supplier")), List(List("data", "part")))

      val supplier = ctx ! mkDoc(auth, "supplier")
      val part1 = ctx ! mkDoc(auth, "part")
      val part2 = ctx ! mkDoc(auth, "part")
      ctx ! mkDoc(auth, "supplier_parts", MkObject("data" -> MkObject(
        "supplier" -> supplier.refObj,
        "part" -> part1.refObj
      )))

      (ctx ! collection(auth, Match(IndexRef("all_parts")))).elems shouldBe Seq(RefL(scope, part1.id), RefL(scope, part2.id))
      (ctx ! collection(auth, Match(IndexRef("parts_by_supplier"), supplier.refObj))).elems shouldBe Seq(RefL(scope, part1.id))

      //all parts
      val set = Intersection(
        Match(IndexRef("all_parts")),
        Match(IndexRef("parts_by_supplier"), supplier.refObj)
      )

      val parts = ctx ! collection(auth, set)

      parts.elems shouldBe Seq(RefL(scope, part1.id))
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

      val set = Intersection(timelineFor(alice))

      val stats = new StatsRequestBuffer(Set("Storage.Cells.Read"))

      ctx ! collection(auth, set, size = 32) // preload cache

      CassandraHelper.withStats(stats) {
        ctx ! collection(auth, set, size = 32)
      }

      // 2 follow events + 32 post events + 3 row timestamps
      stats.countOrZero("Storage.Cells.Read") should equal (37)
    }

    "events view correctly filters out non-matching entries when index has covered values" in {
      ctx ! mkCollection(auth, MkObject("name" -> "cats"))
      ctx ! mkIndex(auth, "cat_cats", "cats", List(List("data", "cat")), List(List("data", "subcat")))

      val short = Match(IndexRef("cat_cats"), "shorthair")
      val coon = Match(IndexRef("cat_cats"), "coon")

      ctx ! mkDoc(auth, "cats", MkObject(
        "data" -> MkObject(
          "cat" -> "shorthair",
          "subcat" -> "american")))

      ctx ! mkDoc(auth, "cats", MkObject(
        "data" -> MkObject(
          "cat" -> "coon",
          "subcat" -> "exotic")))

      (ctx ! collection(auth, short)).elems.size should equal (1)
      (ctx ! events(auth, short)).elems.size should equal (1)

      (ctx ! collection(auth, coon)).elems.size should equal (1)
      (ctx ! events(auth, coon)).elems.size should equal (1)

      (ctx ! collection(auth, Intersection(short, coon))).elems.size should equal (0)
      (ctx ! events(auth, Intersection(short, coon))).elems.size should equal (0)
    }
  }

  "Difference" - {
    "basics work" in {
      val alice = ctx ! mkPerson(auth)
      val bob = ctx ! mkPerson(auth)
      val charlie = ctx ! mkPerson(auth)

      ctx ! mkFollow(auth, alice, bob)
      ctx ! mkFollow(auth, alice, charlie)

      ctx ! mkFollow(auth, charlie, bob)

      val set = Difference(followsFor(alice), followsFor(charlie))

      val ids = ctx ! collection(auth, set)
      ids.elems.size should equal (1)
    }

    "mix value and value-less indexes" in {
      val scope = ctx ! newScope
      val auth = Auth.forScope(scope)

      ctx ! mkCollection(auth, MkObject("name" -> "supplier"))
      ctx ! mkCollection(auth, MkObject("name" -> "part"))
      ctx ! mkCollection(auth, MkObject("name" -> "supplier_parts"))

      ctx ! mkIndex(auth, "all_parts", "part", List.empty)
      ctx ! mkIndex(auth, "parts_by_supplier", "supplier_parts", List(List("data", "supplier")), List(List("data", "part")))

      val supplier = ctx ! mkDoc(auth, "supplier")
      val part1 = ctx ! mkDoc(auth, "part")
      val part2 = ctx ! mkDoc(auth, "part")
      ctx ! mkDoc(auth, "supplier_parts", MkObject("data" -> MkObject(
        "supplier" -> supplier.refObj,
        "part" -> part1.refObj
      )))

      (ctx ! collection(auth, Match(IndexRef("all_parts")))).elems shouldBe Seq(RefL(scope, part1.id), RefL(scope, part2.id))
      (ctx ! collection(auth, Match(IndexRef("parts_by_supplier"), supplier.refObj))).elems shouldBe Seq(RefL(scope, part1.id))

      //all parts except those from the supplier
      val set = Difference(
        Match(IndexRef("all_parts")),
        Match(IndexRef("parts_by_supplier"), supplier.refObj)
      )

      val parts = ctx ! collection(auth, set)

      parts.elems shouldBe Seq(RefL(scope, part2.id))
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

      // FIXME: This just tests that difference does nothing if passed
      // a single set. This should probably also test correct behavior
      // of an actual difference calculation.
      val set = Difference(timelineFor(alice))

      val stats = new StatsRequestBuffer(Set("Storage.Cells.Read"))

      ctx ! collection(auth, set, size = 32) // preload cache

      CassandraHelper.withStats(stats) {
        ctx ! collection(auth, set, size = 32)
      }

      // 2 follow events + 32 post events + 3 row timestamps
      stats.countOrZero("Storage.Cells.Read") should equal (37)
    }
  }
}
