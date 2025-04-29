package fauna.atoms.test

import fauna.atoms._

class SegmentSpec extends Spec {
  val M = Long.MinValue

  "Segment" - {
    "represents the ring" in {
      noException should be thrownBy {
        Segment(Location.MinValue, Location.MinValue)
      }

      an[IllegalArgumentException] should be thrownBy {
        Segment(Location(1), Location(0))
      }

      an[IllegalArgumentException] should be thrownBy {
        Segment(Location(0), Location(0))
      }

    }

    "normalizes" in {
      val everything = Segment(Location.MinValue, Location.MinValue)
      val big = Segment(Location.MinValue, Location.MaxValue)
      val small = Segment(Location(0), Location(1))
      val disjoint = Segment(Location(10), Location(20))

      Segment.normalize(Seq.empty[Segment]).isEmpty should be(true)
      Segment.normalize(Seq(everything, big, small, disjoint)) should equal(
        Seq(everything)
      )
      Segment.normalize(Seq(big, small, disjoint)) should equal(Seq(big))
      Segment.normalize(Seq(small, disjoint)) should equal(Seq(small, disjoint))

      val border = Location(4)
      val left = Segment(Location.MinValue, border)
      val right = Segment(border, Location.MinValue)
      Segment.normalize(Seq(left, right)) should equal(Seq(everything))
    }

    "contains" in {
      Segment.All.contains(Location.MinValue) should equal(true)

      val seg = Segment(Location(0), Location(20))
      seg.contains(Location.MinValue) should equal(false)
      seg.contains(Location(0)) should equal(true)
      seg.contains(Location(10)) should equal(true)
      seg.contains(Location(20)) should equal(false)
      seg.contains(Location(21)) should equal(false)

      val ringEnd = Segment(Location(10), Location.MinValue)
      ringEnd.contains(Location.MaxValue) should equal(true)
      ringEnd.contains(Location.MinValue) should equal(false)
    }

    "splits" in {
      val step = (Long.MaxValue / 10)
      ((Long.MinValue + step) until Long.MaxValue by step) foreach { tkn =>
        val loc = Location(tkn)
        val Seq(leftSeg, rightSeg) = Segment.All.split(loc)
        leftSeg.left should equal(Segment.All.left)
        leftSeg.right should equal(loc)
        rightSeg.left should equal(loc)
        rightSeg.right should equal(Segment.All.right)
      }

      val Seq(min) = Segment.All.split(Location.MinValue)
      min should equal(Segment.All)

      an[IllegalArgumentException] should be thrownBy {
        Segment(Location(0), Location(10)).split(Location(20))
      }
    }

    "subSegments" in {
      an[IllegalArgumentException] should be thrownBy {
        Segment.All.subSegments(0)
      }

      def assertSeg(start: Long, end: Long, quantity: Int, segs: Seq[(Int, Int)]): Unit = {
        val split = Segment(Location(start), Location(end)).subSegments(quantity)

        split.size shouldBe quantity
        split shouldBe toSegments(segs)
        split.head.left.token shouldBe start
        split.last.right.token shouldBe end

        if (quantity > 1) {
          val sizes = split.dropRight(1) map { s =>
            val right = if (s.right.isMin) Location.MaxValue else s.right
            right.token - s.left.token
          } toSet

          sizes.size shouldBe 1
        }
      }

      assertSeg(start = -99, end = 99, quantity = 3, Seq(-99 -> -33, -33 -> 33, 33 -> 99))
      assertSeg(start = 0, end = 99, quantity = 3, Seq(0 -> 33, 33 -> 66, 66 -> 99))
      assertSeg(start = -99, end = 0, quantity = 3, Seq(-99 -> -66, -66 -> -33, -33 -> 0))

      assertSeg(start = -100, end = 100, quantity = 1, Seq(-100 -> 100))
      assertSeg(start = -100, end = 100, quantity = 2, Seq(-100 -> 0, 0 -> 100))
      assertSeg(start = -100, end = 100, quantity = 3, Seq(-100 -> -33, -33 -> 34, 34 -> 100))
      assertSeg(start = -100, end = 100, quantity = 4, Seq(-100 -> -50, -50 -> 0, 0 -> 50, 50 -> 100))
      assertSeg(start = -100, end = 100, quantity = 5, Seq(-100 -> -60, -60 -> -20, -20 -> 20, 20 -> 60, 60 -> 100))

      assertSeg(start = 0, end = 100, quantity = 1, Seq(0 -> 100))
      assertSeg(start = 0, end = 100, quantity = 2, Seq(0 -> 50, 50 -> 100))
      assertSeg(start = 0, end = 100, quantity = 3, Seq(0 -> 33, 33 -> 66, 66 -> 100))
      assertSeg(start = 0, end = 100, quantity = 4, Seq(0 -> 25, 25 -> 50, 50 -> 75, 75 -> 100))
      assertSeg(start = 0, end = 100, quantity = 5, Seq(0 -> 20, 20 -> 40, 40 -> 60, 60 -> 80, 80 -> 100))

      Segment.All.subSegments(3) shouldBe toSegmentsL(
        Seq(
          -9223372036854775808L -> -3074457345618258603L,
          -3074457345618258603L -> 3074457345618258602L,
          3074457345618258602L -> -9223372036854775808L
        ))

      Segment.All.subSegments(5) shouldBe toSegmentsL(
        Seq(
          -9223372036854775808L -> -5534023222112865485L,
          -5534023222112865485L -> -1844674407370955162L,
          -1844674407370955162L -> 1844674407370955161L,
          1844674407370955161L -> 5534023222112865484L,
          5534023222112865484L -> -9223372036854775808L
        ))

      Segment.All.subSegments(14) shouldBe toSegmentsL(
        Seq(
          -9223372036854775808L -> -7905747460161236407L,
          -7905747460161236407L -> -6588122883467697006L,
          -6588122883467697006L -> -5270498306774157605L,
          -5270498306774157605L -> -3952873730080618204L,
          -3952873730080618204L -> -2635249153387078803L,
          -2635249153387078803L -> -1317624576693539402L,
          -1317624576693539402L -> -1L,
          -1L -> 1317624576693539400L,
          1317624576693539400L -> 2635249153387078801L,
          2635249153387078801L -> 3952873730080618202L,
          3952873730080618202L -> 5270498306774157603L,
          5270498306774157603L -> 6588122883467697004L,
          6588122883467697004L -> 7905747460161236405L,
          7905747460161236405L -> -9223372036854775808L
        ))
    }

    "diff" in {
      // precedes
      expectDiff(10, 20, 9, 10, Seq((10, 20)))
      expectDiff(10, 20, 20, 21, Seq((10, 20)))

      // covers
      expectDiff(10, 20, 10, 20, Seq.empty)
      expectDiff(10, 20, 9, 20, Seq.empty)
      expectDiff(10, 20, 10, 21, Seq.empty)
      expectDiff(10, 20, 9, 21, Seq.empty)

      // just nibs at it
      expectDiff(10, 20, 10, 11, Seq((11, 20)))
      expectDiff(10, 20, 19, 20, Seq((10, 19)))

      // proper overlaps
      expectDiff(10, 20, 15, 25, Seq((10, 15)))
      expectDiff(10, 20, 15, M, Seq((10, 15)))
      expectDiff(10, 20, 5, 15, Seq((15, 20)))
      expectDiff(10, 20, M, 15, Seq((15, 20)))

      // Minimal proper non-aligned subset
      expectDiff(10, 13, 11, 12, Seq((10, 11), (12, 13)))

      expectDiff(M, M, M, M, Seq.empty)
      expectDiff(M, M, 0, M, Seq((M, 0)))
      expectDiff(M, M, 100, 200, Seq((M, 100), (200, M)))
      expectDiff(M, 0, 0, M, Seq((M, 0)))
    }

    "diff multiple" in {
      expectDiffs(
        Seq(1 -> 7, 8 -> 13, 14 -> 16, 16 -> 17),
        Seq(0 -> 1, 2 -> 3, 4 -> 5, 6 -> 9, 10 -> 11, 12 -> 13, 15 -> 17, 18 -> 19),
        Seq(1 -> 2, 3 -> 4, 5 -> 6, 9 -> 10, 11 -> 12, 14 -> 15)
      )
      // Diff with empty
      expectDiffs(
        Seq(1 -> 7, 8 -> 13, 14 -> 16, 16 -> 17),
        Seq.empty,
        Seq(1 -> 7, 8 -> 13, 14 -> 16, 16 -> 17)
      )
      // Diff empty
      expectDiffs(
        Seq.empty,
        Seq(1 -> 7, 8 -> 13, 14 -> 16, 16 -> 17),
        Seq.empty
      )
    }

    "intersect" in {
      // precedes
      expectIntersect(10, 20, 9, 10, None)
      expectIntersect(10, 20, 20, 21, None)

      // covers
      expectIntersect(10, 20, 10, 20, Some((10, 20)))
      expectIntersect(10, 20, 9, 20, Some((10, 20)))
      expectIntersect(10, 20, 10, 21, Some((10, 20)))
      expectIntersect(10, 20, 9, 21, Some((10, 20)))

      // just barely overlap
      expectIntersect(10, 20, 10, 11, Some((10, 11)))
      expectIntersect(10, 20, 19, 20, Some((19, 20)))

      // proper overlaps
      expectIntersect(10, 20, 15, 25, Some((15, 20)))
      expectIntersect(10, 20, 15, M, Some((15, 20)))
      expectIntersect(10, 20, 5, 15, Some((10, 15)))
      expectIntersect(10, 20, M, 15, Some((10, 15)))

      // Minimal proper non-aligned subset
      expectIntersect(10, 13, 11, 12, Some((11, 12)))

      expectIntersect(M, M, M, M, Some((M, M)))
      expectIntersect(M, M, 0, M, Some((0, M)))
      expectIntersect(M, M, 100, 200, Some((100, 200)))
      expectIntersect(M, 0, 0, M, None)
    }

    "midpoint" in {
      Segment.All.midpoint should equal(Location(0L))
      Segment(Location.MaxValue, Location.MinValue).midpoint should equal(
        Location.MaxValue
      )
      Segment(Location(0L), Location.MinValue).midpoint should equal(
        Location(4611686018427387903L)
      )
    }
  }

  private def expectDiff(
    left1: Long,
    right1: Long,
    left2: Long,
    right2: Long,
    expected: Seq[(Long, Long)]
  ) =
    Segment(Location(left1), Location(right1)) diff Segment(
      Location(left2),
      Location(right2)
    ) map { s =>
      (s.left.token, s.right.token)
    } should equal(expected)

  private def expectDiffs(
    min: Seq[(Int, Int)],
    sub: Seq[(Int, Int)],
    diff: Seq[(Int, Int)]) = {
    Segment.diff(toSegments(min), toSegments(sub)) should equal(toSegments(diff))
  }

  private def toSegmentsL(s: Seq[(Long, Long)]) =
    s map { case (l, r) => Segment(Location(l), Location(r)) }

  private def toSegments(s: Seq[(Int, Int)]) =
    s map { case (l, r) => Segment(Location(l), Location(r)) }

  private def expectIntersect(
    left1: Long,
    right1: Long,
    left2: Long,
    right2: Long,
    expected: Option[(Long, Long)]
  ): Unit = {
    val s1 = Segment(Location(left1), Location(right1))
    val s2 = Segment(Location(left2), Location(right2))
    // intersect is symmetric
    expectIntersect(s1, s2, expected)
    expectIntersect(s2, s1, expected)
  }

  private def expectIntersect(
    s1: Segment,
    s2: Segment,
    expected: Option[(Long, Long)]
  ): Unit =
    s1 intersect s2 map { s =>
      (s.left.token, s.right.token)
    } should equal(expected)
}
