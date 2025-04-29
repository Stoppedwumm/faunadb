package fauna.cluster.test

import fauna.atoms.HostID
import fauna.cluster.topology._
import fauna.tx.transaction.{ Epoch, SegmentID, SegmentInfo, Uninitialized }
import java.util.UUID

class LogTopologySpec extends Spec {
  val id = 0 to 19 map { i => HostID(new UUID(i, 0)) }

  def newProposal(
    topo: Seq[(SegmentID, Seq[HostID])],
    logReplicas: Seq[(String, Seq[HostID])]): Seq[(SegmentID, Vector[HostID])] =
    LogTopology.newProposal(
      topo map { case (seg, hs) =>
        (seg, SegmentInfo(hs.toVector, Uninitialized, SegmentInfo.FirstInitRound))
      },
      logReplicas,
      Set.empty,
      (topo map { _._1 }).foldLeft(SegmentID(-1)) { _ max _ },
      Epoch.MaxValue
    ) map { case (seg, SegmentInfo(hs, _, _)) => (seg, hs) }

  private def expectedSegments(count: Int) =
    (0 until count) map { i => SegmentID(i) -> Seq(id(i)) }

  "LogTopology should" - {

    "not create anything in an empty cluster" in {
      val p = newProposal(Seq.empty, Seq.empty)
      p should equal(expectedSegments(0))
    }

    "create a segment in a single-node cluster" in {
      val p = newProposal(Seq.empty, Seq("r1" -> Seq(id(0))))
      p should equal(expectedSegments(1))
    }

    "create a segment for every node in a single-replica cluster..." in {
      val p = newProposal(Seq.empty, Seq("r1" -> Seq(id(0), id(1), id(2))))
      p should equal(expectedSegments(3))
    }

    "... but no more than MaxSegments even in one replica" in {
      val p = newProposal(Seq.empty, Seq("r1" -> Seq(id(0), id(1), id(2), id(3))))
      p should equal(expectedSegments(LogTopology.MaxSegments))
    }

    "add segments when a single-node cluster grows..." in {
      val p =
        newProposal(Seq(SegmentID(0) -> Seq(id(0))), Seq("r1" -> Seq(id(0), id(1))))
      p should equal(expectedSegments(2))
    }

    "... but not ending with more than MaxSegments" in {
      val p = newProposal(
        Seq(SegmentID(0) -> Seq(id(0))),
        Seq("r1" -> Seq(id(0), id(1), id(2), id(3))))
      p should equal(expectedSegments(LogTopology.MaxSegments))
    }

    "remove segments from a single-replica cluster..." in {
      val p = newProposal(
        Seq(
          SegmentID(0) -> Seq(id(0)),
          SegmentID(1) -> Seq(id(1)),
          SegmentID(2) -> Seq(id(2))
        ),
        Seq("r1" -> Seq(id(0), id(2))))
      p should equal(
        Seq(
          SegmentID(0) -> Seq(id(0)),
          SegmentID(2) -> Seq(id(2))
        ))
    }

    "...including when there are more than MaxSegments segments" in {
      val p = newProposal(Seq.empty, Seq("r1" -> Seq(id(0), id(1), id(2), id(3))))
      p should equal(expectedSegments(LogTopology.MaxSegments))
    }

    "create multiple segments in two replicas" in {
      val p = newProposal(
        Seq.empty,
        Seq(
          "r1" -> Seq(id(0), id(1)),
          "r2" -> Seq(id(2), id(3))
        ))
      p should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(2)),
          SegmentID(1) -> Seq(id(1), id(3))
        ))
    }

    "create multiple segments in three replicas" in {
      val p = newProposal(
        Seq.empty,
        Seq(
          "r1" -> Seq(id(0), id(1)),
          "r2" -> Seq(id(2), id(3)),
          "r3" -> Seq(id(4), id(5))
        ))
      p should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(2), id(4)),
          SegmentID(1) -> Seq(id(1), id(3), id(5))
        ))
    }

    "create only as many segments as the smallest replica can hold..." in {
      val p = newProposal(
        Seq.empty,
        Seq(
          "r1" -> Seq(id(0), id(1), id(2)),
          "r2" -> Seq(id(3), id(4), id(5)),
          "r3" -> Seq(id(6), id(7))
        ))
      p should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3), id(6)),
          SegmentID(1) -> Seq(id(1), id(4), id(7))
        ))
    }

    "... but no more than MaxSegments if smaller" in {
      val p = newProposal(
        Seq.empty,
        Seq(
          "r1" -> Seq(id(0), id(1), id(2), id(3)),
          "r2" -> Seq(id(4), id(5), id(6), id(7), id(8)),
          "r3" -> Seq(id(9), id(10), id(11), id(12))
        ))
      p should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(4), id(9)),
          SegmentID(1) -> Seq(id(1), id(5), id(10)),
          SegmentID(2) -> Seq(id(2), id(6), id(11))
        ))
    }

    "drop segments when a smaller replica is added" in {
      val r1 = Seq(
        "r1" -> Seq(id(0), id(1), id(2)),
        "r2" -> Seq(id(3), id(4), id(5))
      )
      val p1 = newProposal(Seq.empty, r1)
      p1 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3)),
          SegmentID(1) -> Seq(id(1), id(4)),
          SegmentID(2) -> Seq(id(2), id(5))
        ))
      val p2 = newProposal(p1, r1 :+ "r3" -> Seq(id(6), id(7)))
      p2 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3), id(6)),
          SegmentID(1) -> Seq(id(1), id(4), id(7))
        ))
    }

    "expand segments when a right-size replica is added..." in {
      val r1 = Seq(
        "r1" -> Seq(id(0), id(1), id(2)),
        "r2" -> Seq(id(3), id(4), id(5))
      )
      val p1 = newProposal(Seq.empty, r1)
      p1 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3)),
          SegmentID(1) -> Seq(id(1), id(4)),
          SegmentID(2) -> Seq(id(2), id(5))
        ))
      val p2 = newProposal(p1, r1 :+ "r3" -> Seq(id(6), id(7), id(8)))
      p2 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3), id(6)),
          SegmentID(1) -> Seq(id(1), id(4), id(7)),
          SegmentID(2) -> Seq(id(2), id(5), id(8))
        ))
    }

    "... but not expanding to more than MaxSegments segments" in {
      val r1 = Seq(
        "r1" -> Seq(id(0), id(1), id(2), id(3)),
        "r2" -> Seq(id(4), id(5), id(6), id(7))
      )
      val p1 = newProposal(Seq.empty, r1)
      p1 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(4)),
          SegmentID(1) -> Seq(id(1), id(5)),
          SegmentID(2) -> Seq(id(2), id(6))
        ))
      val p2 = newProposal(p1, r1 :+ "r3" -> Seq(id(8), id(9), id(10), id(11)))
      p2 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(4), id(8)),
          SegmentID(1) -> Seq(id(1), id(5), id(9)),
          SegmentID(2) -> Seq(id(2), id(6), id(10))
        ))
    }

    // NOTE: this is slightly counterintuitive but it seems to be the correct
    // behavior.
    "create a new segment when a smaller replica is removed..." in {
      val r1 = Seq(
        "r1" -> Seq(id(0), id(1), id(2)),
        "r2" -> Seq(id(3), id(4), id(5)),
        "r3" -> Seq(id(6), id(7))
      )
      val p1 = newProposal(Seq.empty, r1)
      p1 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3), id(6)),
          SegmentID(1) -> Seq(id(1), id(4), id(7))
        ))
      val p2 = newProposal(p1, r1.dropRight(1)) // drop r3
      // The remaining 2 replicas can support 3 segments fully, so a new one gets
      // created.
      p2 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3)),
          SegmentID(1) -> Seq(id(1), id(4)),
          SegmentID(2) -> Seq(id(2), id(5))
        ))
    }

    "... but not adding a segment if there are MaxSegment segments" in {
      val r1 = Seq(
        "r1" -> Seq(id(0), id(1), id(2), id(3)),
        "r2" -> Seq(id(4), id(5), id(6), id(7)),
        "r3" -> Seq(id(8), id(9))
      )
      val p1 = newProposal(Seq.empty, r1)
      p1 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(4), id(8)),
          SegmentID(1) -> Seq(id(1), id(5), id(9))
        ))
      val p2 = newProposal(p1, r1.dropRight(1)) // drop r3
      // The remaining 2 replicas can support 4 segments, but the max is 3.
      p2 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(4)),
          SegmentID(1) -> Seq(id(1), id(5)),
          SegmentID(2) -> Seq(id(2), id(6))
        ))
    }

    "reuse spare nodes from closed segments to backfill existing segments" in {
      val p1 = newProposal(
        Seq.empty,
        Seq(
          "r1" -> Seq(id(0), id(1), id(2)),
          "r2" -> Seq(id(3), id(4), id(5)),
          "r3" -> Seq(id(6), id(7), id(8))
        ))
      p1 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3), id(6)),
          SegmentID(1) -> Seq(id(1), id(4), id(7)),
          SegmentID(2) -> Seq(id(2), id(5), id(8))
        ))
      // shrink a replica by dropping a node from the middle
      val p2 = newProposal(
        p1,
        Seq(
          "r1" -> Seq(id(0), id(1), id(2)),
          "r2" -> Seq(id(3), id(5)),
          "r3" -> Seq(id(6), id(7), id(8))
        ))
      p2 should equal(
        Seq(
          SegmentID(0) -> Seq(id(0), id(3), id(6)),
          SegmentID(2) -> Seq(id(2), id(5), id(8))
        ))
      // Drop node 0 and node 8
      val p3 = newProposal(
        p2,
        Seq(
          "r1" -> Seq(id(1), id(2)),
          "r2" -> Seq(id(3), id(5)),
          "r3" -> Seq(id(6), id(7))
        ))
      // node 1 and node 7 that used to belong to seg 1 will be recruited
      // into seg 0 and 2 to replace nodes 0 and 8, respectively.
      p3 should equal(
        Seq(
          SegmentID(0) -> Seq(id(1), id(3), id(6)),
          SegmentID(2) -> Seq(id(2), id(5), id(7))
        ))
    }
  }
}
