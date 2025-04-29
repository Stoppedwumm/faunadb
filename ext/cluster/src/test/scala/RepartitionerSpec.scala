package fauna.cluster.test

import fauna.atoms.{ HostID, Location }
import fauna.cluster.topology._

import scala.util.Random

class RepartitionerSpec extends Spec {
  "Repartitioner should" - {
    "add tokens for a new host into an empty topology" in {
      val id1 = HostID.randomID
      val proposal =
        Repartitioner.newTopologyProposal(Set(id1), Set.empty, ReplicaTopology.Empty)
      proposal.newPending.size should equal(Location.PerHostCount)
      allPresent(proposal, Seq(id1))
      proposal.newReusables.isEmpty should equal(true)
      proposal.oldPending.isEmpty should equal(true)
    }

    "remove tokens for a single host back to empty topology" in {
      val id1 = HostID.randomID
      val proposal =
        Repartitioner.newTopologyProposal(Set(id1), Set.empty, ReplicaTopology.Empty)
      val newTop = ReplicaTopology.Empty.copy(pending = proposal.newPending)
      val proposal2 = Repartitioner.newTopologyProposal(Set.empty, Set(id1), newTop)
      proposal2.newPending.isEmpty should equal(true)
      proposal2.newReusables.isEmpty should equal(true)
      proposal2.oldPending should equal(proposal.newPending)
    }

    "adds tokens for a new host into an existing topology" in {
      val id1 = HostID.randomID
      val proposal =
        Repartitioner.newTopologyProposal(Set(id1), Set.empty, ReplicaTopology.Empty)
      val id2 = HostID.randomID
      val newTop = ReplicaTopology.Empty.copy(pending = proposal.newPending)
      val proposal2 =
        Repartitioner.newTopologyProposal(Set(id1, id2), Set.empty, newTop)
      allPresent(proposal2, Seq(id1, id2))
      proposal2.newPending filter { _.host == id1 } should equal(proposal.newPending)
      proposal2.newReusables.isEmpty should equal(true)
      proposal2.oldPending should equal(proposal.newPending)
    }

    "adds tokens for two new hosts at once" in {
      val id1 = HostID.randomID
      val id2 = HostID.randomID
      val proposal = Repartitioner.newTopologyProposal(Set(id1, id2),
                                                       Set.empty,
                                                       ReplicaTopology.Empty)
      allPresent(proposal, Seq(id1, id2))
      proposal.newReusables.isEmpty should equal(true)
      proposal.oldPending.isEmpty should equal(true)
    }

    "remove tokens for two hosts from a four-host topology and add a new host in one operation" in {
      val (id1, id2, id3, id4) =
        (HostID.randomID, HostID.randomID, HostID.randomID, HostID.randomID)
      val proposal = Repartitioner.newTopologyProposal(Set(id1, id2, id3, id4),
                                                       Set.empty,
                                                       ReplicaTopology.Empty)
      allPresent(proposal, Seq(id1, id2, id3, id4))
      val newTop = ReplicaTopology.Empty.copy(pending = proposal.newPending)
      val id5 = HostID.randomID
      val proposal2 =
        Repartitioner.newTopologyProposal(Set(id1, id3, id5), Set(id2, id4), newTop)
      allPresent(proposal2, Seq(id1, id3, id5))
      nonePresent(proposal2, id2, id4)
    }

    "produce reusable tokens after nodes owning data are removed" in {
      val (id1, id2, id3, id4) =
        (HostID.randomID, HostID.randomID, HostID.randomID, HostID.randomID)

      val seed = Random.nextLong()
      val rnd = new Random(seed)
      withClue(s"Random seed is $seed") {
        val proposal = Repartitioner.newTopologyProposal(Set(id1, id2, id3, id4),
                                                         Set.empty,
                                                         ReplicaTopology.Empty,
                                                         rnd)

        val owned = SegmentOwnership.toOwnedSegments(proposal.newPending)
        val grouped = owned.groupBy { _.host }

        // Take half of id1 and id2 segments, as well as all of id3 and id4, and add them to current ring
        val id1Owned = takeOwnedRandom(grouped(id1), Location.PerHostCount / 2, rnd)
        val id2Owned = takeOwnedRandom(grouped(id2), Location.PerHostCount / 2, rnd)
        val top2 = List(id1Owned, id2Owned, grouped(id3), grouped(id4)).foldLeft(
          ReplicaTopology.Empty.copy(pending = proposal.newPending))(markOwned)

        // Now when we remove id1 and id2, the tokens for segments from owned
        // should show up in reusables.
        val proposal2 =
          Repartitioner.newTopologyProposal(Set(id3, id4), Set(id1, id2), top2, rnd)
        allPresent(proposal2, Seq(id3, id4))
        nonePresent(proposal2, id1, id2)
        proposal2.newReusables.sorted should equal((id1Owned ++ id2Owned) map {
          _.segment.left
        } sorted)
        proposal2.newReusables.size should equal(Location.PerHostCount)

        // If we add a new host now, it should completely take over those reusable segments
        val id5 = HostID.randomID
        val top3 =
          top2.copy(pending = proposal2.newPending,
                    reusable = proposal2.newReusables)
        val proposal3 =
          Repartitioner.newTopologyProposal(Set(id3, id4, id5), Set(id1, id2), top3)

        // All reusables were consumed
        proposal3.newReusables.isEmpty should equal(true)
        // They all went to id5
        proposal3.newPending filter { _.host == id5 } map { _.from } should equal(
          proposal2.newReusables.sorted)
      }
    }

    "reusable tokens are immediately assigned to currently incomplete nodes" in {
      val (id1, id2, id3, id4) =
        (HostID.randomID, HostID.randomID, HostID.randomID, HostID.randomID)

      val seed = Random.nextLong()
      val rnd = new Random(seed)
      withClue(s"Random seed is $seed") {
        val proposal = Repartitioner.newTopologyProposal(Set(id1, id2, id3, id4),
                                                         Set.empty,
                                                         ReplicaTopology.Empty,
                                                         rnd)

        val owned = SegmentOwnership.toOwnedSegments(proposal.newPending)
        val grouped = owned.groupBy { _.host }

        // Take half of id1 and id2 segments, as well as 3/4 of id3 and id4, and add them to current ring
        val id1Owned =
          takeOwnedRandom(grouped(id1), Location.PerHostCount * 2 / 4, rnd)
        val id2Owned =
          takeOwnedRandom(grouped(id2), Location.PerHostCount * 2 / 4, rnd)
        val id3Owned =
          takeOwnedRandom(grouped(id3), Location.PerHostCount * 3 / 4, rnd)
        val id4Owned =
          takeOwnedRandom(grouped(id4), Location.PerHostCount * 3 / 4, rnd)
        val top2 = List(id1Owned, id2Owned, id3Owned, id4Owned).foldLeft(
          ReplicaTopology.Empty.copy(pending = proposal.newPending))(markOwned)

        // Now when we remove id1 and id2, the tokens for segments from owned
        // should partially be assigned to id3 and id4 as well as show up in reusables
        val proposal2 =
          Repartitioner.newTopologyProposal(Set(id3, id4), Set(id1, id2), top2, rnd)
        allPresent(proposal2, Seq(id3, id4))
        nonePresent(proposal2, id1, id2)

        // As many reusables as could have been used
        ((proposal2.newPending map { _.from } toSet) intersect ((id1Owned ++ id2Owned) map {
          _.segment.left
        } toSet)).size should equal(Location.PerHostCount / 2)
        // Those that couldn't be used remain
        proposal2.newReusables.size should equal(Location.PerHostCount / 2)
        // The two don't overlap
        ((proposal2.newPending map { _.from } toSet) intersect proposal2.newReusables.toSet).isEmpty should equal(
          true)
      }
    }

    "non-reusable tokens are preferred to be discarded when location count decreases" in {
      val (id1, id2, id3, id4) =
        (HostID.randomID, HostID.randomID, HostID.randomID, HostID.randomID)

      val seed = Random.nextLong()
      val rnd = new Random(seed)
      withClue(s"Random seed is $seed") {
        val proposal = Repartitioner.newTopologyProposal(Set(id1, id2, id3, id4),
                                                         Set.empty,
                                                         ReplicaTopology.Empty,
                                                         rnd)

        val owned = SegmentOwnership.toOwnedSegments(proposal.newPending)
        val grouped = owned.groupBy { _.host }

        // Take 90/60/40/10% respectively of id1/id2/id3/id4 segments, and add them to current ring.
        val id1Owned =
          takeOwnedRandom(grouped(id1), Location.PerHostCount * 9 / 10, rnd)
        val id2Owned =
          takeOwnedRandom(grouped(id2), Location.PerHostCount * 6 / 10, rnd)
        val id3Owned =
          takeOwnedRandom(grouped(id3), Location.PerHostCount * 4 / 10, rnd)
        val id4Owned =
          takeOwnedRandom(grouped(id4), Location.PerHostCount * 1 / 10, rnd)
        val top2 = List(id1Owned, id2Owned, id3Owned, id4Owned).foldLeft(
          ReplicaTopology.Empty.copy(pending = proposal.newPending))(markOwned)

        // Now reduce the number of tokens by half
        val proposal2 = Repartitioner.newTopologyProposal(Set(id1, id2, id3, id4),
                                                          Set.empty,
                                                          top2,
                                                          rnd,
                                                          Location.PerHostCount / 2)

        allPresent(proposal2, Seq(id1, id2, id3, id4), Location.PerHostCount / 2)

        def assertQuantity(id: HostID, owned: Seq[OwnedSegment], count: Int) = {
          val before = owned map { _.segment.left } toSet
          val after = proposal2.newPending filter { _.host == id } map { _.from } toSet

          (before intersect after).size should equal(count)
        }

        // id1 had 90% reusable tokens, so all of the remaining 50% should come out of it
        assertQuantity(id1, id1Owned, Location.PerHostCount / 2)
        // id2 had 60% reusable tokens, so all of the remaining 50% should come out of it
        assertQuantity(id2, id2Owned, Location.PerHostCount / 2)
        // id3 had 40% reusable tokens, so of the remaining 50%, they should all be in it
        assertQuantity(id3, id3Owned, Location.PerHostCount * 4 / 10)
        // id4 had 10% reusable tokens, so of the remaining 50%, they should all be in it
        assertQuantity(id4, id4Owned, Location.PerHostCount * 1 / 10)
      }
    }

    "minimize variance in new proposals" in {
      implicit val doubleOrd = Ordering.Double.TotalOrdering
      (1 to 10) foreach { _ =>
        val live = ((1 to 3) map { _ => HostID.randomID }).toSet
        Repartitioner.newTopologyProposal(live, Set.empty, ReplicaTopology.Empty).variance should be < 1E-4
      }
    }
  }

  private def allPresent(
    proposal: RepartitioningProposal,
    ids: Seq[HostID],
    locations: Int = Location.PerHostCount) =
    ids foreach { id =>
      proposal.newPending count { _.host == id } should equal(locations)
    }

  private def nonePresent(proposal: RepartitioningProposal, ids: HostID*) =
    proposal.newPending exists { so =>
      ids.contains(so.host)
    } should equal(false)

  private def markOwned(
    top: ReplicaTopology,
    segs: Iterable[OwnedSegment]): ReplicaTopology =
    segs.foldLeft(top) { (t, s) =>
      t.updateCurrent(s.segment, s.host)
    }

  private def takeOwnedRandom(owned: Vector[OwnedSegment], count: Int, rnd: Random) =
    // Excluding min location from owned segments helps the testing by not having to deal with
    // the case of half-claiming the wraparound segment.
    rnd.shuffle(owned) filterNot { _.segment.left.isMin } take count
}
