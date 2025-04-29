package fauna.cluster.test

import fauna.atoms.{ HostID, Location, Segment }
import fauna.cluster.topology.{ ReplicaTopology, SegmentOwnership }
import fauna.cluster.DataNodeArbitratorInternal
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks._
import fauna.tx.transaction.TransactionStorage
import scala.concurrent.duration._
import scala.concurrent.Await

class DataNodeArbitratorSpec extends Spec {
  def pendingOnlyTopo(host: HostID) =
    ReplicaTopology(ReplicaTopology.NullOwnership, Vector(SegmentOwnership(Location.MinValue, host)), Vector.empty)

  def ownsEverythingTopo(host: HostID) =
    ReplicaTopology.create(Vector(SegmentOwnership(Location.MinValue, host)))

  "DataNodeArbitrator should" - {
    "prevent data node from starting when " - {
      "there's no worker ID" in {
        val dna = new DataNodeArbitratorInternal(HostID.NullID, null, null, false, null, null, null)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(false)
      }

      "the node is not in a data replica" in {
        val dna = new DataNodeArbitratorInternal(HostID.randomID, "r", null, true, Seq.empty, null, null)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(false)
      }
   }

    "prevent fast-forwarding LAT when" - {
      "storage already has persisted data" in {
        // Storage already has persisted data
        val st = new MockTransactionStorage(Timestamp.Epoch + 1.hour)

        val id1 = HostID.randomID
        val id2 = HostID.randomID
        id1 should not equal (id2)
        // id1 owns nothing yet in r1
        val r1 = pendingOnlyTopo(id1)

        val dna = new DataNodeArbitratorInternal(id1, "r1", Set(id1, id2), true,
          Seq("r1" -> r1), st, null)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // Not FF'd
        st.appliedTimestamp should equal (st.persisted)
      }

      "node is the first one in the cluster" in {
        // Storage already has persisted data
        val st = new MockTransactionStorage(Timestamp.Epoch)

        val id1 = HostID.randomID
        // id1 owns nothing yet in r1
        val r1 = pendingOnlyTopo(id1)

        val dna = new DataNodeArbitratorInternal(id1, "r1", Set(id1), true,
          Seq("r1" -> r1), st, null)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // Not FF'd
        st.appliedTimestamp should equal (st.persisted)
      }

      "node already owns some segments" in {
        // Storage has no persisted data yet
        val st = new MockTransactionStorage(Timestamp.Epoch)
        val id1 = HostID.randomID
        // id1 owns something in r1
        val r1 = pendingOnlyTopo(id1).updateCurrent(Segment(Location(1000), Location(2000)), id1)

        val dna = new DataNodeArbitratorInternal(id1, "r1", Set(id1, HostID.randomID), true,
          Seq("r1" -> r1), st, null)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // Not FF'd
        st.appliedTimestamp should equal (st.persisted)
      }

      "it already declared the node to be a data node" in {
        val st = new MockTransactionStorage(Timestamp.Epoch)

        val id1 = HostID.randomID
        // id1 owns everything in r1
        var r1 = ownsEverythingTopo(id1)

        def replicas = Seq("r1" -> r1)

        val dna = new DataNodeArbitratorInternal(id1, "r1", Set(id1, HostID.randomID), true,
          replicas, st, null)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // Not FF'd
        st.appliedTimestamp should equal (st.persisted)

        // Now create the conditions for FF by taking away ownership in r1
        r1 = pendingOnlyTopo(id1)
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // Still not FF'd because it previously returned true from tick
        st.appliedTimestamp should equal (st.persisted)

        // Verify that a different arbitrator started from scratch would in fact FF under same conditions
        val st2 = new MockTransactionStorage(Timestamp.Epoch)
        val dna2 = new DataNodeArbitratorInternal(id1, "r1", Set(id1, HostID.randomID), true,
          replicas, st2, new TestClock(Timestamp.Epoch + 1.hour))
        Await.result(dna2.tick(), 10.seconds)
        dna2.isDataNode should equal(true)
        // This one FF'd
        st2.appliedTimestamp should equal (Timestamp.Epoch + 1.hour)
      }
    }

    "allow fast-forwarding when" - {
      "the stars all align" in {
        // Storage has no persisted data yet
        val st = new MockTransactionStorage(Timestamp.Epoch)
        val id1 = HostID.randomID
        // id1 owns nothing yet in r1
        val r1 = pendingOnlyTopo(id1)

        val dna = new DataNodeArbitratorInternal(id1, "r1", Set(id1, HostID.randomID), true,
          Seq("r1" -> r1), st, new TestClock(Timestamp.Epoch + 1.day))
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // FF'd
        st.appliedTimestamp should equal (Timestamp.Epoch + 1.day)
      }

      "the node reverted to non-data node and back" in {
        val st = new MockTransactionStorage(Timestamp.Epoch)

        val id1 = HostID.randomID
        // id1 owns everything in r1
        var replicas = Seq("r1" -> ownsEverythingTopo(id1))

        val dna = new DataNodeArbitratorInternal(id1, "r1", Set(id1, HostID.randomID), true,
          replicas, st, new TestClock(Timestamp.Epoch + 1.day))
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // Not FF'd
        st.appliedTimestamp should equal (st.persisted)

        // Simulate this node becoming a compute node
        replicas = Seq.empty
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(false)
        st.updateMaxAppliedTimestamp(Timestamp.Epoch)

        // Now it becomes a data node again
        replicas = Seq("r1" -> pendingOnlyTopo(id1))
        Await.result(dna.tick(), 10.seconds)
        dna.isDataNode should equal(true)
        // FF'd
        st.appliedTimestamp should equal (Timestamp.Epoch + 1.day)
      }
    }
  }

  class MockTransactionStorage(var persisted: Timestamp)
      extends TransactionStorage[Unit, Unit, Unit, Unit, Unit] {
    
    updateMaxAppliedTimestamp(persisted)

    override def persistedTimestamp =
      persisted

    override def readsForTxn(expr: Unit) = ???
    override def evalTxnRead(ts: Timestamp, read: Unit, deadline: TimeBound) = ???
    override def evalTxnApply(ts: Timestamp, reads: Map[Unit, Unit], expr: Unit) = ???
    override def sync(syncMode: TransactionStorage.SyncMode): Unit = ???
    override def lock(): Unit = ???
    override def unlock(): Unit = ???
    override def isUnlocked = ???
  }
}
