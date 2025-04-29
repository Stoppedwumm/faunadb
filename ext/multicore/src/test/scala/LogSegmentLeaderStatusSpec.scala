package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.atoms.HostID
import fauna.prop.api.CoreLauncher
import fauna.tx.transaction.SegmentID
import org.scalatest.concurrent.Eventually
import org.scalatest.{ BeforeAndAfterEach, OptionValues }
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

/**
  * Tests cases to assert the result of leaders returned from the admin status command.
  */
class LogSegmentLeaderStatusSpec extends Spec with Eventually with OptionValues with BeforeAndAfterEach {

  override protected def afterEach() =
    CoreLauncher.terminateAll()

  "getLeader" - {

    "on a single node cluster should return itself as a leader" in {
      val api = CoreLauncher.adminClient(1, mcAPIVers)
      CoreLauncher.launch(1, mcAPIVers)

      init(api, "dc1")

      val id = getHostID(api)

      eventually(timeout(30.seconds), interval(1.second)) {
        getSegmentStatus(api) should contain only ((id, SegmentID(0), true))
      }
    }

    "should return one of the two replica nodes as a leader if they are in different replicas" in {
      val api1 = CoreLauncher.adminClient(1, mcAPIVers)
      val api2 = CoreLauncher.adminClient(2, mcAPIVers)

      CoreLauncher.launchMultiple(Seq(1, 2))

      init(api1, "dc1")
      join(api2, "dc2")

      // Sleep for a painfully long time. This avoids a race between
      // initialization and the replication state change. The characteristic of
      // this race is that the first node will abdicate to the second and kick
      // itself out of the segment Raft group, instead of joining the second node
      // into the Raft group.
      // Note that overall we save time with the sleep, because a test failure takes
      // one minute :).
      Thread.sleep(30000)

      setReplication(api1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log)

      val id1 = getHostID(api1)
      val id2 = getHostID(api2)

      val expectedResponse1 =
        Seq(
          (id1, SegmentID(0), true),
          (id2, SegmentID(0), false)
        )

      val expectedResponse2 =
        Seq(
          (id1, SegmentID(0), false),
          (id2, SegmentID(0), true)
        )

      //either one of node1 or node2 can be the leader. Test succeeds if any one of these conditions is true.
      def assert(result: Seq[(HostID, SegmentID, Boolean)]) =
        Try(result should contain theSameElementsAs expectedResponse1) match {
          case Success(_) =>
            expectedResponse1

          case Failure(_) => //if the first assert failed try the second.
            result should contain theSameElementsAs expectedResponse2
            expectedResponse2
        }

      eventually(timeout(30.seconds), interval(1.second)) {
        //request to both the nodes should result in the same output.
        val api1Status = assert(getSegmentStatus(api1))
        getSegmentStatus(api2) should contain theSameElementsAs api1Status
      }
    }

    "should return both replica nodes as a leader of different Segments if they are in the same DC" in {
      val api1 = CoreLauncher.adminClient(1, mcAPIVers)
      val api2 = CoreLauncher.adminClient(2, mcAPIVers)

      CoreLauncher.launchMultiple(Seq(1, 2))

      init(api1, "SameDC")
      join(api2, "SameDC")

      // See the comment in the first multiple-node test.
      Thread.sleep(30000)

      setReplication(api1, "SameDC" -> ReplicaTypeNames.Log, "SameDC" -> ReplicaTypeNames.Log)

      val id1 = getHostID(api1)
      val id2 = getHostID(api2)

      val expectedResponse1 =
        Seq(
          (id1, SegmentID(0), true),
          (id2, SegmentID(1), true)
        )

      val expectedResponse2 =
        Seq(
          (id2, SegmentID(0), true),
          (id1, SegmentID(1), true)
        )

      //either one of node1 or node2 can be the leader. Test succeeds if any one of these conditions is true.
      def assert(result: Seq[(HostID, SegmentID, Boolean)]) =
        Try(result should contain theSameElementsAs expectedResponse1) match {
          case Success(_) =>
            expectedResponse1

          case Failure(_) => //if the first assert failed try the second.
            result should contain theSameElementsAs expectedResponse2
            expectedResponse2
        }

      eventually(timeout(30.seconds), interval(5.second)) {
        //request to both the nodes should result in the same output.
        val api1Status = assert(getSegmentStatus(api1))
        getSegmentStatus(api2) should contain theSameElementsAs api1Status
      }
    }

    "should return a single leader for a three node cluster in three replicas" in {
      val api1 = CoreLauncher.adminClient(1, mcAPIVers)
      val api2 = CoreLauncher.adminClient(2, mcAPIVers)
      val api3 = CoreLauncher.adminClient(3, mcAPIVers)

      CoreLauncher.launchMultiple(Seq(1, 2, 3), 3)

      init(api1, "dc1")
      join(api2, "dc2")
      join(api3, "dc3")

      // See the comment in the first multiple-node test.
      Thread.sleep(30000)

      setReplication(api1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log, "dc3" -> ReplicaTypeNames.Log) should respond(NoContent)

      val id1 = getHostID(api1)
      val id2 = getHostID(api2)
      val id3 = getHostID(api3)

      val apis = Seq(api1, api2, api3)

      eventually(timeout(30.seconds), interval(1.second)) {
        //request to all the nodes should result in a leader being set.
        apis foreach { api =>
          val segmentStatuses = getSegmentStatus(api)
          //anyone can be a leader.
          segmentStatuses should contain atMostOneElementOf
            Seq(
              (id1, SegmentID(0), true),
              (id2, SegmentID(0), true),
              (id3, SegmentID(0), true)
            )
        }
      }
    }

    "should return leaders for a 4 node cluster with 2 nodes in each DC1 and DC2" in {
      val api1 = CoreLauncher.adminClient(1, mcAPIVers)
      val api2 = CoreLauncher.adminClient(2, mcAPIVers)
      val api3 = CoreLauncher.adminClient(3, mcAPIVers)
      val api4 = CoreLauncher.adminClient(4, mcAPIVers)

      CoreLauncher.launchMultiple(Seq(1, 2, 3, 4), 3)

      init(api1, "dc1")
      join(api2, "dc1")
      join(api3, "dc2")
      join(api4, "dc2")

      // See the comment in the first multiple-node test.
      Thread.sleep(30000)

      setReplication(api1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log) should respond(NoContent)

      //both the Segments should have a leaders.
      def assert(result: Seq[(HostID, SegmentID, Boolean)]) = {
        val leaders = result filter {
          case (_, _, isLeader) =>
            isLeader
        }
        val leaderSegmentIDs = leaders map {
          case (_, segmentID, _) =>
            segmentID
        }

        leaderSegmentIDs.sorted should equal(List(SegmentID(0), SegmentID(1)))
      }

      val apis = Seq(api1, api2, api3, api4)

      eventually(timeout(30.seconds), interval(1.second)) {
        //request to all the nodes should result in a leader being set.
        apis foreach { api =>
          assert(getSegmentStatus(api))
        }
      }
    }
  }
}
