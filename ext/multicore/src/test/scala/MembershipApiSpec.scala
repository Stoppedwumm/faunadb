package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.codex.json._
import fauna.prop.api.CoreLauncher
import fauna.tx.transaction.SegmentID
import scala.concurrent.duration._

class MembershipApiSpec extends Spec {
  val admin1 = CoreLauncher.adminClient(1, mcAPIVers)
  val admin2 = CoreLauncher.adminClient(2, mcAPIVers)
  val admin3 = CoreLauncher.adminClient(3, mcAPIVers)

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)
  val api2 = CoreLauncher.apiClient(2, mcAPIVers)
  val api3 = CoreLauncher.apiClient(3, mcAPIVers)

  var node3id: JSValue = _

  override protected def afterAll() =
    CoreLauncher.terminateAll()

  "Cluster should be able to" - {

    "be initialized by a seed node" in {
      CoreLauncher.launchMultiple(Seq(1, 2, 3), 2)

      init(admin1, "dc1")
      waitUntilLive(admin1, getIdentity(admin1))
    }

    "but initialized only once" in {
      notChanged(admin1.post("/admin/init", JSObject("replica_name" -> "dc1"), rootKey))
    }

    "reject updating a replica type with no hosts in it" in {
      // Note: node in dc2 has not joined yet, so dc2 is empty
      setReplication(admin1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log) should respond (BadRequest)
    }

    "have a 2nd node" - {

      "try to join through a non-member and fail" in {
        admin2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(3))), rootKey) should respond (BadRequest)
      }

      "join the cluster" in {
        join(admin2, "dc1")
      }

      "fail at attempting to join the same cluster twice" in {
        notChanged(admin2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc1"), rootKey))
      }

      "re-establish consensus when restarted" in {
        CoreLauncher.terminate(2)
        CoreLauncher.relaunch(2, mcAPIVers)
        // No join needed here
        shouldBeMember(admin2)
        waitForPing(Seq(api2))
      }
    }

    "update a replica type after there's a node in it" in {
      join(admin3, "dc2")
      node3id = getIdentity(admin3)
      setReplication(admin1, "dc1" -> ReplicaTypeNames.Log, "dc2" -> ReplicaTypeNames.Log) should respond (NoContent)
    }

    "shut down the whole cluster and restart it one node at a time" in {
      CoreLauncher.terminateAll()
      List(admin1, admin2, admin3).zipWithIndex foreach {
        case (api, i) =>
          CoreLauncher.relaunch(i + 1, mcAPIVers)
          shouldBeMember(api)
      }
      waitForPing(Seq(api1, api2, api3))
    }

    def markReplica2As(rtype: String) = {
      val repSpec = JSObject("replicas" -> JSArray(
        JSObject("name" -> "dc1", "type" -> ReplicaTypeNames.Log),
        JSObject("name" -> "dc2", "type" -> rtype)
      ))

      admin1.put("/admin/replication", repSpec, rootKey) should respond(NoContent)
      val res1 = admin1.get("/admin/replication", rootKey)
      res1.resource should equal(repSpec)
      waitForPing(Seq(api1, api2, api3))
    }

    "allow marking a replica as compute-only" in {
      markReplica2As(ReplicaTypeNames.Compute)
      eventually(timeout(30.seconds), interval(1.second)) {
        val status = getSegmentStatus(admin1)
        val topos = status collect { case (host, SegmentID(0), _) => host }
        topos.size should be(1)
      }
    }

    "allow marking a replica as data-only" in {
      markReplica2As(ReplicaTypeNames.Data)
    }

    "allow marking a replica as data+log" in {
      markReplica2As(ReplicaTypeNames.Log)

      eventually(timeout(30.seconds), interval(1.second)) {
        val status = getSegmentStatus(admin1)
        val topos = status collect { case (host, SegmentID(0), _) => host }
        topos.size should be(2)
      }
    }

    "refuse to remove the last node from a data replica" in {
      // Should refuse to remove node 3, because it needs to
      // protect against empty data replicas.
      admin2.post(
        "/admin/remove_node",
        JSObject(
          "nodeID" -> node3id,
          // Storage cleanliness is not relevant here.
          "allow_unclean" -> true),
        rootKey) should respond(BadRequest)
    }

    "remove the last node from a compute-only replica" in {
      // Update the replica to be compute-only
      markReplica2As(ReplicaTypeNames.Compute)

      // Now it should succeed.
      removeNode(admin1, node3id)
    }

    "allow to remove an already removed node" in {
      removeNode(admin1, node3id)
    }
  }
}
