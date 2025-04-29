package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.codex.json._
import fauna.prop.api.CoreLauncher
import fauna.tx.transaction.TxnPipeline
import scala.concurrent.Await
import scala.concurrent.duration._

class LeadershipSpec extends Spec {
  val admin1 = CoreLauncher.adminClient(1, mcAPIVers)
  val admin2 = CoreLauncher.adminClient(2, mcAPIVers)
  val admin3 = CoreLauncher.adminClient(3, mcAPIVers)
  val admin4 = CoreLauncher.adminClient(4, mcAPIVers)
  val admin5 = CoreLauncher.adminClient(5, mcAPIVers)
  val admin6 = CoreLauncher.adminClient(6, mcAPIVers)

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)
  val api2 = CoreLauncher.apiClient(2, mcAPIVers)
  val api3 = CoreLauncher.apiClient(3, mcAPIVers)
  val api4 = CoreLauncher.apiClient(4, mcAPIVers)
  val api5 = CoreLauncher.apiClient(5, mcAPIVers)
  val api6 = CoreLauncher.apiClient(6, mcAPIVers)


  val admins = Seq(admin1, admin2, admin3, admin4, admin5, admin6)
  val dcs = Seq("dc1", "dc2", "dc3")

  before {
    CoreLauncher.launchMultiple(Seq(1, 2, 3, 4, 5, 6))

    init(admin1, "dc1")
    waitUntilLive(admin1, getIdentity(admin1))

    join(admin2, "dc1")
    waitUntilLive(admin2, getIdentity(admin2))

    join(admin3, "dc2")
    waitUntilLive(admin3, getIdentity(admin3))

    join(admin4, "dc2")
    waitUntilLive(admin4, getIdentity(admin4))

    join(admin5, "dc3")
    waitUntilLive(admin5, getIdentity(admin5))

    join(admin6, "dc3")
    waitUntilLive(admin6, getIdentity(admin6))

    val repl = dcs map {
      _ -> ReplicaTypeNames.Log
    }

    setReplication(admin1, repl: _*)

    eventually(timeout(30.seconds)) {
      val res = admin1.get("/admin/movement_status", rootKey)
      res should respond (OK)
      val status = (res.resource / "movement_status").as[String]
      status should not equal("No data movement is currently in progress.")
    }

    Await.result(noTopologyChangeInProgress(admin1), 60.seconds)
    waitForPing(Seq(api1, api2, api3, api4, api5, api6))
  }

  after {
    CoreLauncher.terminateAll()
  }

  "Leadership" - {
    "moves when the leader becomes compute-only" in {
      val seg = getSegmentStatus(admin1)

      // find a leader, any leader
      val (leader, _, _) = seg find { case (_, _, isLeader) => isLeader } head

      val status = getStatus(admin1, JSString(leader.toString))
      status.nonEmpty should be (true)

      val replica = (status.get / "replica").as[String]

      // switch the leader's replica to compute
      val repl = dcs map {
        case `replica` => replica -> ReplicaTypeNames.Compute
        case dc        => dc -> ReplicaTypeNames.Log
      }

      setReplication(admin1, repl: _*) should respond (NoContent)
      Thread.sleep((2 * TxnPipeline.LogSegmentsRevalidationPeriod).toMillis)

      val newSeg = getSegmentStatus(admin1)

      val topos = newSeg collect { case (host, _, true) => host }
      topos.size should be (2)

      // none of the new leaders are the old leader
      topos shouldNot contain (leader)
    }


    "moves when the leader becomes unavailable" in {
      val seg = getSegmentStatus(admin1)

      // find a leader, any leader
      val (leader, _, _) = seg find { case (_, _, isLeader) => isLeader } head

      val (client, i) = admins.zipWithIndex find {
        case (client, _) => getHostID(client) == leader
      } get

      val dead = getHostID(client)

      // terminate a leader
      CoreLauncher.terminate(i + 1) // NB. zipWithIndex is 0-based, CoreLauncher is 1-based...

      val coord = admins find { _ != client } get

      waitUntilDown(coord, JSString(dead.toString))

      eventually(timeout(5.seconds)) {
        val newerSeg = getSegmentStatus(coord)

        val topos = newerSeg collect { case (host, _, true) => host }
        topos.size should be (2)

        // none of the new leaders are the dead leader
        topos shouldNot contain (dead)
      }

    }

    "moves when the leader is removed" in {
      pending

      val seg = getSegmentStatus(admin1)

      // find a leader, any leader
      val (leader, _, _) = seg find { case (_, _, isLeader) => isLeader } head

      val client = admins find { client => getHostID(client) == leader } get

      val dead = getHostID(client)

      val coord = admins find { _ != client } get

      // remove a leader
      removeNode(coord, JSString(dead.toString))

      eventually(timeout(5.seconds)) {
        val newerSeg = getSegmentStatus(coord)

        val topos = newerSeg collect { case (host, _, true) => host }
        topos.size should be (2)

        // none of the new leaders are the removed leader
        topos shouldNot contain (dead)
      }
    }
  }
}
