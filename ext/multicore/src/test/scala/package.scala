package fauna.multicore

import fauna.api.test.APISpecMatchers
import fauna.atoms.HostID
import fauna.cluster.{ HostStatus, NodeInfo }
import fauna.cluster.workerid.{
  AssignedWorkerID,
  UnassignedWorkerID,
  UnavailableWorkerID
}
import fauna.codex.json._
import fauna.exec.Timer
import fauna.lang.{ Timestamp, Timing }
import fauna.net.http.{ HttpClient, HttpResponse }
import fauna.prop.api._
import fauna.tx.transaction.SegmentID
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

package test {

  trait Spec extends AnyFreeSpec
      with BeforeAndAfter
      with BeforeAndAfterAll
      with Matchers
      with APISpecMatchers
      with APIResponseHelpers
      with Eventually {

    val mcAPIVers = "2.0"

    val rootKey = FaunaDB.rootKey

    def getIdentity(api: HttpClient): JSValue = {
      val identityRes = api.get("/admin/identity", rootKey)
      identityRes should respond (OK)
      identityRes.json / "identity"
    }

    def getIdentityAtAddress(api: HttpClient, hostName: String) =
      withClue(s"asking ${api.host} to identify $hostName") {
        val identityRes =
          api.post("/admin/host-id", JSObject("node" -> hostName), rootKey)
        identityRes should respond(OK)
        identityRes.json / "identity"
      }

    /**
      * Returns the HostID for the node.
      */
    def getHostID(api: HttpClient): HostID =
      HostID(getIdentity(api).as[String])

    def init(api: HttpClient, replicaName: String) = {
      shouldNotBeMember(api)
      changed(api.post("/admin/init", JSObject("replica_name" -> replicaName), rootKey))
      shouldBeMember(api)
    }

    def join(api: HttpClient, replicaName: String) = {
      shouldNotBeMember(api)
      changed(api.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> replicaName), rootKey))
      shouldBeMember(api)
    }

    def removeNode(api: HttpClient, id: JSValue, force: Boolean = false) = {
      eventually(timeout(20 seconds), interval(1 seconds)) {
        api.post(
          "/admin/remove_node",
          JSObject(
            "nodeID" -> id,
            "force" -> force,
            // Storage cleanliness is not relevant in these tests.
            "allow_unclean" -> true),
          rootKey) should respond(NoContent)
      }
      Await.result(removalDone(api, id), 40.seconds)
    }

    def removalDone(api: HttpClient, hostID: JSValue): Future[Unit] =
      getStatus(api, hostID) match {
        case None => noTopologyChangeInProgress(api)
        case Some(status) if (status / "state").as[String] == "removed" =>
          // Wait a beat for the reconciler to start working.
          Timer.Global.delay(1.second) { noTopologyChangeInProgress(api) }
        case _ => Timer.Global.delay(1.seconds) { removalDone(api, hostID) }
      }

    def waitUntilLive(api: HttpClient, hostID: JSValue): Future[Unit] = {
      val nodeInfo = getStatus(api, hostID)
      if (nodeInfo exists { n =>
        val wid = (n / "worker_id").as[String]
        wid != "pending" && wid != "n/a"
      }) {
        Future.unit
      } else {
        Timer.Global.delay(1.seconds) { waitUntilLive(api, hostID) }
      }
    }

    def getStatus(api: HttpClient, hostID: JSValue): Option[JSValue] =
      getStatuses(api) find { n =>
        (n / "host_id") == hostID
      }

    def getStatuses(api: HttpClient): Seq[JSValue] = {
      eventually(timeout(5 seconds), interval(1 second)) {
        val res1 = api.get("/admin/status", rootKey)
        res1 should respond(OK)
        (res1.json / "nodes").as[JSArray].value
      }
    }

    /**
      * Parses status response to type-object.
      */
    def parseNodeInfo(statuses: Seq[JSValue]): Seq[NodeInfo] =
      statuses map { parseNodeInfo(_) }

    /**
      * Parses status JSON response to [[NodeInfo]].
      */
    def parseNodeInfo(jsValue: JSValue): NodeInfo = {
      val status =
        if ((jsValue / "status").as[String] == "up") {
          HostStatus.Up
        } else {
          HostStatus.Down
        }

      val workerIDString = (jsValue / "worker_id").as[String]
      val workerID = workerIDString match {
        case "n/a"   => UnassignedWorkerID
        case "error" => UnavailableWorkerID
        case id      => AssignedWorkerID(id.toInt)
      }

      val hostAddressString = (jsValue / "address").as[String]
      val hostAddress =
        if (hostAddressString == "n/a") {
          None
        } else {
          Some(hostAddressString)
        }

      val persistedTimestamp = {
        (jsValue / "persisted_timestamp").as[String] match {
           case "not reported"          => None
           case "none"                  => Some(Timestamp.MaxMicros)
           case ts                      => Some(Timestamp.parse(ts))
        }
      }

      NodeInfo(
        status = status,
        state = (jsValue / "state").as[String],
        ownership = (jsValue / "ownership").as[Float],
        ownershipGoal = (jsValue / "ownership_goal").as[Float],
        hostID = HostID((jsValue / "host_id").as[String]),
        hostName = hostAddress,
        replica = (jsValue / "replica").as[String],
        workerID = workerID,
        persistedTimestamp = persistedTimestamp
      )
    }

    /**
      * Reads the [[SegmentID]] and the leader boolean flag from the JSON response.
      */
    def parseLogSegmentInfo(info: JSValue): Option[(SegmentID, Boolean)] = {
      val logSegment = (info / "log_segment").as[String]
      val logSegmentLeader = (info / "log_segment_leader").as[Boolean]
      if (logSegment == "none") {
        None
      } else {
        Some((SegmentID(logSegment.toInt), logSegmentLeader))
      }
    }

    /**
      * Fetches the cluster status and parses status JSON response to [[NodeInfo]].
      *
      * [[NodeInfo]] does not contain Segment related data but the response does so
      * the Segment related response is returned in an optional tuple (SegmentID, Boolean).
      */
    def getClusterStatuses(api: HttpClient): Seq[(NodeInfo, Option[(SegmentID, Boolean)])] = {
      getStatuses(api) map { json =>
        val nodeInfo = parseNodeInfo(json)
        val segmentInfo = parseLogSegmentInfo(json)
        (nodeInfo, segmentInfo)
      }
    }

    /**
      * Convenience function for fetching Segment related data for test-cases.
      */
    def getSegmentStatus(api: HttpClient): Seq[(HostID, SegmentID, Boolean)] = {
      getClusterStatuses(api) flatMap {
        case (nodeInfo, Some((segmentID, leader))) =>
          Seq((nodeInfo.hostID, segmentID, leader))

        case (_, None) =>
          Seq.empty
      }
    }

    def waitUntilDown(api: HttpClient, hostID: JSValue): Unit =
      eventually(timeout(30.seconds)) {
        val status = getStatus(api, hostID)
        status.nonEmpty should be(true)
        (status.get / "status").as[String] should equal ("down")
      }

    def noTopologyChangeInProgress(api: HttpClient): Future[Unit] = {
      val res1 = api.get("/admin/movement_status", rootKey)
      res1 should respond (OK)
      val status = (res1.resource / "movement_status").as[String]
      if ("No data movement is currently in progress." == status) {
        Future.unit
      } else {
        Timer.Global.delay(1.seconds) { noTopologyChangeInProgress(api) }
      }
    }

    def ping(api: HttpClient, scope: String) = {
      val res = api.get(s"/ping?scope=$scope", rootKey)
      res should respond (OK)
    }

    def waitForPing(apis: Seq[HttpClient], scope: String = "write") = {

      val timing = Timing.start
      eventually(timeout(scaled(1.minute)), interval(1.seconds)) {
        apis foreach {
          ping(_, scope)
        }
      }
      val elapsed = timing.elapsedMillis
      println(s"Time to ping $elapsed ms")
    }

    def shouldBeMember(api: HttpClient) = checkMembership(api, OK)

    def shouldNotBeMember(api: HttpClient) = checkMembership(api, InternalServerError)

    def checkMembership(api: HttpClient, responseCode: Int) =
      api.get("/admin/identity", rootKey) should respond (responseCode)

    def changedOrNot(res: Future[HttpResponse], expected: JSBoolean) = {
      res should respond (OK)
      (res.json / "changed") should equal (expected)
    }

    def changed(res: Future[HttpResponse]) = changedOrNot(res, JSTrue)

    def notChanged(res: Future[HttpResponse]) = changedOrNot(res, JSFalse)

    def setReplication(api: HttpClient, replicas: (String, String)*) =
      api.put("/admin/replication", JSObject("replicas" -> new JSArray(replicas map { r => JSObject("name" -> r._1, "type" -> r._2) })), rootKey)

    def assertBadRequest(res: Future[HttpResponse], expectedErrorMessage: String) = {
      res should respond(BadRequest)
      (res.json / "errors" / 0 / "description").as[String] should equal(expectedErrorMessage)
    }

    def assertErrorResponse(res: Future[HttpResponse], expectedErrorMessage: String) = {
      res should respond(InternalServerError)
      (res.json / "errors" / 0 / "description").as[String] should equal(expectedErrorMessage)
    }
  }
}
