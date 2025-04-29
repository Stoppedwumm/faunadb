package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.codex.json.JSValue
import fauna.prop.api.{ CoreLauncher, DefaultQueryHelpers }
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class LoadSnapshotSpec extends Spec with DefaultQueryHelpers {
  val admin1 = CoreLauncher.adminClient(1, mcAPIVers)
  val admin2 = CoreLauncher.adminClient(2, mcAPIVers)

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)
  val api2 = CoreLauncher.apiClient(2, mcAPIVers)

  before {
    CoreLauncher.launchMultiple(Seq(1, 2))

    init(admin1, "rep1")
    waitUntilLive(admin1, getIdentity(admin1))
    join(admin2, "rep2")
    waitUntilLive(admin2, getIdentity(admin2))
    setReplication(
      admin1,
      "rep1" -> ReplicaTypeNames.Log,
      "rep2" -> ReplicaTypeNames.Log
    ) should respond(NoContent)

    waitForPing(Seq(api1, api2))
  }

  after {
    CoreLauncher.terminateAll()
  }

  "Load Snapshot" - {
    "loading a snapshot succeeds if a receiving node restarts during the load" in {
      val snapshotPath = getClass.getClassLoader.getResource("test-snapshot").getPath
      val loadSnapshotCmd =
        Seq("load-snapshot", snapshotPath)

      val hostID = getHostID(admin2).toString
      val loadProcess = Future {
        CoreLauncher.adminCLI(1, loadSnapshotCmd)
      }

      /** Wait for restore to get to the point where it is loading versions
        * before triggering the restart
        */
      eventually(timeout(scaled(15.seconds)), interval(1.seconds)) {
        val res = admin1.get("/admin/restore/status", rootKey)
        val json = res.json
        (json / "restores" / 0 / "host_status" / hostID / "Versions" / "status")
          .as[String] shouldBe "InProgress"
      }
      CoreLauncher.terminate(2)
      assert(
        !loadProcess.isCompleted,
        "the load snapshot process should still in progress prior to node being restarted")
      CoreLauncher.relaunch(2, mcAPIVers)
      waitForPing(Seq(api2))
      val loadProcessResult = awaitLoadComplete(loadProcess)
      loadProcessResult shouldEqual 0
      val res = admin1.get("/admin/restore/status", rootKey).json
      (res / "restores" / 0 / "host_status" / hostID / "Versions" / "retries")
        .as[Int] shouldBe 1
      (res / "restores" / 0 / "status").as[String] shouldBe "Complete"
    }
    "loading snapshot when node is down at start but then comes up should succeed" in {
      val snapshotPath = getClass.getClassLoader.getResource("test-snapshot").getPath
      val loadSnapshotCmd =
        Seq("load-snapshot", snapshotPath)

      val senderID = getHostID(admin1).toString
      val receiverID = getHostID(admin2).toString
      CoreLauncher.terminate(2)
      val loadProcess = Future {
        CoreLauncher.adminCLI(1, loadSnapshotCmd)
      }

      CoreLauncher.relaunch(2, mcAPIVers)
      assert(
        !loadProcess.isCompleted,
        "load snapshot process should still be in progress after node start")
      waitForPing(Seq(api2))
      val loadProcessResult = awaitLoadComplete(loadProcess)
      loadProcessResult shouldEqual 0
      val res = admin1.get("/admin/restore/status", rootKey).json
      (res / "restores" / 0 / "status").as[String] shouldBe "Complete"
      validateHostCompleteStatus(senderID, (res / "restores" / 0))
      validateHostCompleteStatus(receiverID, (res / "restores" / 0))
    }
    "querying status for a loading snapshot correctly shows InProgress and Complete statuses" in {
      val snapshotPath = getClass.getClassLoader.getResource("test-snapshot").getPath
      val loadSnapshotCmd =
        Seq("load-snapshot", snapshotPath)

      val senderID = getHostID(admin1).toString
      val receiverID = getHostID(admin2).toString
      val loadProcess = Future {
        CoreLauncher.adminCLI(1, loadSnapshotCmd)
      }

      /** Validate in progress status
        */
      eventually(timeout(scaled(15.seconds)), interval(1.seconds)) {
        val res = admin1.get("/admin/restore/status", rootKey)
        val json = res.json
        (json / "restores" / 0 / "status").as[String] shouldBe "InProgress"
        (json / "restores" / 0 / "host_status" / receiverID / "Versions" / "status")
          .as[String] shouldBe "InProgress"
      }

      val loadProcessResult = awaitLoadComplete(loadProcess)
      loadProcessResult shouldEqual 0

      /** Validate complete status
        */
      val res = admin1.get("/admin/restore/status", rootKey)
      val json = res.json
      (json / "restores" / 0 / "status").as[String] shouldBe "Complete"
      validateHostCompleteStatus(senderID, (json / "restores" / 0))
      validateHostCompleteStatus(receiverID, (json / "restores" / 0))
    }
    "querying status for a loading snapshot correctly shows Failed status" in {
      val snapshotPath = getClass.getClassLoader.getResource("test-snapshot").getPath
      val loadSnapshotCmd =
        Seq("load-snapshot", snapshotPath)

      val senderID = getHostID(admin1).toString
      val receiverID = getHostID(admin2).toString
      CoreLauncher.terminate(2)
      val loadProcess = Future {
        CoreLauncher.adminCLI(1, loadSnapshotCmd)
      }

      /** Validate in progress status
        */
      eventually(timeout(scaled(15.seconds)), interval(1.seconds)) {
        val res = admin1.get("/admin/restore/status", rootKey)
        val json = res.json
        (json / "restores" / 0 / "status").as[String] shouldBe "InProgress"
      }

      val loadProcessResult = awaitLoadComplete(loadProcess)
      loadProcessResult shouldEqual 1

      /** Validate complete status
        */
      val res = admin1.get("/admin/restore/status", rootKey)
      val json = res.json
      (json / "restores" / 0 / "status").as[String] shouldBe "Failed"
      (json / "restores" / 0 / "host_status" / senderID / "_RowTimestamps_" / "status")
        .as[String] shouldBe "Complete"
      (json / "restores" / 0 / "host_status" / receiverID / "_RowTimestamps_" / "status")
        .as[String] shouldBe "Failed"
    }
  }

  private def validateHostCompleteStatus(
    hostID: String,
    restoreStatusJson: JSValue) = {
    (restoreStatusJson / "host_status" / hostID / "SortedIndex" / "status")
      .as[String] shouldBe "Complete"
    (restoreStatusJson / "host_status" / hostID / "Versions" / "status")
      .as[String] shouldBe "Complete"
    (restoreStatusJson / "host_status" / hostID / "HistoricalIndex" / "status")
      .as[String] shouldBe "Complete"
    (restoreStatusJson / "host_status" / hostID / "HealthChecks" / "status")
      .as[String] shouldBe "Complete"
    (restoreStatusJson / "host_status" / hostID / "SchemaVersions" / "status")
      .as[String] shouldBe "Complete"
    (restoreStatusJson / "host_status" / hostID / "_RowTimestamps_" / "status")
      .as[String] shouldBe "Complete"
  }

  private def awaitLoadComplete(loadFuture: Future[Int]): Int = {
    Await.result(loadFuture, (CoreLauncher.LoadSnapshotTimeoutSeconds + 10).seconds)
  }
}
