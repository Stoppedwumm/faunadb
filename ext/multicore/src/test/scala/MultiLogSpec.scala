package fauna.multicore.test

import fauna.api.ReplicaTypeNames
import fauna.atoms.HostID
import fauna.codex.json._
import fauna.prop.api.CoreLauncher
import org.scalatest.time.SpanSugar
import org.scalatest.BeforeAndAfterEach

class MultiLogSpec extends Spec with SpanSugar with BeforeAndAfterEach {
  val api1 = CoreLauncher.adminClient(1, mcAPIVers)
  val api2 = CoreLauncher.adminClient(2, mcAPIVers)
  val api3 = CoreLauncher.adminClient(3, mcAPIVers)
  val api4 = CoreLauncher.adminClient(4, mcAPIVers)
  val api5 = CoreLauncher.adminClient(5, mcAPIVers)

  override protected def afterEach() =
    CoreLauncher.terminateAll()

  "Multiple Transaction Log Segments" - {
    "validate each node log segment" in {
      /* Start a 2x2 cluster with each node having a log segment */
      CoreLauncher.launchMultiple(Seq(1, 2, 3, 4), 3)

      val res1 = api1.post("/admin/init", JSObject("replica_name" -> "dc1"), rootKey)

      /* Wait for the main node to start, before starting others */
      Thread.sleep(1000)
      val res2 = api2.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc1"),
        rootKey)
      val res3 = api3.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc2"),
        rootKey)
      val res4 = api4.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc2"),
        rootKey)

      changed(res1)
      changed(res2)
      changed(res3)
      changed(res4)

      shouldBeMember(api2)
      shouldBeMember(api3)
      shouldBeMember(api4)

      setReplication(
        api1,
        "dc1" -> ReplicaTypeNames.Log,
        "dc2" -> ReplicaTypeNames.Log) should respond(NoContent)

      validateLogSegments(4, 2)
    }

    "close and reopen segments with a tool" in {
      // Start a 3-node, 2-replica cluster with a single log segment.
      CoreLauncher.launchMultiple(Seq(1, 2, 3))

      val res1 = api1.post("/admin/init", JSObject("replica_name" -> "r1"), rootKey)

      // Allow the first node to start.
      Thread.sleep(1000)
      val res2 = api2.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r1"),
        rootKey)
      val res3 = api3.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r2"),
        rootKey)

      changed(res1)
      changed(res2)
      changed(res3)

      shouldBeMember(api2)
      shouldBeMember(api3)

      // Set up the log.
      setReplication(
        api1,
        "r1" -> ReplicaTypeNames.Log,
        "r2" -> ReplicaTypeNames.Log) should respond(NoContent)

      // Check things are as expected: there is one segment with one member in each
      // replica.
      validateLogSegments(3, 1)

      // Fail to close a segment without specifying the segment.
      assertBadRequest(
        api1.post("/admin/log-topology/close-segment", JSObject(), rootKey),
        "missing segment ID")

      // Fail to close the only segment.
      assertBadRequest(
        api1.post(
          "/admin/log-topology/close-segment",
          JSObject("segmentID" -> 0),
          rootKey),
        "cannot close the only segment")

      // Add a second member of the second replica.
      CoreLauncher.launchMultiple(Seq(4))
      val res4 = api4.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r2"),
        rootKey)
      changed(res4)
      shouldBeMember(api4)

      // Verify the new replica causes the log to add another segment.
      val segs = validateLogSegments(4, 2)

      // Try to close a non-existing segment.
      assertBadRequest(
        api1.post(
          "/admin/log-topology/close-segment",
          JSObject("segmentID" -> 99),
          rootKey),
        "no such segment SegmentID(99)")

      // Close the first segment and verify it causes another to be opened.
      // Retry because it can take some time for segments to initialize.
      eventually(timeout(60 seconds), interval(1 second)) {
        api1.post(
          "/admin/log-topology/close-segment",
          JSObject("segmentID" -> segs(0)),
          rootKey) should respond(OK)
      }
      validateLogSegments(4, 2, segs(0))

      // Close the second segment and verify it causes another to be opened.
      // Retry because it can take some time for segments to initialize.
      eventually(timeout(60 seconds), interval(1 second)) {
        api1.post(
          "/admin/log-topology/close-segment",
          JSObject("segmentID" -> segs(1)),
          rootKey) should respond(OK)
      }
      validateLogSegments(4, 2, segs(1))
    }

    "move segments with a tool" in {
      // Start a 5-node, 2-replica cluster with two log segments.
      CoreLauncher.launchMultiple(Seq(1, 2, 3, 4, 5))

      val res1 = api1.post("/admin/init", JSObject("replica_name" -> "r1"), rootKey)

      // Allow the first node to start.
      Thread.sleep(1000)
      val res2 = api2.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r1"),
        rootKey)
      val res3 = api3.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r1"),
        rootKey)
      val res4 = api4.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r2"),
        rootKey)
      val res5 = api5.post(
        "/admin/join",
        JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "r2"),
        rootKey)

      changed(res1)
      changed(res2)
      changed(res3)
      changed(res4)
      changed(res5)

      shouldBeMember(api2)
      shouldBeMember(api3)
      shouldBeMember(api4)
      shouldBeMember(api5)

      // Set up the log.
      setReplication(
        api1,
        "r1" -> ReplicaTypeNames.Log,
        "r2" -> ReplicaTypeNames.Log) should respond(NoContent)
      validateLogSegments(5, 2)

      // Fail to move a segment without specifying arguments.
      assertBadRequest(
        api1.post("/admin/log-topology/move-segment", JSObject(), rootKey),
        "must specify segment ID and from and to host IDs")

      // Find a node we can move from and a node we we can move to.
      // Sometimes it takes a bit for things to shake out.
      val infos = getClusterStatuses(api1)
      val fromInfoOpt = infos find { case (info, segOpt) =>
        info.replica == "r1" && !segOpt.isEmpty
      }
      fromInfoOpt.isEmpty should equal(false)
      val fromInfo = fromInfoOpt.get
      val fromID = fromInfo._1.hostID.toString
      val segID = fromInfo._2.get._1.toInt
      val toInfoOpt = infos find { case (_, segOpt) => segOpt.isEmpty }
      toInfoOpt.isEmpty should equal(false)
      val toInfo = toInfoOpt.get
      toInfo._1.replica should equal("r1")
      val toID = toInfo._1.hostID.toString

      // Fail to move a non-existing segment.
      assertBadRequest(
        api1.post(
          "/admin/log-topology/move-segment",
          JSObject("segmentID" -> 99, "fromID" -> fromID, "toID" -> toID),
          rootKey),
        "no such segment SegmentID(99)")

      // Fail to move from a non-existing host.
      val badID = HostID.NullID.toString
      assertBadRequest(
        api1.post(
          "/admin/log-topology/move-segment",
          JSObject("segmentID" -> segID.toInt, "fromID" -> badID, "toID" -> toID),
          rootKey),
        s"no host $badID present in segment"
      )

      // Fail to move to a non-existing host.
      val noTo = api1.post(
        "/admin/log-topology/move-segment",
        JSObject("segmentID" -> segID, "fromID" -> fromID, "toID" -> badID),
        rootKey)
      (noTo.json / "errors" / 0 / "description").as[String] should (startWith(
        "from and to hosts not in same replica"))

      // Fail to move between replicas.
      val replicaMismatch = api1.post(
        "/admin/log-topology/move-segment",
        JSObject(
          "segmentID" -> segID,
          "fromID" -> fromID,
          "toID" -> getHostID(api4).toString),
        rootKey)
      (replicaMismatch.json / "errors" / 0 / "description")
        .as[String] should (startWith("from and to hosts not in same replica"))

      // Move segment.
      api1.post(
        "/admin/log-topology/move-segment",
        JSObject("segmentID" -> segID, "fromID" -> fromID, "toID" -> toID),
        rootKey) should respond(OK)

      eventually(timeout(120 seconds), interval(1 second)) {
        val status = getSegmentStatus(api1)
        status.find({ s => s._1.toString == fromID }).isEmpty should equal(true)
        status
          .find({ s => s._1.toString == toID && s._2.toInt == segID })
          .isEmpty should equal(false)
      }
    }
  }

  // Unfortunately, local tests show it can take a while for the change to occur,
  // so the eventually has to be lengthy.
  private def validateLogSegments(
    expNumNodes: Int,
    expNumSegs: Int,
    missingSeg: Int = -1) =
    eventually(timeout(120 seconds), interval(2 seconds)) {
      val info = getClusterStatuses(api1)
      // Each node should have a segment status.
      info.size should equal(expNumNodes)
      // Check for the proper number of segments.
      val segs =
        info.collect({ case (_, Some((seg, _))) => seg.toInt }).toSet.toVector
      segs.size should equal(expNumSegs)
      // Each segment should have a leader. This is also a proxy for all segments
      // being started.
      info.count({
        case (_, Some((_, isLeader))) => isLeader
        case _                        => false
      }) should equal(expNumSegs)
      // The missing segment ID should not be present.
      segs should not contain missingSeg
      segs
    }
}
