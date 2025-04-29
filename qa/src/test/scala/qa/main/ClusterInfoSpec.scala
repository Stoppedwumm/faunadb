package qa.main

import fauna.codex.json._
import java.net.InetAddress
import java.time.Instant
import java.util.UUID
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import qa.operator._
import scala.io.Source

/**
  * ClusterInfo test suite.
  */
class ClusterInfoSpec extends AnyFreeSpec with Matchers with Eventually {

  val json = {
    val jsText = Source.fromResource("admin-status.json").getLines().mkString("\n")
    JS.parse(jsText)
  }

  "NodeInfo" - {
    "extract node status from JSON" in {
      val nodeJson = json / "nodes" / 0
      NodeInfo.extract(nodeJson) shouldBe NodeInfo(
        NodeStatus.Up,
        "live",
        Some(InetAddress.getByName("127.0.0.1")),
        Some("512"),
        1.0f,
        1.0f,
        Some(UUID.fromString("11a38170-3c07-4c13-b700-86c58ca888c2")),
        Some("0"),
        true,
        Some(Instant.parse("1970-01-01T00:00:00Z"))
      )
    }
  }

  "ClusterInfo" - {
    "extract cluster information from JSON" in {
      val dc1Nodes = List(
        NodeInfo(
          NodeStatus.Up,
          "live",
          Some(InetAddress.getByName("127.0.0.1")),
          Some("512"),
          1.0f,
          1.0f,
          Some(UUID.fromString("11a38170-3c07-4c13-b700-86c58ca888c2")),
          Some("0"),
          true,
          Some(Instant.parse("1970-01-01T00:00:00Z"))
        ))
      val dc2Nodes = List(
        NodeInfo(
          NodeStatus.Up,
          "live",
          Some(InetAddress.getByName("127.0.0.2")),
          Some("513"),
          1.0f,
          1.0f,
          Some(UUID.fromString("6c3f2824-25a5-404d-96c3-2838d4d04692")),
          Some("0"),
          false,
          Some(Instant.parse("1970-01-01T00:00:00Z"))
        ))
      ClusterInfo.extract(json) shouldBe ClusterInfo(
        List(
          ReplicaInfo("dc1", ReplicaMode.DataLog, dc1Nodes),
          ReplicaInfo("dc2", ReplicaMode.DataLog, dc2Nodes)))
    }
  }
}
