package fauna.tx.test

import fauna.atoms.HostID
import fauna.tx.consensus.Ring

class RingSpec extends Spec {

  val node1 = HostID.randomID
  val node2 = HostID.randomID
  val node3 = HostID.randomID
  val node4 = HostID.randomID
  val node5 = HostID.randomID

  "quorumValue on" - {
    "1 HostID" in {
      val all = Set(node1)

      Ring(all).quorumValue(Seq(node1)) should contain(node1)
    }

    "2 HostIDs" in {
      val all = Set(node1, node2)

      Ring(all).quorumValue(Seq(node1, node2)) shouldBe empty
      Ring(all).quorumValue(Seq(node1, node1)) should contain(node1)

      Ring(all).quorumValue(Seq(node1)) shouldBe empty
    }


    "3 HostIDs" in {
      val all = Set(node1, node2, node3)

      //check possible combinations of
      Ring(all).quorumValue(Seq(node1, node2, node3)) shouldBe empty
      Ring(all).quorumValue(Seq(node1, node1, node1)) should contain(node1)
      Ring(all).quorumValue(Seq(node1, node1, node2)) should contain(node1)
      Ring(all).quorumValue(Seq(node1, node2, node2)) should contain(node2)

      Ring(all).quorumValue(Seq(node1, node1)) should contain(node1)

      Ring(all).quorumValue(Seq(node1, node2)) shouldBe empty

      Ring(all).quorumValue(Seq(node1)) shouldBe empty
    }

    "4 HostIDs" in {
      val all = Set(node1, node2, node3, node4)

      Ring(all).quorumValue(Seq(node1, node1, node1, node1)) should contain(node1)
      Ring(all).quorumValue(Seq(node1, node1, node1, node2)) should contain(node1)
      Ring(all).quorumValue(Seq(node1, node1, node2, node2)) shouldBe empty
      Ring(all).quorumValue(Seq(node1, node2, node2, node2)) should contain(node2)
      Ring(all).quorumValue(Seq(node1, node2, node2, node3)) shouldBe empty

      Ring(all).quorumValue(Seq(node1, node1, node1)) should contain(node1)
      Ring(all).quorumValue(Seq(node1, node1)) shouldBe empty
      Ring(all).quorumValue(Seq(node1)) shouldBe empty
    }
  }
}
