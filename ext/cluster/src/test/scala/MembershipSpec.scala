package fauna.cluster.test

import fauna.cluster.TombstoneExpirySpecification
import scala.concurrent.duration._

class MembershipSpec extends Spec {
  "Membership" - {
    "should retain but later forget removed hosts" in {
      implicit val tes = TombstoneExpirySpecification(4.seconds, 1.seconds)

      val node1 = Node.create()
      await(node1.membership.init())
      node1.awaitActive()

      val node2 = Node.create()
      await(node2.membership.join(node1.address))
      node2.awaitActive()

      // Add a 3rd node to maintain quorum when the 2nd node is removed
      val node3 = Node.create()
      await(node3.membership.join(node1.address))

      removeGracefully(node2, node1)
      // It's still registered. removeGracefully also ensures HostState.Left was observed
      node1.bus.getHostAddresses(node2.id).nonEmpty should equal (true)

      // Eventually, it is forgotten
      eventually(timeout(40 seconds), interval(200 milliseconds)) {
        node1.membership.leftHosts.contains(node2.id) should equal (false)
      }
      node1.bus.getHostAddresses(node2.id).isEmpty should equal (true)

      node3.stop()
      node1.stop()
    }

    "should not be able to join with a previously retired ID" in {
      val node1 = Node.create()
      await(node1.membership.init())
      node1.awaitActive()

      val node2 = Node.create()
      await(node2.membership.join(node1.address))
      node2.awaitActive()

      // Add a 3rd node to maintain quorum when the 2nd node is removed
      val node3 = Node.create()
      await(node3.membership.join(node1.address))

      removeGracefully(node2, node1)

      val node4 = Node.create(node2.id)

      assertThrows[IllegalArgumentException] {
        await(node4.membership.join(node1.address))
      }

      node4.stop()
      node3.stop()
      node1.stop()
    }

  }
}
