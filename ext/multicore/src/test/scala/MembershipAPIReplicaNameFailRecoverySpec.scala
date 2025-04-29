package fauna.multicore.test

import fauna.codex.json.{ JS, JSObject }
import fauna.prop.api.CoreLauncher
import scala.concurrent.duration._

class MembershipAPIReplicaNameFailRecoverySpec extends Spec {
  /**
    * This test asserts for seed node communication timeouts.
    *
    * [[fauna.cluster.Hello]] times out at 90.seconds but the default HttpClient timeout used
    * for test cases is 60.seconds. So for this test case timeout should be [[fauna.cluster.Hello]]'s
    * timeout plus some grace time for this test-case to run.
    */
  val helloTimeoutPlusGrace = 90.seconds + 5.seconds
  val api1 = CoreLauncher.adminClient(1, mcAPIVers, helloTimeoutPlusGrace)
  val api2 = CoreLauncher.adminClient(2, mcAPIVers, helloTimeoutPlusGrace)

  override protected def afterAll() =
    CoreLauncher.terminateAll()

  /**
    * Test: Assert scenarios when a user enters invalid replica name configurations on a 2 node cluster
    * and when there is a timeout.
    *
    * Expected result: The cluster should always allow user to continue configuration even after invalid inputs.
    *
    * This test is slow to complete because of timeouts in [[fauna.cluster.Hello]].
    */
  "Cluster should" - {
    "recover from invalid replica name configurations" in {
      //start 2 nodes
      CoreLauncher.launchMultiple(Seq(1, 2), 2)

      //try joining with an invalid replica name and expect an error
      assertBadRequest(
        res = api2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "-ab_c"), rootKey),
        expectedErrorMessage = "Replica name is invalid."
      )

      //try joining again with a valid replica_name but since the seed node is not initialized it should fail
      assertErrorResponse(
        res = api2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc2"), rootKey),
        expectedErrorMessage = "Timed out trying to obtain a response from any peer"
      )

      //node2 has it's replica_name already set to dc2 above.
      //Retrying joining with a different replica_name should not allow for the new replica_name.
      assertBadRequest(
        res = api2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc3"), rootKey),
        expectedErrorMessage = "Replica name is already set to 'dc2'."
      )

      //try joining again with the same replica_name and still get ReadTimeoutException
      //this indicates that the state of the node has not changed from the previous successful replica_name set.
      assertErrorResponse(
        res = api2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc2"), rootKey),
        expectedErrorMessage = "Timed out trying to obtain a response from any peer"
      )

      shouldNotBeMember(api2)

      //start node1 successfully
      init(api1, "dc1")

      //node2 is already started with replica_name set to dc2.
      //Retrying joining with a different replica_name should not allow for the new replica_name to be set.
      assertBadRequest(
        res = api2.post("/admin/join", JSObject("seed" -> JS(CoreLauncher.address(1)), "replica_name" -> "dc3"), rootKey),
        expectedErrorMessage = "Replica name is already set to 'dc2'."
      )
      //successfully join node2.
      join(api2, "dc2")

      //node2 is a member
      shouldBeMember(api2)
    }
  }
}
