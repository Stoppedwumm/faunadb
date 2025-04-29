package fauna.multicore.test

import fauna.prop.api.CoreLauncher

class EncryptedChannelsSpec extends Spec {
  val api1 = CoreLauncher.adminClient(1, mcAPIVers)
  val api2 = CoreLauncher.adminClient(2, mcAPIVers)
  val api3 = CoreLauncher.adminClient(3, mcAPIVers)

  override protected def afterAll() =
    CoreLauncher.terminateAll()

  "Encrypted communication" - {
    "works" in {
      CoreLauncher.launchMultiple(Seq(1, 2, 3), 2, withSSL = true)

      init(api1, "dc1")
      waitUntilLive(api1, getIdentity(api1))

      join(api2, "dc2")
      join(api3, "dc3")

      shouldBeMember(api2)
      shouldBeMember(api3)
    }
  }
}
