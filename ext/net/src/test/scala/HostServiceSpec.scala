package fauna.net.test

import fauna.atoms.HostID
import fauna.net.HostService
import fauna.prop.Prop
import fauna.prop.test._
import org.scalatest.matchers.should.Matchers

class HostServiceSpec extends PropSpec(10, 1000) with Matchers {
  val hostA = HostID.randomID
  val hostB = HostID.randomID
  val hostC = HostID.randomID

  val hosts = Seq(hostA, hostB, hostC)

  prop("prefers local") {
    for {
      hosts <- Prop.shuffle(hosts)
      local <- Prop.choose(hosts)
      near <- Prop.choose(hosts)
      if near != local
    } {
      val hs =
        HostService.preferredOrder(
          hosts,
          isLive = hosts.toSet,
          isLocal = Set(local),
          isNear = Set(near)
        )

      hs.head should be(local)
      hs should contain theSameElementsAs hosts

      val dead =
        HostService.preferredOrder(
          hosts,
          isLive = hosts.toSet - local,
          isLocal = Set(local),
          isNear = Set(near)
        )

      dead.last should be(local)
      dead should contain theSameElementsAs hosts
    }
  }

  prop("prefers live") {
    for {
      hosts <- Prop.shuffle(hosts)
      live <- Prop.choose(hosts)
    } {
      val hs =
        HostService.preferredOrder(
          hosts,
          isLive = Set(live),
          isLocal = hosts.toSet,
          isNear = Set.empty
        )

      hs.head should be(live)
      hs should contain theSameElementsAs hosts

      val nearBy =
        HostService.preferredOrder(
          hosts,
          isLive = Set(live),
          isLocal = hosts.toSet - live,
          isNear = Set(live)
        )

      nearBy.head should be(live)
      nearBy should contain theSameElementsAs hosts

      val nonLocal =
        HostService.preferredOrder(
          hosts,
          isLive = Set(live),
          isLocal = hosts.toSet - live,
          isNear = Set.empty
        )

      nonLocal.head should be(live)
      nonLocal should contain theSameElementsAs hosts
    }
  }

  prop("prefers near") {
    for {
      hosts <- Prop.shuffle(hosts)
      near <- Prop.choose(hosts)
    } {
      val hs =
        HostService.preferredOrder(
          hosts,
          isLive = hosts.toSet,
          isLocal = Set.empty,
          isNear = Set(near)
        )

      hs.head shouldBe near
      hs should contain theSameElementsAs hosts
    }
  }

  prop("prefers local and live") {
    for {
      hosts <- Prop.shuffle(hosts)
      local <- Prop.choose(hosts)
      near <- Prop.choose(hosts)
      if local != near
    } {
      val hs =
        HostService.preferredOrder(
          hosts,
          isLive = Set(local, near),
          isLocal = hosts.toSet - near,
          isNear = Set(near)
        )

      hs.head should be(local)
      hs should contain theSameElementsAs hosts
    }
  }
}
