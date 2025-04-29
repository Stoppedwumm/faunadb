package fauna.cluster

import fauna.lang.syntax._
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

package test {
  import fauna.atoms._
  import fauna.cluster.topology.{ LogTopology, Topologies }
  import fauna.cluster.workerid.WorkerIDs
  import fauna.exec.{ FaunaExecutionContext, Timer }
  import fauna.net.HostService
  import fauna.net.bus.{ MessageBus, SignalID }
  import java.net.BindException
  import java.nio.file.{ DirectoryNotEmptyException, Files, Path, Paths }
  import java.util.BitSet
  import org.scalatest.concurrent.Eventually
  import scala.annotation.tailrec
  import scala.concurrent.{ Await, Future }
  import scala.concurrent.duration._
  import scala.util.Random

  trait Spec
      extends AnyFreeSpec
      with BeforeAndAfter
      with BeforeAndAfterAll
      with Matchers
      with LoneElement
      with Eventually {

    def await[T](f: Future[T]) = Await.result(f, 60.seconds)

    def removeGracefully(n: Node, coordinator: Node) = {
      n.stop()
      await(coordinator.membership.remove(n.id, force = false))
    }
  }

  object Node {
    val minWorkerID = Random.nextInt(512)
    val maxWorkerID = minWorkerID + 7

    { fauna.logging.test.enableFileLogging("test.log") }

    private val usedAddresses = new BitSet()

    def create(hostID: HostID = HostID.randomID, replica: String = "dc1")(
      implicit tes: TombstoneExpirySpecification = TombstoneExpirySpecification()) = {

      @tailrec def createBus(addrFrom: Int): MessageBus = {
        val nextIP = usedAddresses.nextClearBit(addrFrom)

        val bus = MessageBus("testcluster",
          hostID,
          s"127.0.0.$nextIP",
          port = 7500)
        try {
          bus.start()
          usedAddresses.set(nextIP)
          bus
        } catch {
          case e: BindException =>
          if (addrFrom < 255) {
            // This is meant to help with recently closed server sockets
            // that in the OS TCP stack still didn't get back to CLOSED state.
            // Our SimpleNettyServer uses SO_REUSEADDR but it doesn't
            // seem sufficient to make this unnecessary.
            createBus(addrFrom + 1)
          } else {
            throw e
          }
        }
      }

      val dir = Files.createTempDirectory("test-consensus-cluster")
      val tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"))

      val bus = usedAddresses.synchronized { createBus(1) }
      val config = ClusterServiceConfig(bus, 1024, tmpDirectory, dir, 100.millis, None)

      val membership =
        Membership(config, SignalID(1), SignalID(2), SignalID(3), replica)(tes)
      membership.start()
      val workerIDs = WorkerIDs(config,
                                SignalID(4),
                                SignalID(5),
                                membership,
                                minWorkerID = minWorkerID,
                                maxWorkerID = maxWorkerID)
      workerIDs.start()
      val topologies = Topologies(config, SignalID(6), SignalID(7), membership)
      topologies.start()
      val logTopology =
        LogTopology(config, SignalID(8), SignalID(9), topologies, membership)
      logTopology.start()

      Node(bus, membership, workerIDs, topologies, logTopology, dir)
    }

    def stopAll(nodes: Node*) = nodes foreach { _.stop() }

    def releaseAddress(bus: MessageBus) =
      usedAddresses.synchronized {
        usedAddresses.clear(bus.hostAddress.address.getAddress()(3))
      }
  }

  object RemovalHostService extends HostService {
    def isLive(host: HostID) = true
    def isLocal(host: HostID) = true
    def isNear(host: HostID) = false
    def subscribeStartsAndRestarts(f: HostID => Unit): Unit = ()
  }

  case class Node(
    bus: MessageBus,
    membership: Membership,
    workerIDs: WorkerIDs,
    topologies: Topologies,
    logTopology: LogTopology,
    dir: Path) {
    @volatile private[this] var running = true

    private[this] val removal =
      new NodeRemoval(membership, topologies, RemovalHostService)(
        FaunaExecutionContext.Implicits.global)

    def id = bus.hostID
    def address = bus.hostAddress

    def awaitActive(): Unit = awaitActivity(this, true)

    def awaitInactive(node: Node): Unit = awaitActivity(node, false)

    def awaitActivity(node: Node, activity: Boolean): Unit = {
      def observeActivity: Future[Unit] =
        if (workerIDs.hostWorkerID(node.id).idOpt.nonEmpty == activity) {
          Future.successful(())
        } else {
          Timer.Global.delay(100.millis) { observeActivity }
        }
      Await.result(observeActivity, 40.seconds)
    }

    def stop() = {
      running = false
      logTopology.stop()
      topologies.stop()
      workerIDs.stop()
      membership.stop()
      bus.stop()
      Node.releaseAddress(bus)
      clearDir(10)
    }

    def removeNode(hostID: HostID, force: Boolean) =
      removal.removeNode(hostID, force)

    def revalidateLogTopology = {
      val lnp = logTopology.logNodeProvider
      val lni = lnp.getLogNodeInfo
      lnp.revalidate(lni.validUntil + 10, lni.version)
    }

    // Account for a possible race against files being flushed.
    @tailrec
    private def clearDir(attempts: Int): Unit =
      try {
        dir.deleteRecursively()
      } catch {
        case e: DirectoryNotEmptyException =>
          if (attempts == 0) throw e
          clearDir(attempts - 1)
      }
  }
}
