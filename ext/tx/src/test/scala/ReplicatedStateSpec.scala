package fauna.tx.test

import fauna.atoms._
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.tx.consensus._
import fauna.tx.log.{ BinaryLogStore, TX }
import java.nio.file.{ Path, Paths }
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

object ReplicatedTestAdder {
  def apply(bus: MessageBus, dir: Path, logFileSize: Int, snapshotBeforeUpdate: Boolean) = {
    val log = ReplicatedLog[(Int, Int)].open(
      new MessageBusTransport("test", bus, SignalID(123), 1024),
      bus.hostID, dir, "adder", 100.millis,
      logFileSize = logFileSize,
      txnLogBackupPath = None)

    val tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
    val transport = new ReplicatedStateTransport("test", bus, SignalID(456), tmpDirectory)
    val state = ReplicatedState(log, transport, dir, "adder", 0, snapshotBeforeUpdate = snapshotBeforeUpdate) { (_, a, b) =>
      if (a == b._1) {
        a + b._2
      } else {
        // don't act on it so it'll cause a test failure
        a
      }
    }

    new ReplicatedTestAdder(log, state)
  }
}

class ReplicatedTestAdder(val log: ReplicatedLog[(Int, Int)], val state: ReplicatedState[(Int, Int), Int]) {
  def get = state.get

  def sync(idx: TX) = Await.result(state.sync(idx, 5.seconds), 10.seconds)

  def add(expectedState: Int, i: Int) = Await.result(log.add((expectedState, i), 5.seconds), 10.seconds)
}

class ReplicatedStateSpec extends Spec {

  def mkReplica(logFileSize: Int = BinaryLogStore.DefaultFileSize, snapshotBeforeUpdate: Boolean = false) = {
    val dir = aTestDir()
    val bus = MessageBus("state test", HostID.randomID, "localhost", port = findFreePort())

    bus.start()
    (ReplicatedTestAdder(bus, dir, logFileSize, snapshotBeforeUpdate), bus)
  }

  "ReplicatedState" - {
    "works" in {
      var buses: List[MessageBus] = Nil

      val (a1, bus1) = mkReplica()
      val (a2, bus2) = mkReplica()
      val (a3, bus3) = mkReplica()

      buses = List(bus1, bus2, bus3)
      buses foreach { b1 => buses foreach { b2 => b1.registerHostID(b2.hostID, b2.hostAddress) } }

      Await.result(a1.log.init(), 10.seconds)
      Await.result(a2.log.join(a1.state.self), 10.seconds)
      Await.result(a3.log.join(a1.state.self), 10.seconds)

      1 to 2000 foreach { i => a1.add(i - 1, 1) }

      val idx = a1.log.committedIdx
      a2.sync(idx)
      a2.get should equal (2000)

      a3.sync(idx)
      a3.get should equal (2000)

      val (a4, bus4) = mkReplica()

      buses = bus4 :: buses
      buses foreach { b1 => buses foreach { b2 => b1.registerHostID(b2.hostID, b2.hostAddress) } }

      Await.result(a4.log.join(a1.state.self), 10.seconds)

      a4.sync(idx)
      a4.get should equal (2000)

      Seq(a1, a2, a3, a4) foreach { _.log.close() }
      Seq(bus1, bus2, bus3, bus4) foreach { _.stop() }
    }

    "doesn't diverge under heavy load" ignore {
      val replicaCount = 50
      val opsCount = 1000

      val (adders, buses) = (1 to replicaCount map { _ => mkReplica(1024, true) } toList).unzip
      buses foreach { b1 => buses foreach { b2 => b1.registerHostID(b2.hostID, b2.hostAddress) } }

      Await.result(adders.head.log.init(), 10.seconds)

      Await.result((adders.tail map { _.log.join(adders.head.state.self) }).join, 30.seconds)

      1 to opsCount foreach { i =>
        Random.choose(adders).add(i - 1, 1)
      }

      val idx = adders map { _.log.committedIdx } max

      adders foreach { _.sync(idx) }

      val failed = adders map { a =>
        (a.state.self, a.get, a.log.lastIdx)
      } filter {
        _._2 != opsCount
      }

      failed should equal(List.empty[(HostID, Int)])

      adders foreach { _.log.close() }
      buses foreach { _.stop() }
    }
  }
}
