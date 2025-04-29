package fauna.tx.test

import fauna.atoms.HostID
import fauna.lang.syntax._
import fauna.net.bus.{ MessageBus, SignalID }
import fauna.stats.CSVStats
import fauna.tx.consensus._
import fauna.tx.log.TX
import fauna.tx.test.ReplicatedMapSpec._
import java.util.concurrent._
import java.io.{ File, FileWriter, PrintWriter, StringWriter }
import java.lang.management.ManagementFactory
import org.scalatest.Informer
import scala.collection.mutable.WrappedArray
import scala.collection.SortedMap
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Random

object ReplicatedMapSpec {
  type KEY   = Int
  type VALUE = Int

  val instances = 5 // this many maps
  val keys = 100 // this many keys
  val smallWait = 15.seconds

  val logDir = {
    val logFile = File.createTempFile("ReplicatedMapSpecTest", null)
    logFile.deleteOnExit()
    logFile.delete()
    logFile.mkdirs()
    assert(logFile.isDirectory, "Could not create" + logFile)
    logFile.toPath
  }

  def awaitResult[T](f: Future[T]) = Await.result(f, smallWait)
}

case class MapAndService(
  map: ReplicatedMap[KEY, VALUE],
  service: ReplicatedLog[CompareAndSet[KEY, VALUE]],
  transport: UnreliableTransport) {

  def suspend() = transport.suspend()
  def resume() = transport.resume()
}

object TestContext {
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

  def apply(
      instances: Int,
      lossRate: Double,
      repeatRate: Double,
      msgDelay: Int,
      msgDelayJitter: Int) =
  {
    val debugStr = new StringWriter()
    val now = System.currentTimeMillis()
    val debug = new PrintWriter(debugStr) {
      override def println(s: String) = super.println(s"${System.currentTimeMillis() - now} $s")
    }

    val buses = (0 until instances) map { _ =>
      val id = HostID.randomID
      val port = findFreePort()
      MessageBus("testCluster", id, "localhost",
        port = port, stats = new CSVStats((logDir / s"stats-$id.csv").toString))
    }

    buses foreach { b =>
      b.start()
      buses foreach { b2 => b.registerHostID(b2.hostID, b2.hostAddress) }
    }

    val mapsAndServices = {
      var seed: Option[HostID] = None
      val mapsAndServicesSeq = buses map { bus =>
        val hostID = bus.hostID
        val transport = new UnreliableTransport(
          bus, SignalID(1),
          lossRate, repeatRate, msgDelay, msgDelayJitter, debug)

        val service = ReplicatedLog[CompareAndSet[KEY, VALUE]].open(
          transport, hostID, logDir, hostID.toString, 25.millis, txnLogBackupPath = None)

        seed match {
          case None =>
            seed = Some(hostID)
            awaitResult(service.init())
          case Some(id) =>
            debug.println(s"Joining node $id")
            awaitResult(service.join(id))
            debug.println(s"Joined node $id")
        }
        MapAndService(ReplicatedMap(service, smallWait), service, transport)
      }
      mapsAndServicesSeq.toSet
    }
    new TestContext(debug, debugStr, buses, mapsAndServices)
  }
}

case class TestContext(
  debug: PrintWriter,
  debugStr: StringWriter,
  buses: Seq[MessageBus],
  mapsAndServices: Set[MapAndService]) {

  def doWithDebug(action: => Unit)(implicit info: Informer) = {
    try {
      action
    } catch {
      case e: Throwable =>
        // To catch any deadlocks or infinite loops
        dumpThreads()
        debug.flush()
        val errorLog = File.createTempFile("ReplicatedMapSpecTest", ".err.log")
        val fileWriter = new FileWriter(errorLog)
        fileWriter.write(debugStr.toString)
        fileWriter.close()
        info(s"RAFT communication log written to $errorLog")
        throw e
    } finally {
      buses foreach { _.stop() }
    }
  }

  def dumpThreads() = {
    val n = "\n"
    val q = "\""
    val threadMXBean = ManagementFactory.getThreadMXBean
    val threadInfos =  threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds, 100)
    new WrappedArray.ofRef(threadInfos) foreach { ti =>
      debug.print(s"$q${ti.getThreadName}$q$n   java.lang.Thread.State: ${ti.getThreadState}")
      new WrappedArray.ofRef(ti.getStackTrace) foreach { ste =>
        debug.print(s"$n        at $ste")
      }
      debug.print(s"$n$n")
    }
  }
}

class ReplicatedMapSpec extends Spec {
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
  val random = new Random()

  "testLongAbsent" in {
    val testContext = TestContext(3, 0, 0, 0, 0)

    val ms1 = testContext.mapsAndServices.drop(0).head
    val ms2 = testContext.mapsAndServices.drop(1).head
    val ms3 = testContext.mapsAndServices.drop(2).head

    // Sanity check set ordering didn't play tricks on us
    assert(!(ms1 eq ms2))
    assert(!(ms1 eq ms3))
    assert(!(ms2 eq ms3))
    val (s1, m1) = (ms1.service, ms1.map)
    val (s2, m2) = (ms2.service, ms2.map)
    val (s3, m3) = (ms3.service, ms3.map)

    val debug = testContext.debug
    implicit val informer = info

    testContext doWithDebug {
      assert(s1.isLeader)

      assert(awaitResult(m1.compareAndSet(-1, None, Some(-1))))

      awaitResult(s2.leave())
      ms2.suspend()
      debug.println(s"${s2.self} has left.")

      (0 until State.MaxAppendEntries + 1).foreach { i =>
        assert(awaitResult(m1.compareAndSet(i, None, Some(i)))) }

      debug.println(s"Added more entries than can fit in one append")

      awaitResult(s3.leave())
      ms3.suspend()
      debug.println(s"${s3.self} has left.")

      assert(awaitResult(m1.compareAndSet(-2, None, Some(-2))))
      debug.println(s"Added next 100 entries")

      ms2.resume()
      debug.println(s"${s2.self} rejoining.")
      awaitResult(s2.join(s1.self))
      debug.println(s"${s2.self} rejoined.")

      val maxApplied = m1.lastApplied max m3.lastApplied
      awaitResult(m2.poll(maxApplied))
    }
  }

  "testLeaderChange" in {
    val testContext = TestContext(3, 0, 0, 0, 0)

    val ms1 = testContext.mapsAndServices.drop(0).head
    val ms2 = testContext.mapsAndServices.drop(1).head
    val ms3 = testContext.mapsAndServices.drop(2).head

    // Sanity check set ordering didn't play tricks on us
    assert(!(ms1 eq ms2))
    assert(!(ms1 eq ms3))
    assert(!(ms2 eq ms3))
    val (s1, m1) = (ms1.service, ms1.map)
    val (s2, m2) = (ms2.service, ms2.map)
    val (s3, m3) = (ms3.service, ms3.map)

    val debug = testContext.debug
    implicit val informer = info

    testContext doWithDebug {
      assert(s1.isLeader)
      // Cluster's operational
      assert(awaitResult(m2.compareAndSet(1, None, Some(2))))
      debug.println(s"Leader ${s1.self} has left.")
      // Leader leaves
      awaitResult(s1.leave())
      debug.println(s"Leader ${s1.self} has left.")
      ms1.suspend()
      // Cluster is still operational
      assert(awaitResult(m2.compareAndSet(3, None, Some(4))))
      // Rejoin former leader
      debug.println(s"Rejoining former leader ${s1.self}")
      ms1.resume()
      awaitResult(s1.join(s2.self))
      debug.println(s"Former leader ${s1.self} rejoined.")
      // Cluster operational still
      assert(awaitResult(m2.compareAndSet(5, None, Some(6))))
      debug.println(s"Former leader ${s1.self} rejoined.")
      val maxApplied = m2.lastApplied max m3.lastApplied
      // node 1 caught up
      debug.println(s"Wait until ${s1.self} catches up.")
      awaitResult(m1.poll(maxApplied))
      assert(m1.unsyncedEntries.equals(m2.unsyncedEntries))
      // node 2 kept up
      debug.println(s"Check that ${s2.self} never fell behind.")
      awaitResult(m2.poll(maxApplied))
      awaitResult(m3.poll(maxApplied))
      assert(m3.unsyncedEntries.equals(m2.unsyncedEntries))
      // node 3 kept up
      debug.println(s"Check that ${s3.self} never fell behind.")
      awaitResult(m3.poll(maxApplied))
      assert(m3.unsyncedEntries.equals(m2.unsyncedEntries))
    }
  }

  "testReliable" in {
    info(s"Stats emitted to $logDir")
    executeTest(750, 30.seconds, 0, 0, 0, 0)
  }

  "testUnreliable" in {
    pending

    // With 10% dropped, 10% repeated, and a 0-50ms delay, the cluster can
    // progress at about 5msg/sec on my laptop, so giving it 1msg/sec max
    // duration should be safe.
    executeTest(120, 120.seconds, 0.1, 0.1, 25, 50)
  }

  private def executeTest(
      operationCount: Int,
      maxDuration: FiniteDuration,
      lossRate: Double,
      repeatRate: Double,
      msgDelay: Int,
      msgDelayJitter: Int) =
  {
    val testContext =
      TestContext(instances, lossRate,
        repeatRate, msgDelay, msgDelayJitter)

    val referenceMap = scala.collection.mutable.Map[KEY, VALUE]()
    val debug = testContext.debug
    implicit val informer = info

    def mapsAndServices = testContext.mapsAndServices

    val t1 = System.currentTimeMillis()
    testContext doWithDebug {
      Await.result(doRandomOps(operationCount), maxDuration)
    }
    {
      val duration = System.currentTimeMillis() - t1
      info(s"Executed $operationCount operations in $duration ms (${duration.toDouble/operationCount} ms/op)")
    }

    def doRandomOps(count: Int): Future[Unit] =
      if (count > 0) {
        debug.println(count.toString)

        // Pick a random map
        val mapAndService = getRandom(mapsAndServices)

        // Set a random key/value mapping
        val key = random.nextInt(keys)
        val value = random.nextInt()
        val map = mapAndService.map
        map.compareAndSet(key, referenceMap.get(key), Some(value)) flatMap { success =>
          // Must have succeeded
          assert(success)

          // Ensure all maps reflect the value
          referenceMap.put(key, value)

          checkConsistency(map.lastApplied)

          // Do the next operation
          doRandomOps(count - 1)
        }
      } else {
        Future.unit
      }

    def checkConsistency(lastTX: TX): Unit = {
      mapsAndServices map { ms =>
        (catchUpAndGetEntries(ms.map, lastTX), ms.service.self)
      } foreach { p =>
        val (mapFuture, id) = p
        val actualMap = awaitResult(mapFuture)
        if (!actualMap.equals(referenceMap)) {
          val sortedActual = SortedMap[KEY, VALUE]() ++ actualMap
          val sortedReference = SortedMap[KEY, VALUE]() ++ referenceMap

          fail(s"Map for $id differs.\nExpected: $sortedReference\nActual  : $sortedActual")
        }
      }
    }
  }

  private def catchUpAndGetEntries[K, V](map: ReplicatedMap[K, V], tx: TX) =
    map.poll(tx) map { _.unsyncedEntries }

  private def getRandom[T](iterable: Iterable[T]) =
    (iterable drop random.nextInt(iterable.size)) head
}
