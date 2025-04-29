package fauna.tx.test

import fauna.atoms.HostID
import fauna.lang.syntax._
import fauna.lang.Timing
import fauna.net.bus._
import fauna.net.bus.{ MessageBus, SignalID }
import fauna.tx.consensus._
import fauna.tx.consensus.role._
import fauna.tx.log._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.Semaphore
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext }
import scala.util.Random

class ReplicatedLogSpec extends Spec {
  private implicit def informer = info

  def randomToken = Random.nextLong()

  def ID(id: Int) = HostID(new UUID(0, id))

  "throughput" in {
    val sig = SignalID(7)
    val dir = Files.createTempDirectory("replog-tp-test")

    val buses = 0 to 2 map { _ =>
      val id = HostID.randomID
      val port = findFreePort()
      MessageBus("testCluster", id, "localhost", port = port, maxMessageSize = 16 * 1024)
    }

    val SendProtocol = Protocol[(ByteBuf, SignalID)]("send")
    implicit val ReplyProtocol = Protocol.Reply[(ByteBuf, SignalID), Unit]("reply")

    buses foreach { b =>
      b.start()
      buses foreach { b2 => b.registerHostID(b2.hostID, b2.hostAddress) }
    }

    val logs = 0 to 2 map { i =>
      implicit val ec = ExecutionContext.Implicits.global
      val tx = new MessageBusTransport(i.toString, buses(i), sig, 1024, statsPrefix = s"RepLog$i")
      ReplicatedLog[Array[Byte]].open(tx, buses(i).hostID, dir / buses(i).hostID.toString, i.toString, 100.millis, txnLogBackupPath = None)
    }

    logs(0).init()
    logs(1).join(logs(0).self)
    logs(2).join(logs(0).self)

    val h = buses(0).handler(SendProtocol, SignalID(10)) { case (from, (arr, replyTo), _) =>
      implicit val ec = ExecutionContext.parasitic
      logs(0).add(arr.toByteArray, Duration.Inf) map { _ =>
        buses(0).sink(ReplyProtocol, replyTo.at(from)).send(())
        ()
      }
    }

    Thread.sleep(2000)

    val timings = List.newBuilder[(Int, Long)]

    val arr = new Array[Byte](1024)
    val entries = 500000

    val sem = new Semaphore(256)
    implicit val ec = ExecutionContext.parasitic

    val t = Timing.start
    1 to entries foreach { i =>
      sem.acquire()
      val et = Timing.start
      buses(i % 3).sink(SendProtocol, h.id).request((Unpooled.wrappedBuffer(arr), _)) ensure {
        if (i % 100 == 0) timings += ((i % 3) -> et.elapsedMillis)
        //if (i % 10000 == 0) println(s"log ${i % 3} entry ms: ${et.elapsedMillis}")
        sem.release()
      }
    }

    logs foreach { log => Await.result(log.poll(TX(entries), Duration.Inf), Duration.Inf) }

    val elapsed = t.elapsedMillis.toFloat / 1000
    info(s"$entries entries (x ${arr.size}B): $elapsed secs, ${entries / elapsed} entries/sec")

    val logTimings = timings.result().groupBy(_._1) map { case (k, v) => k -> (v map {
      _._2
    }).sorted }
    val log0p99 = logTimings(0)((logTimings(0).size * 0.99).toInt)
    val log1p99 = logTimings(1)((logTimings(1).size * 0.99).toInt)
    val log2p99 = logTimings(2)((logTimings(2).size * 0.99).toInt)

    info(s"log0 p99: $log0p99 log1 p99: $log1p99 log2 p99: $log2p99")

    logs foreach { _.close() }
    buses foreach { _.stop() }
    dir.deleteRecursively()
  }

  "proposals from followers do not get duplicated" in {
    val sig = SignalID(7)
    val dir = Files.createTempDirectory("replog-tp-test")

    val buses = 1 to 3 map { i =>
      val id = HostID(new java.util.UUID(0, i))
      val port = findFreePort()
      MessageBus("testCluster", id, "localhost", port = port)
    }

    buses foreach { b =>
      b.start()
      buses foreach { b2 => b.registerHostID(b2.hostID, b2.hostAddress) }
    }

    val logs = 0 to 1 map { i =>
      implicit val ec = ExecutionContext.Implicits.global
      val tx = new MessageBusTransport(i.toString, buses(i), sig, 1024, statsPrefix = s"RepLog$i")
      ReplicatedLog[Array[Byte]].open(tx, buses(i).hostID, dir / buses(i).hostID.toString, i.toString, 100.millis, txnLogBackupPath = None)
    }

    logs(0).init()
    logs(1).join(logs(0).self)

    Thread.sleep(500)

    val entries = 100

    1 to entries foreach { _ =>
      logs(1).add(new Array[Byte](1), Duration.Inf)
    }

    logs foreach { log => Await.result(log.poll(TX(entries), Duration.Inf), Duration.Inf) }

    Thread.sleep(1000)

    logs foreach { log =>
      val tokens = log.entries(TX(0)) releaseAfter { _ map { _.token } toList }
      val dupes = tokens groupBy identity filter { _._2.size > 1 }

      dupes.keys.isEmpty should be (true)
    }

    logs foreach { _.close() }
    buses foreach { _.stop() }
    dir.deleteRecursively()
  }

  "one member" in {
    TestExchange.repeat(msgLossRate = 1, times = 10) { ex =>

      ex.addReplica()
      ex.replicas(0).init()

      val t1 = randomToken
      ex.replicas(0).add("foo", 30.seconds, t1)

      ex.resolveAll()

      ex.replicas foreach { log =>
        log.committedIdx should equal (TX(3))
        val e1 = log.entries(TX(3) - 1) releaseAfter { _.next() }
        e1.token should equal (t1)
        e1.get should equal ("foo")
      }

      ex.messageCount should be <= (0)
    }
  }

  "no messages when there are no pending entries" in {
    TestExchange.repeat(msgLossRate = 0.2) { ex =>

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      val t1 = randomToken
      ex.replicas(2).add("foo", 30.seconds, t1)
      ex.resolveAll()

      ex.replicas foreach { log =>
        val e1 = (log.entries(TX(0)) releaseAfter { _ find { _.token == t1 } }).get
        e1.token should equal (t1)
        e1.get should equal ("foo")
      }
    }
  }

  "minimal messages" in {
    TestExchange.repeat(times = 10) { ex =>

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.resolveAll()

      val c1 = ex.messageCount
      c1 should equal (0)

      ex.replicas(1).join(ID(0))
      ex.resolveAll()

      val c2 = ex.messageCount - c1
      c2 should equal (18)

      ex.replicas(2).join(ID(0))
      ex.resolveAll()

      val c3 = ex.messageCount - (c2 + c1)
      c3 should equal (27)

      val t1 = randomToken
      ex.replicas(0).add("foo", 30.seconds, t1)
      ex.resolveAll()

      val c4 = ex.messageCount - (c3 + c2 + c1)
      c4 should equal (8)

      val t2 = randomToken
      ex.replicas(1).add("bar", 30.seconds, t2)
      ex.resolveAll()

      val c5 = ex.messageCount - (c4 + c3 + c2 + c1)
      c5 should equal (9)

      ex.replicas foreach { log =>
        val e1 = (log.entries(TX(0)) releaseAfter { _ find { _.token == t1 } }).get
        e1.token should equal (t1)
        e1.get should equal ("foo")

        val e2 = (log.entries(TX(0)) releaseAfter { _ find { _.token == t2 } }).get
        e2.token should equal (t2)
        e2.get should equal ("bar")
      }
    }
  }

  "leader may leave the ring" in {
    TestExchange.repeat(times = 10) { ex =>

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      ex.resolveAll()

      val t1 = randomToken
      ex.replicas(0).add("foo", 30.seconds, t1)

      1 to 10 foreach { _ => ex.tick() }

      ex.replicas(0).leave()
      ex.resolveAll()
      ex.replicas(0).isRingMember should equal (false)

      val t2 = randomToken
      ex.replicas(1).add("bar", 30.seconds, t2)

      1 to 10 foreach { _ => ex.tick() }

      ex.replicas(1).leave()
      ex.resolveAll()
      ex.replicas(1).isRingMember should equal (false)

      // last node cannot leave
      ex.replicas(2).leave()
      ex.resolveAll()
      ex.replicas(2).isRingMember should equal (true)

      ex.replicas(0).join(ID(2))
      ex.resolveAll()
      ex.replicas(0).isRingMember should equal (true)

      ex.replicas(1).join(ID(0))
      ex.resolveAll()
      ex.replicas(1).isRingMember should equal (true)

      ex.replicas foreach { log =>
        val e1 = (log.entries(TX(0)) releaseAfter { _ find { _.token == t1 } }).get
        e1.token should equal (t1)
        e1.get should equal ("foo")

        val e2 = (log.entries(TX(0)) releaseAfter { _ find { _.token == t2 } }).get
        e2.token should equal (t2)
        e2.get should equal ("bar")
      }
    }
  }

  "leader may abdicate" in {
    TestExchange.repeat(times = 10) { ex =>
      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      ex.resolveAll()
      ex.replicas(0).isLeader should equal (true)

      1 to 10 foreach { _ => ex.tick() }

      val t1 = randomToken
      ex.replicas(0).add("foo", 30.seconds, t1)

      1 to 10 foreach { _ => ex.tick() }

      ex.replicas(0).abdicate("testing")
      ex.resolveAll()
      ex.replicas(0).isLeader should equal (false)

      ex.replicas foreach { log =>
        val e1 = (log.entries(TX(0)) releaseAfter { _ find { _.token == t1 } }).get
        e1.token should equal (t1)
        e1.get should equal ("foo")
      }
    }
  }

  "agrees an a unified log" in {
    TestExchange.repeat(msgLossRate = 0.1) { ex =>

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      val Seq(t1, t2, t3) = (1 to 3) map { _ => randomToken }
      ex.replicas(0).add("foo", 30.seconds, t1)
      ex.replicas(1).add("bar", 30.seconds, t2)
      ex.replicas(2).add("baz", 30.seconds, t3)

      ex.resolveAll()

      ex.replicas foreach { log =>
        val tokens = log.entries(TX(0)) releaseAfter { _ map { _.token } toList }

        tokens should contain (t1)
        tokens should contain (t2)
        tokens should contain (t3)
      }
    }
  }

  "works with general disruption" in {
    TestExchange.repeat(msgLossRate = 0.1, msgRepeatRate = 0.1, pollInterval = 4, pollJitter = 6) { ex =>

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      val t1 = randomToken
      ex.replicas(2).add("foo", 30.seconds, t1)

      ex.resolveAll()

      ex.replicas foreach { log =>
        (log.entries(TX(0)) releaseAfter { _ map { _.token } toList }) should contain (t1)
      }
    }
  }

  "works with some logs delegating membership" in {
    TestExchange.repeat(msgLossRate = 0.1, msgRepeatRate = 0.1, pollInterval = 4, pollJitter = 6) { ex =>

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.addReplicaForSig(1, 0)
      ex.addReplicaForSig(1, 0)
      ex.addReplicaForSig(1, 0)

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      val t1 = randomToken
      ex.replicas(2).add("foo", 30.seconds, t1)

      val t2 = randomToken
      ex.replicasForSig(1)(2).add("bar", 30.seconds, t2)

      ex.resolveAll()

      ex.replicas foreach { log =>
        (log.entries(TX(0)) releaseAfter { _ map { _.token } toList }) should contain (t1)
      }

      ex.replicasForSig(1) foreach { log =>
        (log.entries(TX(0)) releaseAfter { _ map { _.token } toList }) should contain (t2)
      }
    }
  }

  "membership delegating log works on single node" in {
    TestExchange.repeat(msgLossRate = 0.1, msgRepeatRate = 0.1, pollInterval = 4, pollJitter = 6) { ex =>

      ex.addReplica()

      ex.addReplicaForSig(1, 0)

      ex.replicas(0).init()

      val t2 = randomToken
      ex.replicasForSig(1)(0).add("bar", 30.seconds, t2)

      ex.resolveAll()

      ex.replicasForSig(1) foreach { log =>
        (log.entries(TX(0)) releaseAfter { _ map { _.token } toList }) should contain (t2)
      }
    }
  }

  "node being removed sees its own removal message committed" in {
    TestExchange.repeat(msgLossRate = 0.1) { ex =>
      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.replicas(2).join(ID(0))

      ex.resolveAll()

      val leaver = ex.randomReplicaID
      ex.replicas(leaver).leave()

      ex.resolveAll()

      ex.replicas foreach { log =>
        val leaves = log.entries(TX(0)) releaseAfter {
          _ collect { case ReplicatedLogEntry.RemoveMember(_, _, _, m) => m } toList
        }
        leaves should equal (List(ID(leaver)))
      }
    }
  }

  "node can be joined to cluster from a different node" in {
    TestExchange.repeat(times=1) { ex =>
      ex.addReplica()

      ex.replicas(0).init()
      val t1 = randomToken
      ex.replicas(0).add("foo", 30.seconds, t1)

      ex.resolveAll()

      ex.addReplica()
      ex.replicas(0).addMember(ID(1))
      ex.resolveAll()

      ex.replicas foreach { log =>
        (log.entries(TX(0)) releaseAfter { _ map { _.token } toList }) should contain (t1)
      }
    }
  }

  "Can shrink from 2 to 1 node" in {
    TestExchange.repeat(times = 1) { ex =>
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.resolveAll()

      val t1 = randomToken
      val t2 = randomToken
      val t3 = randomToken
      val t4 = randomToken
      ex.replicas(0).add("1", 30.seconds, t1)
      ex.replicas(0).add("2", 30.seconds, t2)
      ex.resolveAll()

      ex.closeReplica(1)
      ex.replicas(0).removeMember(ID(1))
      ex.resolveAll()

      ex.replicas(0).ring.size should equal (1)

      ex.replicas(0).add("3", 30.seconds, t3)
      ex.replicas(0).add("4", 30.seconds, t4)
      ex.resolveAll()

      val allTokens = ex.replicas(0).entries(TX(0)) releaseAfter { _ map { _.token } toSet }
      (Set(t1, t2, t3, t4) diff allTokens).isEmpty should equal(true)
    }
  }

  "sync works with 1, 2, 3-node clusters" in {
    TestExchange.repeat() { ex =>
      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()

      val f0 = ex.replicas(0).sync(30.seconds)
      f0.isCompleted should equal (true)

      ex.replicas(1).join(ID(0))
      ex.resolveAll()

      val f1 = ex.replicas(1).sync(30.seconds)

      f1.isCompleted should equal (false)
      ex.resolveAll()
      // the future should complete quickly. if not, throw an exception
      Await.ready(f1, 5.seconds)

      ex.replicas(2).join(ID(0))
      ex.resolveAll()

      val f2 = ex.replicas(2).sync(30.seconds)

      f2.isCompleted should equal (false)
      ex.resolveAll()
      // the future should complete quickly. if not, throw an exception
      Await.ready(f2, 5.seconds)
    }
  }

  "A restarted node in 2-node cluster can sync" in {
    TestExchange.repeat(times = 1) { ex =>
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()
      ex.replicas(1).join(ID(0))
      ex.resolveAll()

      val Seq(t1, t2) = (1 to 2) map { _ => randomToken }
      ex.replicas(1).add("foo", 30.seconds, t1)
      ex.replicas(1).add("bar", 30.seconds, t2)
      ex.resolveAll()

      ex.restart(1)
      ex.resolveAll()
      ex.replicas(1).committedIdx should equal (TX(5))

      val p1 = ex.replicas(1).sync(30.seconds)

      while (!p1.isCompleted) ex.tick()

      p1.value.get.get should equal (TX(5))
      ex.replicas(1).committedIdx should equal (TX(5))
    }
  }

  // FIXME: This test runs more slowly than others. Probably due to
  // larger log size and consistency comparisons in TestExchange.repeat
  "5 nodes, random writes and state changes" in {
    TestExchange.repeat(
      msgLossRate = 0.05,
      msgRepeatRate = 0.05,
      msgDelay = 4,
      msgDelayJitter = 3,
      pollJitter = 3,
      times = 50) { ex =>

      val totalValues = 200
      var values = Vector.empty[(Long, String)]

      ex.addReplica()
      ex.addReplica()
      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()

      while (values.size < totalValues) {
        Random.nextInt(100) match {
          case it if it < 20 =>
            1 to Random.nextInt(50) foreach { _ => ex.tick() }
            ex.resetLeft()

          case it if it < 90 =>
            1 to Random.nextInt(10) foreach { _ =>
              val i = values.size.toString
              val t = randomToken

              ex.replicas(ex.randomMemberID).add(i, 30.seconds, t)
              values = values :+ (t -> i)
            }

            ex.tick()

          case _ =>
            1 to Random.nextInt(3) foreach { _ =>
              val id = ex.randomReplicaID
              val inst = ex.replicas(id)
              if (inst.isRingMember) inst.leave() else inst.join(ID(ex.randomMemberID))
            }

            ex.tick()
        }
      }

      // rejoin all replicas and resolve state

      ex.nonMemberIDs map ex.replicas foreach { _.join(ID(ex.randomMemberID)) }

      ex.resolveAll()

      ex.replicas foreach { log =>
        val committed = log.entries(TX(0)) releaseAfter { _ map { v => v.token -> v } toList } toMap

        values foreach { case (token, str) =>
          committed(token).get should equal (str)
        }
      }
    }
  }

  "writes with truncations" in {
    // FIXME: This test requires turning off TestExchanges internal
    // state validations, since it assumes are never truncated (ie
    // start at TX(0), and are always identical with each other.
    // Rather than turning these off completely, a mode to relax them
    // for the presence of truncation would be better.
    TestExchange.repeat(
      msgLossRate = 0.05,
      msgRepeatRate = 0.05,
      msgDelay = 4,
      msgDelayJitter = 3,
      pollJitter = 3,
      times = 10,
      checkInternalState = false) { ex =>

      val replicas = 5
      val totalValues = 200
      var count = 0

      0 until replicas foreach { _ => ex.addReplica() }

      ex.replicas(0).init()
      1 until replicas foreach { ex.replicas(_).join(ID(0)) }

      ex.resolveAll()

      while (count < totalValues) {
        Random.nextInt(100) match {
          case it if it < 20 =>
            1 to Random.nextInt(50) foreach { _ => ex.tick() }

          case it if it < 90 =>
            1 to Random.nextInt(10) foreach { _ =>
              val i = count.toString
              val t = randomToken

              ex.replicas(ex.randomMemberID).add(i, 30.seconds, t)
              count += 1
            }

            ex.tick()

          case _ =>
            val r = ex.replicas(ex.randomMemberID)
            r.truncate(r.lastIdx)
            ex.tick()
        }
      }

      // resolve state

      ex.resolveAll()
    }
  }

  "will not get into an election deadlock" in {
    TestExchange.repeat(times = 1) { ex =>
      ex.addReplica()
      ex.addReplica()
      ex.addReplica()

      ex.replicas(0).init()

      val f0 = ex.replicas(0).sync(30.seconds)
      f0.isCompleted should equal (true)

      ex.replicas(1).join(ID(0))
      ex.resolveAll()

      val f1 = ex.replicas(1).sync(30.seconds)

      f1.isCompleted should equal (false)
      ex.resolveAll()
      // the future should complete quickly. if not, throw an exception
      Await.ready(f1, 5.seconds)

      ex.replicas(2).join(ID(0))
      ex.resolveAll()

      val f2 = ex.replicas(2).sync(30.seconds)

      f2.isCompleted should equal (false)
      ex.resolveAll()
      // the future should complete quickly. if not, throw an exception
      Await.ready(f2, 5.seconds)

      val rep0 = ex.replicas(0)
      val rep1 = ex.replicas(1)
      val rep2 = ex.replicas(2)

      val setRole = PrivateMethod[Unit](Symbol("setRole"))
      val getState = PrivateMethod[State](Symbol("state"))

      rep0.invokePrivate(setRole(Follower(
        rep0.invokePrivate(getState()),
        Follower.Observed(rep1.self))))

      rep1.invokePrivate(setRole(Follower(
        rep1.invokePrivate(getState()),
        Follower.Null)))

      rep2.invokePrivate(setRole(Follower(
        rep2.invokePrivate(getState()),
        Follower.Observed(rep0.self))))

      !ex.replicas.exists { _.isLeader } should equal(true)

      val f3 = ex.replicas(2).sync(30.seconds)
      f3.isCompleted should equal (false)
      ex.resolveAll()
      // the future should complete quickly. if not, throw an exception
      Await.ready(f3, 5.seconds)

      !ex.replicas.exists { _.isLeader } should equal(false)
    }
  }
}
