package fauna.tx.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.tx.consensus._
import fauna.tx.log._
import fauna.tx.transaction._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

import Batch._

class BatchLogSpec extends Spec {
  def testLog(): BatchLog = {
    val dir = aTestDir()
    dir.toFile.deleteOnExit()

    val self = HostID.randomID
    val bus = MessageBus("", self, "127.0.0.1", port = findFreePort())
    val replog = BatchLog.Log.open(new MessageBusTransport("batch", bus, SignalID(1), 1024), self, dir, "testbatch", 1.second, txnLogBackupPath = None)

    replog.init()
    new BatchLog(SegmentID(1), replog, 100, 0)
  }

  val origin = HandlerID(HostID.NullID, SignalID.temp)

  def txn(i: Int) = Txn(Array[Byte](i.toByte), origin, Timestamp.Max)

  def await[T](f: Future[T]) = Await.result(f, Duration.Inf)

  "BatchLog" - {
    "new log starts at the bottom" in {
      val log = testLog()

      log.prevIdx should equal (Epoch.MinValue)
      log.lastIdx should equal (Epoch.MinValue)

      log.entries(Epoch.MinValue).toList should equal (Nil)

      log.close()
    }

    "skips empty epochs" in {
      val log = testLog()

      await(log.add(Epoch(10), Vector(txn(0))))
      await(log.poll(Epoch(9), Duration.Inf))

      val entry = LogEntry(Epoch(10), Vector(ScopeSubEntry(ScopeID.RootID, Vector(txn(0)))))

      log.entries(Epoch.MinValue).toList should equal (List(entry))
      log.entries(Epoch(9)).toList should equal (List(entry))
      log.entries(Epoch(10)).toList should equal (Nil)

      log.close()
    }

    "emits ordered epochs" in {
      val log = testLog()

      await(log.add(Epoch(3), Vector(txn(0))))
      await(log.add(Epoch(2), Vector(txn(1))))
      await(log.add(Epoch(1), Vector(txn(2))))

      await(log.poll(Epoch(2), Duration.Inf))

      log.entries(Epoch.MinValue).toList should equal (List(
        LogEntry(Epoch(3), Vector(ScopeSubEntry(ScopeID.RootID, Vector(txn(0)))))))

      await(log.add(Epoch(6), Vector(txn(3))))
      await(log.add(Epoch(5), Vector(txn(4))))
      await(log.add(Epoch(4), Vector(txn(5))))

      await(log.poll(Epoch(5), Duration.Inf))

      log.entries(Epoch.MinValue).toList should equal (List(
        LogEntry(Epoch(3), Vector(ScopeSubEntry(ScopeID.RootID, Vector(txn(0))))),
        LogEntry(Epoch(6), Vector(ScopeSubEntry(ScopeID.RootID, (1 to 3 map txn).toVector)))))

      await(log.add(Epoch(7), Vector(txn(6))))

      await(log.poll(Epoch(6), Duration.Inf))

      log.entries(Epoch.MinValue).toList should equal (List(
        LogEntry(Epoch(3), Vector(ScopeSubEntry(ScopeID.RootID, Vector(txn(0))))),
        LogEntry(Epoch(6), Vector(ScopeSubEntry(ScopeID.RootID, (1 to 3 map txn).toVector))),
        LogEntry(Epoch(7), Vector(ScopeSubEntry(ScopeID.RootID, (4 to 6 map txn).toVector)))))

      log.close()
    }

    "correctly reconstruct batches mid-stream" in {
      val log = testLog()

      1 to 41 foreach { i =>
        await(log.add(Epoch(i), Vector(txn(i))))
        if (i % 2 == 0) await(log.add(Epoch(i - 2), Vector(txn(i + 1))))
        if (i % 4 == 0) await(log.add(Epoch(i - 1), Vector(txn(i + 1))))
      }

      await(log.poll(Epoch(40), Duration.Inf))

      val es = log.entries(Epoch(0)) releaseAfter { _.toVector }

      0 to 40 foreach { i =>
        val es0 = log.entries(Epoch(i)) releaseAfter { _.toVector }
        es0 should equal (es dropWhile { _.idx <= Epoch(i) })
      }

      log.truncate(Epoch(17))

      0 to 40 foreach { i =>
        val es0 = log.entries(Epoch(i)) releaseAfter { _.toVector }
        es0 should equal (es dropWhile { _.idx <= Epoch(i max 17) })
      }
    }
  }
}
