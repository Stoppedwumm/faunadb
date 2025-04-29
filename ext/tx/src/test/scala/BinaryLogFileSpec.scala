package fauna.tx.test

import fauna.lang.syntax._
import fauna.tx.log._
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.util.IllegalReferenceCountException
import java.nio.file._
import scala.util.Random

class BinaryLogFileSpec extends Spec {
  "Checksum" - {
    val size = 100000
    val tolerance = 1.0 / size

    def testChecksums(count: Int, buf: => ByteBuf) = {
      val stream = LazyList.iterate(Checksum.Null) { _ append buf }
      val chks = (stream take count).toSet
      val variance = 1.0 - (chks.size.toDouble / count)

      info("collision %% rate: %.3f" format (variance * 100))
      implicit val doubleOrd = Ordering.Double.TotalOrdering
      variance should be <= (tolerance)
    }

    "random bytes" in {
      testChecksums(size, {
        val arr = new Array[Byte](500 + Random.nextInt(1000))
        Random.nextBytes(arr)
        Unpooled.wrappedBuffer(arr)
      })
    }

    "identical bytes" in {
      val arr = new Array[Byte](500 + Random.nextInt(1000))
      Random.nextBytes(arr)
      testChecksums(size, Unpooled.wrappedBuffer(arr))
    }

    "somewhat identical bytes" in {
      val arr = new Array[Byte](10000)
      Random.nextBytes(arr)

      val prev = scala.collection.mutable.Set.empty[(Int, Int)]

      @annotation.tailrec def mkArr: ByteBuf = {
        val i1 = Random.nextInt(arr.size)
        val i2 = Random.nextInt(arr.size)

        if (prev contains ((i1, i2))) mkArr else {
          prev += ((i1, i2))
          val arr2 = arr.clone
          arr2(i1) = (arr2(i1) + 1).toByte
          arr2(i2) = (arr2(i2) - 1).toByte
          Unpooled.wrappedBuffer(arr2)
        }
      }

      testChecksums(size, mkArr)
    }

    "zero bytes" in {
      val arr = Unpooled.buffer.writeBytes(new Array[Byte](500 + Random.nextInt(1000)))
      testChecksums(size, arr)
    }
  }

  "BinaryLogFile" - {
    "empty log" in {
      Seq(0, 123) foreach { i =>
        val dir = aTestDir()
        val idx = TX(i)
        val chk = Checksum.Null
        val binlog = BinaryLogFile.create(dir, "empty", None, i, idx, chk)

        binlog.path should equal (dir / s"empty.binlog.$i")
        binlog.ordinal should equal (i)

        binlog.prevIdx should equal (idx)
        binlog.prevChk should equal (chk)

        binlog.lastIdx should equal (idx)
        binlog.lastChk should equal (chk)

        binlog.isEmpty should equal (true)

        val logBits = Unpooled.buffer
          .writeBytes("fntx".getBytes)  // magic
          .writeInt(0) // version
          .writeBytes(chk.toBytes) // prevChk
          .writeLong(i) // prevIdx

        Unpooled.wrappedBuffer(Files.readAllBytes(binlog.path)) should equal (logBits)

        BinaryLogFile.isLog(binlog.path, "empty") should equal (true)
        val b2 = BinaryLogFile.open(binlog.path, None)
        b2 should equal (Some(binlog))

        binlog.close()
        b2 foreach { _.close() }
        dir.deleteRecursively()
      }
    }

    def withBinlog(name: String)(f: BinaryLogFile => Any) = {
      val dir = aTestDir()
      val binlog = BinaryLogFile.create(dir, name, None)

      try f(binlog) finally {
        binlog.close()
        dir.deleteRecursively()
      }
    }

    "writes some entries" in {
      withBinlog("entries") { binlog =>
        val e1 = Unpooled.wrappedBuffer("foo".getBytes)
        val e2 = Unpooled.wrappedBuffer("bar".getBytes)
        val e3 = Unpooled.wrappedBuffer("baz".getBytes)
        val e4 = Unpooled.wrappedBuffer("qux".getBytes)

        val chk1 = Checksum.Null.append(Unpooled.copyLong(1), e1)
        val chk2 = chk1.append(Unpooled.copyLong(2), e2)
        val chk3 = chk2.append(Unpooled.copyLong(3), e3)
        val chk4 = chk3.append(Unpooled.copyLong(4), e4)

        binlog.entries(TX.MinValue) releaseAfter {
          _.toSeq should equal (Nil)
        }

        binlog.add(Seq(Unpooled.wrappedBuffer("foo".getBytes))) should equal (TX(1))
        binlog.add(Seq(Unpooled.wrappedBuffer("bar".getBytes),
          Unpooled.wrappedBuffer("baz".getBytes))) should equal (TX(3))

        binlog.entries(TX.MinValue) releaseAfter {
          _.toSeq should equal (Seq(
            BinaryLogEntry(TX(1), chk1, e1, 32),
            BinaryLogEntry(TX(2), chk2, e2, 63),
            BinaryLogEntry(TX(3), chk3, e3, 94)))
        }

        binlog.flush()

        binlog.entries(TX.MinValue) releaseAfter {
          _.toSeq should equal (Seq(
            BinaryLogEntry(TX(1), chk1, e1, 32),
            BinaryLogEntry(TX(2), chk2, e2, 63),
            BinaryLogEntry(TX(3), chk3, e3, 94)))
        }

        binlog.add(Seq(Unpooled.wrappedBuffer("qux".getBytes))) should equal (TX(4))

        binlog.entries(TX.MinValue) releaseAfter {
          _.toSeq should equal (Seq(
            BinaryLogEntry(TX(1), chk1, e1, 32),
            BinaryLogEntry(TX(2), chk2, e2, 63),
            BinaryLogEntry(TX(3), chk3, e3, 94),
            BinaryLogEntry(TX(4), chk4, e4, 125)))
        }

        binlog.flush()

        val logBits = Unpooled.buffer
          .writeBytes("fntx".getBytes)  // magic
          .writeInt(0) // version
          .writeBytes(Checksum.Null.toBytes) // prevChk
          .writeLong(0) // prevIdx

          .writeInt(27).writeBytes(chk1.toBytes).writeLong(1).writeBytes("foo".getBytes)
          .writeInt(27).writeBytes(chk2.toBytes).writeLong(2).writeBytes("bar".getBytes)
          .writeInt(27).writeBytes(chk3.toBytes).writeLong(3).writeBytes("baz".getBytes)
          .writeInt(27).writeBytes(chk4.toBytes).writeLong(4).writeBytes("qux".getBytes)

        Unpooled.wrappedBuffer(Files.readAllBytes(binlog.path)) should equal (logBits)
      }
    }

    "properly refcounts entries" in {
      withBinlog("unretained") { log =>
        log.add(Seq(Unpooled.wrappedBuffer(Array[Byte](0))))
        log.flush()

        val es = log.entries(TX.MinValue) releaseAfter { _.toSeq map { _.get } }

        log.add(Seq(Unpooled.wrappedBuffer(Array[Byte](0))))
        log.flush()

        an[IllegalReferenceCountException] should be thrownBy {
          es foreach { _.readByte }
        }
      }

      withBinlog("unreleased") { log =>
        log.add(Seq(Unpooled.wrappedBuffer(Array[Byte](0))))
        log.flush()

        val iter = log.entries(TX.MinValue)
        val es = iter.toSeq map { _.get }

        log.add(Seq(Unpooled.wrappedBuffer(Array[Byte](0))))
        log.flush()

        noException should be thrownBy {
          es foreach { _.readByte }
        }

        iter.release()
      }

      withBinlog("retained") { log =>
        log.add(Seq(Unpooled.wrappedBuffer(Array[Byte](0))))
        log.flush()

        val es = log.entries(TX.MinValue) releaseAfter { _.toSeq map { _.get.retainedSlice } }

        log.add(Seq(Unpooled.wrappedBuffer(Array[Byte](0))))
        log.flush()

        noException should be thrownBy {
          es foreach { _.readByte }
        }
      }
    }
  }
}
