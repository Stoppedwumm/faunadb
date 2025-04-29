package fauna.tx.test

import fauna.atoms.HostID
import fauna.lang.syntax._
import fauna.tx.consensus.Term
import fauna.tx.consensus.log._
import fauna.tx.log._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.nio.file._

class BinaryLogStoreSpec extends Spec {
  var dir: Path = _

  @annotation.nowarn
  def entriesWithoutPrecreatedLog(dir: Path) = {
    val grouped = dir.entries.groupBy {
      case BinaryLogFile.FileName(_, _) => 1
      case _ => 0
    }
    grouped(1)
      .sortBy { case BinaryLogFile.FileName(_, idx) => -idx }
      .drop(1) ++ grouped(0)
  }

  "BinaryLogStore" - {

    "ByteBuf log skips encoding" in {
      val dir = aTestDir()
      val raw = BinaryLogStore.open[ByteBuf](dir, None, "raw")
      val cbor = BinaryLogStore.open[String](dir, None, "cbor")

      raw.isInstanceOf[RawBinaryLogStore] should equal (true)
      cbor.isInstanceOf[CBORBinaryLogStore[_]] should equal (true)

      raw.close()
      cbor.close()

      dir.deleteRecursively()
    }

    "typical log" - {
      before {
        dir = aTestDir()

        val b = BinaryLogStore.open[ByteBuf](dir, None, "typical", fileSize = 1024)
        1 to 100 foreach { _ => b.add(Seq("entry".toUTF8Buf)) }
        b.close()
      }

      after {
        dir.deleteRecursively()
      }

      "has more than one file" in {
        entriesWithoutPrecreatedLog(dir).size should be > (1)
      }

      "names files according to ordinal" in {
        entriesWithoutPrecreatedLog(dir) foreach { file =>
          file.getFileName match {
            case BinaryLogFile.FileName(name, idx) =>
              val b = BinaryLogFile.open(dir, name, idx, None).get
              b.ordinal should equal (idx)

            case p if p.toString == "typical.binlog.manifest" => ()
            case p => fail(s"Unexpected file $p")
          }
        }
      }

      "has no gaps in TX" in {
        val log = BinaryLogStore.open[ByteBuf](dir, None, "typical", fileSize = 1024)
        val logfiles = entriesWithoutPrecreatedLog(dir) flatMap { BinaryLogFile.open(_, txnLogBackupPath = None) } sortBy { _.prevIdx }

        log.prevIdx should equal (TX.MinValue)
        log.uncommittedLastIdx should equal (TX(100))

        (logfiles foldLeft log.prevIdx) { (prevIdx, file) =>
          file.prevIdx should equal (prevIdx)
          file.lastIdx
        }

        logfiles.last.lastIdx should equal (log.uncommittedLastIdx)
      }

      "iterates entries across files" in {
        val log = BinaryLogStore.open[ByteBuf](dir, None, "typical", fileSize = 1024)

        log.uncommittedEntries(TX.MinValue) releaseAfter { iter =>
          val es = iter.toSeq
          es.size should equal (100)
          es map { _.idx } should equal (1 to 100 map (TX(_)))
        }
      }
    }

    "encodes and decodes" in {
      val logdir = aTestDir()

      val log = BinaryLogStore.open[Entry](logdir, None, "typed", fileSize = 1024)

      val entries = Seq(
        Entry.Null(Term(42), 0),
        Entry.Value(Term(43), 12345678901234L, "value1".toUTF8Bytes),
        Entry.AddMember(Term(44), 12345678901235L, HostID.randomID),
        Entry.RemoveMember(Term(45), 12345678901236L, HostID.randomID))

      // Encode all test entry specimens
      log.add(entries)

      // Decode all test entry specimens
      log.uncommittedEntries(TX.MinValue) releaseAfter {
        _.toSeq map { _.get } should equal (entries)
      }

      log.close()

      logdir.deleteRecursively()
    }

    "log with missing file does not verify" in {
      val dir = aTestDir()
      val log = BinaryLogStore.open[ByteBuf](dir, None, "missing", fileSize = 1)

      1 to 3 foreach { _ => log.add(Seq("entry".toUTF8Buf)) }
      log.close()

      val logfiles = entriesWithoutPrecreatedLog(dir) flatMap { BinaryLogFile.open(_, txnLogBackupPath = None) } sortBy { _.prevIdx }
      logfiles.foreach { _.close() }
      Files.delete(logfiles(1).path)

      a[LogMissingFileException] should be thrownBy BinaryLogStore.open[ByteBuf](dir, None, "missing", fileSize = 1)

      dir.deleteRecursively()
    }

    "log with invalid file does not verify" in {
      val dir = aTestDir()
      val log = BinaryLogStore.open[ByteBuf](dir, None, "invalid", fileSize = 1)

      1 to 3 foreach { _ => log.add(Seq("entry".toUTF8Buf)) }
      log.close()

      val badManifest = Unpooled.buffer
        .writeLong(0)
        .writeLong(2)
        .writeLong(0)
        .writeLong(1)

      Files.write(dir / "invalid.binlog.manifest", badManifest.toByteArray)

      a[LogInvalidFileException] should be thrownBy BinaryLogStore.open[ByteBuf](dir,None,"invalid", fileSize = 1)

      val l1 = BinaryLogStore.open[ByteBuf](dir, None, "invalid", fileSize = 1, moveInvalid = true)
      l1.close()

      (dir.entries map { _.getFileName.toString } toSet) should equal (Set(
        "invalid.binlog.manifest",
        "invalid.binlog.0",
        "invalid.binlog.1",
        "invalid.binlog.2",
        "invalid.binlog.3.invalid"))

      val l2 = BinaryLogStore.open[ByteBuf](dir, None, "invalid", fileSize = 1)
      l2.close()

      dir.deleteRecursively()
    }

    "log with bad data file does not verify" in {
      val dir = aTestDir()
      val log = BinaryLogStore.open[ByteBuf](dir, None, "bad_data", fileSize = 1024)

      1 to 100 foreach { _ => log.add(Seq("entry".toUTF8Buf)) }
      log.close()

      val logfiles = entriesWithoutPrecreatedLog(dir) flatMap { BinaryLogFile.open(_, txnLogBackupPath = None) } sortBy { _.prevIdx }

      val badBits = Unpooled.buffer
        .writeBytes("fntx".toUTF8Buf)  // magic
        .writeInt(0) // version
        .writeBytes(Checksum.Null.toBytes) // prevChk
        .writeLong(0) // prevIdx

      Files.write(logfiles.head.path, badBits.toByteArray)

      a[LogMissingTransactionsException] should be thrownBy BinaryLogStore.open[ByteBuf](dir, None, "bad_data", fileSize = 1024)

      dir.deleteRecursively()
    }

    "log with bad checksum does not verify" in {
      val dir = aTestDir()

      // touch the log file
      BinaryLogStore.open[ByteBuf](dir, None, "bad_chk").close()

      val e1 = Unpooled.wrappedBuffer("foo".getBytes)
      val e2 = Unpooled.wrappedBuffer("bar".getBytes)
      val e3 = Unpooled.wrappedBuffer("baz".getBytes)

      val chk1 = Checksum.Null.append(Unpooled.copyLong(1), e1)
      val chk2 = chk1.append(Unpooled.copyLong(2), e2)
      val chk3 = chk2.append(Unpooled.copyLong(3), e3)

      val badBits = Unpooled.buffer
        .writeBytes("fntx".getBytes)  // magic
        .writeInt(0) // version
        .writeBytes(Checksum.Null.toBytes) // prevChk
        .writeLong(0) // prevIdx

        .writeInt(27).writeBytes(chk1.toBytes).writeLong(1).writeBytes("foo".getBytes)
        .writeInt(27).writeBytes(chk2.toBytes).writeLong(2).writeBytes("bar".getBytes)
        .writeInt(27).writeBytes(chk3.toBytes).writeLong(3).writeBytes("bad".getBytes)

      Files.write(dir / "bad_chk.binlog.0", badBits.toByteArray)

      a[LogChecksumException] should be thrownBy BinaryLogStore.open[ByteBuf](dir, None, "bad_chk")

      dir.deleteRecursively()
    }

    "allows idxs larger than rotate size" in {
      val dir = aTestDir()
      val log = BinaryLogStore.open[ByteBuf](dir, None, "large", fileSize = 1024)

      1 to 10 foreach { _ => log.add(Seq(("entry" * 1000).toUTF8Buf)) }
      log.close()

      val files = entriesWithoutPrecreatedLog(dir)

      (files map { file =>
        (file.getFileName: @unchecked) match {
          case BinaryLogFile.FileName(name, idx) if name == "large" => idx
          case p if p.toString == "large.binlog.manifest" => -1
        }
      }).sorted should equal (-1 to 9)

      val logs = files flatMap { BinaryLogFile.open(_, txnLogBackupPath = None) } sortBy { _.prevIdx }

      logs map { _.prevIdx } should equal (0 to 9 map (TX(_)))
      logs map { _.lastIdx } should equal (1 to 10 map (TX(_)))

      dir.deleteRecursively()
    }

    "doesn't reset idx" in {
      val dir = aTestDir()

      // set the prevIdx
      val log0 = BinaryLogStore.open[ByteBuf](dir, None, "reset")
      log0.reinit(TX(3))
      log0.close()

      val log = BinaryLogStore.open[ByteBuf](dir, None, "reset")
      log.prevIdx should equal (TX(3))
      log.close()

      dir.deleteRecursively()
    }

    "reinit doesn't race with background file precreate" in {
      1 to 200 foreach { _ =>
        val dir = aTestDir()

        val log = BinaryLogStore.open[ByteBuf](dir, None)
        log.reinit(TX(3))
        log.close()

        dir.deleteRecursively()
      }
    }

    "stores metadata" in {
      val dir = aTestDir()

      // set the prevIdx
      val log0 = BinaryLogStore.open[ByteBuf](dir, None, "meta")
      log0.metadata should equal (None)
      log0.metadata("foo".toUTF8Buf)
      log0.metadata.get releaseAfter { _.toUTF8String should equal ("foo") }
      log0.close()

      val log = BinaryLogStore.open[ByteBuf](dir, None, "meta")
      log.metadata.get releaseAfter { _.toUTF8String should equal ("foo") }
      log.close()

      dir.deleteRecursively()
    }

    "file precreate triggered from truncate doesn't race" in {
      val dir = aTestDir()

      val l0 = BinaryLogStore.open[Int](dir, None, fileSize = 3)
      1 to 1000 foreach { i =>
        val tx = l0.add(Seq(i))
        l0.flush()
        l0.updateCommittedIdx(tx)
        l0.truncate(l0.lastIdx)
      }

      dir.deleteRecursively()
    }
  }
}
