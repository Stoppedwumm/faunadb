package fauna.tx.log

import fauna.lang.syntax._
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, Unpooled }
import io.netty.util.IllegalReferenceCountException
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption._
import java.nio.file._
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import scala.util.{ Failure, Success, Using }
import scala.util.control.NonFatal

// THREAD SAFETY: Single writer, many readers. Must take care not to
// discard entries that have been made visible to downstream readers.

// Binary formats (num types are network byte order):

// V0 log header format:

//   66 6e 74 78                # ASCII 'fntx'
//   xx xx xx xx                # Log version (int)
//
//   xx xx xx xx  xx xx xx xx   # prev entry md5 (16b)
//   xx xx xx xx  xx xx xx xx
//
//   xx xx xx xx  xx xx xx xx   # prev entry TX ID (long)
//
//   ...                        # entries

// V0 log entry format:

//   xx xx xx xx                # entry size, including metadata (int)
//
//   xx xx xx xx  xx xx xx xx   # md5 of prev entry's md5 + tx + entry (16b)
//   xx xx xx xx  xx xx xx xx
//
//   xx xx xx xx  xx xx xx xx   # TX ID (long)
//
//   ...                        # entry data

import BinaryLogFile.Constants._
import BinaryLogFile.{ Info, RefCountedMapping }

object BinaryLogFile {

  object Constants {
    final val MagicPrefix = "fntx".toUTF8Bytes
    final val Version = 0

    final val LogPrefixBytes = 4
    final val LogVersionBytes = 4
    final val CheckBytes = Checksum.ByteLength
    final val TXBytes = 8
    final val SizeBytes = 4

    final val EntryMetaBytes = TXBytes + CheckBytes
    final val EntryHeaderBytes = SizeBytes + EntryMetaBytes

    final val HeaderBytes = LogPrefixBytes + LogVersionBytes + TXBytes + CheckBytes
  }

  final val StartPos = HeaderBytes

  final case class Info(
    lastIdx: TX,
    lastChk: Checksum,
    size: Long,
    flushedSize: Long,
    flushing: List[BinaryLogEntry],
    buffered: List[BinaryLogEntry])

  final case class RefCountedMapping(map: MappedByteBuffer) {
    val buf = Unpooled.wrappedBuffer(map)

    def clean() = {
      buf.release()
      RefCountedMapping.queue.add(this)
    }
  }

  object RefCountedMapping {
    private[RefCountedMapping] val queue = new LinkedBlockingQueue[RefCountedMapping]

    private val cleaner = new Thread("BinaryLogFile Async Cleaner") {
      override def run() = {
        while (true) {
          val ms = List.newBuilder[RefCountedMapping]

          ms += queue.take // block
          while (queue.peek ne null) {
            ms += queue.poll // drain
          }

          ms.result() foreach { m =>
            if (m.buf.refCnt == 0) m.map.clean() else queue.add(m)
          }

          Thread.sleep(1000)
        }
      }
    }

    cleaner.setDaemon(true)
    cleaner.start()
  }

  object FileName {
    final val Regex = """(.+)\.binlog.(\d+)""".r

    def apply(name: String, ordinal: Long) = s"$name.binlog.$ordinal"

    def unapply(path: Path): Option[(String, Long)] =
      path.getFileName.toString match {
        case Regex(name, ordinal) => Some((name, ordinal.toLong))
        case _                    => None
      }
  }


  def writeHeader(fileChannel: FileChannel,
                  prevIdx: TX,
                  prevChk: Checksum
                 ): Unit =
    ByteBufAllocator.DEFAULT.buffer releaseAfter { b =>
      b.writeBytes(MagicPrefix)
        .writeInt(Version)
        .writeBytes(prevChk.toBytes)
        .writeLong(prevIdx.toLong)
      val written = fileChannel.write(b.nioBuffer)
      assert(written == HeaderBytes)
    }

  def createPath(dir: Path, name: String, ordinal: Long) = {
    dir / FileName(name, ordinal)
  }

  def precreate(dir: Path, name: String, ordinal: Long): FileChannel =
    precreate(createPath(dir, name, ordinal))

  def precreate(path: Path): FileChannel = {
    val c = FileChannel.open(path, READ, WRITE, CREATE)
    Using.Manager { use =>
      c.truncate(0)
      c.force(true)

      // to ensure the file will be visible from the parent directory we need to sync said parent #sad #posixGuarantees
      val parent = use(FileChannel.open(path.getParent, READ))
      parent.force(true)
    } match {
      case Failure(exception) =>
        c.close()
        throw exception
      case Success(()) =>
        c
    }
  }

  def create(
    dir: Path,
    name: String,
    txnLogBackupPath: Option[Path],
    ordinal: Long = 0,
    prevIdx: TX = TX.MinValue,
    prevChk: Checksum = Checksum.Null,
    sizeHint: Long = 0): BinaryLogFile =
    create(createPath(dir, name, ordinal),
           prevIdx,
           prevChk,
           sizeHint,
           txnLogBackupPath)

  def create(
    path: Path,
    prevIdx: TX,
    prevChk: Checksum,
    sizeHint: Long,
    txnLogBackupPath: Option[Path]): BinaryLogFile = {
    val c = precreate(path)
    writeHeader(c, prevIdx, prevChk)
    c.force(true)

    open(path, txnLogBackupPath, sizeHint, fileChannel = Some(c)).get
  }

  def isLog(path: Path, name: String = null): Boolean =
    path match {
      case FileName(prefix, _) => (name eq null) || name == prefix
      case _                   => false
    }

  def openPrecreated(dir: Path,
                     name: String,
                     ordinal: Long,
                     fileChannel: FileChannel,
                     prevIdx: TX,
                     prevChk: Checksum,
                     sizeHint: Long,
                     txnLogBackupPath: Option[Path],
                    ): BinaryLogFile = {
    writeHeader(fileChannel, prevIdx, prevChk)
    fileChannel.force(false)
    val blfPath = createPath(dir, name, ordinal)
    open(blfPath, txnLogBackupPath, sizeHint, fileChannel = Some(fileChannel)).get
  }

  def open(
    dir: Path,
    name: String,
    ordinal: Long,
    txnLogBackupPath: Option[Path]): Option[BinaryLogFile] =
    open(dir / FileName(name, ordinal), txnLogBackupPath)

  def open(
    dir: Path,
    name: String,
    ordinal: Long,
    txnLogBackupPath: Option[Path],
    sizeHint: Long): Option[BinaryLogFile] =
    open(createPath(dir, name, ordinal), txnLogBackupPath, sizeHint)

  def open(
    path: Path,
    txnLogBackupPath: Option[Path],
    sizeHint: Long = 0,
    verify: Boolean = true,
    // transfer ownership of this fileChannel
    fileChannel: Option[FileChannel] = None
          ): Option[BinaryLogFile] = {

    path match {
      case FileName(_, ord) =>
        try {
          val fc = fileChannel.fold(FileChannel.open(path, READ, WRITE)) { fileChannel =>
            fileChannel.position(0)
            fileChannel
          }
          val log = new BinaryLogFile(path,
                                      ord,
                                      fc,
                                      txnLogBackupPath)
          if (verify) {
            log.runVerify()
          }
          log.sizeHint(sizeHint)
          Some(log)
        } catch {
          case _: NoSuchFileException =>
            fileChannel.foreach { _.close() }
            None
          case e: Throwable =>
            fileChannel.foreach { _.close() }
            throw e
        }

      case _ =>
        fileChannel.foreach { _.close() }
        None
    }
  }
}

class BinaryLogFile(
  val path: Path,
  val ordinal: Long,
  ch: FileChannel,
  txnLogBackupPath: Option[Path]) { self =>

  private val log = getLogger

  private[this] val flushLock = new Object
  private[this] val info = new AtomicReference[Info]
  private[this] val mapping = new AtomicReference(
    RefCountedMapping(ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size)))

  val (fileVersion, prevIdx, prevChk) =
    ByteBufAllocator.DEFAULT.buffer(HeaderBytes) releaseAfter { b =>
      val bytesRead = b.writeBytes(ch, HeaderBytes)
      assert(bytesRead == HeaderBytes)

      val magic = b.readSlice(4)
      val vers = b.readInt
      val prevChk = Checksum.readBytes(b)
      val prevIdx = TX(b.readLong)

      if (magic != Unpooled.wrappedBuffer(MagicPrefix)) {
        throw LogUnknownFormatException(path)
      }

      if (vers != Version) {
        throw LogUnknownVersionException(path)
      }

      info.set(Info(prevIdx, prevChk, HeaderBytes, ch.size, Nil, Nil))

      (vers, prevIdx, prevChk)
    }

  val index = new TailIndex(BinaryLogFile.StartPos)

  override def toString =
    s"BinaryLogFile($path, $prevIdx -> $prevChk, $lastIdx -> $lastChk)"

  override def equals(other: Any) = other match {
    case o: BinaryLogFile =>
      (prevIdx == o.prevIdx) && (prevChk == o.prevChk) &&
        (lastIdx == o.lastIdx) && (lastChk == o.lastChk)
    case _ => false
  }

  def size = info.get.size

  def lastIdx = info.get.lastIdx

  def lastChk = info.get.lastChk

  def isEmpty = prevIdx == lastIdx

  /**
    * Closing the log file by
    * 1. Flushing all data to disk
    * 2. Closing the file/channel
    * 3. Cleaning the channel which release the buffers and send it to the
    *    "BinaryLogFile Async Cleaner" thread for cleaning
    *
    * @return boolean  is the Log File successfully added to the cleaner queue
    */
  def close(): Boolean = {
    try {
      flush()
    } catch {
      case NonFatal(_) => ()
    }
    ch.close()
    mapping.get.clean()
  }

  /**
    * This function removes an individual log file from active duty. It
    * does this in one of two ways.
    * 1. If the we are backing up transaction logs then the txnLogBackupPath
    *    must be set to a directory and we move the log file to this directory.
    *    At this point it is the users responsibility to store the log file
    *    offsite and physically remove the log file.
    * 2. If the core config file is set to NOT backup log files
    *    then this function will delete the file.
    *
    * @return  Unit
    */
  def delete(): Unit = {
    try {
      close()
    } catch {
      case NonFatal(_) => ()
    }

    try {
      txnLogBackupPath match {
        case Some(backupPath) =>
          val dst: Path = backupPath.resolve(path.getFileName)
          Files.move(path, dst)
        case None => Files.delete(path)
      }
    } catch {
      case ex: Throwable =>
        log.error(s"ERROR: Unable to backup transaction log $path", ex)
    }
  }

  /**
    * This function places log entries into the raft log.
    *   Not thread safe (callers must ensure the threads safety)
    *
    * Create a temporary list with all the new entries.  For each entry we need to
    *   1. Caculate our id to be one more than previous id
    *   2. Set the previous entries id in our entry
    *   3. Calculate the checksum
    *   4. Append to the temporary list
    *   5. Advance our position past this current entry
    *  After the temporary list has been created we append it to the end
    *  of the master list with an atomic operation.
    *
    * @param bufs   One or more items to add to the raft log
    * @return TX    The index of the last item pasted in to be added
    */
  def add(bufs: Iterable[ByteBuf]): TX =
    try {
      val i0 = info.get

      var idx = i0.lastIdx
      var chk = i0.lastChk
      var pos = i0.size

      val es = List.newBuilder[BinaryLogEntry]

      bufs foreach { buf =>
        idx += 1
        chk = chk.append(Unpooled.copyLong(idx.toLong), buf)

        es += BinaryLogEntry(idx, chk, buf, pos)

        pos += onDiskSize(buf)
      }

      info.updateAndGet { i =>
        i.copy(lastIdx = idx,
               lastChk = chk,
               size = pos,
               buffered = i.buffered ++ es.result())
      }

      idx
    } catch {
      case NonFatal(e) =>
        // Any exception in add should close the file
        try close()
        catch { case NonFatal(_) => () }
        throw e
    }

  // use a lock to ensure only 1 thread is flushing at a time. This is
  // usually a dedicated flush thread, but in discard() we need to
  // flush the log, so need to cooordinate there.
  def flush() =
    flushLock.synchronized {
      val i0 = info.updateAndGet { i =>
        i.copy(flushing = i.buffered, buffered = Nil)
      }

      val es = i0.flushing
      val hbuf = ByteBufAllocator.DEFAULT.buffer(EntryHeaderBytes)

      es foreach { e =>
        val size = e.get.readableBytes + EntryMetaBytes
        assert(size > e.get.readableBytes) // don't overflow...

        hbuf.writeInt(size).writeBytes(e.chk.toBytes).writeLong(e.idx.toLong)
        hbuf.readAllBytes(ch)
        hbuf.clear()

        e.get.slice.readAllBytes(ch)
      }

      ch.force(false)
      sizeHint(ch.position)
      info.updateAndGet { _.copy(flushedSize = ch.position, flushing = Nil) }

      hbuf.release()
      es foreach { e =>
        index.add(e.idx.toLong, e.position)
        e.get.release()
      }
    }

  def sizeHint(hintedSize: Long) =
    if (hintedSize > mapping.get.map.remaining) {
      // Store/reset the channel position; Windows set the channel
      // position to `hintedSize` after the re-map, not the current
      // position.
      val pos = ch.position
      val prev = mapping.getAndSet(
        RefCountedMapping(ch.map(FileChannel.MapMode.READ_ONLY, 0, hintedSize)))
      prev.clean()
      ch.position(pos)
    }

  // FIXME: This would be a lot more efficient if BinaryLogFile
  // indexes were offsets...
  def discard(afterIdx: TX) =
    if (afterIdx < prevIdx) {
      throw new IllegalStateException("Cannot discard entries before prevIdx")
    } else if (afterIdx < lastIdx) {
      flush()

      val (idx, chk, pos): (TX, Checksum, Long) = if (afterIdx == prevIdx) {
        (prevIdx, prevChk, BinaryLogFile.StartPos)
      } else {
        entries(afterIdx - 1) releaseAfter { es =>
          val last = es.next()
          (last.idx, last.chk, last.position + onDiskSize(last.get))
        }
      }

      index.dropAfter(afterIdx.toLong)
      info.set(Info(idx, chk, pos, pos, Nil, Nil))

      val bytes = ch.position - pos
      if (bytes > 0) {
        ByteBufAllocator.DEFAULT.buffer(bytes.toInt) releaseAfter { b =>
          b.setZero(b.readerIndex, bytes.toInt)
          b.readAllBytes(ch)
        }
      }

      flush()
      ch.position(pos)
    }

  def entries(afterIdx: TX): EntriesIterator[BinaryLogEntry] =
    if (lastIdx <= afterIdx) {
      EntriesIterator.empty
    } else {
      entriesFromPos(index(afterIdx.toLong)) dropWhile { _.idx <= afterIdx }
    }

  private[log] def entriesFromPos(from: Long): EntriesIterator[BinaryLogEntry] =
    new AbstractEntriesIterator[BinaryLogEntry] {

      @annotation.tailrec
      private def getInfoAndMappingBuf: (Info, ByteBuf) = {
        val i = info.get
        val m = mapping.get.buf
        var retained = 0

        try {
          m.retain()
          retained += 1

          i.flushing foreach { e =>
            e.get.retain()
            retained += 1
          }

          i.buffered foreach { e =>
            e.get.retain()
            retained += 1
          }

          if (i.flushedSize < from) {
            throw new IllegalStateException(
              s"flushed position (${i.flushedSize}) lags requested index position ($from)")
          }
          (i, m.slice(from.toInt, (i.flushedSize - from).toInt))
        } catch {
          case _: IllegalReferenceCountException =>
            if (retained > 0) {
              m.release()
              retained -= 1
            }

            val fs = i.flushing.iterator

            while (retained > 0 && fs.hasNext) {
              fs.next().get.release()
              retained -= 1
            }

            val bs = i.buffered.iterator

            while (retained > 0 && bs.hasNext) {
              bs.next().get.release()
              retained -= 1
            }

            getInfoAndMappingBuf
        }
      }

      private val (inf, buf) = getInfoAndMappingBuf
      private val iter1 = inf.flushing.iterator
      private val iter2 = inf.buffered.iterator

      def hasNext = buf.isReadable || iter1.hasNext || iter2.hasNext

      def next() =
        if (buf.isReadable) {
          val position = from + buf.readerIndex
          val size = buf.readInt

          // If size == 0, then it means that we are in uninitialized file
          // data. Only possible in the call from runVerify() below.
          if (size <= EntryMetaBytes) {
            if (size == 0) {
              throw LogEOFException(path)
            } else {
              throw LogEntrySizeException(path, size)
            }
          } else if (buf.readableBytes < size) {
            throw LogEOFException(path)
          }

          val chk = Checksum.readBytes(buf)
          val idx = TX(buf.readLong)
          val data = buf.readSlice(size - EntryMetaBytes)

          BinaryLogEntry(idx, chk, data, position)
        } else if (iter1.hasNext) {
          val e = iter1.next()
          e.copy(get = e.get.slice)
        } else {
          val e = iter2.next()
          e.copy(get = e.get.slice)
        }

      protected def deallocate() = {
        buf.release()
        inf.flushing foreach { _.get.release() }
        inf.buffered foreach { _.get.release() }
      }

      def touch(hint: Any) = this
    }

  private def onDiskSize(buf: ByteBuf) = EntryHeaderBytes + buf.readableBytes

  // VerifiedLog

  def runVerify() = {
    try {
      val es = entriesFromPos(BinaryLogFile.StartPos)

      es releaseAfter {
        _ foreach { entry =>
          val inf = info.get

          if (entry.idx != inf.lastIdx + 1) {
            throw LogMissingTransactionsException(path,
                                                  inf.lastIdx + 1,
                                                  entry.idx - 1)
          }

          if (entry.chk != (inf.lastChk.append(Unpooled.copyLong(entry.idx.toLong), entry.get))) {
            throw LogChecksumException(path, entry.idx)
          }

          // build index
          index.add(entry.idx.toLong, inf.size)

          info.set(
            Info(entry.idx,
                 entry.chk,
                 inf.size + onDiskSize(entry.get),
                 ch.size,
                 Nil,
                 Nil))
        }
      }
    } catch {
      case _: LogEOFException => ()
    }

    if (info.get.size != ch.size) {
      log.debug(s"Truncating log entries after $lastIdx in $path")
      // It would be convenient to simply call `truncate()`, but
      // Windows cannot truncate a file while a mapping is open, so we
      // must release/remap.
      mapping.get.clean()
      ch.truncate(info.get.size)
      ch.force(false)
      info.set(info.get.copy(flushedSize = info.get.size))
      mapping.set(
        RefCountedMapping(ch.map(FileChannel.MapMode.READ_ONLY, 0, info.get.size)))
    }

    ch.position(info.get.size)
  }
}
