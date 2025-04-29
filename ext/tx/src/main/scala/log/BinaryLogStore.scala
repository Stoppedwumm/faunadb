package fauna.tx.log

import fauna.codex.cbor.CBOR
import fauna.exec._
import fauna.lang.syntax._
import fauna.lang.AtomicFile
import fauna.tx.log.BinaryLogStore.Constants._
import io.netty.buffer._
import java.nio.channels.{ ClosedChannelException, FileChannel }
import java.nio.file._
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, blocking }
import scala.util.control.NonFatal

case class BinaryLogEntry(
  idx: TX,
  chk: Checksum,
  get: ByteBuf,
  position: Long) extends LogEntry[TX, ByteBuf] {

  def isEmpty = false
  def toOption = Some(get)

  def onDiskSize = BinaryLogFile.Constants.EntryHeaderBytes + get.readableBytes
}

object BinaryLogStore {
  val HeaderSize = BinaryLogFile.Constants.HeaderBytes
  val DefaultFileSize = 32 * 1024 * 1024 // 32MB

  object Constants {
    final val StateBytes = 8 * 4

    final val PrevOffset = 0
    final val CommittedOffset = 8
    final val FirstOffset = 16
    final val LastOffset = 24

    final val ManifestSuffix = ".binlog.manifest"
  }

  def open[T](
    dir: Path,
    txnLogBackupPath: Option[Path],
    name: String = "BinaryLog",
    fileSize: Int = DefaultFileSize,
    moveInvalid: Boolean = false,
    verifyFiles: Boolean = true)(implicit codec: CBOR.Codec[T]): BinaryLogStore[T] = {

    Files.createDirectories(dir)

    if (codec == CBOR.ByteBufCodec) {
      new RawBinaryLogStore(dir, name, fileSize + HeaderSize, moveInvalid, verifyFiles).asInstanceOf[BinaryLogStore[T]]
    } else {
      new CBORBinaryLogStore(dir,
                             name,
                             fileSize + HeaderSize,
                             moveInvalid,
                             verifyFiles,
                             txnLogBackupPath)
    }
  }
}

class RawBinaryLogStore(
  dir: Path,
  name: String,
  maxFileSize: Int,
  moveInvalid: Boolean,
  verifyFiles: Boolean)
    extends BinaryLogStore[ByteBuf](dir, name, maxFileSize, moveInvalid, verifyFiles, None) {

  type E = BinaryLogEntry

  protected def encode(buf: ByteBuf) = buf

  protected def decode(e: BinaryLogEntry) = e
}

class CBORBinaryLogStore[T: CBOR.Codec](
  dir: Path,
  name: String,
  maxFileSize: Int,
  moveInvalid: Boolean,
  verifyFiles: Boolean,
  txnLogBackupPath: Option[Path])
    extends BinaryLogStore[T](dir,
                              name,
                              maxFileSize,
                              moveInvalid,
                              verifyFiles,
                              txnLogBackupPath) {

  type E = LogEntry[TX, T]

  protected def encode(value: T) = CBOR.encode(ByteBufAllocator.DEFAULT.ioBuffer, value)

  protected def decode(e: BinaryLogEntry) = {
    val buf = e.get
    buf.markReaderIndex()

    try {
      LogEntry(e.idx, CBOR.decode[T](buf))
    } catch {
      case e: NoSuchElementException =>
        buf.resetReaderIndex
        throw new NoSuchElementException(s"buf: ${buf.toHexString}").initCause(e)
    }
  }
}

abstract class BinaryLogStore[T](
  val dir: Path,
  val name: String,
  val maxFileSize: Int,
  val moveInvalid: Boolean,
  val verifyFiles: Boolean,
  val txnLogBackupPath: Option[Path])
    extends LogStore[TX, T] {

  private[this] val logger = getLogger

  private[this] val manifest = new Manifest(dir / name + ManifestSuffix)

  protected def encode(value: T): ByteBuf

  protected def decode(entry: BinaryLogEntry): E

  @volatile private[this] var files = Vector.empty[BinaryLogFile]

  @volatile private[this] var _flushedIdx = TX.MinValue

  private[this] val committedIdxState = new IdxWaitersMap[TX](manifest.committedIdx)

  try {
    // cleanup

    dir.entries foreach { path =>
      val rng = manifest.fileRange
      def fileInRange(i: Long): Boolean = {
        // in range, or just 1 outside it because we pre-create the next log file
        (rng contains i) || (rng.last + 1 == i)
      }
      path match {
        case BinaryLogFile.FileName(n, i) if (n == name) && !fileInRange(i) =>
          val ex = LogInvalidFileException(dir, n, rng.head, rng.last, i)
          if (moveInvalid) {
            path move (path + ".invalid")
            logger.warn(ex.getMessage)
          } else {
            throw ex
          }
        case _ => ()
      }
    }

    val fs = manifest.fileRange map { i =>
      BinaryLogFile.open(dir, name, i, txnLogBackupPath, maxFileSize) getOrElse {
        throw LogMissingFileException(dir, name, i)
      }
    }

    if (fs.isEmpty) {
      setFiles(Vector(newFile(0, TX.MinValue, Checksum.Null)))
    } else {
      files = fs.toVector
      precreateNextLogFile()
    }

    files.sliding(2) foreach {
      case Seq(a, b) if (a.lastIdx != b.prevIdx) || (a.lastChk != b.prevChk) =>
        throw LogMissingTransactionsException(a.path, a.lastIdx + 1, b.prevIdx)
      case _ => ()
    }

    flush()
  } catch {
    case NonFatal(e) =>
      try close() finally throw e
  }

  // closeable

  @volatile private[this] var _closed: Boolean = false
  def isClosed = _closed

  def close() = synchronized {
    if (!isClosed) {
      _closed = true
      committedIdxState.shutdown(new LogClosedException)
      manifest.close()
      files.foreach { _.close() }
    }
  }

  // equals

  override def equals(other: Any) = other match {
    case o: BinaryLogStore[_] =>
      (prevIdx == o.prevIdx) && (prevChk == o.prevChk) &&
      (lastIdx == o.lastIdx) && (lastChk == o.lastChk)
    case _ => false
  }

  // metadata

  def metadata: Option[ByteBuf] = manifest.meta

  def metadata(bytes: ByteBuf) =
    synchronized {
      manifest.setState(meta = bytes)
    }

  // LogLike

  def prevIdx = manifest.prevIdx

  def lastIdx = prevIdx max manifest.committedIdx

  def entries(afterIdx: TX): EntriesIterator[E] =
    entriesTo(afterIdx, lastIdx)

  def poll(afterIdx: TX, within: Duration) = {
    implicit val ec = ImmediateExecutionContext
    committedIdxState.get(afterIdx + 1, within) recover {
      case _: LogClosedException => false
    }
  }

  def subscribe(afterIdx: TX, idle: Duration)(f: Log.Sink[TX, E])(implicit ec: ExecutionContext): Future[Unit] = {
    def subscribe0(afterIdx: TX, fileOrd: Long, filePos: Long): Future[Unit] =
      poll(afterIdx, idle) flatMap {
        case true =>
          val lst = lastIdx // limit to committed entries
          iterFromPos(fileOrd, filePos, lst) match {
            case Some(iter) =>
              subscribe1(afterIdx, lst, iter)
            case None =>
              subscribe(afterIdx, idle)(f)
          }

        case false =>
          if (isClosed) {
            f(Log.Closed) map { _ => () }
          } else {
            f(Log.Idle(afterIdx)) flatMap { if (_) subscribe0(afterIdx, fileOrd, filePos) else Future.unit }
          }
      }

    def subscribe1(afterIdx: TX, toIdx: TX, entries: AggIter): Future[Unit] = {
      val prev = prevIdx

      if (prev > afterIdx) {
        entries.release()
        f(Log.Reinit(prev)) flatMap { if (_) subscribe(prev, idle)(f) else Future.unit }
      } else {
        f(Log.Entries(afterIdx, entries)) transformWith { res =>
          entries releaseAfter { es =>
            if (res.get) subscribe0(toIdx, es.nextFileOrd, es.nextFilePos) else Future.unit
          }
        }
      }
    }

    poll(afterIdx, idle) flatMap {
      case true =>
        val lst = lastIdx // limit to committed entries
        subscribe1(afterIdx, lst, iterAfterIdx(afterIdx, lst))

      case false =>
        if (isClosed) {
          f(Log.Closed) map { _ => () }
        } else {
          f(Log.Idle(afterIdx)) flatMap { if (_) subscribe(afterIdx, idle)(f) else Future.unit }
        }
    }
  }

  // LogStore

  private def currFile = files.last

  def uncommittedLastIdx = currFile.lastIdx

  def flushedLastIdx = _flushedIdx

  def uncommittedEntries(afterIdx: TX): EntriesIterator[E] =
    entriesTo(afterIdx, uncommittedLastIdx)

  @tailrec
  final def add(values: Iterable[T]): TX = {
    val waitForAndRetry: Option[Future[_]] =
      synchronized {
        if (currFile.size > maxFileSize) {
          nextFileF.value match {
            case Some(value) =>
              val (ordinal, fc) = value.get
              addFile(ordinal, fc)
              None
            case None =>
              Some(nextFileF)
          }
        } else {
          None
        }
      }
    waitForAndRetry match {
      case None =>
        // happy case add values to currFile
        currFile.add(values map encode)
      case Some(f) =>
        // wait for the future, which we captured while holding the lock
        Await.result(f, Duration.Inf)
        add(values)
    }
  }

  def flush() = {
    val ((_, fs), idx) = synchronized {
      (files span { _.lastIdx <= _flushedIdx }, uncommittedLastIdx)
    }

    fs foreach { f =>
      try f.flush() catch { case _: ClosedChannelException => () }
    }

    synchronized {
      _flushedIdx = idx
      _flushedIdx
    }
  }

  def updateCommittedIdx(idx: TX, metaOpt: Option[ByteBuf]): Unit =
    if (flushedLastIdx < idx) {
      metaOpt foreach { _.release }
      throw new IllegalArgumentException("Cannot set committed index past last log index")
    } else if (idx > manifest.committedIdx) {

      synchronized {
        metaOpt match {
          case Some(meta) => manifest.setState(meta = meta, committed = idx)
          case None       => manifest.setCommittedIdx(idx)
        }
      }

      committedIdxState.update(idx)
    } else {
      metaOpt foreach { _.release }
    }

  def truncate(toIdx: TX, metaOpt: Option[ByteBuf]) = {
    if (toIdx > lastIdx) {
      metaOpt foreach { _.release }
      throw new IllegalArgumentException(s"$toIdx is greater than log's lastIdx $lastIdx")
    }

    val dropped = synchronized {
      val lst = toIdx min (lastIdx - 1)
      val (dead, live) = files span { _.lastIdx <= lst }

      metaOpt match {
        case Some(meta) => manifest.setState(meta = meta, prev = toIdx)
        case None       => manifest.setPrevIdx(toIdx)
      }

      if (dead.nonEmpty) {
        setFiles(live)
      }

      dead
    }

    dropped foreach { _.delete() }
  }

  def discard(afterIdx: TX) = {
    if (afterIdx < lastIdx) {
      throw new IllegalArgumentException("Cannot discard entries before lastIdx")
    }

    val dropped = synchronized {
      files span { _.lastIdx <= afterIdx } match {
        case (keep, Seq(containing, rest @ _*)) =>
          setFiles(keep :+ containing)
          containing.discard(afterIdx)
          flush()
          rest
        case _ => Nil
      }
    }

    dropped foreach { _.delete() }
  }

  def reinit(prevIdx: TX, metaOpt: Option[ByteBuf]) = {
    if (prevIdx > uncommittedLastIdx) {
      val dropped = synchronized {
        val prev = files

        // ensure we're not racing the call to `newFile` below with any background file creation
        Option(nextFileF) foreach { Await.ready(_, 100.millis) }

        metaOpt match {
          case Some(meta) => manifest.setState(meta = meta, prev = prevIdx)
          case None       => manifest.setPrevIdx(prevIdx)
        }

        setFiles(Vector(newFile(manifest.fileRange.last + 1, prevIdx, Checksum.Null)))
        prev
      }

      dropped foreach { _.delete() }
    } else {
      metaOpt foreach { _.release() }
    }

    // It is safe to update the committed index at this point, because
    // at least one other replica out there has committed and then
    // truncated to prevIdx.
    flush()
    updateCommittedIdx(prevIdx)
  }

  // helpers

  private[BinaryLogStore] def prevChk = files.head.prevChk

  private[BinaryLogStore] def lastChk = currFile.lastChk

  private def newFile(ordinal: Long, idx: TX, chk: Checksum) =
    BinaryLogFile.create(dir,
                         name,
                         txnLogBackupPath,
                         ordinal,
                         idx,
                         chk,
                         maxFileSize
    )

  private var nextFileF: Future[(Long, FileChannel)] = _

  private def addFile(precreatedOrdinal: Long, precreatedFileChannel: FileChannel) = {
    logger.info(s"BinaryLogStore adding precreated file $precreatedOrdinal (manifest.fileRange.last=${manifest.fileRange.last})")
    val nextOrdinal = manifest.fileRange.last + 1
    val file = if (precreatedOrdinal == nextOrdinal) {
      BinaryLogFile.openPrecreated(
        dir, name, nextOrdinal,
        precreatedFileChannel,
        uncommittedLastIdx, lastChk,
        maxFileSize, txnLogBackupPath)
    } else {
      BinaryLogFile.open(dir, name, nextOrdinal, txnLogBackupPath).get
    }
    setFiles(files :+ file)
  }

  private def precreateNextLogFile(): Unit = synchronized {
    if (Option(nextFileF) forall { _.isCompleted }) {
      nextFileF = {
        implicit val ec = FaunaExecutionContext.Implicits.global
        Future {
          blocking {
            val ordinal = manifest.fileRange.last + 1
            (ordinal, BinaryLogFile.precreate(dir, name, ordinal))
          }
        }
      }
    }
  }

  private def setFiles(fs: Vector[BinaryLogFile]): Unit = {
    require(fs.nonEmpty)
    logger.info(s"BinaryLogStore setting files to ${fs.head.ordinal}-${fs.last.ordinal}")
    manifest.setFiles(fs.head.ordinal, fs.last.ordinal)
    files = fs
    precreateNextLogFile()
  }

  private def iterFromPos(fileOrd: Long, filePos: Long, toIdx: TX) = {
    val fs = files dropWhile { _.ordinal < fileOrd } // snapshot state
    if (fs.head.ordinal != fileOrd) {
      None
    } else {
      Some(new AggIter(toIdx, fs.head.entriesFromPos(filePos), fileOrd, filePos, fs.tail.iterator))
    }
  }

  private def iterAfterIdx(afterIdx: TX, toIdx: TX) = {
    val fs = files dropWhile { _.lastIdx <= afterIdx } // snapshot state
    new AggIter(toIdx, fs.head.entries(afterIdx), fs.head.ordinal, BinaryLogFile.StartPos, fs.tail.iterator)
  }

  private def entriesTo(afterIdx: TX, toIdx: TX) = {
    val after = prevIdx max afterIdx
    if (after >= toIdx) EntriesIterator.empty else iterAfterIdx(after, toIdx)
  }

  private final class AggIter(
    toIdx: TX,
    start: EntriesIterator[BinaryLogEntry],
    startOrd: Long,
    startPos: Long,
    files: Iterator[BinaryLogFile]) extends AbstractBufferedEntriesIterator[E] {

    private[this] var _fileOrd = startOrd
    private[this] var _filePos = startPos

    private[this] var file = start
    private[this] var toRelease = List(file)
    private[this] var cur: E = null.asInstanceOf[E]

    private[this] var continue = true

    protected def deallocate() = toRelease foreach { _.release() }
    def touch(hint: Any) = this

    private def ff() = while (hasNext) next()

    def nextFileOrd = { ff(); _fileOrd }
    def nextFilePos = { ff(); _filePos }

    def hasNext = {
      if (continue && (cur eq null)) {
        while (file.isEmpty && files.hasNext) {
          val f = files.next()
          file = f.entriesFromPos(BinaryLogFile.StartPos)
          toRelease = file :: toRelease
          _fileOrd = f.ordinal
          _filePos = BinaryLogFile.StartPos
        }

        if (file.isEmpty) {
          file = null
          continue = false
        } else {
          val e = file.next()

          if (e.idx <= toIdx) {
            _filePos = e.position + e.onDiskSize
            cur = decode(e)
          } else {
            continue = false
          }
        }
      }

      continue
    }

    def head = if (hasNext) cur else EntriesIterator.empty.next()

    def next() = {
      val rv = head
      cur = null.asInstanceOf[E]
      rv
    }
  }

  /**
    * The manifest serves two purposes: it describes the collection of
    * BinaryLogFiles currently in use, and stores a snapshot of the
    * state (see `meta()` and `setState()`).
    *
    * Callers may update the manifest one of two ways: by rewriting
    * the entire file using `setState()`, or by updating a single
    * field. In either case, it is only safe for a single writer to
    * update the manifest, but many readers may access it
    * concurrently.
    *
    * Concurrency control MUST be enforced by the caller.
    */
  private final class Manifest(val path: Path) {
    private[this] val file = AtomicFile(path)
    private[this] val longBuf = Unpooled.unreleasableBuffer(Unpooled.buffer(8, 8))

    private[this] var ch: FileChannel = _
    @volatile private[this] var _prev: TX = _
    @volatile private[this] var _committed: TX = _
    @volatile private[this] var _first: Long = _
    @volatile private[this] var _last: Long = _

    file.create {
      writeState(_, TX.MinValue, TX.MinValue, 0, -1, Unpooled.EMPTY_BUFFER)
    }
    load()

    def close() = {
      Option(ch) foreach { _.close() }
      ch = null
    }

    /**
      * The index of the log entry immediately preceding the first
      * file in log.
      */
    def prevIdx = _prev

    /**
      * The index of the most recent committed log entry in the log.
      */
    def committedIdx = _committed

    /**
      * The range of BinaryLogFile ordinals currently in use.
      */
    def fileRange = _first to _last

    /**
      * Returns the extra data, if any, appended to the end of the
      * manifest by `setState()`.
      */
    def meta: Option[ByteBuf] =
      file.read { c =>
        c.position(StateBytes)
        // WARNING: if the state exceeds Int.MaxValue, this will
        // break!
        val readable = (c.size - c.position).toInt

        if (readable > 0) {
          val a = ByteBufAllocator.DEFAULT.buffer(readable)
          a.writeAllBytes(c)

          Some(a)
        } else {
          None
        }
      }

    def setPrevIdx(idx: TX) =
      if (_prev != idx) {
        setLong(PrevOffset, idx.toLong, true)
        _prev = idx
      }

    def setCommittedIdx(idx: TX) =
      if (_committed != idx) {
        setLong(CommittedOffset, idx.toLong, false)
        _committed = idx
      }

    def setFiles(fst: Long, lst: Long) =
      if (_first != fst || _last != lst) {
        setLong(FirstOffset, fst, false)
        setLong(LastOffset, lst, true)
        _first = fst
        _last = lst
      }

    def setState(
      meta: ByteBuf,
      prev: TX = _prev,
      committed: TX = _committed,
      first: Long = _first,
      last: Long = _last): Unit = {
      file.write { writeState(_, prev, committed, first, last, meta) }
      load()
    }

    private def setLong(off: Int, v: Long, sync: Boolean) = {
      longBuf.readerIndex(0)
      longBuf.writerIndex(0)
      longBuf.writeLong(v)
      ch.position(off)
      ch.write(longBuf.nioBuffer)
      if (sync) ch.force(false)
    }

    private def readState(ch: FileChannel) =
      ByteBufAllocator.DEFAULT.buffer releaseAfter { b =>
        b.writeBytes(ch, StateBytes)
        (TX(b.readLong), TX(b.readLong), b.readLong, b.readLong)
      }

    private def writeState(
      ch: FileChannel,
      prev: TX,
      committed: TX,
      first: Long,
      last: Long,
      meta: ByteBuf): ByteBuf =
      ByteBufAllocator.DEFAULT.compositeBuffer(2) releaseAfter { comp =>
        val header = ByteBufAllocator.DEFAULT
          .ioBuffer(StateBytes)
          .writeLong(prev.toLong)
          .writeLong(committed.toLong)
          .writeLong(first)
          .writeLong(last)
        comp.addComponents(true, header, meta)
        comp.readAllBytes(ch)
      }

    private def load() = {
      close()

      val (p, c, f, l) = file.read { readState(_) }

      _prev = p
      _committed = c
      _first = f
      _last = l

      ch = FileChannel.open(file.path, StandardOpenOption.WRITE)
    }
  }
}
