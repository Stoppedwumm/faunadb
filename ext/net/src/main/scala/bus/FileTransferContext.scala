package fauna.net.bus

import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.RateLimiter
import fauna.stats.StatsRecorder
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, PooledByteBufAllocator, Unpooled }
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file._
import java.util.concurrent.TimeoutException
import java.security.MessageDigest
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * FileTransferContext manages creation of file transfer sessions
  * between two nodes over a bus. General flow is this:
  *
  * sending node:
  *
  *     val handle = ctx.prepare(bus, ...)
  *     // send handle.manifest to dest
  *
  * dest node:
  *
  *     // receive manifest
  *     val fut = ctx.receive(bus, manifest) { tmpdir =>
  *       // copy/move files from tmpdir to permanent destinations
  *     }
  *
  *     // `fut` completes when files are processed, or results in a
  *     // timeout, or other exception.
  */

final case class FileTransferManifest(
  handler: HandlerID,
  fileCount: Int)

final class FileTransferHandle(completion: Promise[Boolean], val manifest: FileTransferManifest) {
  def future = completion.future
  def close(success: Boolean = true) = completion.trySuccess(success)
}

object FileTransferManifest {
  implicit val CBORCodec = CBOR.TupleCodec[FileTransferManifest]
}

sealed trait FileTransferException extends Exception

case class FileTransferTimeoutException(manifest: FileTransferManifest)
    extends TimeoutException(
      s"Timed out waiting for transfer from ${manifest.handler}")
    with FileTransferException

case class FileTransferClosedException(manifest: FileTransferManifest)
    extends Exception(s"Handler closed for transfer from ${manifest.handler}")
    with FileTransferException

object FileTransferContext {
  def apply(
    tmpDirectory: Path,
    transferExpiry: FiniteDuration = 30.seconds,
    chunkSize: Int = 32768,
    enableDigests: Boolean = true,
    // This file (hopefully) retains/releases buffers correctly, so use the pooled impl.
    alloc: ByteBufAllocator = PooledByteBufAllocator.DEFAULT,
    limiter: RateLimiter = RateLimiter.NoLimit,
    statsRecorder: StatsRecorder = StatsRecorder.Null,
    statsPrefix: String = "FileTransferContext") =
    new FileTransferContext(
      tmpDirectory,
      transferExpiry,
      chunkSize,
      enableDigests,
      alloc,
      limiter,
      statsRecorder,
      statsPrefix)

  val Done = -1

  type DigestType = String
  val DigestLength = 16
  object Digest {
    val MD5= "MD5"
    val Static= "Static"
    val StaticValue: Array[Byte] = Array(0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF,
                                         0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF).map(_.toByte)

    def newDigest(alg: DigestType): Option[MessageDigest] = alg match {
      case "MD5" => Some(MessageDigest.getInstance(alg))
      case "Static" => None
      case alg => throw new IllegalArgumentException(s"Unknown message digest config $alg")
    }

    def reset(md: Option[MessageDigest]) = md.foreach(_.reset())
    def get(md: Option[MessageDigest]) = md.fold(StaticValue)(_.digest())
    def update(md: Option[MessageDigest])(buf: => ByteBuf): Unit =
      md.foreach { md => buf releaseAfter { b => md.update(b.nioBuffer) } }
  }

  final class Stats(prefix: String, recorder: StatsRecorder) {
    val outgoingStarts = s"$prefix.FileTransfer.Outgoing.Starts"
    def incrOutgoingTransferStarts() = recorder.incr(outgoingStarts)
    val outgoingBytes = s"$prefix.FileTransfer.Outgoing.Bytes"
    def incrOutgoingTransferBytes(bytes: Int) = recorder.count(outgoingBytes, bytes)
    val outgoingFiles = s"$prefix.FileTransfer.Outgoing.Files"
    def incrOutgoingTransferFiles() = recorder.incr(outgoingFiles)
    val outgoingCompletes = s"$prefix.FileTransfer.Outgoing.Completes"
    def incrOutgoingTransferCompletes() = recorder.incr(outgoingCompletes)
    val outgoingErrors = s"$prefix.FileTransfer.Outgoing.Errors"
    def incrOutgoingTransferErrors() = recorder.incr(outgoingErrors)
    val incomingStarts = s"$prefix.FileTransfer.Incoming.Starts"
    def incrIncomingTransferStarts() = recorder.incr(incomingStarts)
    val incomingBytes = s"$prefix.FileTransfer.Incoming.Bytes"
    def incrIncomingTransferBytes(bytes: Int) = recorder.count(incomingBytes, bytes)
    val incomingFiles = s"$prefix.FileTransfer.Incoming.Files"
    def incrIncomingTransferFiles() = recorder.incr(incomingFiles)
    val incomingCompletes = s"$prefix.FileTransfer.Incoming.Completes"
    def incrIncomingTransferCompletes() = recorder.incr(incomingCompletes)
    val incomingErrors = s"$prefix.FileTransfer.Incoming.Errors"
    def incrIncomingTransferErrors() = recorder.incr(incomingErrors)
    val transferDelayTime = s"$prefix.FileTransfer.Delay.Time"
    def recordTransferDelayTime(millis: Long) =
      recorder.timing(transferDelayTime, millis)

  }
}

// FIXME: This should maintain stats and inspectable info per
// sesssion, for progress meters, etc.
class FileTransferContext(
  tmpDirectory: Path,
  transferExpiry: FiniteDuration,
  chunkSize: Int,
  // FIXME: this is temporarily unused to maintain manifest message compatibility
  @annotation.unused enableDigests: Boolean,
  alloc: ByteBufAllocator,
  limiter: RateLimiter,
  statsRecorder: StatsRecorder,
  statsPrefix: String
) extends ExceptionLogging {

  import FileTransferContext._
  import StandardOpenOption._

  private val log = getLogger
  private val stats = new Stats(statsPrefix, statsRecorder)

  // FIXME: this has been temporarily hardcoded to maintain manifest message compatibility
  private val digestType = Digest.MD5

  /**
    * Prepares the transfer of a set of byte streams in `sources`. The
    * streams are defined by a unique name, a total size, and a
    * function from (offset, len) to the next buffer in the stream.
    */
  def prepare(bus: MessageBus, sourceMap: Map[String, Chunked], autoclose: Boolean = true)(implicit ec: ExecutionContext) = {
    val sources = sourceMap.toVector
    val completion = Promise[Boolean]()

    val startTimeout = Timer.Global.scheduleTimeout(transferExpiry) {
      completion.trySuccess(false)
    }

    val handler = bus.tempTCPStreamHandler { (from, ch) =>

      startTimeout.cancel()
      ch.idleTimeout(transferExpiry)

      val md = Digest.newDigest(digestType)

      log.info(s"Initiating outgoing transfer session to $from. Sending ${sources.size} files.")
      stats.incrOutgoingTransferStarts()

      def controlLoop(): Future[Boolean] = 
        ch.recv() flatMap {
          _ releaseAfter { buf =>
            try {
              buf.readInt match {
                case FileTransferContext.Done =>
                  log.info(s"Outgoing session to $from complete.")
                  stats.incrOutgoingTransferCompletes()
                  Future.successful(buf.readBoolean)

                case idx if idx >= 0 && idx < sources.size =>
                  val (filename, source) = sources(idx)
                  var offset = 0L
                  val length = source.totalSize

                  // assert digest is reset defensively, even though it should already be.
                  Digest.reset(md)

                  log.info(s"Sending file ${idx+1}/${sources.size} of ${length/1024}KB to $from. ($filename)")
                  stats.incrOutgoingTransferFiles()

                  val filenameBuf = filename.toUTF8Buf
                  val header = ch.alloc.buffer
                    .writeLong(filenameBuf.readableBytes())
                    .writeLong(length)
                    .writeBytes(filenameBuf)

                  stats.incrOutgoingTransferBytes(header.readableBytes)
                  ch.writeAndFlush(header)

                  def sendLoop(sentSinceFlush: Int = 0): Future[Boolean] =
                    if (offset < length) {
                      val chunk = source.nextChunk(alloc, chunkSize)
                      val read = chunk.readableBytes

                      // update the message digest if necessary
                      Digest.update(md) { chunk.retain() }

                      stats.incrOutgoingTransferBytes(read)
                      offset += read
                      if (read + sentSinceFlush >= chunkSize) {
                        ch.sendAndFlush(chunk) flatMap { _ => sendLoop(0) }
                      } else {
                        ch.send(chunk) flatMap { _ => sendLoop(read + sentSinceFlush) }
                      }
                    } else {
                      // We still send a static digest value to provide some
                      // small assurance we didn't mess up the transfer.
                      val digest = Digest.get(md)
                      val buf = Unpooled.wrappedBuffer(digest)
                      assert(buf.readableBytes == DigestLength, "Unexpected digest length!")
                      ch.writeAndFlush(buf)
                      controlLoop()
                    }

                  sendLoop()

                case idx =>
                  ch.write(ch.alloc.buffer.writeInt(-1))
                  log.warn(s"Outgoing session to $from failed: Requested out of bounds file index $idx.")
                  stats.incrOutgoingTransferErrors()
                  FutureFalse
              }
            } catch {
              case _: IndexOutOfBoundsException =>
                log.warn(s"Outgoing session to $from failed: FTC protocol error.")
                stats.incrOutgoingTransferErrors()
                FutureFalse
            }
          }
        }

      val fut = controlLoop() map { success =>
        if (success) {
          log.info(s"Outgoing session to $from complete.")
        } else {
          log.info(s"Outgoing session to $from failed: Recipient indicated failure.")
        }
        success
      } recover {
        case _: TimeoutException =>
          log.info(s"Outgoing session to $from failed: Recipient timed out.")
          false
        case e: IOException if e.getMessage == "Channel closed" =>
          log.info(s"Outgoing session to $from failed: Connection closed before transfer complete.")
          false
      } ensure {
        ch.closeQuietly()
      }

      completion.completeWith(fut)                                                                      

      Future.unit
    } 

    completion.future ensure {
      handler.close()
      if (autoclose) sources foreach { case (_, src) => src.close() }
    }

    new FileTransferHandle(completion, FileTransferManifest(handler.id, sources.size))
  }

  def prepareBuffers(bus: MessageBus, buffers: Map[String, ByteBuf])(implicit ec: ExecutionContext) =
    prepare(bus, buffers map { case (name, buf) =>
      name -> ChunkedByteBuf(buf)
    })

  def prepareChannels(bus: MessageBus, channels: Map[String, FileChannel])(implicit ec: ExecutionContext) =
    prepare(bus, channels map { case (name, c) =>
      name -> ChunkedChannel(c)
    })

  def close(bus: MessageBus, manifest: FileTransferManifest, success: Boolean = true)
           (implicit ec: ExecutionContext): Future[Unit] =
    bus.openTCPStream(manifest.handler, "close file transfer") map { ch =>
      ch.writeAndFlush(ch.alloc.buffer(5).writeInt(FileTransferContext.Done).writeBoolean(success))
      ch.closeQuietly()
    }

  /**
    * Receives the files described by `manifest`, storing them in a
    * temporary directory. After the transfer is complete, `f` will be
    * executed with the path to the directory. After `f` returns, the
    * temporary directory will be deleted.
    */
  def receive[T](bus: MessageBus, manifest: FileTransferManifest)
    (f: Path => Future[T])
    (implicit ec: ExecutionContext): Future[T] = {

    val tmp = Files.createTempDirectory(tmpDirectory, "File-Transfers")
    val total = manifest.fileCount
    val from = manifest.handler.host

    log.info(s"Initiating incoming session from $from. Receiving $total files.")
    stats.incrIncomingTransferStarts()

    val completed = bus.openTCPStream(manifest.handler, s"file transfer from $from") flatMap { ch =>

      ch.idleTimeout(transferExpiry)

      // FIXME: temporarily hardcoded to MD5.
      val md = Digest.newDigest(Digest.MD5)
      var idx = 0

      def recvBuf(f: ByteBuf => Future[T]) = {
        ch.recv() flatMap { buf =>
          val bytes = buf.readableBytes
          stats.incrIncomingTransferBytes(bytes)
          buf releaseAfter { buf =>
            // Use blocking + Thread.sleep() for the limiter to
            // achieve greater precision than the netty HWT. The
            // context switch to blocking adds some overhead (i.e. the
            // time slept is always greater than requested) but it is
            // several orders of magnitude smaller than the HWT tick
            // precision + context switch.
            blocking {
              // Ask forgiveness, not permission.
              val delay = limiter.acquire(bytes)
              stats.recordTransferDelayTime(delay.toMillis)
            }

            f(buf)
          }
        }
      }

      def requestNextFile() = 
        if (idx < total) {
          ch.writeAndFlush(ch.alloc.buffer(4).writeInt(idx))
          // reset digest defensively
          Digest.reset(md)
          recvBuf { readHeader(_) }
        } else {
          val msg = ch.alloc.buffer(5)
            .writeInt(FileTransferContext.Done)
            .writeBoolean(true)
          ch.writeAndFlush(msg)
          finalizeTransfer()
        }

      def readHeader(buf: ByteBuf): Future[T] = {
        val nameLen = buf.readLong().toInt
        val fileLen = buf.readLong()

        readName(buf, ch.alloc.buffer(nameLen, nameLen), fileLen)
      }

      def readName(buf: ByteBuf, nameBuf: ByteBuf, fileLen: Long): Future[T] = {
        buf.readBytes(nameBuf)

        if (nameBuf.isWritable) {
          recvBuf { readName(_, nameBuf, fileLen) }
        } else {
          val name = nameBuf releaseAfter { _.toUTF8String }
          log.info(s"Receiving file ${idx+1}/$total of ${fileLen/1024}KB from $from. ($name)")
          stats.incrIncomingTransferFiles()
          val path = tmp / name
          val file = FileChannel.open(path, WRITE, CREATE_NEW)

          readFileContents(buf, path, file, fileLen)
        }
      }

      def readFileContents(buf: ByteBuf, path: Path, file: FileChannel, remaining: Long): Future[T] = {
        val len = (remaining min buf.readableBytes).toInt

        // update running digest
        Digest.update(md) { buf.duplicate.readBytes(len) }

        val read = buf.readBytes(file, len)
        val remaining0 = remaining - read
        if (remaining0 == 0) {
          file.close()
          validateDigest(buf, ch.alloc.buffer(DigestLength), path, file)
        } else {
          require(
            buf.readableBytes() == 0,
            s"readFileContents: ${buf.readableBytes()} bytes to read")
          recvBuf { readFileContents(_, path, file, remaining0) }
        }
      }

      def validateDigest(buf: ByteBuf, senderDigest: ByteBuf, path: Path, file: FileChannel): Future[T] = {
        val rem = DigestLength - senderDigest.readableBytes
        buf.readBytes(senderDigest, buf.readableBytes min rem)

        if (senderDigest.readableBytes < DigestLength) {
          recvBuf { validateDigest(_, senderDigest, path, file) }
        } else {
          val digest = {
            val arr = Digest.get(md)
            Unpooled.wrappedBuffer(arr)
          }
          assert(digest.readableBytes == DigestLength, "Unexpected digest length!")

          if (digest == senderDigest) {
            idx += 1
          } else {
            val name = path.getFileName
            log.warn(s"Received checksum differs from sender's for file ${idx+1}/$total from $from. Retrying. ($name)")
            Files.delete(path)
          }

          senderDigest.release()
          requestNextFile()
        }
      }

      def finalizeTransfer() = GuardFuture(f(tmp)) 

      requestNextFile() recover {
        case _: TimeoutException =>
          throw FileTransferTimeoutException(manifest)
        case e: IOException if e.getMessage == "Channel closed" =>
          throw FileTransferClosedException(manifest)
      } ensure {
        ch.closeQuietly()
      }
    }

    completed transform { rv =>
      try {
        tmp.deleteRecursively()
      } catch {
        case NonFatal(e) => logException(e)
      }

      if (rv.isSuccess) {
        log.info(s"Inbound session from $from complete.")
        stats.incrIncomingTransferCompletes()
      } else {
        log.info(s"Inbound session from $from failed.")
        stats.incrIncomingTransferErrors()
      }

      rv
    }
  }
}
