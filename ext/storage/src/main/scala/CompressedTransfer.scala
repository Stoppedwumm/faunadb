package fauna.storage

import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.stats.StatsRecorder
import java.io.{ Closeable, DataInputStream, IOException }
import java.lang.{ Long => JLong }
import java.nio.channels.{ Channels, FileChannel }
import java.nio.file.{ Path, Paths }
import java.util.{ List => JList }
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, Unpooled }
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.io.sstable.{
  Component,
  Descriptor,
  SSTableReader,
  SSTableWriter
}
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.streaming.compress._
import org.apache.cassandra.utils.{ ByteBufferUtil, BytesReadTracker, Pair }
import org.apache.cassandra.utils.concurrent.Ref
import scala.util.control.NonFatal

object CompressedTransfer {
  val CRCBytes = 4

  private[this] val logger = getLogger

  case class SSTableFileInfo(
    path: Path,
    compressionInfo: CompressionInfo,
    columnFamily: String,
    estimatedKeys: Long,
    sections: Vector[(Long, Long)],
    level: Long,
    ssTableStartIndex: Long)

  def loadFileInfo(
    path: Path,
    file: FileChannel,
    stats: StatsRecorder): SSTableFileInfo = {
    // this mapping is a bit silly - the header is likely only a few
    // dozen bytes - but codex likes ByteBufs, and we shall oblige.
    val channel =
      file.map(FileChannel.MapMode.READ_ONLY, 0, file.size min Int.MaxValue)

    val (header, startIndex) =
      try {
        val buf = Unpooled.wrappedBuffer(channel)
        val hed = CBOR.decode[TransferHeader](buf)
        stats.count("Transfer.Bytes.Received", buf.readerIndex)
        (hed, buf.readerIndex())
      } finally {
        channel.clean()
      }

    require(
      header.sections.nonEmpty,
      s"meaningless transfer: 0 sections found for ${header.cf}")

    SSTableFileInfo(
      path = path,
      compressionInfo = header.compression.getOrElse(
        throw new IllegalArgumentException("uncompressed sstable!")),
      columnFamily = header.cf,
      estimatedKeys = header.estimatedKeys,
      sections = header.sections,
      level = header.sstableLevel,
      ssTableStartIndex = startIndex
    )
  }

  /** Creates a new compressed SSTable from the contents of `file`, which must
    * have been created using `CompressedTransfer` and must be opened for
    * reading. It is the caller's responsibility to close the file after this
    * method returns.
    *
    * NOTE: The provided fileName is used for diagnostic output only.
    */
  def load(
    keyspaceName: String,
    fileInfo: SSTableFileInfo,
    file: FileChannel,
    stats: StatsRecorder): SSTableWriter = {
    stats.incr("Transfer.Files.Received")

    // the file that was transferred includes the TransferHeader, when we are loading
    // the sstable we want to start after the header, where the sstable begins
    file.position(fileInfo.ssTableStartIndex)

    val fileName = fileInfo.path.getFileName()

    logger.info(s"Loading transfered file $fileName for level ${fileInfo.level}...")

    val compressionInfo = fileInfo.compressionInfo

    // XXX: do we need to support multiple versions?
    val version = Descriptor.Version.CURRENT

    val ks = Keyspace.open(keyspaceName)
    val cfs = ks.getColumnFamilyStore(fileInfo.columnFamily)

    if (cfs eq null) {
      throw new IllegalArgumentException(
        s"unknown column family ${fileInfo.columnFamily}")
    }

    val size = compressionInfo.chunks.foldLeft(0L) { (acc, chunk) =>
      acc + chunk.length + CRCBytes
    }

    val cis = new CompressedInputStream(
      Channels.newInputStream(file),
      compressionInfo,
      version.hasPostCompressionAdlerChecksums)
    val in = new BytesReadTracker(new DataInputStream(cis))

    val dir = cfs.directories.getWriteableLocation(size)

    if (dir eq null) {
      throw new IOException(s"Insufficient disk space to store $size bytes")
    }

    val desc = Descriptor.fromFilename(
      cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(dir)))

    // We set repairedAt (the final parameter) here to 0 because we make no use of
    // this field so its
    // value is irrelevant. The repairedAt value is used to coordinate C* repair
    // across hosts and
    // we do not use this functionality.
    val writer =
      new SSTableWriter(desc.filenameFor(Component.DATA), fileInfo.estimatedKeys, 0)
    var loaded = 0L
    try {
      fileInfo.sections foreach { case (offset, end) =>
          val length = end - offset

          // skip to beginning of section inside chunk
          cis.position(offset)
          in.reset(0)

          while (in.getBytesRead() < length) {
            val key = StorageService.getPartitioner.decorateKey(
              ByteBufferUtil.readWithShortLength(in))
            writer.appendFromStream(key, cfs.metadata, in, version)
          }


          loaded += in.getBytesRead
          logger.debug(s"Loaded $loaded/$size from $fileName.")
          stats.count("Transfer.Bytes.Received", in.getBytesRead.toInt)
      }
    } catch {
      case NonFatal(ex) =>
        logger.warn(s"Failed to load transfer $fileName.")
        stats.incr("Transfer.Load.Failure")
        try {
          writer.abort()
        } catch {
          case NonFatal(ex) => ex.addSuppressed(ex)
        }
        throw ex
    }

    logger.info(s"Completed loading transfer $fileName.")
    stats.incr("Transfer.Files.Loaded")
    writer
  }
}

/**
  * Provides for the transfer of sections (offset/end) of a compressed
  * SSTable through FileTransferContext.
  */
final class CompressedTransfer(
  sstable: Ref[SSTableReader],
  estimatedKeys: Long,
  sections: JList[Pair[JLong, JLong]],
  stats: StatsRecorder)
    extends Chunked
    with Closeable {

  require(sstable.get.compression, "uncompressed sstable!")

  stats.incr("Transfer.Files.Open")

  import CompressedTransfer._

  def filename: String =
    Paths.get(sstable.get.descriptor.baseFilename).getFileName.toString

  // Contains compression format metadata, as well as a list of compressed
  // chunks (original offset and length) which contain the sections to be
  // transferred.
  // 
  // TODO: On the receiving side, only the length field of each chunk is
  // used. (See CompressedInputStream.java[1]). We could save some bytes here
  // but it probably is not worth it.
  //
  // 1: https://github.com/fauna/cassandra/blob/master/src/java/org/apache/cassandra/streaming/compress/CompressedInputStream.java#L168-L195
  private[this] val info = {
    val meta = sstable.get.getCompressionMetadata
    val chunks = meta.getChunksForSections(sections)
    new CompressionInfo(chunks, meta.parameters)
  }

  // The header contains logical sstable metadata (file sections (pairs of
  // locations/tokens), estimated keys, etc., as well as the physical
  // compression info of the file to be transferred.
  private[this] val header = {
    val b = Vector.newBuilder[(Long, Long)]
    sections forEach { pair =>
      b += pair.left.toLong -> pair.right.toLong
    }

    val cf = sstable.get.metadata.cfName
    val level = sstable.get.getSSTableLevel()

    val hed = TransferHeader(cf, estimatedKeys, b.result(), level, Some(info))
    CBOR.encode(hed)
  }

  val totalSize: Long = {
    val data = info.chunks.foldLeft(0L) { (acc, chunk) =>
      acc + chunk.length + CRCBytes
    }

    header.readableBytes + data
  }

  private[this] val file = sstable.get.openDataReader
  private[this] val channel = file.getChannel
  private[this] var fileRemaining = totalSize
  private[this] var ssChunks = info.chunks.iterator
  private[this] var chunkPos = 0L
  private[this] var chunkRemaining = 0L

  private[this] var closed = false

  def canReset(): Boolean = true

  def reset(): Unit = synchronized {
    if (!closed) {
      fileRemaining = totalSize
      ssChunks = info.chunks.iterator
      chunkPos = 0L
      chunkRemaining = 0L
      header.readerIndex(0)
    } else {
      throw new IllegalStateException("Cannot reset closed transfer.")
    }
  }

  // `nextChunk` is allowed to return fewer bytes than the transfer chunk size,
  // which lets us simplify the code here: We don't have to bother creating a
  // ByteBuf containing the end of the header and the start of the first
  // compressed chunk.
  def nextChunk(alloc: ByteBufAllocator, chunkSize: Int): ByteBuf = {
    assert(chunkSize > 0)
    assert(fileRemaining > 0, "FileTransferContext requested too many chunks")
    val buf = {
      val l = (chunkSize.toLong min fileRemaining).toInt
      alloc.buffer(l, l)
    }

    while (buf.isWritable) {
      if (header.isReadable) {
        val len = (buf.writableBytes.toLong min header.readableBytes).toInt
        buf.writeBytes(header, len)
        fileRemaining -= len
      } else {
        if (chunkRemaining == 0) {
          val c = ssChunks.next()
          chunkPos = c.offset
          chunkRemaining = c.length + CRCBytes
        }

        val len = (buf.writableBytes.toLong min chunkRemaining).toInt
        buf.writeBytes(channel, chunkPos, len)
        chunkPos += len
        chunkRemaining -= len
        fileRemaining -= len
      }
    }

    buf
  }

  /** A transfer may be closed multiple times, e.g. in a race. Only the
    * first close() will succeed in releasing the transfer's
    * resources. */
  def close() = synchronized {
    if (!closed) {
      closed = true
      stats.decr("Transfer.Files.Open")
      FileUtils.closeQuietly(file)
      sstable.release()
      header.release()
    }
  }
}
