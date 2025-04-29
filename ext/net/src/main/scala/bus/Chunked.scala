package fauna.net.bus

import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import java.io.Closeable
import java.nio.channels.{ FileChannel, ScatteringByteChannel }

/**
  * Represents a chunked stream of bytes. `nextChunk` may be called repeatedly
  * until `totalSize` bytes' worth of ByteBufs have been received by the caller.
  */
trait Chunked extends Closeable {

  /**
    * The total number of bytes in this source.
    */
  def totalSize: Long

  /**
    * Gets the next chunk for transferring. `chunkSize` must be greater than
    * zero. The returned buffer follows netty's retention rules and must be
    * released by the caller.
    */
  def nextChunk(alloc: ByteBufAllocator, chunksize: Int): ByteBuf

  /**
    * Called exactly once to clean up stream resources, after which
    * the results of `nextChunk` are undefined.
    */
  def close(): Unit

  /** Resets this Chunked such that the next call to nextChunk() will
    * yield the first chunk.
    *
    * A reset() after close() is undefined.
    *
    * Not all Chunked may implement reset(), see canReset().
    */
  def reset(): Unit

  /** Returns true if this Chunked supports reset(). */
  def canReset(): Boolean
}

object ChunkedByteBuf {
  def apply(buf: ByteBuf) = new ChunkedByteBuf(buf.asReadOnly)
}
final class ChunkedByteBuf(buf: ByteBuf)
    extends Chunked {

  val totalSize = buf.readableBytes.toLong

  def nextChunk(alloc: ByteBufAllocator, chunkSize: Int): ByteBuf = {
    assert(chunkSize > 0)
    val len = chunkSize min buf.readableBytes
    val rv = buf.retainedSlice(buf.readerIndex, len)
    buf.readerIndex(buf.readerIndex + len)
    rv
  }

  def close(): Unit = buf.release()

  def canReset(): Boolean = true
  def reset(): Unit = buf.resetReaderIndex()
}

object ChunkedChannel {
  def apply(ch: FileChannel) = new ChunkedChannel(ch, ch.size - ch.position)
  def apply(ch: ScatteringByteChannel, totalSize: Long) = new ChunkedChannel(ch, totalSize)
}
final class ChunkedChannel(channel: ScatteringByteChannel, val totalSize: Long)
    extends Chunked {

  private[this] var remaining = totalSize

  override def nextChunk(alloc: ByteBufAllocator, chunkSize: Int): ByteBuf = {
    assert(chunkSize > 0)
    val len = (chunkSize.toLong min remaining).toInt
    val buf = alloc.buffer(len, len)
    buf.writeBytes(channel, len)
    remaining -= len
    buf
  }

  def close(): Unit = channel.close()

  def canReset(): Boolean = false
  def reset(): Unit = throw new UnsupportedOperationException("Cannot reset() a channel.")
}
