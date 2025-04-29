package fauna.util

import java.io._

import net.jpountz.lz4.LZ4Factory
import net.jpountz.xxhash.XXHashFactory
import LZ4FrameHeader._

/**
  * Input stream for LZ4 compressed data
  *
  * Reads data from underlying stream and returns decompressed bytes
  *
  * References:
  *
  * 1. https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/record/KafkaLZ4BlockInputStream.java
  * 2. http://fastcompression.blogspot.in/2013/04/lz4-streaming-format-final.html
  *
  */

case class LZ4FrameInputStream(stream: InputStream) extends FilterInputStream(stream) {

  private final val InvalidHeader = "Invalid header"
  private final val InvalidMagicBytes = "Invalid magic bytes"
  private final val InvalidHeaderChecksum = "Invalid header checksum"
  private final val PrematureEOS = "Premature EOS"

  private[this] val decompressor = LZ4Factory.fastestInstance.safeDecompressor
  private[this] val checksum = XXHashFactory.fastestInstance.hash32

  private[this] val header = new Array[Byte](LZ4_MAX_HEADER_LENGTH)
  private[this] val headerOffset = 7
  if (stream.read(header, 0, headerOffset) != headerOffset) throw new Exception(InvalidHeader)
  if (MAGIC != readUnsignedIntLE(header, 0)) throw new Exception(InvalidMagicBytes)

  private[this] val bd = BD.fromByte(header(5))
  private[this] val headerChecksum = header(6)
  if (headerChecksum != calculateChecksum(header, 4, 2)) throw new Exception(InvalidHeaderChecksum)

  private[this] val buffer = new Array[Byte](bd.getBlockMaximumSize)
  private[this] var bufferOffset = 0
  private[this] var bufferSize = 0
  private[this] val compressed = new Array[Byte](bd.getBlockMaximumSize)
  private[this] var finished = false

  private def readBlock() = {
    val blockSize = readUnsignedIntLE(stream)
    if (blockSize == 0) {
      finished = true
    } else {
      if (stream.read(compressed, 0, blockSize) != blockSize) throw new Exception(PrematureEOS)
      bufferSize = decompressor.decompress(compressed, 0, blockSize, buffer, 0, bd.getBlockMaximumSize)
      bufferOffset = 0
    }
  }

  override def available = bufferSize - bufferOffset

  override def read(b: Array[Byte], off: Int, len: Int) = {
    if (finished) { -1 } else {
      if (available == 0) readBlock()
      if (finished) { -1 } else {
        val length = Math.min(len, available)
        System.arraycopy(buffer, bufferOffset, b, off, length)
        bufferOffset += length
        length
      }
    }
  }

  override def read() = {
    if (finished) { -1 } else {
      if (available == 0) readBlock()
      if (finished) { -1 } else {
        val b = buffer(bufferOffset)
        bufferOffset += 1
        b
      }
    }
  }

  private def calculateChecksum(bytes: Array[Byte], offset: Int, len: Int) =
    ((checksum.hash(bytes, offset, len, 0) >> 8) & 0xff).toByte
}
