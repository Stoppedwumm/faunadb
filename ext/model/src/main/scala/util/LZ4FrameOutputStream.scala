package fauna.util

import java.io._

import net.jpountz.lz4.LZ4Factory
import net.jpountz.xxhash.XXHashFactory
import LZ4FrameHeader._

/**
  * Output stream for LZ4 compression
  *
  * Writes data to underlying stream after compressing it by the LZ4 scheme. The header and trailer are compatible
  * with the CLI format (see References 2).
  *
  * References:
  *
  * 1. https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/record/KafkaLZ4BlockOutputStream.java
  * 2. http://fastcompression.blogspot.in/2013/04/lz4-streaming-format-final.html
  *
  */

case class LZ4FrameOutputStream(
  stream: OutputStream, blockSize: Int = BLOCKSIZE_64KB) extends FilterOutputStream(stream) {

  private[this] val compressor = LZ4Factory.fastestInstance.fastCompressor
  private[this] val checksum = XXHashFactory.fastestInstance.hash32
  private[this] val flg = FLG(false)
  private[this] val bd = BD(blockSize)
  private[this] val maxBlockSize = bd.getBlockMaximumSize
  private[this] val buffer = new Array[Byte](maxBlockSize)
  private[this] var bufferOffset = 0
  private[this] val compressedBuffer = new Array[Byte](compressor.maxCompressedLength(maxBlockSize))

  writeHeader()

  private def bufferCapacity = maxBlockSize - bufferOffset

  override def write(b: Int) = {
    if (bufferOffset == maxBlockSize) writeBlock()

    buffer.update(bufferOffset, b.toByte)
    bufferOffset += 1
  }

  private def fillBuffer(b: Array[Byte], off: Int, len: Int): Int = {
    if (bufferOffset == maxBlockSize) writeBlock()

    val length = len min bufferCapacity
    System.arraycopy(b, off, buffer, bufferOffset, length)
    bufferOffset += length
    length
  }

  override def write(b: Array[Byte], off: Int, len: Int) = {
    var offset = off
    var remaining = len

    while (remaining > 0) {
      val written = fillBuffer(b, offset, remaining)
      offset += written
      remaining -= written
    }
  }

  override def flush() = {
    writeBlock()
    stream.flush()
  }

  override def close() = {
    flush()
    writeEndMark()
    stream.close()
  }

  private def writeHeader() = {
    bufferOffset = writeUnsignedIntLE(buffer, bufferOffset, MAGIC)
    bufferOffset = writeByte(buffer, bufferOffset, flg.toByte)
    bufferOffset = writeByte(buffer, bufferOffset, bd.toByte)
    val headerChecksum = calculateChecksum(buffer, 4, bufferOffset - 4)
    bufferOffset = writeByte(buffer, bufferOffset, headerChecksum)
    stream.write(buffer, 0, bufferOffset)
    bufferOffset = 0
  }

  private def writeEndMark() = {
    writeUnsignedIntLE(stream, 0)
    stream.flush()
  }

  private def writeBlock() = {
    if (bufferOffset > 0) {
      val compressedLen = compressor.compress(buffer, 0, bufferOffset, compressedBuffer, 0)
      writeUnsignedIntLE(stream, compressedLen)
      stream.write(compressedBuffer, 0, compressedLen)
      bufferOffset = 0
    }
  }

  private def calculateChecksum(bytes: Array[Byte], offset: Int, len: Int) =
    ((checksum.hash(bytes, offset, len, 0) >> 8) & 0xff).toByte

  private def writeUnsignedIntLE(out: OutputStream, value: Int) = {
    out.write(value >>> 8 * 0)
    out.write(value >>> 8 * 1)
    out.write(value >>> 8 * 2)
    out.write(value >>> 8 * 3)
  }

  private def writeUnsignedIntLE(buffer: Array[Byte], offset: Int, value: Int) = {
    buffer.update(offset, (value >>> 8 * 0).toByte)
    buffer.update(offset + 1, (value >>> 8 * 1).toByte)
    buffer.update(offset + 2, (value >>> 8 * 2).toByte)
    buffer.update(offset + 3, (value >>> 8 * 3).toByte)
    offset + 4
  }

  private def writeByte(buffer: Array[Byte], offset: Int, value: Int) = {
    buffer.update(offset, value.toByte)
    offset + 1
  }

}
