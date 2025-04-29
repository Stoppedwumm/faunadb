package fauna.util

import java.io._

/**
  * Header format and constants for LZ4 compression
  *
  * References:
  *
  * 1. https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/record/KafkaLZ4BlockOutputStream.java
  * 2. http://fastcompression.blogspot.in/2013/04/lz4-streaming-format-final.html
  *
  */

object LZ4FrameHeader {

  val MAGIC = 0x184D2204

  val BLOCKSIZE_64KB = 4
  val BLOCKSIZE_256KB = 5
  val BLOCKSIZE_1MB = 6
  val BLOCKSIZE_4MB = 7

  val LZ4_MAX_HEADER_LENGTH = 19

  case class FLG(
    reserved: Int = 0, contentChecksum: Int = 0, contentSize: Int = 0,
    blockChecksum: Int = 0, blockIndependence: Int = 1, version: Int = 1) {

    def toByte =
      (((reserved & 3) << 0) |
        ((contentChecksum & 1) << 2) |
        ((contentSize & 1) << 3) |
        ((blockChecksum & 1) << 4) |
        ((blockIndependence & 1) << 5) |
        ((version & 3) << 6)).toByte
  }

  object FLG {
    def apply(checksumBlock: Boolean) = new FLG(blockChecksum = if (checksumBlock) 1 else 0)
    def fromByte(b: Byte) = {
      val reserved = (b >>> 0) & 3
      val contentChecksum = (b >>> 2) & 1
      val contentSize = (b >>> 3) & 1
      val blockChecksum = (b >>> 4) & 1
      val blockIndependence = (b >>> 5) & 1
      val version = (b >>> 6) & 3
      FLG(reserved, contentChecksum, contentSize, blockChecksum, blockIndependence, version)
    }
  }

  case class BD(reserved2: Int, blockSizeValue: Int, reserved3: Int) {
    def toByte =
      (((reserved2 & 15) << 0) |
        ((blockSizeValue & 7) << 4) |
        ((reserved3 & 1) << 7)).toByte
    def getBlockMaximumSize = 1 << ((2 * blockSizeValue) + 8)
  }

  object BD {
    def apply(blockSizeValue: Int) = new BD(0, blockSizeValue, 0)
    def fromByte(b: Byte) = {
      val reserved2 = (b >>> 0) & 15
      val blockMaximumSize = (b >>> 4) & 7
      val reserved3 = (b >>> 7) & 1
      BD(reserved2, blockMaximumSize, reserved3)
    }
  }

  def readUnsignedIntLE(in: InputStream) =
    (in.read() << 8 * 0) | (in.read() << 8 * 1) | (in.read() << 8 * 2) | (in.read() << 8 * 3)

  def readUnsignedIntLE(b: Array[Byte], off: Int) =
    (b(off) << 8 * 0) | (b(off+1) << 8 * 1)| (b(off+2) << 8 * 2)| (b(off+3) << 8 * 3)


}