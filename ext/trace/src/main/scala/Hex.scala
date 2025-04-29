package fauna.trace

import java.lang.{ Byte => JByte, Long => JLong }
import java.util.Arrays

/**
  * Helpers for translating back-and-forth between hex-encoded
  * CharSequences and Java primitives.
  */
object Hex {
  val BytesPerLong = JLong.SIZE / JByte.SIZE

  // the size in chars of a hex-encoded byte
  val CharsPerByte = 2

  // the size in chars of a hex-encoded long
  val CharsPerLong = CharsPerByte * BytesPerLong

  val Alphabet = "0123456789abcdef"

  val ByteToChar = {
    val arr = new Array[Char](512)

    (0 until 256) foreach { i =>
      arr(i) = Alphabet.charAt(i >>> 4)
      arr(i | 0x100) = Alphabet.charAt(i & 0xf)
    }

    arr
  }

  val CharToByte = {
    val arr = new Array[Byte](256)
    Arrays.fill(arr, -1.toByte)

    (0 until Alphabet.length) foreach { i =>
      val c = Alphabet.charAt(i)
      arr(c) = i.toByte
    }

    arr
  }

  def toHex(num: Long, dest: Array[Char], offset: Int): Unit = {
    toHex((num >> 56 & 0xff).toByte, dest, offset)
    toHex((num >> 48 & 0xff).toByte, dest, offset + CharsPerByte)
    toHex((num >> 40 & 0xff).toByte, dest, offset + 2 * CharsPerByte)
    toHex((num >> 32 & 0xff).toByte, dest, offset + 3 * CharsPerByte)
    toHex((num >> 24 & 0xff).toByte, dest, offset + 4 * CharsPerByte)
    toHex((num >> 16 & 0xff).toByte, dest, offset + 5 * CharsPerByte)
    toHex((num >> 8 & 0xff).toByte, dest, offset + 6 * CharsPerByte)
    toHex((num & 0xff).toByte, dest, offset + 7 * CharsPerByte)
  }

  def toHex(byte: Byte, dest: Array[Char], offset: Int): Unit = {
    val b = byte & 0xff
    dest(offset) = ByteToChar(b)
    dest(offset + 1) = ByteToChar(b | 0x100)
  }

  def toByte(str: CharSequence, offset: Int): Byte = {
    require(str.length >= offset + CharsPerByte)
    getLong(str.charAt(offset), str.charAt(offset + 1)).toByte
  }

  private def getLong(hi: Char, lo: Char): Long =
    (CharToByte(hi) << 4 | CharToByte(lo)) & 0xff
}
