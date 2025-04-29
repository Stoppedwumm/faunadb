package fauna.util

import java.nio.charset.StandardCharsets

class InvalidZBase32Exception(msg: String) extends Exception(msg)

object ZBase32 {
  val DecodeTable = Array[Byte](
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 18, -1, 25, 26, 27, 30, 29,  7, 31, -1, -1, -1, -1, -1, -1,
    -1, 24,  1, 12,  3,  8,  5,  6, 28, 21,  9, 10, -1, 11,  2, 16,
    13, 14,  4, 22, 17, 19, -1, 20, 15,  0, 23, -1, -1, -1, -1, -1,
    -1, 24,  1, 12,  3,  8,  5,  6, 28, 21,  9, 10, -1, 11,  2, 16,
    13, 14,  4, 22, 17, 19, -1, 20, 15,  0, 23,
  )

  val EncodeTable = Array[Byte](
    'y','b','n','d','r','f','g','8','e','j','k','m','c','p','q','x',
    'o','t','1','u','w','i','s','z','a','3','4','5','h','7','6','9',
  )

  private def encode5bits(offset: Long): Byte = EncodeTable(offset.toInt & 0x1f)

  private def decode5bits(byte: Byte): Long = {
    val offset = DecodeTable(byte)

    if (offset < 0) {
      throw new InvalidZBase32Exception(s"`${byte.toChar}` is not a valid z-base-32 character")
    }

    offset.toLong
  }

  /**
    * How it works
    *
    * The algorithm works encoding blocks of 5bits each, given
    * long has 64bits, it has 12 complete blocks of 5bits and one
    * incomplete block of 4bits, so we need to introduce one extra
    * bit to make it complete.
    *
    * Given the algorithm starts encoding the higher bits first,
    * the extra bit needs to be introduced on the right size.
    *
    * xxxx => xxxx0
    */
  def encodeLong(long: Long): String = {
    val buffer = new Array[Byte](13)

    buffer(0x0) = encode5bits(long >> 59)
    buffer(0x1) = encode5bits(long >> 54)
    buffer(0x2) = encode5bits(long >> 49)
    buffer(0x3) = encode5bits(long >> 44)
    buffer(0x4) = encode5bits(long >> 39)
    buffer(0x5) = encode5bits(long >> 34)
    buffer(0x6) = encode5bits(long >> 29)
    buffer(0x7) = encode5bits(long >> 24)
    buffer(0x8) = encode5bits(long >> 19)
    buffer(0x9) = encode5bits(long >> 14)
    buffer(0xa) = encode5bits(long >> 9)
    buffer(0xb) = encode5bits(long >> 4)
    buffer(0xc) = encode5bits(long << 1) //shift left to introduce one extra bit

    new String(buffer, 0, 13, StandardCharsets.ISO_8859_1)
  }

  def decodeLong(zBase32: String): Long = {
    val bytes = zBase32.getBytes(StandardCharsets.ISO_8859_1)

    if (bytes.lengthIs != 13) {
      throw new InvalidZBase32Exception(s"String length required to decode longs is 13")
    }

    (decode5bits(bytes(0x0)) << 59) |
      (decode5bits(bytes(0x1)) << 54) |
      (decode5bits(bytes(0x2)) << 49) |
      (decode5bits(bytes(0x3)) << 44) |
      (decode5bits(bytes(0x4)) << 39) |
      (decode5bits(bytes(0x5)) << 34) |
      (decode5bits(bytes(0x6)) << 29) |
      (decode5bits(bytes(0x7)) << 24) |
      (decode5bits(bytes(0x8)) << 19) |
      (decode5bits(bytes(0x9)) << 14) |
      (decode5bits(bytes(0xa)) << 9) |
      (decode5bits(bytes(0xb)) << 4) |
      (decode5bits(bytes(0xc)) >> 1) //shift right to remove the extra bit
  }
}

