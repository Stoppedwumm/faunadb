package fauna.lang

import java.util.Arrays
import scala.language.implicitConversions

trait ArraySyntax {
  implicit def asRichArrayOfBytes(a: Array[Byte]): ArraySyntax.RichArrayOfBytes =
    ArraySyntax.RichArrayOfBytes(a)
}

object ArraySyntax {

  // See toHexString.
  private[this] val HexBytes =
    Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')

  case class RichArrayOfBytes(a: Array[Byte]) extends AnyVal {

    /**
      * Returns the bitwise XOR of this byte array and another. If the
      * two arrays differ in length, the output will be the size of
      * the longer input.
      */
    def xor(b: Array[Byte]): Array[Byte] = {
      val (left, right) = if (a.length > b.length) {
        (b, a)
      } else {
        (a, b)
      }

      val out = Arrays.copyOf(right, right.length)
      var i = 0
      while (i < left.length) {
        out(i) = ((left(i) & 0xff) ^ (right(i) & 0xff)).toByte
        i += 1
      }
      out
    }


    def toHexString: String = {
      val builder = new StringBuilder("[")

      val iter = a.iterator

      if (iter.hasNext) {
        val byte = iter.next()
        builder.append("0x")
        builder.append(HexBytes((byte & 0xf0) >>> 4))
        builder.append(HexBytes(byte & 0x0f))

        while (iter.hasNext) {
          builder.append(" ")

          val byte = iter.next()
          builder.append("0x")
          builder.append(HexBytes((byte & 0xf0) >>> 4))
          builder.append(HexBytes(byte & 0x0f))
        }
      }

      builder.append("]")
      builder.toString
    }
  }
}
