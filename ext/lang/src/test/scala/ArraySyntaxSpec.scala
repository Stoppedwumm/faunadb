package fauna.lang.test

import fauna.lang.syntax._

class ArraySyntaxSpec extends Spec {
  "toHexString" - {
    "empty array" in {
      Array.empty[Byte].toHexString should equal ("[]")
    }

    "one element" in {
      val bytes = Array[Byte](0xaa.toByte)
      bytes.toHexString should equal ("[0xAA]")
    }

    "several elements" in {
      val bytes = Array[Int](0xde, 0xad, 0xbe, 0xef) map { _.toByte }
      bytes.toHexString should equal ("[0xDE 0xAD 0xBE 0xEF]")
    }
  }
}
