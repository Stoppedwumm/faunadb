package fauna.codex.test

import fauna.util.Base64

class Base64Spec extends Spec {

  "encode url safe" in {
    Base64.encodeUrlSafe(Array[Byte](0xfc.toByte)) shouldBe "_A"
    Base64.encodeUrlSafe(Array[Byte](0xf8.toByte)) shouldBe "-A"
  }

  "decode url safe" in {
    Base64.decodeUrlSafe("_A") shouldBe Array[Byte](0xfc.toByte)
    Base64.decodeUrlSafe("-A") shouldBe Array[Byte](0xf8.toByte)

    Base64.decodeUrlSafe("_A==") shouldBe Array[Byte](0xfc.toByte)
    Base64.decodeUrlSafe("-A==") shouldBe Array[Byte](0xf8.toByte)
  }

  "encode standard" in {
    Base64.encodeStandard(Array[Byte](0xfc.toByte)) shouldBe "/A=="
    Base64.encodeStandard(Array[Byte](0xf8.toByte)) shouldBe "+A=="
  }

  "decode standard" in {
    Base64.decodeStandard("/A") shouldBe Array[Byte](0xfc.toByte)
    Base64.decodeStandard("+A") shouldBe Array[Byte](0xf8.toByte)

    Base64.decodeStandard("/A==") shouldBe Array[Byte](0xfc.toByte)
    Base64.decodeStandard("+A==") shouldBe Array[Byte](0xf8.toByte)
  }

  "decode both variants" in {
    //url-safe
    Base64.decode("_A") shouldBe Array[Byte](0xfc.toByte)
    Base64.decode("-A") shouldBe Array[Byte](0xf8.toByte)
    Base64.decode("_A==") shouldBe Array[Byte](0xfc.toByte)
    Base64.decode("-A==") shouldBe Array[Byte](0xf8.toByte)

    //standard
    Base64.decode("/A") shouldBe Array[Byte](0xfc.toByte)
    Base64.decode("+A") shouldBe Array[Byte](0xf8.toByte)
    Base64.decode("/A==") shouldBe Array[Byte](0xfc.toByte)
    Base64.decode("+A==") shouldBe Array[Byte](0xf8.toByte)
  }
}

