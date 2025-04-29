package fauna.codex.test
import fauna.lang.syntax._
import fauna.codex.cbor.{ CBOR, CBOROrdering }

class UTFSpec extends Spec {

  "heart eyes" in {
    val buf1 = CBOR.encode("X\uD83D\uDE0D")
    val buf2 = CBOR.encode("\uD83D\uDE0DY")

    buf1.toHexString should equal ("[0x65 0x58 0xF0 0x9F 0x98 0x8D]")
    buf2.toHexString should equal ("[0x65 0xF0 0x9F 0x98 0x8D 0x59]")

    CBOROrdering.compare(buf1, buf2) > 0 should equal (true)
  }

  "compare emojis" in {
    val buf1 = CBOR.encode("\uD83D\uDE0C")
    val buf2 = CBOR.encode("\uD83D\uDE0D")

    buf1.toHexString should equal ("[0x64 0xF0 0x9F 0x98 0x8C]")
    buf2.toHexString should equal ("[0x64 0xF0 0x9F 0x98 0x8D]")

    CBOROrdering.compare(buf1, buf2) < 0 should equal (true)
  }
}

