package fauna.codex.test

import fauna.util.InvalidZBase32Exception
import fauna.util.ZBase32._

class ZBase32Spec extends Spec {

  /*
  00000=y 00001=b 00010=n 00011=d
  00100=r 00101=f 00110=g 00111=8
  01000=e 01001=j 01010=k 01011=m
  01100=c 01101=p 01110=q 01111=x
  10000=o 10001=t 10010=1 10011=u
  10100=w 10101=i 10110=s 10111=z
  11000=a 11001=3 11010=4 11011=5
  11100=h 11101=7 11110=6 11111=9
  */

  "encode/decode lower 4 bits" in {
    //00000.....0001 => 00010 = n
    encodeLong(1) shouldBe "yyyyyyyyyyyyn"
    decodeLong("yyyyyyyyyyyyn") shouldBe 1L

    (0x0 until 0xF) foreach { fourBits =>
      val fiveBits = fourBits << 1
      val symbol = EncodeTable(fiveBits)

      encodeLong(fourBits) shouldBe s"yyyyyyyyyyyy${symbol.toChar}"
      decodeLong(s"yyyyyyyyyyyy${symbol.toChar}") shouldBe fourBits.toLong
    }
  }

  "encode/decode higher 5 bits" in {
    //10111.....0000 => z
    encodeLong(0xB800000000000000L) shouldBe "zyyyyyyyyyyyy"
    decodeLong("zyyyyyyyyyyyy") shouldBe 0xB800000000000000L

    (0x0 until 0x1F) foreach { fiveBits =>
      val symbol = EncodeTable(fiveBits)

      encodeLong(fiveBits.toLong << 59) shouldBe s"${symbol.toChar}yyyyyyyyyyyy"
      decodeLong(s"${symbol.toChar}yyyyyyyyyyyy") shouldBe (fiveBits.toLong << 59)
    }
  }

  "encode/decode longs" in {
    val long = 0xFF00FF00FF00FF00L
    encodeLong(long) shouldBe "9hyx6y89yd9oy"
    decodeLong("9hyx6y89yd9oy") shouldBe long

    Seq(
      0x0000000000000000L, 0x1111111111111111L, 0x2222222222222222L, 0x3333333333333333L,
      0x4444444444444444L, 0x5555555555555555L, 0x6666666666666666L, 0x7777777777777777L,
      0x8888888888888888L, 0x9999999999999999L, 0xAAAAAAAAAAAAAAAAL, 0xBBBBBBBBBBBBBBBBL,
      0xCCCCCCCCCCCCCCCCL, 0xDDDDDDDDDDDDDDDDL, 0xEEEEEEEEEEEEEEEEL, 0xFFFFFFFFFFFFFFFFL
    ) foreach { long =>
      decodeLong(encodeLong(long)) shouldBe long
    }
  }

  "errors" in {
    val ex0 = the [InvalidZBase32Exception] thrownBy {
      decodeLong("9hyx6y89yd9o")
    }

    ex0.getMessage shouldBe "String length required to decode longs is 13"

    val ex1 = the [InvalidZBase32Exception] thrownBy {
      decodeLong("lllllllllllll")
    }

    ex1.getMessage shouldBe "`l` is not a valid z-base-32 character"
  }
}
