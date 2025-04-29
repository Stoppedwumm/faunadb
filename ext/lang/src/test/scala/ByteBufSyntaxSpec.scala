package fauna.lang.test

import fauna.lang.syntax._
import io.netty.buffer.Unpooled

class ByteBufSyntaxSpec extends Spec {

  "ByteBufSyntax" - {

    "insertPadding pads buffer" in {
      val buf = Unpooled.buffer(0)
      buf.writeBytes(Array[Byte](1, 2, 3))
      buf.insertPadding(1, 3)
      // padding does not clear the padded interval
      buf.toByteArray shouldBe Array[Byte](1, 2, 3, 0, 2, 3)
    }

    "insertPadding grows the buffer correctly" in {
      val buf = Unpooled.buffer(0)
      buf.insertPadding(0, 64) // the edge of the first buffer expansion
      buf.toByteArray should have size 64
    }
  }
}
