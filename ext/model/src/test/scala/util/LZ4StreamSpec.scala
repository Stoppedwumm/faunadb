package fauna.model.test

import fauna.lang.syntax._
import fauna.util.{ LZ4FrameInputStream, LZ4FrameOutputStream }
import java.io._
import scala.util.Random


class LZ4StreamSpec extends Spec {
  "can read own writes" - {
    val randomData = Random.alphanumeric.take(32).mkString.toUTF8Bytes
    val lz4Stream = new ByteArrayOutputStream()
    val lz4Writer = LZ4FrameOutputStream(lz4Stream)
    lz4Writer.write(randomData, 0, randomData.size)
    lz4Writer.close()
    val lz4Bytes = lz4Stream.toByteArray

    val byteStream = new ByteArrayOutputStream()
    val in = LZ4FrameInputStream(new ByteArrayInputStream(lz4Bytes))
    var b = in.read()
    while (b != -1) {
      byteStream.write(b)
      b = in.read()
    }
    byteStream.toByteArray should equal (randomData)
  }
}
