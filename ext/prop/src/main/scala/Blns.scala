package fauna

import java.util.Base64
import scala.io.{ Codec, Source }

package object prop {

  val BLNS: Array[String] =
    Source
      .fromResource("blns.base64.txt")(Codec.UTF8)
      .getLines()
      .collect {
        case l if l != "" && !l.startsWith("#") =>
          new String(Base64.getDecoder.decode(l))
      }
      .toArray
}
