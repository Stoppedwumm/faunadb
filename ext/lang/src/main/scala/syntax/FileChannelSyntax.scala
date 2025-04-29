package fauna.lang

import java.nio.channels.FileChannel
import scala.language.implicitConversions

trait FileChannelSyntax {
  implicit def asRichFileChannel(f: FileChannel): FileChannelSyntax.RichFileChannel =
    FileChannelSyntax.RichFileChannel(f)
}

object FileChannelSyntax {
  case class RichFileChannel(f: FileChannel) extends AnyVal {
    def remaining: Long = f.size - f.position
  }
}
