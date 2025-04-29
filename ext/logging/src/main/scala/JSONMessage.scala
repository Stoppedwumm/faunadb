package fauna.logging

import fauna.codex.json2._
import fauna.lang.syntax._
import io.netty.buffer.ByteBufAllocator
import org.apache.logging.log4j.message.{
  AsynchronouslyFormattable,
  Message
}

object JSONMessage {
  def apply[T: JSON.Encoder](message: T): Message =
    new JSONMessage(message)
}

@AsynchronouslyFormattable
final class JSONMessage[T: JSON.Encoder] private(message: T)
    extends Message {

  def getParameters: Array[AnyRef] = null
  def getThrowable: Throwable = null

  def getFormattedMessage(): String = {
    val alloc = ByteBufAllocator.DEFAULT
    val buf = alloc.ioBuffer

    try {
      JSON.encode(buf, message).toUTF8String
    } finally {
      buf.release()
    }
  }

}
