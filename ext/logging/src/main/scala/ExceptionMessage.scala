package fauna.logging

import fauna.codex.json2._
import fauna.lang.syntax._
import io.netty.buffer.ByteBufAllocator
import java.time.Instant
import org.apache.logging.log4j.message.{
  AsynchronouslyFormattable,
  Message
}

object ExceptionMessage {
  /**
    * An object containing an exception which caused the failure.
    */
  val CauseField = JSON.Escaped("cause")

  /**
    * The exception class name.
    */
  val ClassField = JSON.Escaped("class")

  /**
    * The human-readable description of the failure.
    */
  val MessageField = JSON.Escaped("message")

  /**
    * The thread stack when the exception was thrown.
    */
  val StackField = JSON.Escaped("stack")

  /**
    * An object containing a exception suppressed by the failure.
    */
  val SuppressedField = JSON.Escaped("suppressed")

  /**
    * The timestamp of the failure.
    */
  val TSField = JSON.Escaped("ts")

  /**
    * The name of the running thread when the exception was thrown.
    */
  val ThreadField = JSON.Escaped("thread")

  def apply(thread: Option[Thread], exception: Throwable): ExceptionMessage =
    new ExceptionMessage(thread map { _.getName }, exception, Instant.now)
}

@AsynchronouslyFormattable
final class ExceptionMessage private(
  threadName: Option[String],
  exception: Throwable,
  timestamp: Instant)
    extends Message {

  // XXX: impl StringBuilderFormattable?

  import ExceptionMessage._

  def getParameters: Array[AnyRef] = null
  def getThrowable: Throwable = null

  def getFormattedMessage(): String = {
    val alloc = ByteBufAllocator.DEFAULT
    val buf = alloc.ioBuffer
    try {
      val out = JSONWriter(buf)

      out.writeObjectStart()

      out.writeObjectField(TSField, out.writeString(timestamp.toString))

      threadName foreach { name =>
        out.writeObjectField(ThreadField, out.writeString(name))
      }

      writeCause(out, exception)

      out.writeObjectEnd()

      buf.toUTF8String
    } finally {
      buf.release()
    }
  }

  private def writeCause(
    out: JSONWriter,
    e: Throwable
  ): Unit = {
    out.writeObjectField(ClassField, out.writeString(e.getClass.getName))
    val msg = Option(e.getMessage) getOrElse ""
    out.writeObjectField(MessageField, out.writeString(msg))

    out.writeObjectField(
      StackField, {
        out.writeArrayStart()
        e.getStackTrace foreach { f =>
          out.writeString(f.toString)
        }
        out.writeArrayEnd()
      }
    )

    Option(e.getCause) foreach { c =>
      out.writeObjectField(CauseField, {
        out.writeObjectStart()
        writeCause(out, c)
        out.writeObjectEnd()
      })
    }

    Option(e.getSuppressed) foreach { sups =>
      if (sups.nonEmpty) {
        out.writeObjectField(SuppressedField, {
          out.writeArrayStart()
          sups foreach { sup =>
            out.writeObjectStart()
            writeCause(out, sup)
            out.writeObjectEnd()
          }
          out.writeArrayEnd()
        })
      }
    }
  }
}
