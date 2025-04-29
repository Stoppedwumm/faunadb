package fauna.lang

import fauna.lang.syntax.array._
import java.lang.invoke.MethodHandles
import java.nio.charset.{ Charset, StandardCharsets }
import java.nio.{ ByteBuffer, MappedByteBuffer }
import scala.language.implicitConversions

trait ByteBufferSyntax {
  implicit def asRichByteBuffer(buf: ByteBuffer): ByteBufferSyntax.RichByteBuffer =
    ByteBufferSyntax.RichByteBuffer(buf)

  implicit def asRichMappedByteBuffer(buf: MappedByteBuffer): ByteBufferSyntax.RichMappedByteBuffer =
    ByteBufferSyntax.RichMappedByteBuffer(buf)
}

object ByteBufferSyntax {
  case class RichByteBuffer(b: ByteBuffer) extends AnyVal {
    def toArray = {
      val arr = new Array[Byte](b.remaining)
      b.duplicate.get(arr)
      arr
    }

    def toUTF8String = toEncodedString(StandardCharsets.UTF_8)

    def toISO8859 = toEncodedString(StandardCharsets.ISO_8859_1)

    def toEncodedString(enc: Charset) =
      if (b.hasArray) new String(b.array, b.position(), b.remaining, enc) else enc.decode(b.duplicate).toString

    def toHexString = toArray.toHexString

    def clean(): Boolean = cleanBuffer(b)
  }

  case class RichMappedByteBuffer(b: MappedByteBuffer) extends AnyVal {
    def clean(): Boolean = cleanBuffer(b)
  }

  final def cleanBuffer(buf: ByteBuffer): Boolean =
    if (!buf.isDirect) {
      false
    } else {
      cleaner(buf)
    }

  private[this] val NoopCleaner: ByteBuffer => Boolean = Function.const(false)

  private[this] final val cleaner: ByteBuffer => Boolean = {
    lazy val j11c = try {
      val unsafeCls = classOf[sun.misc.Unsafe]
      val f = unsafeCls.getDeclaredField("theUnsafe")
      f.setAccessible(true)
      val unsafe = f.get(null).asInstanceOf[sun.misc.Unsafe]

      val invokeM = unsafeCls.getDeclaredMethod("invokeCleaner", classOf[ByteBuffer])
      val invokeMH = MethodHandles.lookup().unreflect(invokeM)

      val c = { buf: ByteBuffer =>
        try {
          invokeMH.invoke(unsafe, buf)
          true
        } catch {
          case _: Throwable => false
        }
      }

      if (c(ByteBuffer.allocateDirect(1))) c else null
    } catch {
      case _: Throwable => null
    }

    if (j11c ne null) {
      j11c
    } else {
      System.err.println("Warning: ByteBuffer cleaner is unavailable: Unable to eagerly un-map direct buffers, which could lead to out-of-memory errors. Please use OpenJDK or another Java distribution with support for buffer cleaning.")
      NoopCleaner
    }
  }

  final val canClean = cleaner ne NoopCleaner
}
