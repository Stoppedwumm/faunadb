package fauna.tx

import fauna.codex.cbor.CBOR
import io.netty.buffer.{ ByteBuf, ByteBufUtil, Unpooled }
import java.security.MessageDigest
import java.lang.{ Long => JLong }
import scala.util.control.NonFatal

package log {
  case class TX(toLong: Long) extends AnyVal with Ordered[TX] {
    def +(l: Long) = TX(toLong + l)
    def -(l: Long) = TX(toLong - l)

    def compare(other: TX): Int = JLong.compare(toLong, other.toLong)

    def diff(o: TX): Long = toLong - o.toLong
    def max(o: TX): TX = if (toLong > o.toLong) this else o
    def min(o: TX): TX = if (toLong < o.toLong) this else o

    override def >(o: TX): Boolean = toLong > o.toLong
    override def >=(o: TX): Boolean = toLong >= o.toLong
    override def <(o: TX): Boolean = toLong < o.toLong
    override def <=(o: TX): Boolean = toLong <= o.toLong
  }

  object TX {
    final val MinValue = TX(0)
    final val MaxValue = TX(Long.MaxValue)

    implicit val CBORCodec = CBOR.TupleCodec[TX]
  }

  class Checksum(val _buf: ByteBuf) extends AnyVal {
    def toBytes = _buf.slice()
    def append(bufs: ByteBuf*) = Checksum(_buf, bufs: _*)
    override def toString = s"Checksum(${ByteBufUtil.hexDump(toBytes)})"
  }

  object Checksum {
    val ByteLength = 16

    // Reusing a thread-local vs creating a new digest object cuts about
    // 25% off of pure digest calculation cost
    private val digest = new ThreadLocal[MessageDigest] {
      override protected def initialValue = MessageDigest.getInstance("MD5")
    }

    def apply(buf: ByteBuf, bufs: ByteBuf*): Checksum = {
      val md = digest.get
      try {
        md.update(buf.nioBuffer)
        bufs foreach { b => md.update(b.nioBuffer) }
        new Checksum(Unpooled.wrappedBuffer(md.digest))
      } catch {
        case NonFatal(e) =>
          md.reset()
          throw e
      }
    }

    // Creates a new Checksum object by reading 16 bytes from a buffer
    def readBytes(buf: ByteBuf) = {
      val chkbuf = newBuffer().writerIndex(0)
      buf.readBytes(chkbuf, ByteLength)
      new Checksum(chkbuf)
    }

    final val Null = new Checksum(Unpooled.unreleasableBuffer(newBuffer()))

    private def newBuffer() = Unpooled.wrappedBuffer(new Array[Byte](ByteLength))
  }
}
