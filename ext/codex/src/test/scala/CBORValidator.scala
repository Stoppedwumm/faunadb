package fauna.codex.test

import fauna.codex.cbor._
import io.netty.buffer.{ ByteBuf, Unpooled }

object CBORValidator {
  def apply(bytes: Array[Byte]): CBORValidator =
    CBORValidator(Unpooled.wrappedBuffer(bytes))
}

final case class CBORValidator(buf: ByteBuf) {

  def validate(): Unit = {
    validate0()
    if (buf.isReadable) {
      throw CBORValidationException
    }
  }

  private def validateIndefinite(mt: Long, breakable: Boolean): Unit = {
    mt match {
      case 2 | 3 =>
        var it = 0
        do {
          it = validate0(true)
          if (it != mt) throw CBORValidationException
        } while (it != -1)
      case 4 =>
        while (validate0(true) != -1) { }
      case 5 =>
        while (validate0(true) != -1) {
          validate0()
        }
      case 7 =>
        if (breakable) return
        else throw CBORValidationException
      case _ => throw CBORValidationException
    }
  }

  private def validate0(breakable: Boolean = false): Int = {
    val ib = buf.readByte & 0xff
    val mt = ib >> 5
    var vl: Long = ib & 0x1f
    val ai = vl

    ai match {
      case InitialByte.UInt8 => vl = buf.readByte
      case InitialByte.UInt16 => vl = buf.readShort
      case InitialByte.UInt32 => vl = buf.readInt
      case InitialByte.UInt64 => vl = buf.readLong
      case 28 | 29 | 30 => throw CBORValidationException
      case 31 =>
        validateIndefinite(mt, breakable)
        return 0
      case _ => ()
    }

    (mt: @annotation.switch) match {
      // case 0, 1, 7 do not have content; just use val
      case 2 | 3 => // bytes/UTF-8
        val bytes = new Array[Byte](vl.toInt)
        buf.readBytes(bytes, 0, vl.toInt)
      case 4 => // array
        for (_ <- 0L until vl) validate0(breakable)
      case 5 => // map
        for (_ <- 0L until vl * 2) validate0(breakable)
      case 6 => validate0(breakable) // tag
      case _ => ()
    }
    mt
  }
}
