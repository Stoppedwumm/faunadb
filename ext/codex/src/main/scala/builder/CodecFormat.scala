package fauna.codex.builder

import io.netty.buffer.ByteBuf

trait CodecFormat[In, Out] extends DecoderFormat[In] with EncoderFormat[Out]
trait NullCodecFormat[In, Out] extends NullDecoderFormat[In] with NullEncoderFormat[Out]
trait BinaryCodecFormat[In, Out] extends BinaryDecoderFormat[In] with BinaryEncoderFormat[Out]
trait TupleCodecFormat[In, Out] extends TupleDecoderFormat[In] with TupleEncoderFormat[Out]
trait SequenceCodecFormat[In, Out] extends SequenceDecoderFormat[In] with SequenceEncoderFormat[Out]
trait MapCodecFormat[In, Out] extends MapDecoderFormat[In] with MapEncoderFormat[Out]
trait RecordCodecFormat[In, Out] extends RecordDecoderFormat[In] with RecordEncoderFormat[Out]
trait UnionCodecFormat[In, Out] extends UnionDecoderFormat[In] with UnionEncoderFormat[Out]

// Primitives

trait DecoderFormat[In] {
  def readByte(stream: In): Byte
  def readShort(stream: In): Short
  def readInt(stream: In): Int
  def readLong(stream: In): Long
  def readFloat(stream: In): Float
  def readDouble(stream: In): Double
  def readBoolean(stream: In): Boolean
  def readChar(stream: In): Char
  def readString(stream: In): String
}

// Null/Optional fields

trait NullDecoderFormat[In] extends DecoderFormat[In] {
  def readNull(stream: In): Unit
  def readIfNull(stream: In): Boolean
}

// Binary Data

trait BinaryDecoderFormat[In] extends DecoderFormat[In] {
  def readBytes(stream: In): ByteBuf
}

// Fixed-size Tuples

trait TupleDecoderFormat[In] extends DecoderFormat[In] {
  def readTuple[T](stream: In, count: Int)(f: (Int, In) => Unit): Unit
  def skipField(stream: In): Unit
}

// Sequences

trait SequenceDecoderFormat[In] extends DecoderFormat[In] {
  def readSequence(stream: In)(f: (Int, In) => Unit): Unit
}

// Maps

trait MapDecoderFormat[In] extends DecoderFormat[In] {
  def readMap(stream: In)(f: (In, In) => Unit): Unit
  def readKey[T](stream: In)(f: In => T): T
  def readValue[T](stream: In)(f: In => T): T
}

// Records/Objects

trait RecordDecoderFormat[In] extends DecoderFormat[In] {
  def readRecord(stream: In, count: Int)(f: (String, In) => Unit): Unit
  def skipField(stream: In): Unit
}

// Unions

trait UnionDecoderFormat[In] extends DecoderFormat[In] {
  type TypeTag

  def readUnion[T](stream: In)(f: (TypeTag, In) => T): T
}

// Primitives

trait EncoderFormat[Out] {
  def writeByte(stream: Out, value: Byte): Unit
  def writeShort(stream: Out, value: Short): Unit
  def writeInt(stream: Out, value: Int): Unit
  def writeLong(stream: Out, value: Long): Unit
  def writeFloat(stream: Out, value: Float): Unit
  def writeDouble(stream: Out, value: Double): Unit
  def writeBoolean(stream: Out, value: Boolean): Unit
  def writeChar(stream: Out, value: Char): Unit
  def writeString(stream: Out, value: String): Unit
}

// Null/Optional fields

trait NullEncoderFormat[Out] extends EncoderFormat[Out] {
  def writeNull(stream: Out): Unit
}

// Binary Data

trait BinaryEncoderFormat[Out] extends EncoderFormat[Out] {
  def writeBytes(stream: Out, value: ByteBuf): Unit
}

// Fixed-size Tuples

trait TupleEncoderFormat[Out] extends EncoderFormat[Out] {
  def writeTuple(stream: Out, size: Int)(f: Out => Unit): Unit
}

// Sequences

trait SequenceEncoderFormat[Out] extends EncoderFormat[Out] {
  def writeSequence(stream: Out, size: Int)(f: Out => Unit): Unit
}

// Maps

trait MapEncoderFormat[Out] extends EncoderFormat[Out] {
  def writeMap(stream: Out, size: Int)(f: Out => Unit): Unit
  def writeKey(stream: Out)(f: Out => Unit): Unit
  def writeValue(stream: Out)(f: Out => Unit): Unit
}

// Records/Objects

trait RecordEncoderFormat[Out] extends EncoderFormat[Out] {
  def shouldEncode(v: Any) = v match {
    case None => false
    case _    => true
  }

  def writeRecord(stream: Out, size: Int)(f: Out => Unit): Unit
  def writeField(stream: Out, key: String)(f: Out => Unit): Unit
}

// Unions

trait UnionEncoderFormat[Out] extends EncoderFormat[Out] {
  type TypeTag

  def writeUnion(stream: Out, tag: TypeTag)(f: Out => Unit): Unit
}
