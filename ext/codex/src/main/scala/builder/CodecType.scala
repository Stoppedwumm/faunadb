package fauna.codex.builder

import io.netty.buffer.{ ByteBuf, Unpooled }
import language.experimental.macros
import scala.util.Try

/**
  * Codec family module base. Codec families for specific formats may
  * inherit from the CodecType trait, which provides helpers for
  * generating new encoders & decoders for types that compose
  * format-specific encoders/decoders.
  */

trait CodecByteHelpers { self: CodecType2[_, _] =>
  def encode[T: Encoder](buf: ByteBuf, t: T): ByteBuf

  def decode[T: Decoder](buf: ByteBuf): T

  // encode

  def encode[T: Encoder](t: T): ByteBuf = encode(Unpooled.buffer, t)

  // decode

  // non-consuming decode

  def parse[T: Decoder](buf: ByteBuf): T =
    decode(buf.duplicate)

  def tryParse[T: Decoder](buf: ByteBuf): Try[T] =
    Try(decode(buf.duplicate))

  def parse[T: Decoder](arr: Array[Byte]): T =
    decode(Unpooled.wrappedBuffer(arr))

  def tryParse[T: Decoder](arr: Array[Byte]): Try[T] =
    Try(decode(Unpooled.wrappedBuffer(arr)))
}

trait CodecType2[I, O] {

  type In = I
  type Out = O

  def encode[T](stream: Out, t: T)(implicit enc: Encoder[T]): Out =
    enc.encode(stream, t)

  def decode[T](stream: In)(implicit dec: Decoder[T]): T =
    dec.decode(stream)

  trait Decoder[T] {
    def decode(stream: In): T
  }

  trait Encoder[-T] { enc =>
    def encode(stream: Out, opt: T): Out
  }

  trait Codec[T] extends Decoder[T] with Encoder[T]


  object Codec {
    implicit def synthesizedCodec[T](implicit dec: Decoder[T], enc: Encoder[T]): Codec[T] =
      dec match {
        case dec: Codec[_] if dec eq enc => dec.asInstanceOf[Codec[T]]
        case _                           => new SynthesizedCodec[T](dec, enc)
      }
  }

  class SynthesizedCodec[T](val dec: Decoder[T], val enc: Encoder[T]) extends Codec[T] {
    def decode(stream: I) = dec.decode(stream)
    def encode(stream: O, t: T) = enc.encode(stream, t)
  }


  def AliasEncoder[A, T](to: A => T)(implicit enc: Encoder[T]): Encoder[A] =
    new Encoder[A] {
      def encode(stream: Out, a: A) = enc.encode(stream, to(a))
    }

  def AliasDecoder[A, T](from: T => A)(implicit dec: Decoder[T]): Decoder[A] =
    new Decoder[A] {
      def decode(stream: In) = from(dec.decode(stream))
    }

  def AliasCodec[A, T](from: T => A, to: A => T)(implicit dec: Decoder[T], enc: Encoder[T]): Codec[A] =
    new Codec[A] {
      def decode(stream: In) = from(dec.decode(stream))
      def encode(stream: Out, a: A) = enc.encode(stream, to(a))
    }

  /**
    * Creates a codec for a singleton object. Typical use for this codec is as
    * a component of a SumCodec, encoding a case object as one of the summed types.
    */
  def SingletonCodec(v: AnyRef)(implicit dec: Decoder[Unit], enc: Encoder[Unit]): Codec[v.type] =
    AliasCodec[v.type, Unit](_ => v, _ => ())

  /**
    * Codec that can't decode or encode anything. Useful as a placeholder for
    * removed values in SumCodec.
    */
  object DefunctCodec extends Codec[AnyRef] {
    def decode(stream: In) = throw new UnsupportedOperationException("Attempted to decode with DefunctCodec")
    def encode(stream: Out, a: AnyRef) = throw new UnsupportedOperationException(s"Attempted to encode with DefunctCodec: $a")

    // Use as a defunct codec for a singleton
    def apply(v: AnyRef) =
      DefunctCodec.asInstanceOf[Codec[v.type]]
  }

  /**
    * Macro-based Tuple, Sum Type, and Record Codecs
    *
    * Module implementers should provide the necessary shared
    * implementation for codec generation macros via the abstract
    * classes TupleEncoder/Decoder, SumTypeEncoder/Decoder, and
    * RecordEncoder/Decoder. The macros themselves emit codec
    * implementations that attempt to subclass these member classes of
    * the codec family module.
    */

  // trait TupleDecoder {
  //   def decodeStart(stream: In, expectedArity: Int): Unit
  //   def decodeEnd(stream: In): Unit
  // }

  // trait TupleEncoder {
  //   def encodeStart(stream: Out, arity: Int): Unit
  //   def encodeEnd(stream: Out): Unit
  // }

  // trait SumTypeValidator {
  //   def validateTags(ts: List[???]) // called on construction
  // }

  // trait SumTypeDecoder {
  //   def decodeSumStart(stream: In): ???
  //   def decodeEnd(stream: In): Unit
  // }

  // trait SumTypeEncoder {
  //   def encodeSumStart(stream: Out, tag: ???): Unit
  //   def encodeEnd(stream: Out): Unit
  // }

  // type RecordNameFormat = ???

  // trait RecordDecoder {
  //   def decodeStart(stream: In): Iterator[Unit]
  //   def decodeFieldName(stream: In): ???
  //   def decodeEnd(stream: In): Unit
  // }

  // trait RecordEncoder {
  //   def encodeStart(stream: Out, arity: Int): Unit
  //   def encodeFieldName(stream: Out, name: ???): Unit
  //   def encodeEnd(stream: Out): Unit
  // }

  // Required to correctly infer T in the context of tupleEnc macro.
  type InvEncoder[T] = Encoder[T]

  implicit def tupleDec[T]: Decoder[T] = macro TupleMacros.implicitDecoder[T]
  implicit def tupleEnc[T]: InvEncoder[T] = macro TupleMacros.implicitEncoder[T]

  def TupleDecoder[T]: Decoder[T] = macro TupleMacros.genDecoder[T]
  def TupleEncoder[T]: Encoder[T] = macro TupleMacros.genEncoder[T]
  def TupleCodec[T]: Codec[T] = macro TupleMacros.genCodec[T]

  def SumDecoder[T](variants: Decoder[_ <: T]*): Decoder[T] = macro SumTypeMacros.genDecoder[T]
  def SumEncoder[T](variants: Encoder[_ <: T]*): Encoder[T] = macro SumTypeMacros.genEncoder[T]
  def SumCodec[T](variants: Codec[_ <: T]*): Codec[T] = macro SumTypeMacros.genCodec[T]

  def RecordDecoder[T]: Decoder[T] = macro RecordMacros.genDecoder[T]

  def RecordEncoder[T]: Encoder[T] = macro RecordMacros.genEncoder[T]

  def RecordCodec[T]: Codec[T] = macro RecordMacros.genCodec[T]
}
