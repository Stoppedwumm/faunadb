package fauna.codex.builder

import fauna.lang.syntax._
import io.netty.buffer.ByteBuf
import language.implicitConversions
import scala.annotation.implicitNotFound

trait DecoderType[I] {
  type In = I

  val format: DecoderFormat[In]
  val base: this.type = this

  @implicitNotFound("Could not find a Decoder for ${T}.")
  trait Decoder[+T] {
    def decode(stream: In): T
  }

  trait AliasDecoder[+A, T] extends Decoder[A] {
    val innerDecoder: Decoder[T]
  }

  implicit object ByteDecoder extends Decoder[Byte] { def decode(stream: In) = format.readByte(stream) }
  implicit object ShortDecoder extends Decoder[Short] { def decode(stream: In) = format.readShort(stream) }
  implicit object IntDecoder extends Decoder[Int] { def decode(stream: In) = format.readInt(stream) }
  implicit object LongDecoder extends Decoder[Long] { def decode(stream: In) = format.readLong(stream) }
  implicit object FloatDecoder extends Decoder[Float] { def decode(stream: In) = format.readFloat(stream) }
  implicit object DoubleDecoder extends Decoder[Double] { def decode(stream: In) = format.readDouble(stream) }
  implicit object BooleanDecoder extends Decoder[Boolean] { def decode(stream: In) = format.readBoolean(stream) }
  implicit object CharDecoder extends Decoder[Char] { def decode(stream: In) = format.readChar(stream) }
  implicit object StringDecoder extends Decoder[String] { def decode(stream: In) = format.readString(stream) }
}

trait NullDecoderType[In] extends DecoderType[In] {
  val format: NullDecoderFormat[In]

  class OptionDecoder[+T](val innerDecoder: Decoder[T]) extends Decoder[Option[T]] {
    def decode(stream: In) =
      if (format.readIfNull(stream)) None else Some(innerDecoder.decode(stream))
  }

  implicit def optionDecoder[T](implicit dec: Decoder[T]): Decoder[Option[T]] = new OptionDecoder(dec)
}

trait BinaryDecoderType[In] extends DecoderType[In] {
  val format: BinaryDecoderFormat[In]

  implicit object ByteBufDecoder extends Decoder[ByteBuf] {
    def decode(stream: In) = format.readBytes(stream)
  }

  implicit object BytesDecoder extends Decoder[Array[Byte]] {
    def decode(stream: In) = format.readBytes(stream).toByteArray
  }
}

trait TupleDecoderType[In] extends DecoderType[In]
    with TupleDecoderClasses[In] {

  val format: TupleDecoderFormat[In]

  @implicitNotFound("Could not find a Decoder for ${T}.")
  trait TupleDecoder[+T] extends Decoder[T] {
    def innerDecoders: Vector[Decoder[_]]
  }
}

trait SeqDecoderType[In] extends DecoderType[In] {
  val format: SequenceDecoderFormat[In]

  class SeqDecoder[+T](val innerDecoder: Decoder[T]) extends Decoder[Seq[T]] {
    def decode(stream: In) = {
      val b = Seq.newBuilder[T]
      format.readSequence(stream) { (_, s) => b += innerDecoder.decode(s) }
      b.result()
    }
  }

  implicit def seqDecoder[T](implicit dec: Decoder[T]): Decoder[Seq[T]] = new SeqDecoder(dec)
}

trait MapDecoderType[In] extends DecoderType[In] {
  val format: MapDecoderFormat[In]

  class MapDecoder[K, +V](val keyDecoder: Decoder[K], val valueDecoder: Decoder[V]) extends Decoder[Map[K, V]] {
    def decode(stream: In) = {
      val b = Map.newBuilder[K, V]

      format.readMap(stream) { (keyStream, valueStream) =>
        val k = format.readKey(keyStream) { s => keyDecoder.decode(s) }
        val v = format.readValue(valueStream) { s => valueDecoder.decode(s) }

        b += (k -> v)
      }

      b.result()
    }
  }

  implicit def mapDecoder[K, V](implicit k: Decoder[K], v: Decoder[V]): Decoder[Map[K, V]] = new MapDecoder(k, v)
}

trait RecordDecoderType[In] extends DecoderType[In] {
  val format: RecordDecoderFormat[In]

  @implicitNotFound("Could not find a Decoder for ${T}.")
  trait RecordDecoder[+T] extends Decoder[T] {
    def keys: Set[String]
    def innerDecoders: Map[String, Decoder[_]]
  }

  // Provides a level of indirection that allows for decoding of optional fields
  trait FieldDecoder[T] {
    def innerDecoder: Decoder[_]
    def decode(stream: In): T
    def check(k: Any, t: T): T
  }

  class LiftedFieldDecoder[T](val innerDecoder: Decoder[T]) extends FieldDecoder[T] {
    def decode(stream: In) = innerDecoder.decode(stream)
    def check(k: Any, t: T): T = if (null != t) t else {
      throw new NoSuchElementException("Value for key "+k+" not found.")
    }
  }

  class OptionFieldDecoder[T](val innerDecoder: Decoder[T]) extends FieldDecoder[Option[T]] {
    def decode(stream: In) = Some(innerDecoder.decode(stream))
    def check(k: Any, t: Option[T]) = if (null != t) t else None
  }

  trait FieldDecoder_0 {
    implicit def liftedFieldDecoder[T](implicit dec: Decoder[T]): FieldDecoder[T] =
      new LiftedFieldDecoder(dec)
  }

  object FieldDecoder extends FieldDecoder_0 {
    implicit def optionFieldDecoder[T](implicit dec: Decoder[T]): FieldDecoder[Option[T]] =
      new OptionFieldDecoder(dec)
  }
}

trait UnionDecoderType[In] extends DecoderType[In] {
  val format: UnionDecoderFormat[In]

  trait UnionDecoder[+T] extends Decoder[T] {
    val types: Set[format.TypeTag]
    val innerDecoders: Map[format.TypeTag, Decoder[T]]
  }
}

trait DecoderBuilder[In] {
  val base: DecoderType[In]

  def Alias[A, T: base.Decoder](apply: T => A) = new base.AliasDecoder[A, T] {
    val innerDecoder = implicitly[base.Decoder[T]]
    def decode(stream: In) = apply(innerDecoder.decode(stream))
  }

  def Lazy[T](dec: => base.Decoder[T]): base.Decoder[T] = new base.AliasDecoder[T, T] {
    lazy val innerDecoder = dec
    def decode(stream: In) = innerDecoder.decode(stream)
  }
}

trait RecordDecoderBuilder[In] extends DecoderBuilder[In]
    with RecordDecoderConstructors[In] {

  val base: RecordDecoderType[In]
}

trait UnionDecoderBuilder[In] extends DecoderBuilder[In] {
  val base: UnionDecoderType[In]

  class Summand[+T](val tag: base.format.TypeTag, val cls: Class[_], val dec: base.Decoder[T])

  object Summand {
    implicit def pairToSummand[T](kv: (base.format.TypeTag, base.Decoder[T]))(implicit m: Manifest[T]): Summand[T] =
      new Summand[T](kv._1, m.runtimeClass, kv._2)
  }

  def Union[T](summands: Summand[T]*): base.UnionDecoder[T] =
    new base.UnionDecoder[T] {
      val innerDecoders = summands map { s => s.tag -> s.dec } toMap
      val types = innerDecoders.keySet

      def decode(stream: In) =
        base.format.readUnion(stream) { (tag, s) => innerDecoders(tag).decode(s) }
    }
}
