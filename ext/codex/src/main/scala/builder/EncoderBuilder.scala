package fauna.codex.builder

import language.implicitConversions
import scala.annotation.implicitNotFound
import io.netty.buffer.{ ByteBuf, Unpooled }

trait EncoderType[O] {
  type Out = O

  val format: EncoderFormat[Out]
  val base: this.type = this

  @implicitNotFound("Could not find an Encoder for ${T}.")
  trait Encoder[-T] { enc =>
    def encodeTo(stream: Out, opt: T): Unit
  }

  trait AliasEncoder[-A, T] extends Encoder[A] {
    val innerEncoder: Encoder[T]
  }

  implicit object ByteEncoder extends Encoder[Byte] { def encodeTo(stream: Out, t: Byte) = format.writeByte(stream, t) }
  implicit object ShortEncoder extends Encoder[Short] { def encodeTo(stream: Out, t: Short) = format.writeShort(stream, t) }
  implicit object IntEncoder extends Encoder[Int] { def encodeTo(stream: Out, t: Int) = format.writeInt(stream, t) }
  implicit object LongEncoder extends Encoder[Long] { def encodeTo(stream: Out, t: Long) = format.writeLong(stream, t) }
  implicit object FloatEncoder extends Encoder[Float] { def encodeTo(stream: Out, t: Float) = format.writeFloat(stream, t) }
  implicit object DoubleEncoder extends Encoder[Double] { def encodeTo(stream: Out, t: Double) = format.writeDouble(stream, t) }
  implicit object BooleanEncoder extends Encoder[Boolean] { def encodeTo(stream: Out, t: Boolean) = format.writeBoolean(stream, t) }
  implicit object CharEncoder extends Encoder[Char] { def encodeTo(stream: Out, t: Char) = format.writeChar(stream, t) }
  implicit object StringEncoder extends Encoder[String] { def encodeTo(stream: Out, t: String) = format.writeString(stream, t) }
}

trait NullEncoderType[Out] extends EncoderType[Out] {
  val format: NullEncoderFormat[Out]

  class OptionEncoder[-T](val innerEncoder: Encoder[T]) extends Encoder[Option[T]] {
    def encodeTo(stream: Out, opt: Option[T]) = opt match {
      case Some(t) => innerEncoder.encodeTo(stream, t)
      case None    => format.writeNull(stream)
    }
  }

  implicit def optionEncoder[T](implicit enc: Encoder[T]): Encoder[Option[T]] = new OptionEncoder(enc)
}

trait BinaryEncoderType[Out] extends EncoderType[Out] {
  val format: BinaryEncoderFormat[Out]

  implicit object ByteBufEncoder extends Encoder[ByteBuf] {
    def encodeTo(stream: Out, t: ByteBuf) = format.writeBytes(stream, t)
  }

  implicit object BytesEncoder extends Encoder[Array[Byte]] {
    def encodeTo(stream: Out, t: Array[Byte]) = format.writeBytes(stream, Unpooled.wrappedBuffer(t))
  }
}

trait TupleEncoderType[Out] extends EncoderType[Out]
    with TupleEncoderClasses[Out] {

  val format: TupleEncoderFormat[Out]

  @implicitNotFound("Could not find an Encoder for ${T}.")
  trait TupleEncoder[-T] extends Encoder[T] {
    def innerEncoders: Seq[Encoder[_]]
  }
}

trait SeqEncoderType[Out] extends EncoderType[Out] {
  val format: SequenceEncoderFormat[Out]

  class SeqEncoder[T](val innerEncoder: Encoder[T]) extends Encoder[Seq[T]] {
    def encodeTo(stream: Out, seq: Seq[T]) =
      format.writeSequence(stream, seq.size) { sequence =>
        seq foreach { innerEncoder.encodeTo(sequence, _) }
      }
  }

  implicit def seqEncoder[T](implicit enc: Encoder[T]): Encoder[Seq[T]] = new SeqEncoder(enc)
}

trait MapEncoderType[Out] extends EncoderType[Out] {
  val format: MapEncoderFormat[Out]

  class MapEncoder[K, V](val keyEncoder: Encoder[K], val valueEncoder: Encoder[V]) extends Encoder[Map[K, V]] {
    def encodeTo(stream: Out, map: Map[K, V]) =
      format.writeMap(stream, map.size) { stream =>
        for ((key, value) <- map) {
          format.writeKey(stream) { s => keyEncoder.encodeTo(s, key) }
          format.writeValue(stream) { s => valueEncoder.encodeTo(s, value) }
        }
      }
  }

  implicit def mapEncoder[K, V](implicit k: Encoder[K], v: Encoder[V]): Encoder[Map[K, V]] = new MapEncoder(k, v)
}

trait RecordEncoderType[Out] extends EncoderType[Out] {
  val format: RecordEncoderFormat[Out]

  @implicitNotFound("Could not find an Encoder for ${T}.")
  trait RecordEncoder[-T] extends Encoder[T] {
    def keys: Set[String]
    def innerEncoders: Map[String, Encoder[_]]
  }

  trait FieldEncoder[T] {
    def innerEncoder: Encoder[_]
    def encodeTo(stream: Out, t: T): Unit
  }

  class LiftedFieldEncoder[T](val innerEncoder: Encoder[T]) extends FieldEncoder[T] {
    def encodeTo(stream: Out, t: T) = innerEncoder.encodeTo(stream, t)
  }

  class OptionFieldEncoder[T](val innerEncoder: Encoder[T]) extends FieldEncoder[Option[T]] {
    def encodeTo(stream: Out, opt: Option[T]) = opt match {
      case Some(t) => innerEncoder.encodeTo(stream, t)
      case None    => throw new IllegalStateException("Unexpected None")
    }
  }

  trait FieldEncoder_0 {
    implicit def liftedFieldEncoder[T](implicit enc: Encoder[T]): FieldEncoder[T] =
      new LiftedFieldEncoder(enc)
  }

  object FieldEncoder extends FieldEncoder_0 {
    implicit def optionFieldEncoder[T](implicit enc: Encoder[T]): FieldEncoder[Option[T]] =
      new OptionFieldEncoder(enc)
  }
}

trait UnionEncoderType[Out] extends EncoderType[Out] {
  val format: UnionEncoderFormat[Out]

  trait UnionEncoder[-T] extends Encoder[T] {
    val types: Set[format.TypeTag]
    val innerEncoders: Map[format.TypeTag, Encoder[Nothing]]
  }
}

trait EncoderBuilder[Out] {
  val base: EncoderType[Out]

  def Alias[A, T: base.Encoder](unapply: A => Option[T]) = new base.AliasEncoder[A, T] {
    val innerEncoder = implicitly[base.Encoder[T]]
    def encodeTo(stream: Out, a: A) = innerEncoder.encodeTo(stream, unapply(a).get)
  }

  def Lazy[T](enc: => base.Encoder[T]): base.Encoder[T] = new base.AliasEncoder[T, T] {
    lazy val innerEncoder = enc
    def encodeTo(stream: Out, t: T) = innerEncoder.encodeTo(stream, t)
  }
}

trait RecordEncoderBuilder[Out] extends EncoderBuilder[Out]
    with RecordEncoderConstructors[Out] {

  val base: RecordEncoderType[Out]
}

trait UnionEncoderBuilder[Out] extends EncoderBuilder[Out] {
  val base: UnionEncoderType[Out]

  class Summand[+T](val tag: base.format.TypeTag, val cls: Class[_], val enc: base.Encoder[_])

  object Summand {
    implicit def pairToSummand[T](kv: (base.format.TypeTag, base.Encoder[T]))(implicit m: Manifest[T]): Summand[T] =
      new Summand[T](kv._1, m.runtimeClass, kv._2)
  }

  def Union[T](summands: Summand[T]*): base.UnionEncoder[T] =
    new base.UnionEncoder[T] {
      val innerEncoders = summands map { s => s.tag -> s.enc.asInstanceOf[base.Encoder[Nothing]] } toMap
      val types = innerEncoders.keySet

      def encodeTo(stream: Out, t: T) =
        summands find { _.cls.isInstance(t) } match {
          case None => throw new MatchError(s"No sub-encoder for $t")
          case Some(summand) => base.format.writeUnion(stream, summand.tag) { s =>
            summand.enc.asInstanceOf[base.Encoder[T]].encodeTo(s, t)
          }
        }
    }
}
