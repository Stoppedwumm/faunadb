package fauna.codex.builder

import language.implicitConversions

trait CodecType[I, O] extends DecoderType[I] with EncoderType[O] {
  val format: DecoderFormat[In] with EncoderFormat[Out]
  override val base: this.type = this

  trait Codec[T] extends Decoder[T] with Encoder[T]

  trait AliasCodec[A, T] extends Codec[A] with AliasDecoder[A, T] with AliasEncoder[A, T]

  class SynthesizedCodec[T](val innerDecoder: Decoder[T], val innerEncoder: Encoder[T]) extends Codec[T] {
    def decode(stream: In) = innerDecoder.decode(stream)
    def encodeTo(stream: Out, t: T) = innerEncoder.encodeTo(stream, t)
  }

  object Codec {
    implicit def synthesizedCodec[T](implicit dec: Decoder[T], enc: Encoder[T]): Codec[T] =
      dec match {
        case dec: Codec[_] if dec eq enc => dec.asInstanceOf[Codec[T]]
        case _                           => new SynthesizedCodec[T](dec, enc)
      }
  }
}

trait NullCodecType[In, Out] extends CodecType[In, Out]
    with NullDecoderType[In]
    with NullEncoderType[Out] {

  val format: NullDecoderFormat[In] with NullEncoderFormat[Out]
}

trait BinaryCodecType[In, Out] extends CodecType[In, Out]
    with BinaryDecoderType[In]
    with BinaryEncoderType[Out] {

  val format: BinaryDecoderFormat[In] with BinaryEncoderFormat[Out]
}

trait TupleCodecType[In, Out] extends CodecType[In, Out]
    with TupleDecoderType[In]
    with TupleEncoderType[Out] {

  val format: TupleDecoderFormat[In] with TupleEncoderFormat[Out]

  trait TupleCodec[T] extends Codec[T] with TupleDecoder[T] with TupleEncoder[T]
}

trait SeqCodecType[In, Out] extends CodecType[In, Out]
    with SeqDecoderType[In]
    with SeqEncoderType[Out] {

  val format: SequenceDecoderFormat[In] with SequenceEncoderFormat[Out]
}

trait MapCodecType[In, Out] extends CodecType[In, Out]
    with MapDecoderType[In]
    with MapEncoderType[Out] {

  val format: MapDecoderFormat[In] with MapEncoderFormat[Out]
}

trait RecordCodecType[In, Out] extends CodecType[In, Out]
    with RecordDecoderType[In]
    with RecordEncoderType[Out] {

  val format: RecordDecoderFormat[In] with RecordEncoderFormat[Out]

  trait RecordCodec[T] extends Codec[T] with RecordDecoder[T] with RecordEncoder[T]
}

trait UnionCodecType[In, Out] extends CodecType[In, Out]
    with UnionDecoderType[In]
    with UnionEncoderType[Out] {

  val format: UnionDecoderFormat[In] with UnionEncoderFormat[Out]

  trait UnionCodec[T] extends Codec[T] with UnionDecoder[T] with UnionEncoder[T]
}

trait CodecBuilder[In, Out] {
  val base: CodecType[In, Out]

  def Alias[A, T](apply: T => A, unapply: A => Option[T])(implicit dec: base.Decoder[T], enc: base.Encoder[T]): base.Codec[A] =
    new base.AliasCodec[A, T] {
      val innerDecoder = dec
      val innerEncoder = enc

      def decode(stream: In) = apply(innerDecoder.decode(stream))
      def encodeTo(stream: Out, a: A) = innerEncoder.encodeTo(stream, unapply(a).get)
    }

  def Lazy[T](codec: => base.Codec[T]): base.Codec[T] = new base.AliasCodec[T, T] {
    lazy val innerDecoder: base.Decoder[T] = codec
    lazy val innerEncoder: base.Encoder[T] = codec

    def decode(stream: In) = innerDecoder.decode(stream)
    def encodeTo(stream: Out, t: T) = innerEncoder.encodeTo(stream, t)
  }
}

trait RecordCodecBuilder[In, Out] extends CodecBuilder[In, Out]
    with RecordCodecConstructors[In, Out] {

  val base: RecordCodecType[In, Out]
}

trait UnionCodecBuilder[In, Out] {
  val base: UnionCodecType[In, Out]

  class Summand[+T](val tag: base.format.TypeTag, val cls: Class[_], val codec: base.Codec[_])

  object Summand {
    implicit def pairToSummand[T](kv: (base.format.TypeTag, base.Codec[T]))(implicit m: Manifest[T]): Summand[T] =
      new Summand[T](kv._1, m.runtimeClass, kv._2)
  }

  def Union[T](summands: Summand[T]*): base.UnionCodec[T] =
    new base.UnionCodec[T] {
      val innerEncoders = summands map { s => s.tag -> s.codec.asInstanceOf[base.Encoder[Nothing]] } toMap
      val innerDecoders = summands map { s => s.tag -> s.codec.asInstanceOf[base.Codec[T]] } toMap
      val types = innerEncoders.keySet

      def decode(stream: In) =
        base.format.readUnion(stream) { (tag, s) => innerDecoders(tag).decode(s) }

      def encodeTo(stream: Out, t: T): Unit =
        summands find { _.cls.isInstance(t) } match {
          case None => throw new MatchError(s"No sub-encoder for $t")
          case Some(summand) => base.format.writeUnion(stream, summand.tag) { s =>
            summand.codec.asInstanceOf[base.Encoder[T]].encodeTo(s, t)
          }
        }
    }
}
