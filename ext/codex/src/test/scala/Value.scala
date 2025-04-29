package fauna.codex.test

import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.prop.Prop
import io.netty.buffer.ByteBuf
import scala.util.Random

object Value {
  implicit object Codec extends CBOR.SwitchCodec[Value] {
    def encode(stream: CBORWriter, value: Value): CBORWriter =
      value match {
        case i: IntV     => stream.writeInt(i.value)
        case FalseV      => stream.writeBoolean(false)
        case TrueV       => stream.writeBoolean(true)
        case NilV        => stream.writeNil()
        case f: FloatV   => stream.writeFloat(f.value)
        case d: DoubleV  => stream.writeDouble(d.value)
        case n: BigIntV  => stream.writeBigNum(n.value)
        case t: TimeV    => stream.writeTimestamp(t.value)
        case b: ByteStrV => stream.writeBytes(b.value)
        case s: StrV     => stream.writeString(s.value)

        case a: ArrayV =>
          stream.writeArrayStart(a.values.size)
          a.values foreach { encode(stream, _) }
          stream

        case m: MapV =>
          stream.writeMapStart(m.values.size)
          m.values foreach { case (k, v) => encode(stream, k); encode(stream, v) }
          stream

        case t: TagV =>
          stream.writeTag(t.tag)
          encode(stream, t.value)
      }

    // CBORSwitch

    def readInt(l: Long, stream: CBORParser) = IntV(l)

    def readBoolean(b: Boolean, stream: CBORParser) = if (b) TrueV else FalseV

    def readNil(stream: CBORParser) = NilV

    def readFloat(f: Float, stream: CBORParser) = FloatV(f)

    def readDouble(f: Double, stream: CBORParser) = DoubleV(f)

    def readBigNum(n: BigInt, stream: CBORParser) = BigIntV(n)

    def readTimestamp(ts: Timestamp, stream: CBORParser) = TimeV(ts)

    def readBytes(b: ByteBuf, stream: CBORParser) =
      ByteStrV(stream.bufRetentionPolicy.read(b))

    def readString(b: ByteBuf, stream: CBORParser) =
      StrV(stream.bufRetentionPolicy.read(b))

    def readArrayStart(length: Long, stream: CBORParser) = {
      val b = Vector.newBuilder[Value]
      b.sizeHint(length.toInt)
      for (_ <- 0L until length) b += stream.read(this)
      ArrayV(b.result())
    }

    def readMapStart(length: Long, stream: CBORParser) = {
      val b = Vector.newBuilder[(Value, Value)]
      b.sizeHint(length.toInt)
      for (_ <- 0L until length) b += (stream.read(this) -> stream.read(this))
      MapV(b.result())
    }

    def readTag(tag: Long, stream: CBORParser) = TagV(tag, stream.read(this))
  }

  private def randomValues[T](minSize: Int, maxSize: Int, maxDepth: Int)(mk: Int => Prop[T]): Prop[List[T]] =
    Prop.int(minSize to maxSize) flatMap { length =>
      Prop.int(maxDepth) flatMap { depth =>
        val vals = 0 until length map { _ =>
          mk(depth - 1)
        }

        vals.foldRight(Prop.const(List.empty[T])) {
          case (vp, lp) =>
            lp flatMap { l =>
              vp map { v =>
                v +: l
              }
            }
        }
      }
    }

  def random(
    minSize: Int = 0,
    maxSize: Int = 128,
    maxDepth: Int = 5): Prop[Value] = {

    def value0(choice: Int): Prop[Value] =
      choice match {
        case 0 => Prop.const(NilV)
        case 1 => Prop.long map { IntV(_) }
        case 2 => Prop.double map { DoubleV(_) }
        case 3 => Prop.float map { FloatV(_) }
        case 4 => Prop.string(minSize, maxSize) map { StrV(_) }
        case 5 => Prop.string(minSize, maxSize) map { ByteStrV(_) }
        case 6 => Prop.long map { l => TimeV(Timestamp.ofMicros(l)) }
        case 7 =>
          Prop.long flatMap { tag =>
            random(minSize, maxSize, maxDepth) map { v =>
              TagV(tag, v)
            }
          }
        case 8 =>
          Prop.boolean map {
            if (_) {
              TrueV
            } else {
              FalseV
            }
          }
        case 9 =>
          Prop.int(128) map { bits =>
            BigIntV(BigInt(bits, Random))
          }
        case 10 =>
          val prop = randomValues(minSize, maxSize, maxDepth) {
            random(minSize, maxSize, _)
          }

          prop map { vs => ArrayV(vs.toVector) }
        case 11 =>
          val prop = randomValues(minSize, maxSize, maxDepth) { depth =>
            random(minSize, maxSize, depth) flatMap { key =>
              random(minSize, maxSize, depth) map { (key, _) }
            }
          }
          prop map { ps => MapV(ps.toVector) }
      }

    val max = if (maxDepth <= 0) { // scalars only
      9
    } else {
      11
    }

    Prop.int(0 to max) flatMap { value0(_) }
  }

}

sealed abstract class Value

case object TrueV extends Value
case object FalseV extends Value
case object NilV extends Value
case class TagV(tag: Long, value: Value) extends Value
case class IntV(value: Long) extends Value
case class BigIntV(value: BigInt) extends Value
case class TimeV(value: Timestamp) extends Value
case class FloatV(value: Float) extends Value
case class DoubleV(value: Double) extends Value
case class ArrayV(values: Vector[Value]) extends Value
case class MapV(values: Vector[(Value, Value)]) extends Value

object ByteStrV {
  def apply(s: String): ByteStrV = ByteStrV(s.toUTF8Buf)
}
case class ByteStrV(value: ByteBuf) extends Value

object StrV {
  def apply(s: String): StrV = StrV(s.toUTF8Buf)
}
case class StrV(value: ByteBuf) extends Value
