package benchmarks.codex.json

import fauna.codex.json2.JSON
import fauna.codex.json2.JSONParser
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.collection.immutable.ArraySeq
import scala.collection.SeqMap
import scala.util.Random

trait UndecidedValue

object UndecidedValue {
  final case class Object(values: SeqMap[String, UndecidedValue])
      extends UndecidedValue
  final case class Array(values: Seq[UndecidedValue]) extends UndecidedValue
  final case class Int(value: Long) extends UndecidedValue
  final case class Float(value: Double) extends UndecidedValue
  final case class Str(value: String) extends UndecidedValue
  final case class Bool(value: Boolean) extends UndecidedValue
  final case class Null() extends UndecidedValue

  implicit object UndecidedValueDecoder extends JSON.SwitchDecoder[UndecidedValue] {
    def readInt(int: Long, stream: JSON.In) = UndecidedValue.Int(int)
    def readBoolean(bool: Boolean, stream: JSON.In) = UndecidedValue.Bool(bool)
    def readNull(stream: JSON.In) = UndecidedValue.Null()
    def readDouble(double: Double, stream: JSON.In) = UndecidedValue.Float(double)
    def readString(str: String, stream: JSON.In) = UndecidedValue.Str(str)

    def readArrayStart(stream: JSONParser): UndecidedValue = {
      val b = ArraySeq.newBuilder[UndecidedValue]
      while (!stream.skipArrayEnd) { b += decode(stream) }
      UndecidedValue.Array(b.result())
    }

    def readObjectStart(stream: JSONParser): UndecidedValue = {
      val b = SeqMap.newBuilder[String, UndecidedValue]
      while (!stream.skipObjectEnd) {
        b += (stream.read(JSONParser.ObjectFieldNameSwitch) -> decode(stream))
      }
      UndecidedValue.Object(b.result())
    }
  }
}

object JSONDecodeBench {
  val validBuffers =
    (0 to 16) map { n =>
      val kbytes = math.pow(2, n).toInt
      val buffer = genValidBuff(bytes = kbytes * 1024)
      kbytes.toString -> buffer
    } toMap
  val invalidBuffers =
    (0 to 16) map { n =>
      val kbytes = math.pow(2, n).toInt
      val buffer = genInvalidBuff(bytes = kbytes * 1024)
      kbytes.toString -> buffer
    } toMap

  private def genValidBuff(bytes: Int): ByteBuf = {
    val buf = Vector.newBuilder[Byte]
    buf += '"'
    Random.nextBytes(bytes).foreach { byte =>
      byte match {
        case '"' | '\\' =>
          buf += '\\'
          buf += byte
        case v if v > 60 && v < 65 => buf += v
        case _                     => buf += '?'
      }
    }
    buf += '"'
    Unpooled.wrappedBuffer(buf.result().toArray).asReadOnly()
  }
  private def genInvalidBuff(bytes: Int): ByteBuf = {
    Unpooled.wrappedBuffer(Random.nextBytes(bytes)).asReadOnly()
  }
}

@Fork(1)
@State(Scope.Benchmark)
class ValidJSONDecodeBench {
  import JSONDecodeBench._

  @Param(Array("1024", "8192", "32768", "65536"))
  var size = ""

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @BenchmarkMode(Array(Mode.AverageTime))
  @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
  @Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
  def data() = {
    val buf = validBuffers(size)
    buf.readerIndex(0)
    val _ = JSON.decode[UndecidedValue](buf)
  }
}

@Fork(1)
@State(Scope.Benchmark)
class InvalidJSONDecodeBench {
  import JSONDecodeBench._

  @Param(Array("1024", "8192", "32768", "65536"))
  var size = ""

  @Benchmark
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @BenchmarkMode(Array(Mode.AverageTime))
  @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
  @Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
  def data() = {
    val buf = invalidBuffers(size)
    buf.readerIndex(0)
    try {
      val _ = JSON.decode[UndecidedValue](buf)
    } catch {
      case _: Throwable =>
    }
  }
}
