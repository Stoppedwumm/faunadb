package fauna.storage.doc

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.storage.ir._
import io.netty.buffer.ByteBuf
import java.time.LocalDate
import java.util.UUID
import scala.annotation.tailrec

object DataSwitch {
  final object StringSwitch extends PartialDataSwitch[ByteBuf] {
    override def readString(b: ByteBuf, stream: DataStream) = b
  }

  // Returns the number of fields remaining to read from the data stream
  final object VersionDataStartSwitch extends PartialDataSwitch[Long] {
    override def readObjectStart(l: Long, stream: DataStream) = l
    override def readEndOfStream(stream: DataStream) = 0
  }

  final object SkipSwitch extends DataSwitch[Unit] {
    def readTransactionTime(b: Boolean, stream: DataStream) = ()
    def readLong(l: Long, stream: DataStream) = ()
    def readDouble(d: Double, stream: DataStream) = ()
    def readBoolean(b: Boolean, stream: DataStream) = ()
    def readNull(stream: DataStream) = ()
    def readString(s: ByteBuf, stream: DataStream) = ()
    def readBytes(b: ByteBuf, stream: DataStream) = ()
    def readDocID(id: DocID, stream: DataStream) = ()
    def readTime(ts: Timestamp, stream: DataStream) = ()
    def readDate(d: LocalDate, stream: DataStream) = ()
    def readQuery(q: QueryV, stream: DataStream) = ()
    def readUUID(uuid: UUID, stream: DataStream) = ()

    def readArrayStart(length: Long, stream: DataStream) = {
      var i = 0
      while (i < length) {
        stream.read(this)
        i += 1
      }
    }

    def readObjectStart(length: Long, stream: DataStream) = {
      var i = 0
      while (i < length) {
        stream.read(DataSwitch.StringSwitch)
        stream.read(this)
        i += 1
      }
    }
  }
}

trait DataSwitch[+R] {
  def readTransactionTime(isMicros: Boolean, stream: DataStream): R
  def readLong(l: Long, stream: DataStream): R
  def readDouble(d: Double, stream: DataStream): R
  def readBoolean(b: Boolean, stream: DataStream): R
  def readNull(stream: DataStream): R
  def readString(s: ByteBuf, stream: DataStream): R
  def readBytes(b: ByteBuf, stream: DataStream): R
  def readDocID(id: DocID, stream: DataStream): R
  def readTime(ts: Timestamp, stream: DataStream): R
  def readDate(d: LocalDate, stream: DataStream): R
  def readQuery(q: QueryV, stream: DataStream): R
  def readArrayStart(length: Long, stream: DataStream): R
  def readObjectStart(length: Long, stream: DataStream): R
  def readUUID(uuid: UUID, stream: DataStream): R

  def readEndOfStream(stream: DataStream): R = throw new NoSuchElementException("End of stream.")
}

case class DataUnexpectedTypeException(provided: String) extends Exception(s"Unexpected token of type $provided")

case object FailingDataSwitch extends DataSwitch[Nothing] {
  private def error(provided: String) = throw DataUnexpectedTypeException(provided)

  def readTransactionTime(isMicros: Boolean, stream: DataStream) = error("Unresolved Transaction Time")
  def readLong(l: Long, stream: DataStream) = error("Long")
  def readDouble(d: Double, stream: DataStream) = error("Double")
  def readBoolean(b: Boolean, stream: DataStream) = error("Boolean")
  def readNull(stream: DataStream) = error("Null")
  def readString(s: ByteBuf, stream: DataStream) = error("String")
  def readBytes(b: ByteBuf, stream: DataStream) = error("Bytes")
  def readDocID(id: DocID, stream: DataStream) = error("DocID")
  def readTime(ts: Timestamp, stream: DataStream) = error("Time")
  def readDate(d: LocalDate, stream: DataStream) = error("Data")
  def readQuery(q: QueryV, stream: DataStream) = error("Query")
  def readArrayStart(length: Long, stream: DataStream) = error("Array")
  def readObjectStart(length: Long, stream: DataStream) = error("Object")
  def readUUID(uuid: UUID, stream: DataStream) = error("UUID")
}

trait DelegatingDataSwitch[I, R] extends DataSwitch[R] {
  val delegate: DataSwitch[I]

  def transform(i: I): R

  def readTransactionTime(b: Boolean, stream: DataStream) = transform(delegate.readTransactionTime(b, stream))
  def readLong(l: Long, stream: DataStream) = transform(delegate.readLong(l, stream))
  def readDouble(d: Double, stream: DataStream) = transform(delegate.readDouble(d, stream))
  def readBoolean(b: Boolean, stream: DataStream) = transform(delegate.readBoolean(b, stream))
  def readNull(stream: DataStream) = transform(delegate.readNull(stream))
  def readString(s: ByteBuf, stream: DataStream) = transform(delegate.readString(s, stream))
  def readBytes(b: ByteBuf, stream: DataStream) = transform(delegate.readBytes(b, stream))
  def readDocID(id: DocID, stream: DataStream) = transform(delegate.readDocID(id, stream))
  def readTime(ts: Timestamp, stream: DataStream) = transform(delegate.readTime(ts, stream))
  def readDate(d: LocalDate, stream: DataStream) = transform(delegate.readDate(d, stream))
  def readQuery(q: QueryV, stream: DataStream) = transform(delegate.readQuery(q, stream))
  def readArrayStart(length: Long, stream: DataStream) = transform(delegate.readArrayStart(length, stream))
  def readObjectStart(length: Long, stream: DataStream) = transform(delegate.readObjectStart(length, stream))
  def readUUID(uuid: UUID, stream: DataStream) = transform(delegate.readUUID(uuid, stream))
}

abstract class PartialDataSwitch[R] extends DelegatingDataSwitch[R, R] {
  val delegate = FailingDataSwitch
  def transform(r: R) = r
}

object DataStream {
  private final object EmptyStream extends DataStream {
    def read[T](switch: DataSwitch[T]): T = switch.readEndOfStream(this)
  }

  val empty: DataStream = EmptyStream
}

sealed abstract class DataStream {
  def read[T](switch: DataSwitch[T]): T
}

final case class CBORDataStream(parser: CBORParser) extends DataStream { self =>
  def read[T](switch: DataSwitch[T]): T = parser.read(new SwitchAdaptor(switch))

  private final class SwitchAdaptor[R](val switch: DataSwitch[R]) extends CBORSwitch[R] {
    def readInt(int: Long, stream: CBORParser) = switch.readLong(int, self)
    def readBoolean(bool: Boolean, stream: CBORParser) = switch.readBoolean(bool, self)
    def readNil(stream: CBORParser) = switch.readNull(self)
    def readFloat(float: Float, stream: CBORParser) = switch.readDouble(float, self)
    def readDouble(double: Double, stream: CBORParser) = switch.readDouble(double, self)
    def readBigNum(num: BigInt, stream: CBORParser) = switch.readLong(num.toLong, self)
    def readTimestamp(ts: Timestamp, stream: CBORParser) = switch.readTime(ts, self)
    def readBytes(bytes: ByteBuf, stream: CBORParser) = switch.readBytes(bytes, self)
    def readString(str: ByteBuf, stream: CBORParser) = switch.readString(str, self)

    def readArrayStart(length: Long, stream: CBORParser) = switch.readArrayStart(length, self)
    def readMapStart(length: Long, stream: CBORParser) = switch.readObjectStart(length, self)

    def readTag(tag: Long, stream: CBORParser) =
      (tag.toInt: @annotation.switch) match {
        case CBOR.UUIDTag =>
          switch.readUUID(CBOR.UUIDCodec.decode(stream), self)

        case CBOR.DocIDTag =>
          switch.readDocID(DocID.fromBytes(CBOR.ByteArrayCodec.decode(stream)), self)

        // FIXME: we should drop this in lieu of readTimestamp, except
        // that index data is moved around as a result. See ticket ###
        case CBOR.EpochNanosTag =>
          val len = stream.read(CBORParser.ArrayStartSwitch)
          assert(len == 2)
          val secs = CBOR.LongCodec.decode(stream)
          val nanos = CBOR.LongCodec.decode(stream)
          switch.readTime(Timestamp(secs, nanos), self)

        case CBOR.EpochDaysTag =>
          val days = CBOR.LongCodec.decode(stream)
          switch.readDate(LocalDate.ofEpochDay(days), self)

        case CBOR.QueryTag =>
          switch.readQuery(IRValue.CBORCodec.decodeQuery(stream), self)

        case CBOR.TransactionTimeTag =>
          val isMicros = CBOR.BooleanCodec.decode(stream)
          switch.readTransactionTime(isMicros, self)

        case _ => error(TypeLabels.TagLabel)
      }

    override def readEndOfStream(stream: CBORParser) = switch.readEndOfStream(self)

    protected def error(provided: String) =
      throw CBORUnexpectedTypeException(provided)
  }
}

final case class IRValueDataStream(start: IRValue) extends DataStream {
  private case class Elems(iter: Iterator[IRValue])
  private case class Fields(iter: Iterator[(String, IRValue)])

  private[this] var stack: List[Any] = List(start)

  def nonEmpty = stack.nonEmpty

  def isEmpty = stack.isEmpty

  @tailrec
  def read[T](switch: DataSwitch[T]): T =
    stack match {
      case Nil => switch.readEndOfStream(this)

      case (r: IRValue) :: tail =>
        stack = tail
        readResult(r, switch)

      case Elems(iter) :: tail =>
        if (iter.isEmpty) {
          stack = tail
          read(switch)
        } else {
          readResult(iter.next(), switch)
        }

      case Fields(iter) :: tail =>
        if (iter.isEmpty) {
          stack = tail
          read(switch)
        } else {
          val (k, v) = iter.next()
          stack = v :: stack
          switch.readString(k.toUTF8Buf, this)
        }

      case v :: _ => throw new AssertionError(s"Unexpected stack value $v")
    }

  private def readResult[T](ir: IRValue, switch: DataSwitch[T]): T =
    ir match {
      case TransactionTimeV(b)      => switch.readTransactionTime(b, this)
      case LongV(l)                 => switch.readLong(l, this)
      case DoubleV(d)               => switch.readDouble(d, this)
      case TrueV                    => switch.readBoolean(true, this)
      case FalseV                   => switch.readBoolean(false, this)
      case NullV                    => switch.readNull(this)
      case StringV(s)               => switch.readString(s.toUTF8Buf, this)
      case BytesV(b)                => switch.readBytes(b, this)
      case TimeV(ts)                => switch.readTime(ts, this)
      case DateV(d)                 => switch.readDate(d, this)
      case DocIDV(id)               => switch.readDocID(id, this)
      case UUIDV(id)                => switch.readUUID(id, this)

      case ArrayV(elems) =>
        stack = Elems(elems.iterator) :: stack
        switch.readArrayStart(elems.length, this)

      case MapV(elems) =>
        stack = Fields(elems.iterator) :: stack
        switch.readObjectStart(elems.length, this)

      case q: QueryV =>
        switch.readQuery(q, this)
    }
}
