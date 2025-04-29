package fauna.storage.cassandra.comparators

import fauna.storage._
import fauna.storage.cassandra.CValue
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

object CompositeEncoding {
  val CValueClassTag: ClassTag[CValue] = ClassTag(classOf[CValue])
  val ByteBufClassTag: ClassTag[ByteBuf] = ClassTag(classOf[ByteBuf])

  def encode(components: Seq[ByteBuf], op: Byte): ByteBuf = {
    val buf = Unpooled.buffer
    val iter = components.iterator

    while (iter.hasNext) {
      val c = iter.next()
      val eoc = if (iter.hasNext) Predicate.EQ else op

      buf.writeShort(c.readableBytes.toShort)
      buf.writeBytes(c.duplicate)
      buf.writeByte(eoc)
    }

    buf
  }

  def compare(a: ByteBuf, b: ByteBuf, schema: Vector[BasicComparator]): Int = {
    val a0 = a.duplicate
    val b0 = b.duplicate
    val size = schema.size
    var i = 0
    var rv = 0

    while (rv == 0 && i < size && a0.isReadable && b0.isReadable) {
      rv = schema(i).compare(readComponent(a0), readComponent(b0))
      a0.readByte
      b0.readByte
      i += 1
    }

    rv
  }

  def readComponent(buf: ByteBuf): ByteBuf =
    buf.readSlice(buf.readUnsignedShort)
}

case class CompositeType(schema: Vector[BasicComparator]) extends Comparator {
  private lazy val schemaSize = schema.size
  lazy val comparator = s"CompositeType(${schema mkString ","})"

  private def throwSchemaViolation =
    throw SchemaViolationException(s"Value does not match comparator $comparator")

  def compare0(a: ByteBuf, b: ByteBuf) = CompositeEncoding.compare(a, b, schema)

  def matches(other: Comparator) =
    other match {
      case CompositeType(otherSchema) =>
        schema.iterator.zip(otherSchema) forall { case (a, b) => a matches b }
      case _ => false
    }

  def canEncodeValue(components: Seq[CValue], compareOp: Byte): Boolean = {
    val componentIter = components.iterator
    val schemaIter = schema.iterator

    while (componentIter.hasNext) {
      if (!schemaIter.hasNext) return false

      val c = componentIter.next()
      val s = schemaIter.next()

      if (!s.canEncodeValue(c)) return false
    }

    true
  }

  def cvaluesToBytes(components: Seq[CValue], compareOp: Byte): ByteBuf = {
    val buf = Unpooled.buffer
    val componentIter = components.iterator
    val schemaIter = schema.iterator

    while (componentIter.hasNext) {
      if (!schemaIter.hasNext) throwSchemaViolation

      val c = componentIter.next()
      val s = schemaIter.next()

      if (!s.canEncodeValue(c)) throwSchemaViolation

      val eoc = if (componentIter.hasNext) Predicate.EQ else compareOp

      buf.writeShort(c.bytes.readableBytes.toShort)
      buf.writeBytes(c.bytes.duplicate)
      buf.writeByte(eoc)
    }

    buf
  }

  def bytesToCValues(buf: ByteBuf): Seq[CValue] = {
    // go only once through the lazy val
    val _schemaSize = schemaSize

    val dup = buf.duplicate()

    val res = new Array[CValue](_schemaSize)
    var i = 0
    while (i < _schemaSize && dup.isReadable) {
      res(i) = CValue(schema(i), CompositeEncoding.readComponent(dup))
      dup.readByte() // consume op
      i += 1
    }

    if (dup.isReadable) {
      throw SchemaConfigurationException(
        s"Too many composite value segments to decode $comparator")
    } else if (i == _schemaSize) {
      ArraySeq.unsafeWrapArray(res)
    } else {
      ArraySeq.unsafeWrapArray(res.slice(0, i))
    }
  }

  def show(buf: ByteBuf) = {
    val bufString = bytesToCValues(buf).iterator map { _.toString } mkString ", "
    s"CompositeType($bufString)"
  }
}
