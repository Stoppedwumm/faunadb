package fauna.atoms

import fauna.codex.cbor._
import java.lang.{ Long => JLong }

case class InvalidClassIDException(msg: String) extends Exception(msg)
case class SerializationException(message: String) extends Exception(message)

object SubID {
  val MaxValue = apply(Long.MaxValue)
  val MinValue = apply(0)

  implicit val CBORCodec = CBOR.TupleCodec[SubID]
}

final case class SubID(toLong: Long) extends AnyVal

final case class DocID(subID: SubID, collID: CollectionID) extends Ordered[DocID] {

  def compare(other: DocID) =
    JLong.compare(subID.toLong, other.subID.toLong) match {
      case 0   => JLong.compare(collID.toLong, other.collID.toLong)
      case cmp => cmp
    }

  def asOpt[T](implicit tag: CollectionIDTag[T]) = tag.fromDocID(this)

  def as[T](implicit tag: CollectionIDTag[T]) = tag.fromDocID(this) getOrElse {
    throw InvalidClassIDException(s"Expected ${tag.collID} for $this")
  }

  def is[T](implicit tag: CollectionIDTag[T]) = asOpt[T].isDefined

  override def toString = s"DocID(${subID.toLong},C${collID.toLong})"

  override def hashCode = subID.hashCode
}

object DocID {
  val BytesSize = 10

  val MaxValue = DocID(SubID.MaxValue, CollectionID.MaxValue)
  val MinValue = DocID(SubID.MinValue, CollectionID.MinValue)

  implicit val CBORCodec = CBOR.AliasCodec(DocID.fromBytes, DocID.toBytes)

  def fromBytes(a: Array[Byte]): DocID = {
    if (a.length != BytesSize) throw SerializationException("Invalid DocID bytes.")

    def mv(b: Byte, offset: Int) = ((b: Long) & 0xff) << offset

    val subID =
      mv(a(0), 56) |
        mv(a(1), 48) |
        mv(a(2), 40) |
        mv(a(3), 32) |
        mv(a(4), 24) |
        mv(a(5), 16) |
        mv(a(6), 8) |
        mv(a(7), 0)

    val collID = mv(a(8), 8) | mv(a(9), 0)

    DocID(SubID(subID), CollectionID(collID))
  }

  def toBytes(id: DocID) = {
    val DocID(SubID(subID), CollectionID(collID)) = id
    val arr = new Array[Byte](10)

    arr(0) = (subID >> 56).toByte
    arr(1) = (subID >> 48).toByte
    arr(2) = (subID >> 40).toByte
    arr(3) = (subID >> 32).toByte
    arr(4) = (subID >> 24).toByte
    arr(5) = (subID >> 16).toByte
    arr(6) = (subID >> 8).toByte
    arr(7) = (subID >> 0).toByte
    arr(8) = (collID >> 8).toByte
    arr(9) = (collID >> 0).toByte

    arr
  }
}
