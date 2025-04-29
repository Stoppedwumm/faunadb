package fauna.storage

import language.implicitConversions
import fauna.storage.cassandra.CValue
import fauna.storage.cassandra.comparators._
import io.netty.buffer.Unpooled

object Predicate {
  val EQ: Byte = 0
  val GTE: Byte = 1
  val LTE: Byte = -1

  def apply[T: CassandraEncoder](t: T): Predicate = CassandraCodec.encode(t)

  implicit def encodableToPredicate[T: CassandraEncoder](t: T) = apply(t)
}

final case class Predicate(rowKey: CValue, columnName: Seq[CValue]) {
  @volatile private var _memoizedHash = 0

  override def hashCode: Int = {
    if (_memoizedHash == 0) _memoizedHash = 87589 * rowKey.hashCode * columnName.hashCode
    _memoizedHash
  }

  override def toString = s"Predicate(${(rowKey +: columnName).mkString(", ")})"

  def as[T: CassandraDecoder] = CassandraCodec.decode[T](this)

  def isPrefixOf(o: Predicate) =
    columnName.size <= o.columnName.size && rowKey == o.rowKey &&
      (columnName zip o.columnName forall { case (a, b) => a == b })

  def isPrefixOrGreater(o: Predicate)(implicit ord: Ordering[Predicate]) =
    isPrefixOf(o) || ord.gteq(this, o)

  def isPrefixOrLesser(o: Predicate)(implicit ord: Ordering[Predicate]) =
    isPrefixOf(o) || ord.lteq(this, o)

  def uncheckedRowKeyBytes = rowKey.bytes

  def uncheckedColumnNameBytes = columnName match {
    case Seq()   => Unpooled.EMPTY_BUFFER
    case Seq(cv) => cv.bytes
    case cvs     => CompositeEncoding.encode(cvs map { _.bytes }, Predicate.EQ)
  }
}
