package fauna.tx.transaction

import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp

object Epoch {
  val MillisPerEpoch = 10
  val MicrosPerEpoch = MillisPerEpoch * 1000
  val NanosPerEpoch = MicrosPerEpoch * 1000

  val MinValue = Epoch(0)
  val MaxValue = Epoch(Long.MaxValue)

  def apply(ts: Timestamp): Epoch =
    Epoch((ts.seconds * 1000 / MillisPerEpoch) + (ts.nanoOffset / NanosPerEpoch))

  implicit val CBORCodec = CBOR.TupleCodec[Epoch]
}

case class Epoch(idx: Long) extends AnyVal with Ordered[Epoch] {
  import Epoch._

  def compare(o: Epoch) = java.lang.Long.compare(idx, o.idx)

  def +(l: Long) = Epoch(idx + l) max MinValue
  def -(l: Long) = this + (l * -1)

  def min(o: Epoch) = if (idx < o.idx) this else o
  def max(o: Epoch) = if (idx >= o.idx) this else o

  override def >(o: Epoch): Boolean = idx > o.idx
  override def >=(o: Epoch): Boolean = idx >= o.idx
  override def <(o: Epoch): Boolean = idx < o.idx
  override def <=(o: Epoch): Boolean = idx <= o.idx

  // The smallest timestamp in the epoch's bounds (inclusive)
  def floorTimestamp = Timestamp.ofMillis(idx * MillisPerEpoch)

  // The largest timestamp in the epoch's bounds (inclusive)
  def ceilTimestamp = Timestamp.ofMillis((idx + 1) * MillisPerEpoch).prevNano

  def timestamp(txn: Int, txnCount: Int) =
    Timestamp(baseSeconds, baseNanoOffset + txOffset(txn, txnCount))

  private def baseSeconds: Long = idx * MillisPerEpoch / 1000

  private def baseNanoOffset: Long = (idx * MillisPerEpoch % 1000) * 1_000_000

  private def txOffset(txn: Int, txnCount: Int): Long = {
    require(txnCount <= NanosPerEpoch, s"Cannot have more than ${NanosPerEpoch} transactions per epoch.")
    require(txnCount > 0)
    val p = precision(txnCount)
    NanosPerEpoch / txnCount * txn / p * p
  }

  private def precision(num: Int) = num match {
    case n if n <= 10        => 1_000_000
    case n if n <= 100       => 100_000
    case n if n <= 1000      => 10_000
    case n if n <= 10_000    => 1000
    case n if n <= 100_000   => 100
    case n if n <= 1_000_000 => 10
    case _                   => 1
  }
}
