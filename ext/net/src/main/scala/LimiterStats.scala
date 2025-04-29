package fauna.net

import fauna.codex.json2._
import java.util.concurrent.atomic.LongAdder

object LimiterStats {
  final val ReadField = JSON.Escaped("read")
  final val WriteField = JSON.Escaped("write")
  final val ComputeField = JSON.Escaped("compute")
  final val StreamField = JSON.Escaped("stream")

  val empty = LimiterStats(0, 0, 0, 0)

  def apply(
    readCount: Long,
    writeCount: Long,
    computeCount: Long,
    streamCount: Long): LimiterStats = {
    require(
      readCount >= 0 && writeCount >= 0 && computeCount >= 0 && streamCount >= 0)
    new LimiterStats(readCount, writeCount, computeCount, streamCount)
  }
}

/** A snapshot of cumulative acquisition counts. */
final class LimiterStats private (
  val readCount: Long,
  val writeCount: Long,
  val computeCount: Long,
  val streamCount: Long) {
  import LimiterStats._

  def nonEmpty: Boolean =
    readCount != 0 || writeCount != 0 || computeCount != 0 || streamCount != 0

  def minus(other: LimiterStats): LimiterStats =
    LimiterStats(
      (readCount - other.readCount) max 0,
      (writeCount - other.writeCount) max 0,
      (computeCount - other.computeCount) max 0,
      (streamCount - other.streamCount) max 0)

  override def toString(): String =
    s"LimiterStats(read=$readCount, write=$writeCount, compute=$computeCount, stream=$streamCount)"

  def toJSON(out: JSON.Out): JSON.Out = {
    out.writeObjectStart()

    out.writeObjectField(ReadField, out.writeNumber(readCount))
    out.writeObjectField(WriteField, out.writeNumber(writeCount))
    out.writeObjectField(ComputeField, out.writeNumber(computeCount))
    out.writeObjectField(StreamField, out.writeNumber(streamCount))

    out.writeObjectEnd()
    out
  }

  override def equals(other: Any): Boolean =
    other match {
      case o: LimiterStats =>
        readCount == o.readCount &&
        writeCount == o.writeCount &&
        computeCount == o.computeCount &&
        streamCount == o.streamCount
      case _ => false
    }
}

final class LimiterCounter {
  private[this] val readCount = new LongAdder()
  private[this] val writeCount = new LongAdder()
  private[this] val computeCount = new LongAdder()
  private[this] val streamCount = new LongAdder()

  def recordRead(permits: Int): Unit =
    readCount.add(permits)

  def recordWrite(permits: Int): Unit =
    writeCount.add(permits)

  def recordCompute(permits: Int): Unit =
    computeCount.add(permits)

  def recordStream(permits: Int): Unit =
    streamCount.add(permits)

  def snapshot(): LimiterStats =
    LimiterStats(
      negativeToMax(readCount.sum()),
      negativeToMax(writeCount.sum()),
      negativeToMax(computeCount.sum()),
      negativeToMax(streamCount.sum()))

  def snapshotAndReset(): LimiterStats = {
    val stats = snapshot()

    readCount.reset()
    writeCount.reset()
    computeCount.reset()
    streamCount.reset()

    stats
  }

  // This provides a sort of saturated add on top of an overflowing
  // LongAdder so long as it may only overflow once.
  private def negativeToMax(l: Long): Long =
    if (l >= 0) {
      l
    } else {
      Long.MaxValue
    }

}
