package fauna.snowflake

import fauna.lang.clocks.Clock
import java.util.concurrent.atomic.AtomicLong

/**
 * Currently it is expected that each ID's most significant 43 bits
 * are a timestamp in milliseconds since IDSource.Epoch
 */
object IDSource {
  val Epoch = 1336258850000L
}

/**
 * Reference IDSource implementation
 *
 * Compact lexical id generator, similar to snowflake or cassie's
 * lexical UUID generator. Capable of generating 1 ID per microsecond
 * per worker (1M IDs/sec). Algorithm exceeds 63 bits (signed 64bit
 * max integer) on September 9th, 2248.
 */
final class IDSource(private[this] val workerID: () => Int, clock: Clock = Clock) {

  // You may add bits to either part, but do not take them away.
  private[this] val sequenceBits = 10L // 0 to 1023, roughly microseconds.
  private[this] val workerIDBits = 10L // 0 to 1023, 1024 workers max.

  val maxSequence = -1L ^ (-1L << sequenceBits)

  private[this] val lastTick = new AtomicLong(0)

  private[this] def tick = {
    var ts = -1L

    while (ts == -1) {
      val last = lastTick.get
      val current = millisSinceEpoch << sequenceBits
      val next = if (current > last) current else last + 1

      // We don't want to race ahead of the clock if the sequence
      // would carry us over into the next millisecond. If this would
      // be the case, sleep in order to context switch, and repeat.
      if (next > (current + maxSequence)) {
        Thread.sleep(0)
      } else if (lastTick.compareAndSet(last, next)) {
        ts = next
      }
    }

    ts
  }

  protected def millisSinceEpoch = clock.time.millis - IDSource.Epoch

  def getID = (tick << workerIDBits) | workerID()
}

