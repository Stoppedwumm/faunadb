package fauna.repo.cache

import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }
import org.github.jamm._
import scala.concurrent.Promise

/**
 * A generic weigher for cached AtomicReference values which uses MemoryMeter
 */
class MemoryMeterWeigher(accuracy: Int) extends ((AnyRef, AtomicReference[Any]) => Int) {
  private val meter = MemoryMeter.builder()
    .omitSharedBufferOverhead
    .withGuessing(MemoryMeter.Guess.BEST)
    .build()

  private val counter = new AtomicInteger(0)

  private var size: Int = 0

  def apply(key: AnyRef, cv: AtomicReference[Any]): Int = {
    // A cheaper strategy than Cassandra's Memtable by not correcting errors over time
    if (counter.getAndIncrement % accuracy == 0) {
      // NB. MemoryMeter hangs indefinitely when measuring Value, but
      // will happily measure its fields...
      val valSize = cv.get() match {
        case _: Promise[_] => 0
        case v => meter.measureDeep(v)
        }
      size = (meter.measure(key) + valSize).toInt
    }
    size
  }
}
