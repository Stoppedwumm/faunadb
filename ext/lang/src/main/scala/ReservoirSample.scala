package fauna.lang

import fauna.lang.clocks._
import java.util.{
  Arrays,
  Collection,
  Comparator
}
import java.util.concurrent.{
  ConcurrentSkipListMap,
  ThreadLocalRandom
}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.concurrent.duration._

object ReservoirSample {
  case class Sample(point: Long, weight: Double)

  object Snapshot {
    def apply(samples: Collection[Sample]): Snapshot = {
      val copy = samples.toArray(new Array[Sample](0))

      Arrays.parallelSort(copy, Comparator.comparingLong { s: Sample => s.point })

      val sum = copy.foldLeft(0.0) { (sum, sample) =>
        sum + sample.weight
      }

      val weights = if (sum != 0.0) {
        copy map { sample =>
          sample.weight / sum
        }
      } else {
        new Array[Double](copy.length)
      }

      val quantiles = new Array[Double](copy.length)

      for (i <- 1 until copy.length) {
        quantiles(i) = quantiles(i - 1) + weights(i - 1)
      }

      new Snapshot(copy map { _.point }, weights, quantiles)
    }
  }

  /**
    * An immutable snapshot of the reservoir at a point in time.
    */
  final class Snapshot(
    val values: Array[Long],
    weights: Array[Double],
    quantiles: Array[Double]) {

    def size: Int = values.length

    def min: Long =
      if (values.isEmpty) {
        0
      } else {
        values.head
      }

    def max: Long =
      if (values.isEmpty) {
        0
      } else {
        values.last
      }

    def mean: Double =
      if (values.isEmpty) {
        0
      } else {
        var sum = 0.0
        for (i <- 0 until size) {
          sum += values(i) * weights(i)
        }
        sum
      }

    def median: Long =
      valueAt(0.5)

    def valueAt(quantile: Double): Long = {
      require(quantile >= 0.0 && quantile <= 1.0 && !quantile.isNaN)

      if (values.isEmpty) {
        0
      } else {
        val pos = Arrays.binarySearch(quantiles, quantile)

        if (pos < 0) {
          // find the quantile 1 position lower than the insertion
          // point.
          //
          // if binary search returns a negative value, it will be
          // <= -2, because quantiles(0) == 0.0 and quantile >= 0.
          values(((-pos) - 1) - 1)
        } else {
          values(pos)
        }
      }
    }

    /**
      * Median absolute deviation (MAD) is a more robust measure of
      * variance than std. deviation, because it does not square each
      * points difference from the mean (which amplifies the effect of
      * outliers).
      */
    def medianAbsoluteDeviation: Long =
      if (size <= 1) {
        0
      } else {
        val m = median
        val deltas = values map { i =>
          Math.abs(i - m)
        }

        Arrays.parallelSort(deltas)

        if (size % 2 == 1) {
          deltas(size / 2)
        } else {
          val mid = size / 2
          (deltas(mid - 1) + deltas(mid)) / 2
        }
      }

    def stddev: Double =
      Math.sqrt(variance)

    def variance: Double = {
      if (size <= 1) {
        0
      } else {
        val m = mean
        var v = 0.0

        for (i <- 0 until size) {
          val diff = values(i) - m
          v += weights(i) * diff * diff
        }

        v
      }
    }
  }

}

/**
  * Reservoir sampler with exponential forward decay.
  *
  * Cormode et al. Forward Decay: A Practical Time Decay Model for
  * Streaming Systems.
  *
  * Duffield et al. Priority sampling for estimation of arbitrary
  * subset sums
  */
final class ReservoirSample(
  capacity: Int,
  alpha: Double,
  clock: DeadlineClock = SystemDeadlineClock) {

  import ReservoirSample._

  /**
    * The interval at which samples are rescaled to keep decay factors
    * within the capacity of standard numerical types. See rescale().
    *
    * For alpha = 1.0, weight will overflow at a time delta of ~700
    * seconds.
    */
  val RescaleThreshold = 10.minutes

  private[this] val values = new ConcurrentSkipListMap[Double, Sample]
  private[this] val lock = new ReentrantReadWriteLock()
  private[this] val count = new AtomicLong(0)
  private[this] val nextScale = new AtomicLong(clock.nanos + RescaleThreshold.toNanos)

  @volatile private[this] var landmark = clock.seconds

  require(alpha > 0.0 && alpha <= 1.0, "alpha must be in (0.0, 1.0]")

  def size: Int = Math.min(capacity, count.get).toInt

  def sample(point: Long): Unit = {
    maybeRescale()
    lock.readLock.lock()
    try {
      val ts = clock.seconds
      val weight = Math.exp(alpha * (ts - landmark))
      val sample = Sample(point, weight)

      // "Priority sampling is simple to define and implement: we
      // consider a stream of items i = 0, ..., n−1 with weights Wi.
      // For each item i, we generate a random number Ai in (0, 1] and
      // create a priority Qi = Wi/Ai. The sample S consists of the k
      // highest priority items."
      val priority = weight / (1 - ThreadLocalRandom.current.nextDouble)

      if (count.incrementAndGet() <= capacity || values.isEmpty) {
        values.put(priority, sample)
      } else {
        var first = values.firstKey
        if (first < priority && values.putIfAbsent(priority, sample) == null) {
          while (values.remove(first) == null) {
            first = values.firstKey
          }
        }
      }
    } finally {
      lock.readLock.unlock()
    }
  }

  def snapshot(): Snapshot = {
    maybeRescale()
    lock.readLock.lock()
    try {
      Snapshot(values.values())
    } finally {
      lock.readLock.unlock()
    }
  }

  private def maybeRescale(): Unit = {
    val now = clock.nanos
    val next = nextScale.get
    if (now >= next) {
      rescale(now, next)
    }
  }

  // "since the values stored by the algorithms are linear
  // combinations of g values (scaled sums), they can be rescaled
  // relative to a new landmark." Sec. VI-A
  private def rescale(now: Long, next: Long): Unit = {
    lock.writeLock.lock()

    try {
      if (nextScale.compareAndSet(next, now + RescaleThreshold.toNanos)) {
        val prev = landmark
        landmark = clock.seconds

        // Where L = prev, and L' = landmark:
        //
        // "We can therefore multiply each value based on L by a
        // factor of exp(−alpha(L' − L)), and obtain the correct
        // value as if we had instead computed relative to a new
        // landmark L'" Sec. VI-A
        val factor = Math.exp(-alpha * (landmark - prev))

        if (factor == 0.0) {
          values.clear() // all values have decayed past their lifetime
        } else {
          val keys = values.keySet

          keys forEach { key =>
            val cur = values.remove(key)
            val nxt = cur.copy(weight = cur.weight * factor)

            if (nxt.weight != 0.0) {
              values.put(key * factor, nxt)
            }
          }
        }

        count.set(values.size)
      }
    } finally {
      lock.writeLock.unlock()
    }
  }
}
