package fauna.lang

trait Average {
  def value: Double
  def sample(point: Long): Unit
}

/**
  * Maintains a running average.
  */
final class UnboundedAverage extends Average {
  private[this] var _populationSize: Long = 0L
  @volatile private[this] var _runningMean: Double = 0.0

  def sample(value: Long): Unit = synchronized {
    val delta = value - _runningMean
    _populationSize += 1
    _runningMean += delta / _populationSize
  }

  def value: Double = _runningMean

  override def toString: String = _runningMean.toLong.toString

  def toLong: Long = _runningMean.toLong
}

/**
  * Maintains a bounded, running average of `history` points.
  */
final class RunningAverage(history: Int) extends Average {
  private[this] val intervals = new Array[Long](history)
  private var sum: Long = 0
  private var index = 0
  private var isFilled = false
  @volatile private var _mean: Double = 0.0

  def sample(point: Long): Unit = {
    if (index == history - 1) {
      isFilled = true
      index = 0
    }

    if (isFilled) {
      sum -= intervals(index)
    }

    intervals(index) = point
    index += 1
    sum += point
    _mean = sum.toDouble / size
  }

  private def size: Int =
    if (isFilled) {
      intervals.length
    } else {
      index + 1
    }

  def value: Double = _mean
}

/**
  * A running, weighted average of some float value. This is our best
  * estimate of a future unknown.
  *
  * Weight is a percentage from 0 - 100.
  */
class WeightedAverage(weight: Int) extends Average {
  @volatile private var _average = 0.0
  private var _count = 0
  private var _isOld = false

  private val OldThreshold = 100

  private def expAverage(avg: Double, pnt: Double, wt: Int): Double =
    (100.0 - wt) * avg / 100.0 + wt * pnt / 100.0

  private def compute(point: Double): Double = {
    val wt = if (!_isOld) OldThreshold / _count else 0
    expAverage(_average, point, weight max wt)
  }

  protected def update(avg: Double) = _average = avg

  def value: Double = _average

  def sample(point: Long): Unit = synchronized {
    _count += 1
    if (!_isOld && _count > OldThreshold) {
      _isOld = true
    }

    update(compute(point.toDouble))
  }
}

/**
  * Combined exponential mean and variance.
  */
final class WeightedMoments(alpha: Double) {
  @volatile private var _mean = 0.0
  @volatile private var _variance = 0.0

  def mean: Double = _mean
  def variance: Double = _variance

  def sample(point: Double): Unit = synchronized {
    val diff = point - mean 
    val incr = alpha * diff
    _mean = mean + incr
    _variance = (1 - alpha) * (variance + diff * incr)
  }
}
