package fauna.stats

class Statistic {
  private var _populationSize: Long = 0L
  private var _sum: Long = 0L
  private var _accumulatedVariance: Double = 0.0
  private var _runningMean: Double = 0.0
  private var _minValue: Long = Long.MaxValue
  private var _maxValue: Long = Long.MinValue

  def add(value: Long): Unit = {
    _populationSize += 1
    _sum += value
    val delta = value - _runningMean
    _runningMean += delta / _populationSize
    _accumulatedVariance += delta * (value - _runningMean)

    if (value < _minValue) _minValue = value
    if (value > _maxValue) _maxValue = value
  }

  def variance: Double = _accumulatedVariance / _populationSize
  def stddev: Double = Math.sqrt(variance)
  def mean: Double = _runningMean
  def min: Long = _minValue
  def max: Long = _maxValue
  def range: Long = _maxValue - _minValue
  def sum: Long = _sum
  def populationSize: Long = _populationSize

  override def toString: String =
    s"Mean: $mean, Min: $min, Max: $max, Range: $range, Stddev: $stddev, " +
      s"Variance: $variance, Population: $populationSize, Sum: $sum"
}
