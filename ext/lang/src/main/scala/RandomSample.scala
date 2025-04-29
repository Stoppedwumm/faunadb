package fauna.lang

import scala.util.Random

object RandomSample {
  /** Chooses a random sample using the cumulative density function
    * (CDF) of the list of samples with their weights.
    */
  def choose[T](samples: IndexedSeq[(T, Double)])(
    implicit rnd: Random = Random): T = {
    require(samples.nonEmpty, "cannot choose a sample from an empty set.")

    var sum = 0.0
    samples foreach { case (_, w) =>
      sum += w
    }

    var needle = rnd.nextDouble().max(Double.MinPositiveValue) * sum
    var idx = 0
    while (needle > 0.0) {
      needle -= samples(idx)._2
      idx += 1
    }

    samples(idx - 1)._1
  }
}
