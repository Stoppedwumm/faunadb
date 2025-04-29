package fauna.tx.log

import fauna.lang.CircularBuffer

class TailIndex(floor: Long) {

  // Avoid the object boxing in Tuple2
  private case class Pair(idx: Long, elem: Long) {
    override def toString = s"$idx -> $elem"
  }

  private[this] val tier1 = CircularBuffer[Pair](128)
  private[this] val tier2 = CircularBuffer[Pair](128)
  private[this] val tier3 = CircularBuffer[Pair](128)
  private[this] val tier4 = CircularBuffer[Pair](128)

  override def toString = {
    val ts = Seq(tier1, tier2, tier3, tier4) map { _.mkString("(", ", ", ")") }

    (ts.zipWithIndex map {
      case (t, i) => s"TIER${i + 1}: $t"
    }).mkString("TailIndex(", ", ", ")")
  }

  def add(idx: Long, elem: Long) =
    synchronized {
      if (!tier1.isEmpty) {
        require(idx > tier1.last.idx, "adds must be increasing")
      }

      if (idx % 16 == 0) {
        tier1.add(Pair(idx, elem))
        if (idx % 128 == 0) {
          tier2.add(Pair(idx, elem))
          if (idx % 1024 == 0) {
            tier3.add(Pair(idx, elem))
            if (idx % 8192 == 0) {
              tier4.add(Pair(idx, elem))
            }
          }
        }
      }
    }

  def dropAfter(idx: Long): Unit =
    synchronized {
      tier1 filtered { case Pair(i, _) => i <= idx }
      tier2 filtered { case Pair(i, _) => i <= idx }
      tier3 filtered { case Pair(i, _) => i <= idx }
      tier4 filtered { case Pair(i, _) => i <= idx }
    }

  def apply(idx: Long): Long =
    synchronized {
      if (!tier1.isEmpty && idx >= tier1.head.idx) {
        getIdx(tier1, idx)
      } else if (!tier2.isEmpty && idx >= tier2.head.idx) {
        getIdx(tier2, idx)
      } else if (!tier3.isEmpty && idx >= tier3.head.idx) {
        getIdx(tier3, idx)
      } else if (!tier4.isEmpty && idx >= tier4.head.idx) {
        getIdx(tier4, idx)
      } else {
        floor
      }
    }

  private def getIdx(tier: CircularBuffer[Pair], idx: Long): Long = {
    var i = tier.size
    while (i > 0) {
      i -= 1
      val Pair(e, elem) = tier(i)
      if (idx >= e) return elem
    }
    floor
  }
}
