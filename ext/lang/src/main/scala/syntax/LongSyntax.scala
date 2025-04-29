package fauna.lang

import java.lang.{ Long => JLong }
import java.math.BigInteger
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.implicitConversions

trait LongSyntax {
  implicit def asRicherLong(l: Long): LongSyntax.RicherLong =
    LongSyntax.RicherLong(l)
}

object LongSyntax {
  private val BigMax = BigInteger.valueOf(Long.MaxValue)
  private val BigMin = BigInteger.valueOf(Long.MinValue)

  private val MaxDays = Duration.fromNanos(Long.MaxValue).toDays

  // NB. RichLong is taken by scala's stdlib
  case class RicherLong(l: Long) extends AnyVal {

    /** Returns a Duration representing `l` number of days, saturating to
      * the maximum or minimum number of representable days.
      */
    def saturatedDays: FiniteDuration =
      // NOTE: saturating down underflows to MaxDays. I don't know why
      // that is, but somebody might depend on it.
      if (l > MaxDays || l < -MaxDays) {
        MaxDays.days
      } else {
        l.days
      }

    /** Returns the sum of `l` and `other` unless it would overflow or
      * underflow in which case Long.MaxValue or Long.MinValue is
      * returned, respectively.
      */
    def saturatedAdd(other: Long) = {
      val sum = l + other
      if ((l ^ other) < 0 | (l ^ sum) >= 0) {
        // l and other have different signs, or l has the same sign as
        // the result: no overflow.
        sum
      } else {
        Long.MaxValue + ((sum >>> (JLong.SIZE - 1)) ^ 1)
      }
    }

    /**
      * Returns the approximate midpoint between two longs.
      */
    def midpoint(other: Long): Long = {
      if (l <= other) {
        // See Hacker's Delight. :)
        (l & other) + (l ^ other) / 2
      } else {
        val left = BigInteger.valueOf(l)
        val right = BigInteger.valueOf(other)
        val mid = (BigMax.subtract(BigMin).add(left).add(right)).shiftRight(1)

        if (mid.compareTo(BigMax) > 0) {
          BigMin.add(mid.subtract(BigMax)).longValue
        } else {
          mid.longValue
        }
      }
    }

    /**
      * Converts a number of bytes into a human-readable string
      *
      * @param si  If true, SI units where 1000 bytes are 1 kB
      *            If false, IEC units where 1024 bytes are 1 KiB
      *
      * @return    a human-readable string with appropriate units
      */
    def humanReadableSize(si: Boolean = true): String = {
      val (baseValue, unitToString) = {
        if (si) {
          (1000, Vector("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"))
        } else {
          (1024, Vector("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"))
        }
      }

      @tailrec
      def getExponent(curBytes: Long, baseValue: Int, curExponent: Int = 0): Int = {
        if (curBytes < baseValue || curExponent > 7) {
          curExponent
        } else {
          val newExponent = 1 + curExponent
          getExponent(curBytes / (baseValue * newExponent), baseValue, newExponent)
        }
      }

      val exponent = getExponent(l, baseValue)
      val divisor = Math.pow(baseValue, exponent)
      val unit = unitToString(exponent)

      f"${l / divisor}%.1f $unit"
    }

    def toUnsignedString: String =
      JLong.toUnsignedString(l)
  }
}
