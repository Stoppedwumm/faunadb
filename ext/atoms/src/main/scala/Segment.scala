package fauna.atoms

import fauna.codex.cbor._
import java.lang.{ Long => JLong }
import java.math.BigInteger
import scala.util.Sorting

/**
  * A range of tokens on the ring [left, right). A segment may not
  * wrap the ring, i.e. left must be <= right. left == right may only
  * occur if both are the minimum token.
  */
final case class Segment(left: Location, right: Location) extends Ordered[Segment] {
  require(
    left < right || right.isMin,
    s"segments may not wrap the token ring. [$left, $right)"
  )

  def contains(loc: Location): Boolean =
    left <= loc && (right > loc || right.isMin)

  def split(loc: Location): Seq[Segment] = {
    require(contains(loc), s"Segment $this does not contain $loc")
    if (loc == left) Seq(this) else Seq(copy(right = loc), copy(left = loc))
  }

  /**
    * Split this segment into N sub segments of roughly the same size.
    * @param quantity the desired number of segments to split
    * @return a list of segments
    */
  def subSegments(quantity: Int): Seq[Segment] = {
    require(quantity >= 1, s"Quantity should be >= 1. Actual value is $quantity.")

    if (quantity == 1) {
      return Seq(this)
    }

    if (quantity == 2) {
      return split(midpoint)
    }

    val leftToken = left.token

    //if right is min, it means this segment wrapped the token ring, to lets unwrap it and set to Long.MaxValue
    val rightToken = if (right.isMin) {
      Long.MaxValue
    } else {
      right.token
    }

    val diff = BigInt(rightToken) - BigInt(leftToken)
    val incr = ((diff + (quantity >> 1)) / quantity).longValue

    var l = leftToken

    (1 to quantity) map { index =>
      if (index < quantity) {
        val r = l + incr

        val seg = Segment(
          left = Location(l),
          right = Location(r)
        )

        l += incr

        seg
      } else {
        Segment(
          left = Location(l),
          right = this.right
        )
      }
    }
  }

  /**
    * Returns an intersection of this segment and another segment,
    * or None if the two segments don't intersect.
    * @param that the other segment
    * @return the intersection segment, or None
    */
  def intersect(that: Segment): Option[Segment] = {
    val left = this.left max that.left
    // needed because of special right.isMin semantics...
    val right = if (this.right.isMin) {
      that.right
    } else if (that.right.isMin) {
      this.right
    } else {
      this.right min that.right
    }
    if (left < right || right.isMin) {
      Some(Segment(left, right))
    } else {
      None
    }
  }

  /**
    * Returns the difference of this segment and another segment.
    * The returned sequence can have 0, 1, or 2 elements.
    * @param that the other segment
    * @return the difference segment(s)
    */
  def diff(that: Segment): Seq[Segment] = {
    this intersect that match {
      case None => Seq(this)
      case Some(sect) =>
        val b = Seq.newBuilder[Segment]
        if (this.left < sect.left) {
          b += Segment(this.left, sect.left)
        }
        if (sect.right < this.right || (!sect.right.isMin && this.right.isMin)) {
          b += Segment(sect.right, this.right)
        }
        b.result()
    }
  }

  def precedes(that: Segment): Boolean =
    this.right <= that.left && !this.right.isMin

  def strictlyPrecedes(that: Segment): Boolean =
    this.right < that.left && !this.right.isMin

  def compare(that: Segment): Int = {
    val lc = JLong.compare(left.token, that.left.token)
    if (lc != 0) {
      lc
    } else {
      // In the right position, Location.MinValue has the highest token
      JLong.compare(right.token - 1, that.right.token - 1)
    }
  }

  def midpoint: Location =
    left midpoint (if (right.isMin) Location.MaxValue else right)

  def length: BigInteger =
    bigRight.subtract(BigInteger.valueOf(left.token))

  def bigRight: BigInteger =
    if (right.isMin) {
      Segment.MaxLocationPlusOne
    } else {
      BigInteger.valueOf(right.token)
    }
}

object Segment {
  implicit val CBORCodec = CBOR.TupleCodec[Segment]

  val All = Segment(Location.MinValue, Location.MinValue)

  // This could be expressed as BigInteger.TWO.pow(63) but this way it is
  // expressed in terms of location maximum.
  private[Segment] val MaxLocationPlusOne =
    BigInteger.valueOf(Location.MaxValue.token).add(BigInteger.ONE)

  // This could be expressed as BigInteger.TWO.pow(64) but this way it is
  // expressed in terms of location ranges.
  val RingLength =
    MaxLocationPlusOne.subtract(BigInteger.valueOf(Location.MinValue.token))

  /**
    * Sorts ascending by left boundary, and merges overlapping
    * segments.
    *
    * Requires all segments to describe the same ring (i.e. share the
    * same partitioner)
    */
  def normalize(segs: Seq[Segment]): Vector[Segment] = {
    Sorting
      .stableSort(segs, { (a: Segment, b: Segment) =>
        a.left < b.left
      })
      .foldLeft(Vector.empty[Segment]) { case (acc, nextSegment) =>
        acc.lastOption match {
          case Some(currentSegment) =>
            if (currentSegment.strictlyPrecedes(nextSegment)) {
              acc :+ nextSegment
            } else {
              val union = Segment(
                currentSegment.left,
                if (currentSegment.right.isMin || nextSegment.right.isMin) {
                  Location.MinValue
                } else {
                  currentSegment.right.max(nextSegment.right)
                }
              )
              acc.dropRight(1) :+ union
            }
          case None => Vector(nextSegment)
        }
    }
  }

  def ringFraction(locations: BigInteger): Float =
    (locations
      .multiply(BigInteger.valueOf(100000))
      .divide(RingLength)
      .doubleValue() / 100000d).toFloat

  def ringFraction(s: Iterable[Segment]): Float =
    ringFraction(sumLength(s))

  def sumLength(s: Iterable[Segment]): BigInteger =
    s.foldLeft(BigInteger.ZERO) { (sum, seg) =>
      sum.add(seg.length)
    }

  /**
    * Computes the difference between two sets of segments. Each set should be
    * represented as an ordered sequence of non-overlapping segments.
    */
  def diff(minuend: Seq[Segment], subtrahend: Seq[Segment]): Seq[Segment] = {
    var sub = subtrahend
    minuend flatMap { m =>
      // drop those that don't intersect current segment
      sub = sub dropWhile { _ precedes m }
      // Get those that do intersect current segment
      val overlaps = sub takeWhile { s => !(m precedes s) }
      // diff m with every element of overlaps cumulatively
      overlaps.foldLeft(Seq(m)) { (diffs, s) =>
        diffs flatMap { _ diff s }
      }
    }
  }
}
