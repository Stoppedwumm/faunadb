package fauna.atoms

import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import scala.util.Random
import java.lang.{ Long => JLong }

object Location {
  val MinValue = Location(Long.MinValue)
  val MaxValue = Location(Long.MaxValue)

  val PerHostCount = 128

  def fromString(str: String): Location = Location(str.toLong)

  def random(r: Random = Random) = Location(r.nextLong())

  implicit val LocationCodec = CBOR.AliasCodec[Location, Long](Location(_), _.token)
}

/**
  * A discrete position on the token ring.
  */
final case class Location(token: Long) extends AnyVal with Ordered[Location] {
  override def toString: String = token.toString

  def compare(that: Location): Int =
    JLong.compare(token, that.token)

  override def >(o: Location): Boolean = token > o.token
  override def >=(o: Location): Boolean = token >= o.token
  override def <(o: Location): Boolean = token < o.token
  override def <=(o: Location): Boolean = token <= o.token

  def isMin = token == Long.MinValue

  def min(that: Location) =
    if (compare(that) <= 0) this else that

  def max(that: Location) =
    if (compare(that) >= 0) this else that

  def midpoint(that: Location) =
    Location(token midpoint that.token)

  /** Returns the next position on the ring, or throws
    * IllegalArgumentException if an overflow would occur.
    */
  def next: Location =
    if (token < Long.MaxValue) {
      Location(token + 1)
    } else {
      throw new IllegalArgumentException(s"Cannot find next Location from $this.")
    }
}
