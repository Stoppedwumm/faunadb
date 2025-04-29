package fauna.atoms

import fauna.codex.cbor._
import java.util.UUID

case class HostID(uuid: UUID) extends AnyVal with Ordered[HostID] {
  def compare(that: HostID) = uuid.compareTo(that.uuid)
  override def toString = uuid.toString
}

object HostID {
  val NullID = HostID(new UUID(0, 0))

  // FIXME: perhaps generate type 1 uuids w/ currtime + randomness
  def randomID = HostID(UUID.randomUUID)

  def apply(id: String): HostID = HostID(UUID.fromString(id))

  // FIXME: Use CBOR tagged type 37 (See CBOR IANA tag registry)
  implicit val CBORCodec = {
    def to(id: HostID) =
      (id.uuid.getMostSignificantBits, id.uuid.getLeastSignificantBits)
    def from(t: (Long, Long)) = HostID(new UUID(t._1, t._2))
    CBOR.AliasCodec(from, to)
  }
}
