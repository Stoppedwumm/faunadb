package fauna.storage

import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp

final case class VersionID(validTS: Timestamp, action: DocAction)
    extends Ordered[VersionID] {

  def saturatingIncr: VersionID =
    if (action < DocAction.MaxValue) {
      VersionID(validTS, DocAction.MaxValue)
    } else {
      if (validTS < Timestamp.MaxMicros) {
        VersionID(validTS.nextMicro, DocAction.MinValue)
      } else { // overflow
        VersionID(Timestamp.MaxMicros, DocAction.MaxValue)
      }
    }

  def saturatingDecr: VersionID =
    if (action > DocAction.MinValue) {
      VersionID(validTS, DocAction.MinValue)
    } else {
      if (validTS > Timestamp.Epoch) {
        VersionID(validTS.prevMicro, DocAction.MaxValue)
      } else { // underflow
        VersionID(Timestamp.Epoch, DocAction.MinValue)
      }
    }

  // Satisfy Ordered[VersionID]

  def compare(that: VersionID) = (validTS compare that.validTS) match {
    case 0   => action compare that.action
    case cmp => cmp
  }
}

object VersionID {
  val MinValue = VersionID(Timestamp.Epoch, DocAction.MinValue)
  val MaxValue = VersionID(Timestamp.MaxMicros, DocAction.MaxValue)

  implicit val Codec = CBOR.TupleCodec[VersionID]
}
