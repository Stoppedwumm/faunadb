package fauna.atoms

import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp

object SchemaVersion {
  implicit val CBORCodec =
    CBOR.AliasCodec[SchemaVersion, Long](SchemaVersion(_), _.toMicros)

  val Min = SchemaVersion(Timestamp.Epoch)
  val Pending = SchemaVersion(Timestamp.MaxMicros)
  val Max = SchemaVersion(Timestamp.Max)

  def apply(ts: Long): SchemaVersion = SchemaVersion(Timestamp.ofMicros(ts))
}

final case class SchemaVersion(ts: Timestamp)
    extends AnyVal
    with Ordered[SchemaVersion] {
  def toMicros = ts.micros

  def compare(that: SchemaVersion): Int = ts compareTo that.ts

  @inline def isPending = this == SchemaVersion.Pending
}
