package fauna.storage

import fauna.atoms.{ IndexID, ScopeID }
import fauna.codex.cbor._
import fauna.lang.Timestamp

package index {

  object IndexKey {
    implicit val codec = CBOR.TupleCodec[IndexKey]

    // Magic Number: c* pukes on keys larger than 64k
    val KeyBytesThreshold = 64_000

  }

  /**
    * An index entry is keyed on the ID of the index to which it
    * belongs and the (possibly compound) terms used to partition and
    * query the entry.
    */
  case class IndexKey(scope: ScopeID, id: IndexID, terms: Vector[IndexTerm])

  object ValueModifier {
    implicit val codec = CBOR.SumCodec[ValueModifier](
      CBOR.DefunctCodec(Unused), CBOR.RecordCodec[TTLModifier])
  }

  sealed abstract class ValueModifier
  object Unused extends ValueModifier // Remains of a defunct Tombstone type.
  case class TTLModifier(ttl: Timestamp) extends ValueModifier
}
