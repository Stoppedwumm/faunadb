package fauna.storage

import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp

import BiTimestamp.UnresolvedSentinel

object BiTimestamp {
  implicit val CBORCodec = CBOR.SumCodec[BiTimestamp](
    CBOR.SingletonCodec(Unresolved),
    CBOR.TupleCodec[AtValid],
    CBOR.TupleCodec[Resolved])

  // Must be encodable in micros and sort highest in order to
  // round-trip through C* storage representation.
  final val UnresolvedSentinel = Timestamp.MaxMicros

  // for reconstructing from a cell name
  def decode(valid: Timestamp, txn: Timestamp) =
    (valid, txn) match {
      case (UnresolvedSentinel, UnresolvedSentinel) =>
        Unresolved
      case (valid, UnresolvedSentinel) =>
        AtValid(valid)
      case (UnresolvedSentinel, txn) =>
        throw new IllegalStateException(
          s"Unresolved validTime with resolved txntime: $txn")
      case (valid, txn) =>
        Resolved(valid, txn)
    }
}

/** The bitemporal point in time at which a write occurs.
  */
sealed abstract class BiTimestamp {
  def validTSOpt: Option[Timestamp]
  def validTS: Timestamp
  def transactionTS: Timestamp

  def isResolved: Boolean
  def resolve(ts: Timestamp): Resolved
  def unresolve: BiTimestamp

  // Pending writes are encoded as C* values in the query read cache
  // with the transaction time sentinel of MaxMicros.
  // FIXME: Cache reads at our instance/index level in order to avoid
  // sentinel-based encoding.
  def resolvePending: Resolved =
    resolve(UnresolvedSentinel)

  def compareValid(o: BiTimestamp) =
    validTS.compare(o.validTS)

  def compareTransaction(o: BiTimestamp) =
    transactionTS.compare(o.transactionTS)
}

/** Both axis of time will assume the log-resolved transaction
  * time when applying the write to storage.
  */
case object Unresolved extends BiTimestamp {
  def validTSOpt = None
  def validTS = UnresolvedSentinel
  def transactionTS = UnresolvedSentinel

  def isResolved = false
  def resolve(ts: Timestamp) = Resolved(ts, ts)
  def unresolve = this
}

/** Override the valid time axis, leaving transaction time resolved by
  * the log. This supports revision of a document's history.
  */
final case class AtValid(validTS: Timestamp) extends BiTimestamp {
  def validTSOpt = Some(validTS)
  def transactionTS = UnresolvedSentinel

  def isResolved = false
  def resolve(ts: Timestamp) = Resolved(validTS, ts)
  def unresolve = this
}

object Resolved {
  implicit val codec = CBOR.TupleCodec[Resolved]

  def apply(ts: Timestamp): Resolved = Resolved(ts, ts)
}

/** Both axis of time are fully-resolved by the log and storage
  * engine.
  */
final case class Resolved(validTS: Timestamp, transactionTS: Timestamp)
    extends BiTimestamp {
  def validTSOpt = Some(validTS)

  def isResolved = true
  def resolve(ts: Timestamp) = this
  def unresolve = AtValid(validTS)

  override def toString(): String =
    if (validTS == transactionTS) {
      s"Resolved(valid=txn=$validTS)"
    } else {
      s"Resolved(valid=$validTS,txn=$transactionTS)"
    }
}
