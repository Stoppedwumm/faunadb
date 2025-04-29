package fauna.storage.index

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.storage._
import fauna.storage.ir.DocIDV
import scala.util.{ Success, Try }

object IndexValue {

  // Once upon a time, there was a tombstone variant of IndexValue, but no more.
  // This one-addend sum codec is its memento. It might return in the future
  // because Fauna-level tombstones may be needed to implement, e.g., atomic ops.
  implicit val codec = CBOR.SumCodec[IndexValue](CBOR.TupleCodec[IndexValue])

  val MinValue =
    IndexValue(IndexTuple.MinValue, AtValid(Timestamp.Epoch), SetAction.MinValue)
  val MaxValue = IndexValue(IndexTuple.MaxValue, Unresolved, SetAction.MaxValue)

  def apply(scope: ScopeID, id: DocID): IndexValue =
    IndexValue(IndexTuple(scope, id), Unresolved, SetAction.MinValue)

  def apply(
    scope: ScopeID,
    id: DocID,
    validTS: Timestamp,
    action: SetAction): IndexValue =
    IndexValue(IndexTuple(scope, id), AtValid(validTS), action)

  def apply(tuple: IndexTuple, validTS: Timestamp, action: SetAction): IndexValue =
    IndexValue(tuple, AtValid(validTS), action)

  object ByValueOrdering extends Ordering[IndexValue] {

    override def equiv(a: IndexValue, b: IndexValue): Boolean =
      a.tuple == b.tuple &&
        a.ts.validTS == b.ts.validTS &&
        a.action == b.action

    def compare(a: IndexValue, b: IndexValue) = {
      def compare0(c: Int, next: => Int) =
        if (c == 0) next else c

      compare0(
        a.tuple compare b.tuple,
        compare0(
          a.ts compareValid b.ts,
          compare0(a.action compare b.action, a.ts compareTransaction b.ts)))
    }
  }

  // Sort events lexicographically by (valid TS asc, action desc, docID asc, txn TS asc).
  // Action is sorted descending to match the C* comparator for the historical index CF.
  object ByEventOrdering extends Ordering[IndexValue] {

    override def equiv(a: IndexValue, b: IndexValue): Boolean =
      a.ts.validTS == b.ts.validTS &&
        a.action == b.action &&
        a.docID == b.docID &&
        a.tuple == b.tuple

    def compare(a: IndexValue, b: IndexValue) = {
      def compare0(c: Int, next: => Int) =
        if (c == 0) next else c

      compare0(
        a.ts compareValid b.ts,
        compare0(
          b.action compare a.action,
          compare0(
            a.docID compare b.docID,
            compare0(a.tuple compare b.tuple, a.ts compareTransaction b.ts))))
    }
  }

  /** Constructs an IndexValue from the remainder of a Value, provided
    * the Value's key has already been decoded.
    */
  // FIXME: refactor with fromSortedValue.
  def fromHistoricalValue(
    scope: ScopeID,
    value: Value[Tables.HistoricalIndex.Key]): IndexValue = {
    val (_, validTS, action, bytes, txnTS) = value.key

    val values = CBOR.parse[Vector[IndexTerm]](bytes)
    val id = values.head.value
    val tuple = IndexTuple(scope, id.asInstanceOf[DocIDV].value, values.tail)

    if (value.data.isReadable) {
      // we anticipate older clusters (< 2.5.5) being affected by the
      // bug fixed in 437f553. If we don't find a valid modifier, it
      // is safe to emit a live value - deleted values did not exist
      // yet.
      Try(CBOR.decode[ValueModifier](value.data.duplicate)) match {
        case Success(TTLModifier(ttl)) =>
          IndexValue(
            tuple.copy(ttl = Some(ttl)),
            BiTimestamp.decode(validTS, txnTS),
            action)
        case _ =>
          IndexValue(tuple, BiTimestamp.decode(validTS, txnTS), action)
      }
    } else {
      IndexValue(tuple, BiTimestamp.decode(validTS, txnTS), action)
    }
  }

  /** Constructs an IndexValue from the remainder of a Value, provided
    * the Value's key has already been decoded.
    */
  def fromSortedValue(
    scope: ScopeID,
    value: Value[Tables.SortedIndex.Key]): IndexValue = {
    val (_, bytes, validTS, action, txnTS) = value.key

    val values = CBOR.parse[Vector[IndexTerm]](bytes)
    val id = values.last.value
    val tuple =
      IndexTuple(scope, id.asInstanceOf[DocIDV].value, values.init, ttl = None)

    if (value.data.isReadable) {
      // we anticipate older clusters (< 2.5.5) being affected by the
      // bug fixed in 437f553. If we don't find a valid modifier, it
      // is safe to emit a live value - deleted values did not exist
      // yet.
      Try(CBOR.decode[ValueModifier](value.data.duplicate)) match {
        case Success(TTLModifier(ttl)) =>
          IndexValue(
            tuple.copy(ttl = Some(ttl)),
            BiTimestamp.decode(validTS, txnTS),
            action)
        case _ =>
          IndexValue(tuple, BiTimestamp.decode(validTS, txnTS), action)
      }
    } else {
      IndexValue(tuple, BiTimestamp.decode(validTS, txnTS), action)
    }
  }

  object ByValueEquiv extends Equiv[IndexValue] {
    // Index value equivalence is a little tricky... when adding a TTL, there should
    // be two events: one removing the TTL-less value and another adding the TTL-ful
    // value. Use tuple's in-disk equivalence to prevent the two events from being in
    // conflict, then take the action into account when they have different TTLs.
    override def equiv(a: IndexValue, b: IndexValue): Boolean =
      a.tuple.equiv(b.tuple) &&
        a.ts.validTS == b.ts.validTS &&
        (a.tuple.ttl == b.tuple.ttl || a.action == b.action)
  }

  def resolveConflicts(values: Iterable[IndexValue]): Seq[IndexValue] =
    Conflict.resolve(values)(ByValueEquiv) map { _.canonical }
}

/** An index entry will, at minimum, cover the ID of the relevant
  * document, the valid time at which the index entry was modified,
  * and an add/remove action; these three pieces of data are the index
  * entry's value.
  *
  * The index entry may also contain other terms in the associated
  * document data. To distinguish these terms from the terms in the
  * key, we refer to this additional data as the entry's "values".
  *
  * When paging through an index, a set of these covered values may be
  * used as a cursor, in addition to the document's ID.
  */
case class IndexValue(tuple: IndexTuple, ts: BiTimestamp, action: SetAction)
    extends Mutation {

  def withTuple(tuple: IndexTuple): IndexValue =
    copy(tuple = tuple)

  def atValidTS(valid: Timestamp) = copy(ts = AtValid(valid))

  def withDocumentID(id: DocID) =
    copy(tuple = tuple.copy(docID = id))

  def scopeID: ScopeID = tuple.scopeID
  def docID: DocID = tuple.docID

  def event = SetEvent(ts, scopeID, docID, action, tuple.values, tuple.ttl)
  def isCreate = action.isCreate
  def isDelete = action.isDelete

  def isChange = false
}
