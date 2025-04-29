package fauna.storage.lookup

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.storage._
import fauna.storage.cassandra.comparators._
import io.netty.buffer.Unpooled
import java.lang.{ Long => JLong }

object LookupEntry {
  implicit val codec = CBOR.SumCodec[LookupEntry](
    CBOR.TupleCodec[LiveLookup],
    CBOR.TupleCodec[DeletedLookup])

  object Order extends Ordering[LookupEntry] {
    override def equiv(a: LookupEntry, b: LookupEntry): Boolean =
      a.globalID.compare(b.globalID) == 0 &&
      a.scope.compare(b.scope) == 0 &&
      a.id.compare(b.id) == 0 &&
      a.ts.compareValid(b.ts) == 0 &&
      a.action.compare(b.action) == 0

    def compare(a: LookupEntry, b: LookupEntry): Int = {
      def compare0(c: Int, next: => Int) =
        if (c == 0) next else c

      compare0(JLong.compare(a.globalID.toLong, b.globalID.toLong),
        compare0(JLong.compare(a.scope.toLong, b.scope.toLong),
          compare0(a.id compare b.id,
            compare0(a.ts compareValid b.ts,
              compare0(a.action compare b.action,
                a.ts compareTransaction b.ts)))))
    }
  }

  val MinValue = LiveLookup(ScopeID.MinValue,
                            ScopeID.MinValue,
                            DocID.MinValue,
                            Resolved(Timestamp.Epoch),
                            SetAction.MinValue)

  val MaxValue = LiveLookup(ScopeID.MaxValue,
                            ScopeID.MaxValue,
                            DocID.MaxValue,
                            Unresolved,
                            SetAction.MaxValue)

  /**
    * Constructs a LookupEntry from Value.
    */
  def apply(v: Value[Tables.Lookups.Key]): LookupEntry = {
    val (gID, sID, id, validTS, action, txnTS) = getLookupKey(v)

    LiveLookup(CBOR.parse[GlobalID](gID),
               ScopeID(sID),
               CBOR.parse[DocID](id),
               BiTimestamp.decode(validTS, txnTS),
               action)
  }

  def decodeDatabase(v: Value[Tables.Lookups.Key]): (ScopeID, DocID) = {
    val (_, sID, id, _, _, _) = getLookupKey(v)

    ScopeID(sID) -> CBOR.parse[DocID](id)
  }

  // Lookup table is sorted by entry's scope and id before valid timestamp.
  // The same global ID can appear in different scopes (e.g.: a moved database).
  // In order to get the latest lookup entry for a given global ID, this method
  // returns the lookup entry with the minimum action in the latest valid timestamp.
  // It uses the minimum action because "add" comes before "remove" in their ordering,
  // hence, it returns the latest "add" action for entries moved in the same valid
  // time.
  def latestForGlobalID(a: LookupEntry, b: LookupEntry): LookupEntry = {
    assert(a.globalID == b.globalID)
    val aTS = (a.ts.validTS, a.ts.transactionTS)
    val bTS = (b.ts.validTS, b.ts.transactionTS)
    (aTS compare bTS) match {
      case 0 => if (a.action < b.action) a else b
      case n => if (n > 0) a else b
    }
  }

  private def getLookupKey(v: Value[Tables.Lookups.Key]): Tables.Lookups.Key = {
    if (v.keyPredicate.columnName.size == 4) { // missing transaction timestamp
      val keyCodec = implicitly[CassandraCodec[Tables.Lookups.OldKey]]
      val (g, s, i, vt, a) = v.keyPredicate.as[Tables.Lookups.OldKey](keyCodec)
      (g, s, i, vt, a, v.transactionTS)
    } else {
      v.key
    }
  }
}

sealed abstract class LookupEntry extends Mutation {
  def globalID: GlobalID
  def scope: ScopeID
  def id: DocID
  def ts: BiTimestamp
  def action: SetAction

  def versionID = VersionID(ts.validTS, action.toDocAction)

  def isDelete = action.isDelete
  def isCreate = action.isCreate

  val isChange = false

  /**
    * Returns a copy of this entry with the given scope.
    */
  def withScope(scope: ScopeID): LookupEntry

  /**
    * Encodes this entry into a Value for persistence to storage.
    */
  def toValue: Value[Tables.Lookups.Key] =
    new Value(
      (Tables.Lookups.rowKey(globalID),
        scope.toLong,
        CBOR.encode(id).toByteArray,
        ts.validTS,
        action,
        ts.transactionTS),
      Unpooled.EMPTY_BUFFER)
}

case class LiveLookup(
  globalID: GlobalID,
  scope: ScopeID,
  id: DocID,
  ts: BiTimestamp,
  action: SetAction)
    extends LookupEntry {

  def withScope(scope: ScopeID) = copy(scope = scope)
}

case class DeletedLookup(
  globalID: GlobalID,
  scope: ScopeID,
  id: DocID,
  ts: BiTimestamp,
  action: SetAction)
    extends LookupEntry {

  def withScope(scope: ScopeID) = copy(scope = scope)
}
