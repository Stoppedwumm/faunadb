package fauna.repo.query

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.storage._
import fauna.storage.ir.IRValue
import fauna.storage.ops._
import fauna.tx.transaction.DataFilter

/** A Stream Definition. At its core, if the definition covers a Write, the Write
  * should be part of the stream.
  */
sealed trait StreamDef extends DataFilter[Txn] {
  def covered(w: Write): Boolean

  def covers(txn: Txn): Boolean = {
    val (_, writes) = txn
    writes.exists { covered(_) }
  }
}

/** A Collection Stream.
  * Matches on any Version writes against the provided scopeID and collID. That
  * is, any write against any document in the collection will match.
  */
final case class CollectionFilter(scopeID: ScopeID, collID: CollectionID)
    extends StreamDef {

  def covered(write: Write): Boolean =
    write match {
      case VersionAdd(`scopeID`, DocID(_, `collID`), _, _, _, _, _) => true
      case VersionRemove(`scopeID`, DocID(_, `collID`), _, _)       => true
      case DocRemove(`scopeID`, DocID(_, `collID`))                 => true

      case VersionAdd(_, _, _, _, _, _, _)           => false
      case VersionRemove(_, _, _, _)                 => false
      case DocRemove(_, _)                           => false
      case SetAdd(_, _, _, _, _, _, _, _)            => false
      case SetRemove(_, _, _, _, _, _, _, _)         => false
      case SetRepair(_, _, _, _, _, _, _, _, _, _)   => false
      case SetBackfill(_, _, _, _, _, _, _, _, _, _) => false
      case LookupAdd(_, _, _, _, _)                  => false
      case LookupRemove(_, _, _, _, _)               => false
      case LookupRepair(_, _, _, _, _)               => false
      case NoopWrite                                 => false

      // Legacy writes we can safely ignore
      case _: ValueWrite        => false
      case RemoveAllWrite(_, _) => false
    }
}

/** A Document Stream
  * Matches on any Version writes against the provided scopeID and docID. This will
  * only match writes against a specific document.
  */
final case class DocumentFilter(scopeID: ScopeID, docID: DocID) extends StreamDef {

  def covered(write: Write): Boolean =
    write match {
      case VersionAdd(`scopeID`, `docID`, _, _, _, _, _) => true
      case VersionRemove(`scopeID`, `docID`, _, _)       => true
      case DocRemove(`scopeID`, `docID`)                 => true

      case VersionAdd(_, _, _, _, _, _, _)           => false
      case VersionRemove(_, _, _, _)                 => false
      case DocRemove(_, _)                           => false
      case SetAdd(_, _, _, _, _, _, _, _)            => false
      case SetRemove(_, _, _, _, _, _, _, _)         => false
      case SetRepair(_, _, _, _, _, _, _, _, _, _)   => false
      case SetBackfill(_, _, _, _, _, _, _, _, _, _) => false
      case LookupAdd(_, _, _, _, _)                  => false
      case LookupRemove(_, _, _, _, _)               => false
      case LookupRepair(_, _, _, _, _)               => false
      case NoopWrite                                 => false

      // Legacy writes we can safely ignore
      case _: ValueWrite        => false
      case RemoveAllWrite(_, _) => false
    }
}

/** An Index Stream
  * Matches all add and remove events for a given index regardless of term.
  */
final case class IndexFilter(scopeID: ScopeID, indexID: IndexID) extends StreamDef {

  def covered(write: Write): Boolean =
    write match {
      case SetAdd(`scopeID`, `indexID`, _, _, _, _, _, _)    => true
      case SetRemove(`scopeID`, `indexID`, _, _, _, _, _, _) => true

      case VersionAdd(_, _, _, _, _, _, _)           => false
      case VersionRemove(_, _, _, _)                 => false
      case DocRemove(_, _)                           => false
      case SetAdd(_, _, _, _, _, _, _, _)            => false
      case SetRemove(_, _, _, _, _, _, _, _)         => false
      case SetRepair(_, _, _, _, _, _, _, _, _, _)   => false
      case SetBackfill(_, _, _, _, _, _, _, _, _, _) => false
      case LookupAdd(_, _, _, _, _)                  => false
      case LookupRemove(_, _, _, _, _)               => false
      case LookupRepair(_, _, _, _, _)               => false
      case NoopWrite                                 => false

      // Legacy writes we can safely ignore
      case _: ValueWrite        => false
      case RemoveAllWrite(_, _) => false
    }
}

/** An Index Match Stream
  * Matches only add and remove events for the specified term and index.
  */
final case class MatchFilter(
  scopeID: ScopeID,
  indexID: IndexID,
  terms: Vector[IRValue]
) extends StreamDef {

  def covered(write: Write): Boolean =
    write match {
      case SetAdd(`scopeID`, `indexID`, `terms`, _, _, _, _, _)    => true
      case SetRemove(`scopeID`, `indexID`, `terms`, _, _, _, _, _) => true

      case VersionAdd(_, _, _, _, _, _, _)           => false
      case VersionRemove(_, _, _, _)                 => false
      case DocRemove(_, _)                           => false
      case SetAdd(_, _, _, _, _, _, _, _)            => false
      case SetRemove(_, _, _, _, _, _, _, _)         => false
      case SetRepair(_, _, _, _, _, _, _, _, _, _)   => false
      case SetBackfill(_, _, _, _, _, _, _, _, _, _) => false
      case LookupAdd(_, _, _, _, _)                  => false
      case LookupRemove(_, _, _, _, _)               => false
      case LookupRepair(_, _, _, _, _)               => false
      case NoopWrite                                 => false

      // Legacy writes we can safely ignore
      case _: ValueWrite        => false
      case RemoveAllWrite(_, _) => false
    }
}

/** A Collection Set Stream.
  * Matches all add and remove events for the specified schema and scope.
  */
final case class CollectionSetFilter(
  optScopeID: Option[ScopeID],
  collIDs: Vector[CollectionID]
) extends StreamDef {

  private[this] lazy val _collIDSet = collIDs.toSet

  private def matches(scopeID: ScopeID, collID: CollectionID) =
    _collIDSet.contains(collID) &&
      (optScopeID.isEmpty || optScopeID.contains(scopeID))

  def covered(write: Write): Boolean =
    write match {
      case VersionAdd(scopeID, DocID(_, collID), _, _, _, _, _) =>
        matches(scopeID, collID)
      case VersionRemove(scopeID, DocID(_, collID), _, _) => matches(scopeID, collID)
      case DocRemove(scopeID, DocID(_, collID))           => matches(scopeID, collID)

      case VersionAdd(_, _, _, _, _, _, _)           => false
      case VersionRemove(_, _, _, _)                 => false
      case DocRemove(_, _)                           => false
      case SetAdd(_, _, _, _, _, _, _, _)            => false
      case SetRemove(_, _, _, _, _, _, _, _)         => false
      case SetRepair(_, _, _, _, _, _, _, _, _, _)   => false
      case SetBackfill(_, _, _, _, _, _, _, _, _, _) => false
      case LookupAdd(_, _, _, _, _)                  => false
      case LookupRemove(_, _, _, _, _)               => false
      case LookupRepair(_, _, _, _, _)               => false
      case NoopWrite                                 => false

      // Legacy writes we can safely ignore
      case _: ValueWrite        => false
      case RemoveAllWrite(_, _) => false
    }
}

object StreamOp {

  val DataFilterCodec =
    CBOR.SumCodec[DataFilter[Txn]](
      CBOR.TupleCodec[CollectionFilter],
      CBOR.TupleCodec[DocumentFilter],
      CBOR.TupleCodec[IndexFilter],
      CBOR.TupleCodec[MatchFilter],
      CBOR.TupleCodec[CollectionSetFilter]
    )

  implicit val StreamDefCodec =
    CBOR.SumCodec[StreamDef](
      CBOR.TupleCodec[CollectionFilter],
      CBOR.TupleCodec[DocumentFilter],
      CBOR.TupleCodec[IndexFilter],
      CBOR.TupleCodec[MatchFilter],
      CBOR.TupleCodec[CollectionSetFilter]
    )
}
