package fauna.storage

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.storage.cassandra.CValue
import io.netty.buffer.{ ByteBuf, Unpooled }

sealed trait Selector {
  def keep(cf: String, key: ByteBuf): Boolean
}

object Selector {

  def from(scopeID: ScopeID): Selector = Scope(scopeID)

  def from(scopeIDs: Iterable[ScopeID]): Selector =
    ScopeSet(scopeIDs.toSet)

  def from(scopeID: Option[ScopeID]): Selector =
    scopeID.fold(All: Selector) {
      Scope(_)
    }

  def from(scopeID: ScopeID, collIDs: Iterable[CollectionID]): Selector =
    ScopeCollections(scopeID, collIDs.toSet)

  case object All extends Selector {
    def keep(cf: String, key: ByteBuf): Boolean = true
  }

  /**
    * This can be used to filter/select on scope.
    * It works for CFs Versions, HistoricalIndex & SortedIndex.
    */
  final case class Scope(scopeID: ScopeID) extends Selector {

    def keep(cf: String, key: ByteBuf): Boolean = {
      require(
        cf == Tables.Versions.CFName ||
          cf == Tables.HistoricalIndex.CFName ||
          cf == Tables.HistoricalIndex.CFName2 ||
          cf == Tables.SortedIndex.CFName ||
          cf == Tables.SortedIndex.CFName2)
      Tables.decodeScope(cf, key) contains scopeID
    }
  }

  /** Selector that keeps all rows from a set of Scopes. */
  final case class ScopeSet(scopeIDs: Set[ScopeID]) extends Selector {
    def keep(cf: String, key: ByteBuf): Boolean = {
      require(
        cf == Tables.Versions.CFName ||
          cf == Tables.HistoricalIndex.CFName ||
          cf == Tables.HistoricalIndex.CFName2 ||
          cf == Tables.SortedIndex.CFName ||
          cf == Tables.SortedIndex.CFName2)
      Tables.decodeScope(cf, key) exists { scopeIDs.contains(_) }
    }
  }

  /**
    * Selector that allows filtering based on collections (belonging to some scope).
    * Only guarantees proper operations with the Versions CF.
    */
  final case class ScopeCollections(scopeID: ScopeID, collIDs: Set[CollectionID])
      extends Selector {

    // Assumption: collIDs is a small set (typically 1 ID), and the
    // number of row keys is much (much!) larger, so pre-encoding the
    // needle and doing simple bytewise comparisons against the
    // haystack is more efficient than decoding each row key for value
    // comparison against the set of collIDs.
    private[this] lazy val encodedIDs = collIDs flatMap { id =>

      // See the schema in Tables.Versions.
      val dummy = (scopeID, id, SubID(0))

      // Presuming a snowflake-generated scope, and a collection ID
      // above 1024:
      //
      // [ 1 byte ] [ 8 bytes ] [ 4 bytes ] [ 1 byte ]
      //    len        scope       coll        sub
      val buf = Unpooled.buffer(14, 14)
      CBOR.encode(buf, dummy)

      Set(buf.slice(1, 8 + 4))
    }

    def keep(cf: String, key: ByteBuf): Boolean = {
      require(cf == Tables.Versions.CFName)

      // 14 bytes for the prefix we're searching for, and 8 bytes for
      // a snowflake-generated SubID.
      if (key.readableBytes == 22) {
        encodedIDs.contains(key.slice(1, 8 + 4))
      } else {
        // Built-in collections (ID < 1024) and documents in the root
        // database (scope 0) will encode shorter than user
        // data. Handle them on the slow path.
        val (scope, DocID(_, id)) = Tables.Versions.decodeRowKey(key)
        scope == scopeID && collIDs.contains(id)
      }
    }
  }

  /**
    * Selector that allows filtering on collections across all scopes.
    * Only really useful to get built-in collections, in particular those defining schema elements.
    * Only guarantees proper operations with the Versions CF.
    */
  final case class Schema(collIDs: Set[CollectionID])
    extends Selector {

    def keep(cf: String, key: ByteBuf): Boolean = {
      require(cf == Tables.Versions.CFName)

      val cval = CValue(Tables.Versions.Schema.keyComparator, key)
      val pkey = Predicate(cval, Nil).as[Array[Byte]]
      val (_, actualCollID, _) =
        CBOR.parse[(ScopeID, CollectionID, SubID)](pkey)

      collIDs.contains(actualCollID)
    }
  }

  /** A Selector which matches the equivalent documents as
    * IndexSources.Custom.
    *
    * This is only useful for (re-)building wildcard indexes.
    */
  final case class UserCollections(scopeID: ScopeID) extends Selector {

    def keep(cf: String, key: ByteBuf): Boolean = {
      require(cf == Tables.Versions.CFName)

      val (scope, DocID(_, id)) = Tables.Versions.decodeRowKey(key)
      if (scope == scopeID) {
        id match {
          case UserCollectionID(_) => true
          case _                   => false
        }
      } else {
        false
      }
    }
  }
}
