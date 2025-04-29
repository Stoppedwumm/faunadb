package fauna.repo

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.doc.Indexer
import fauna.repo.store.SortedIndex
import fauna.storage._
import fauna.storage.api.set.CollectionAtTSSentinel
import fauna.storage.api.MVTMap
import fauna.storage.cassandra._
import fauna.storage.index._
import io.netty.buffer.{ ByteBuf, Unpooled }

object IndexIterator {
  def apply(iter: CassandraIterator, mvts: Map[ScopeID, MVTMap]) = new IndexIterator(
    iter,
    (scope, coll) => {
      mvts.get(scope).flatMap(_.apply(coll)).getOrElse(Timestamp.Epoch)
    })
}

/** Composes a CassandraIterator with an MVTMap to produce set
  * histories which respect both a snapshot time and min. valid time
  * from an offline snapshot of the SortedIndex column family.
  */
final class IndexIterator(
  iter: CassandraIterator,
  lookupMvt: (ScopeID, CollectionID) => Timestamp)
    extends Iterator[IndexRow] {

  require(
    iter.cfs.name == Tables.SortedIndex.CFName ||
      iter.cfs.name == Tables.SortedIndex.CFName2)

  // C* Iterator will hand rows in CF order. Keep the key bytes around
  // to detect when moving to a new row.
  private[this] var currentKey: ByteBuf = Unpooled.EMPTY_BUFFER

  // Cache the decoded row key fields to amortize decoding
  // (particularly terms).
  private[this] var currentScope: ScopeID = _
  private[this] var currentIndex: IndexID = _
  private[this] var currentTerms: Vector[IndexTerm] = _
  private[this] var currentMvt = Timestamp.Epoch

  // This look-behind mirrors the pair-wise reduction in
  // SetSortedValues.applyGCRules().
  private[this] var prev: IndexValue =
    CollectionAtTSSentinel.atValidTS(Timestamp.Min)

  // Stash the next row to return in the typical case where
  // something like this happens:
  // while (iter.hasNext) { val v = iter.next(); ... }
  private[this] var actual: IndexRow = _

  def hasNext: Boolean = {
    if (actual ne null) {
      return true
    }

    actual = computeNext()
    actual ne null
  }

  def next(): IndexRow = {
    val toReturn = if (actual eq null) {
      computeNext()
    } else {
      actual
    }

    require(toReturn ne null)

    actual = null
    toReturn
  }

  @annotation.tailrec
  private def computeNext(): IndexRow =
    if (iter.hasNext) {
      val (rowKey, cell) = iter.next()

      // New row.
      if ((currentKey eq null) || rowKey != currentKey) {
        setKey(rowKey)

        // Forget the last value.
        prev = CollectionAtTSSentinel.atValidTS(Timestamp.Min)
      }

      val key = IndexKey(currentScope, currentIndex, currentTerms)
      val value = IndexValue.fromSortedValue(
        currentScope,
        SortedIndex.decodeIndex(rowKey, cell))

      if (value.docID.collID != prev.docID.collID || currentMvt == Timestamp.Epoch) {
        currentMvt = lookupMvt(currentScope, value.docID.collID)
      }

      // What follows is largely a duplication of
      // SetSortedValues.applyGCRules().

      if (value.tuple != prev.tuple) {
        prev = value

        maybeEmit(value) match {
          case Some(v) => IndexRow(key, v, Indexer.Empty)
          case None    => computeNext()
        }

      } else if (shouldChooseBetween(value, prev)) {
        prev = choose(value, prev)
        computeNext()

      } else {
        prev = value

        maybeEmit(value) match {
          case Some(v) => IndexRow(key, v, Indexer.Empty)
          case None    => computeNext()
        }
      }

    } else {
      null
    }

  private def setKey(rowKey: ByteBuf) = {
    currentKey = rowKey

    val (s, i, ts) = Tables.Indexes.decode(rowKey)
    currentScope = s
    currentIndex = i
    currentTerms = ts map { IndexTerm(_) }
    currentMvt = Timestamp.Epoch
  }

  private def shouldChooseBetween(a: IndexValue, b: IndexValue) =
    a.ts.validTS == b.ts.validTS || (
      a.ts.validTS <= currentMvt &&
        b.ts.validTS <= currentMvt
    )

  private def choose(a: IndexValue, b: IndexValue) =
    if (a.ts.validTS == b.ts.validTS && a.isCreate) a
    else if (a.ts.validTS > b.ts.validTS) a
    else b

  private def maybeEmit(v: IndexValue) =
    Option.when(
      v.ts.validTS > currentMvt || (
        v.isCreate && v.tuple.ttl.forall { _ >= currentMvt }
      )
    )(v)
}
