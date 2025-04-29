package fauna.storage.cassandra

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.storage.Tables
import fauna.storage.ir._
import io.netty.buffer.Unpooled
import java.nio.ByteBuffer
import org.apache.cassandra.db.{
  Cell,
  DecoratedKey,
  OnDiskAtom
}
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.util.RandomAccessReader
import org.apache.cassandra.utils.OutputHandler
import scala.annotation.unused

object Transformer {

  trait Stats extends SSTableIterator.Stats {

    /** Called for each row discarded by a Transformer. */
    def incrDiscardedRows(): Unit

    /** Called for each row rewritten by a Transformer. */
    def incrRowsRewritten(): Unit

    /** Called for each cell rewritten by a Transformer. */
    def incrCellsRewritten(): Unit
  }

  /** This trait is a shim between the ByteBuffers in C* and the
    * ByteBufs in fauna.codex.
    */
  trait Codec[K] {
    def decode(bytes: ByteBuffer): K
    def encode(key: K): ByteBuffer
  }

  object VersionCodec extends Codec[(ScopeID, DocID)] {

    def decode(bytes: ByteBuffer) =
      Tables.Versions.decodeRowKey(Unpooled.wrappedBuffer(bytes))

    def encode(key: (ScopeID, DocID)) =
      ByteBuffer.wrap((Tables.Versions.rowKey _).tupled(key))
  }

  object IndexCodec extends Codec[(ScopeID, IndexID, Vector[IRValue])] {

    def decode(bytes: ByteBuffer) =
      Tables.Indexes.decode(Unpooled.wrappedBuffer(bytes))

    def encode(key: (ScopeID, IndexID, Vector[IRValue])) =
      Tables.Indexes.rowKey(key._1, key._2, key._3).nioBuffer
  }

  object LookupsCodec extends Codec[GlobalID] {

    def decode(bytes: ByteBuffer) =
      Tables.Lookups.decode(Unpooled.wrappedBuffer(bytes))

    def encode(key: GlobalID) =
      ByteBuffer.wrap(Tables.Lookups.rowKey(key))
  }

}

/** Transformers are used during the process of rewriting an
  * SSTable for moving, copying, or restoring data from a snapshot.
  *
  * Subclasses should implement `transformKey()`, and (optionally)
  * `transformCell()`.
  *
  * When constructed, the RAR's position should be at the beginning of
  * the column data, and the row should have `dataSize` bytes
  * remaining before the next row key. This constraint is intended to
  * allow one RAR to be used for every row in the sstable.
  *
  * See also: SSTableIterator.
  */
abstract class Transformer[K](
  private[this] val rowKey: DecoratedKey,
  private[this] val dataSize: Long,
  private[this] val sstable: SSTableReader,
  private[this] val dataFile: RandomAccessReader,
  private[this] val codec: Transformer.Codec[K],
  private[this] val snapshotTS: Timestamp,
  private[this] val output: OutputHandler,
  private[this] val stats: Transformer.Stats) {

  // FIXME: Ideally, a Transformer would be used for multiple
  // rows, just as the RAR is.

  /** Given a row key, returns a transformed key.
    *
    * This function may return a key which is assigned to a different
    * partition.
    */
  protected def transformKey(key: K): K

  /** Given a row key and a Cell, return a transformed Cell.
    */
  protected def transformCell(@unused key: K, cell: Cell): Cell = cell

  /** Controls whether cells with the given row key are yielded by
    * `getRow()`.
    *
    * Note that this is a function of the _original_ row key, not the
    * transformed key.
    */
  protected def includeRow(@unused key: K): Boolean = true

  /** Yields the next row from the SSTable, transformed as necessary.
    */
  def getRow(): OnDiskAtomIterator = {
    val oldKey = codec.decode(rowKey.getKey)

    if (includeRow(oldKey)) {
      val newKey = transformKey(oldKey)

      val key = if (oldKey != newKey) {
        output.debug(() => s"Row key $oldKey transformed to $newKey")
        stats.incrRowsRewritten()

        val bytes = codec.encode(newKey)
        sstable.partitioner.decorateKey(bytes)
      } else {
        output.debug(() => s"Row key $oldKey unchanged")
        rowKey
      }

      val identity =
        new SnapshotFilter(sstable, dataFile, snapshotTS, key, dataSize)

      // NB. The rewrite is based on the value of the _old_ key.
      new CellTransformer(oldKey, identity)
    } else {
      stats.incrDiscardedRows()
      // Drop cells for this row.
      new OnDiskAtomIterator {
        def getColumnFamily() = null
        def getKey = rowKey
        def close() = ()
        def hasNext = false
        def next() = null
      }
    }
  }

  // CellTransformer is a small wrapper around an identity iterator which
  // rewrites Cells as they are yielded.
  private final class CellTransformer(
    private[this] val key: K,
    private[this] val iterator: CellFilter)
      extends OnDiskAtomIterator {

    def next() = {
      if (!iterator.hasNext()) {
        throw new IllegalStateException("Called next() on an empty iterator.")
      }

      rewrite(iterator.next())
    }

    def hasNext() = iterator.hasNext()
    def getKey() = iterator.getKey()
    def getColumnFamily() = iterator.getColumnFamily()

    def close() = {
      val cells = iterator.cellCount
      val unborn = iterator.totalCellCount - cells

      stats.incrCells(cells)
      stats.incrCellsUnborn(unborn)

      iterator.close()
    }

    private def rewrite(atom: OnDiskAtom) = {
      // FaunaDB does not use range tombstones.
      require(atom.isInstanceOf[Cell], "Range tombstones should not exist.")

      val cell = atom.asInstanceOf[Cell]
      stats.incrCellsRewritten()
      transformCell(key, cell)
    }
  }
}
