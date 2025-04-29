package fauna.storage.cassandra

import java.io.{ Closeable, IOException }
import java.util.{ Collections, Comparator, TreeSet }
import org.apache.cassandra.db.{ ColumnFamilyStore, Row }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.sstable.metadata._
import org.apache.cassandra.io.util.FileUtils

/** A RowBuffer is conceptually similar to a memtable, but specific to
  * the needs of offline tools which may process rows out of their
  * natural order.
  *
  * Like a memtable, it accumulates mutations (rows) up to some
  * predefined size limit before flushing them to storage as a new
  * SSTable.
  *
  * The buffer _MUST_ be closed after all calls to add() have
  * completed, or buffered rows may not be flushed to storage.
  */
final class RowBuffer(
  cfs: ColumnFamilyStore,
  level: Int,
  maxSizeMB: Int,
  maxDataAge: Long, // See SSTableReader.maxDataAge for important
                    // notes.
  repairedAt: Long) // Largely irrelevant in FaunaDB, which doesn't
                    // use C* repair.
    extends Comparator[Row]
    with Closeable {

  private[this] val buffer = new TreeSet(this)
  private[this] var bufferBytes = 0L

  /** Adds a row to the buffer, possibly flushing the accumulated rows to a new SSTable. */
  def add(row: Row, dataBytes: Long): Unit = {
    buffer.add(row)
    bufferBytes += dataBytes
    maybeFlush()
  }

  def close(): Unit =
    if (!buffer.isEmpty) {
      flush()
    }

  def compare(r1: Row, r2: Row): Int =
    r1.key.compareTo(r2.key)

  private def maybeFlush(): Unit =
    if ((bufferBytes / 1024 / 1024) > maxSizeMB) {
      flush()
    }

  private def flush() = {
    val dest = cfs.directories.getWriteableLocationAsFile(bufferBytes)

    if (dest eq null) {
      throw new IOException("Disk full.")
    }

    FileUtils.createDirectory(dest)

    val meta = new MetadataCollector(
      Collections.emptySet,
      cfs.metadata.comparator,
      level)

    val writer = new SSTableWriter(
      cfs.getTempSSTablePath(dest),
      cfs.metadata.getMinIndexInterval(),
      repairedAt,
      cfs.metadata,
      cfs.partitioner,
      meta)

    try {
      buffer forEach { row =>
        writer.append(row.key, row.cf)
      }

      buffer.clear()
      bufferBytes = 0

      val reader =
        writer.finish(
          SSTableWriter.FinishType.NORMAL,
          maxDataAge,
          repairedAt)
      reader.selfRef.release()
    } catch {
      case ex: Throwable =>
        // WARNING: Failing to abort() here will leave inconsistent
        // state on disk!
        writer.abort(ex.getMessage)
        throw ex
    }
  }
}
