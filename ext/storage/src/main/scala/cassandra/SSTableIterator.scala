package fauna.storage.cassandra

import fauna.lang.syntax._
import java.io.{ Closeable, File, IOException }
import java.nio.ByteBuffer
import org.apache.cassandra.db.DecoratedKey
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.io.util.RandomAccessReader
import org.apache.cassandra.utils.{ ByteBufferUtil, OutputHandler }
import scala.util.control.NonFatal

object SSTableIterator {
  trait Stats {

    def recordRuntime(elapsedMillis: Long): Unit

    def recordSSTableBytes(bytes: Long): Unit

    /** Called for each cell observed in the input to a SSTableIterator. */
    def incrCells(count: Int = 1): Unit

    /** Called for each cell discarded because its timestamp was later
      * than the snapshot time of the Transformer.
      */
    def incrCellsUnborn(count: Int = 1): Unit

    /** Called for each row observed in the input to an SSTableIterator. */
    def incrRows(): Unit

    /** Called with the size in bytes of each row observed in the input
      * to an SSTableIterator.
      */
    def recordRowBytes(bytes: Long): Unit
  }
}

/** An SSTableIterator yields each key and the size of its cell data
  * to the `apply()` method for processing.
  *
  * The SSTableReader provided to the constructor must be open with a
  * valid PRIMARY_INDEX component.
  *
  * SSTableIterator does _NOT_ close the reader.
  */
abstract class SSTableIterator(
  sstable: SSTableReader,
  output: OutputHandler,
  stats: SSTableIterator.Stats)
    extends Closeable
    with Runnable {

  requireIndex()

  // Subclasses have access to the data file to read the cell data for
  // each row.
  protected val dataFile: RandomAccessReader =
    sstable.openDataReader()

  private[this] val indexFile = {
    val file = new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX))
    RandomAccessReader.open(file)
  }

  private[this] val indexSerializer =
    sstable.metadata.comparator.rowIndexEntrySerializer

  private[this] var currentKey: ByteBuffer = _
  private[this] var nextKey: ByteBuffer = _

  private[this] var currentPosition: Long = 0
  private[this] var nextPosition: Long = 0

  def apply(key: DecoratedKey, dataSize: Long): Unit

  def run(): Unit = {
    require(!indexFile.isEOF(), "index file EOF")

    // Set the next key to the first key in the index.
    nextKey = ByteBufferUtil.readWithShortLength(indexFile)

    val init =
      indexSerializer.deserialize(indexFile, sstable.descriptor.version).position
    require(init == 0, init)

    while (!dataFile.isEOF() && (nextKey ne null)) {
      updateIndexKey()

      val rowStart = dataFile.getFilePointer()
      output.debug(() => s"Reading row at $rowStart")

      val key =
        sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(dataFile))
      if (key eq null) {
        throw new IOException(
          s"Unable to read row key from data file $dataFile at $rowStart")
      }

      // +2 bytes for the length
      val dataStart = currentPosition + 2 + currentKey.remaining()
      val dataSize = nextPosition - dataStart

      output.debug(() =>
        s"Row ${key.getKey.toHexString} at $rowStart is $dataSize bytes")
      stats.incrRows()
      stats.recordRowBytes(dataSize)

      require(
        currentKey.equals(key.getKey),
        "Keys do not match between data and index!")
      require(dataSize <= dataFile.length(), "Row size is greater than file size!")

      apply(key, dataSize)

      // This allows for apply() to avoid consuming cell data, if
      // necessary.
      if (nextPosition < dataFile.length()) {
        dataFile.seek(nextPosition)
      }
    }
  }

  def close(): Unit = {
    def close0(c: Closeable) =
      try {
        c.close()
      } catch {
        case NonFatal(_) => output.warn(s"Failed closing $c")
      }

    close0(dataFile)
    close0(indexFile)
  }

  private def requireIndex() = {
    val file = new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX))
    require(file.exists(), s"Missing index file for $sstable.")
  }

  private def updateIndexKey() = {
    currentKey = nextKey
    currentPosition = nextPosition

    try {
      // This is a normal condition for the final row in the data
      // file - there is no next key.
      if (indexFile.isEOF) {
        nextKey = null
        nextPosition = dataFile.length()
      } else {

        nextKey = ByteBufferUtil.readWithShortLength(indexFile)

        val row = indexSerializer.deserialize(indexFile, sstable.descriptor.version)

        nextPosition = row.position
      }
    } catch {
      case NonFatal(ex) =>
        output.warn("Error reading index file", ex)
        nextKey = null
        nextPosition = dataFile.length()
    }
  }
}
