package fauna.storage.cassandra

import fauna.lang.syntax._
import fauna.lang.Timing
import java.util.{ ArrayList, Comparator, UUID }
import java.util.concurrent.PriorityBlockingQueue
import org.apache.cassandra.db.compaction.CompactionTask
import org.apache.cassandra.db.ColumnFamilyStore
import org.apache.cassandra.dht.Bounds
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.utils.OutputHandler
import scala.jdk.CollectionConverters._

object SizeCompaction {

  def newBuilder(cfs: ColumnFamilyStore): Builder =
    new Builder(cfs)

  final class Builder(cfs: ColumnFamilyStore) {
    private[this] val sstables =
      new PriorityBlockingQueue[SSTableReader](
        128 /* WAG */,
        new Comparator[SSTableReader] {
          def compare(a: SSTableReader, b: SSTableReader): Int =
            a.onDiskLength() compareTo b.onDiskLength()
        })

    private[this] var targetMegabytes: Int = 160
    private[this] var maxBatchSize: Int = Int.MaxValue
    private[this] var checkOverlap: Boolean = true
    private[this] var offline: Boolean = true

    private[this] var handler: OutputHandler =
      new OutputHandler.LogOutput

    def size(): Int =
      sstables.size()

    /** Sets the target SSTable file size in bytes. SSTables smaller than
      * this size will be compacted together until the sum exceeds
      * this value.
      *
      * Defaults to 160MiB.
      */
    def targetMegabytes(mbytes: Int): Builder = {
      targetMegabytes = mbytes
      this
    }

    /** Sets the maximum number of SSTables to be compacted together in
      * one task.
      */
    def maxBatchSize(size: Int): Builder = {
      maxBatchSize = size
      this
    }

    /** Disables compaction of overlapping SSTables in preference to
      * strictly attempting to meet the target file size.
      */
    def disableOverlap(): Builder = {
      checkOverlap = false
      this
    }

    // Used for testing.
    def setOffline(off: Boolean): Builder = {
      offline = off
      this
    }

    def withHandler(output: OutputHandler): Builder = {
      handler = output
      this
    }

    def add(sstable: SSTableReader) =
      sstables.add(sstable)

    def build(): Seq[SizeCompaction] = {
      val compactions = Seq.newBuilder[SizeCompaction]
      var batchTotal = 0L

      val batch = new ArrayList[SSTableReader](128 /* WAG */ )

      def scheduleBatch(): Unit = {
        if (batch.isEmpty) {
          return
        }

        val b = new ArrayList[SSTableReader](batch)
        val bytes = batchTotal

        val id = UUID.randomUUID()
        handler.output(
          s"[$id] Compacting ${b.size} SSTables of size ${bytes.humanReadableSize()}.")
        compactions += new SizeCompaction(id, cfs, b, handler, offline)

        batch.clear()
        batchTotal = 0
      }

      while (!Option(sstables.peek()).isEmpty) {
        val sstable = sstables.poll()

        var overlap = false

        if (checkOverlap) {
          val first = sstable.first.getToken()
          val last = sstable.last.getToken()
          val bounds = new Bounds(first, last)

          handler.debug(s"Checking for sstables overlapping $bounds.")

          // Prefer compacting overlapping sstables together to expunge
          // tombstoned data.
          sstables.iterator.asScala foreach { candidate =>
            val first = candidate.first.getToken()
            val last = candidate.last.getToken()

            val b = new Bounds(first, last)

            if (b.intersects(bounds)) {
              overlap = true

              batch.add(candidate)
              batchTotal += candidate.onDiskLength()

              sstables.remove(candidate)
            }
          }
        }

        if (overlap || (sstable.onDiskLength() / 1024 / 1024) < targetMegabytes) {

          batch.add(sstable)
          batchTotal += sstable.onDiskLength()

          val fullBatch = (batchTotal / 1024 / 1024) >= targetMegabytes
          if (fullBatch || batch.size >= maxBatchSize) {
            scheduleBatch()
          }

        } else if ((sstable.onDiskLength() / 1024 / 1024) >= targetMegabytes) {
          handler.warn(
            s"SSTable size (${sstable.onDiskLength().humanReadableSize()}) exceeds target $targetMegabytes MB, skipping $sstable.")
        }
      }

      // If there remains a batch which does not exceed the batchTotal,
      // schedule it. This is important, for example, when the entire
      // dataset is less than the minimum SSTable size, but there is
      // still more than a single SSTable.
      if (batch.size <= maxBatchSize) {
        scheduleBatch()
      }

      compactions.result()
    }
  }
}

final class SizeCompaction private (
  id: UUID,
  cfs: ColumnFamilyStore,
  sstables: ArrayList[SSTableReader],
  handler: OutputHandler,
  offline: Boolean)
    extends Runnable {

  def run(): Unit = {
    val start = Timing.start

    val task =
      new CompactionTask(cfs, sstables, Int.MinValue /* gcBefore */, offline)

    task.run()

    if (offline) {
      // NOTE: This will remove the input sstable from the
      // filesystem - there are no other live references to this
      // SSTableReader (hence, the DataTracker is null).

      sstables forEach { sst =>
        require(sst.markObsolete(null), s"$sst already marked obsolete?")

        // Don't rely on the reference queue to do this; get rid of the
        // data. Now.
        val comps = SSTable.componentsFor(sst.descriptor)
        SSTable.delete(sst.descriptor, comps)
      }
    }

    handler.output(
      s"[$id] Compacted ${sstables.size} SSTables in ${start.elapsedMillis}ms")
  }
}
