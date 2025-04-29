package fauna.tools

import fauna.lang.clocks._
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.cassandra.db.Directories
import org.apache.cassandra.db.compaction.{ Scrubber => CScrubber }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.cli.Options
import scala.util.control.NonFatal

/**
  * This is a FaunaDB-compatible version of C*'s StandaloneScrubber.
  */
object Scrubber extends SSTableApp("SSTable Scrubber") {
  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "cf", true, "Column family name")
    o.addOption("t", "threads", true, "Number of threads")
    o.addOption("v", "verbose", false, "Enable verbose output")
    o.addOption("d", "debug", false, "Enable debug output")
    o.addOption(null, "snapshot", false, "Enable pre-scrub snapshot")
  }

  start {
    val cf = cli.getOptionValue("cf")
    val threads = Option(cli.getOptionValue("t")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors
    val verbose = cli.hasOption("v")
    val debug = cli.hasOption("d")
    val saveSnapshot = cli.hasOption("snapshot")

    System.setProperty("cassandra.absc.parallel-sort", "true")

    val cfs = openColumnFamily(cf)
    val snapshot = "pre-scrub-" + Clock.time.millis

    val handler = new OutputHandler.SystemOutput(verbose, debug)
    val sstables = new ConcurrentLinkedQueue[SSTableReader]()

    forEachSSTable("Scrubber", cfs) { sstable =>
      sstables.add(sstable)

      if (saveSnapshot) {
        val dir = Directories.getSnapshotDirectory(sstable.descriptor, snapshot)
        sstable.createLinks(dir.getPath)
      }
    }

    if (saveSnapshot) {
      handler.output(s"Pre-scrub sstables snapshotted into snapshot $snapshot")
    }

    var success = true

    val executor = new Executor("Scrubber", threads)

    while (!sstables.isEmpty) {
      val sstable = sstables.poll()

      executor addWorker { () =>
        try {
          val scrubber = new CScrubber(
            cfs,
            sstable,
            false /* skipCorrupted */,
            handler,
            true /* isOffline */,
            true /* checkData */)

          try {
            scrubber.scrub()
          } finally {
            scrubber.close()
          }

          // NOTE: This will remove the input sstable from the
          // filesystem - there are no other live references to this
          // SSTableReader (hence, the DataTracker is null).
          require(sstable.markObsolete(null), s"$sstable already marked obsolete?")
        } catch {
          case NonFatal(ex) =>
            stats.incr("Scrubber.Error")
            success = false
            handler.warn(s"Error scrubbing $sstable", ex)
        } finally {
          // Don't rely on the reference queue to do this; get rid of the
          // data. Now.
          val comps = SSTable.componentsFor(sstable.descriptor)
          SSTable.delete(sstable.descriptor, comps)
        }
      }
    }

    executor.waitWorkers()

    // Wait for deletion tasks to complete.
    SSTableDeletingTask.waitForDeletions()

    if (success) {
      handler.output("Scrubber completed successfully.")
      stats.incr("Scrubber.Success")
      sys.exit(0)
    } else {
      stats.incr("Scrubber.Failure")
      sys.exit(1)
    }
  }
}
