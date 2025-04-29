package fauna.tools

import fauna.lang.clocks._
import fauna.storage.Cassandra
import fauna.storage.cassandra.SizeCompaction
import org.apache.cassandra.db.Directories
import org.apache.cassandra.io.sstable._
import org.apache.commons.cli.Options

/**
  * The SizeCompactor tool processes SSTables in a snapshot, merging
  * files smaller than a target byte size into larger sstables.
  *
  * This process will result in fewer sstables to transfer, process,
  * or load into a cluster.
  *
  * Typical usage is like so:
  *   export FAUNADB_CONFIG=path/to/faunadb.yml
  *   java -cp path/to/faunadb.jar fauna.tools.SizeCompator --cf Versions --size 10485760
  *
  * --size is in Megabytes. If --size is not provided, the
  * storage_memtable_size_mb configuration setting is used.
  *
  * If --threads is not provided, the tool will use one thread per
  * processor.
  */
object SizeCompactor extends SSTableApp("Size-based SSTable Compactor") {
  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "cf", true, "Column family name")
    o.addOption("s", "size", true, "Target sstable size in Megabytes")
    o.addOption("t", "threads", true, "Number of threads")
    o.addOption(null, "snapshot", false, "Snapshot sstables prior to compaction.")
    o.addOption(
      null,
      "disable-overlap",
      false,
      "Disable compaction of overlapping SSTables.")
    o.addOption(
      null,
      "max-batch-size",
      true,
      "Maximum # of SSTables to compact in one batch.")
  }

  start {
    val cf = cli.getOptionValue("cf")
    val target = Option(cli.getOptionValue("s")) flatMap {
      _.toIntOption
    } getOrElse config.storage_memtable_size_mb
    val threads = Option(cli.getOptionValue("t")) flatMap {
      _.toIntOption } getOrElse 0
    val snapshot = cli.hasOption("snapshot")
    val checkOverlap = !cli.hasOption("disable-overlap")
    val maxBatchSize = Option(cli.getOptionValue("max-batch-size")) flatMap {
      _.toIntOption
    } getOrElse Int.MaxValue

    System.setProperty("cassandra.absc.parallel-sort", "true")

    val executor = new Executor("Compaction", threads)

    cf.split(',') foreach {
      run(_, target, snapshot, checkOverlap, maxBatchSize, executor)
    }

    executor.waitWorkers()

    // Wait for deletion tasks to complete.
    SSTableDeletingTask.waitForDeletions()

    sys.exit(0)
  }

  def run(
    cf: String,
    targetSize: Int,
    snapshot: Boolean = false,
    checkOverlap: Boolean = true,
    maxBatchSize: Int = Int.MaxValue,
    executor: Executor): Unit = {
    val cfs = openColumnFamily(cf)

    // Disable bloom filters.
    cfs.metadata.bloomFilterFpChance(1.0)

    val snapshotName = "pre-compact-" + Clock.time.millis

    val builder = SizeCompaction.newBuilder(cfs)
      .targetMegabytes(targetSize)
      .maxBatchSize(maxBatchSize)

    if (!checkOverlap) {
      builder.disableOverlap()
    }

    forEachSSTable("SizeCompactor", cfs) { sstable =>
      builder.add(sstable)

      if (snapshot) {
        val dir = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName)
        sstable.createLinks(dir.getPath)
      }
    }

    handler.output(
      s"Found ${builder.size()} sstables for ${Cassandra.KeyspaceName}.$cf")

    if (snapshot) {
      handler.output(s"Pre-compaction sstables snapshotted into snapshot $snapshotName")
    }

    builder.build() foreach { task =>
      executor.addWorker { () => task.run() } // Annoying.
    }
  }
}
