package fauna.tools

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.LookupHelpers
import fauna.repo.doc.Version
import fauna.storage._
import fauna.storage.cassandra._
import fauna.storage.ops._
import java.time.format.DateTimeParseException
import org.apache.commons.cli.Options

/** This tool (re-)builds the Lookups column family based on data in
  * the Versions column family. The output is useful for comparison
  * with a cluster snapshot to detect corruption.
  */
object LookupBuilder extends SSTableApp("Lookup Builder") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "segments", true, "Number of segments (default: ncpu)")
    o.addOption(null, "snapshot", true, "Snapshot time")
    o.addOption(null, "threads", true, "Number of threads (default: ncpu)")
  }

  start {
    val snapshotTS = Option(cli.getOptionValue("snapshot")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case ex: DateTimeParseException =>
          handler.warn(s"Bad snapshot time: $str", ex)
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val subsegments = Option(cli.getOptionValue("segments")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors()

    val threads = Option(cli.getOptionValue("threads")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors()

    val versions = openColumnFamily(Tables.Versions.CFName, withSSTables = true)
    val executor = new Executor("Lookup Builder", threads)
    val counter = new IteratorStatsCounter
    val selector = Selector.Schema(Set(DatabaseID.collID, KeyID.collID))
    val segments = Segment.All.subSegments(subsegments)

    segments foreach { segment =>
      executor.addWorker { () =>
        val iter = new CassandraIterator(
          counter,
          versions,
          ScanBounds(segment),
          selector,
          snapshotTS)

        while (iter.hasNext) {
          val (key, cell) = iter.next()

          val version = Version.decodeCell(key, cell)

          LookupHelpers.lookups(version) foreach { entry =>
            val write = LookupAdd(entry)

            // FIXME: This duplicates
            // StorageEngine.applyMutationForKey().
            val mut = CassandraMutation(
              Cassandra.KeyspaceName,
              write.rowKey.nioBuffer(),
              index1CF = false,
              index2CF = false
            )

            write.mutateAt(mut, version.ts.transactionTS)
            versions.keyspace.apply(mut.cmut, false /* writeCommitLog */)
          }
        }
      }
    }

    executor.waitWorkers()
    // One final flush, in case any memtables are still around.
    versions.keyspace.getColumnFamilyStores forEach {
      _.forceBlockingFlush()
    }

    val stats = counter.snapshot()
    val msg = s"Rows: ${stats.rows}\n" +
      s"Cells: ${stats.cells} total, " +
      s"${stats.liveCells} live, " +
      s"${stats.unbornCells} unborn, " +
      s"${stats.deletedCells} deleted\n" +
      s"${stats.bytesRead} bytes read"

    handler.output(msg)

    sys.exit(0)
  }
}
