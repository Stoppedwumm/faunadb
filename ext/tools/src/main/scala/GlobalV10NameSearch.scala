package fauna.tools

import fauna.atoms._
import fauna.exec.Timer
import fauna.lang.{ Timestamp, Timing }
import fauna.model.SchemaNames
import fauna.repo.doc.Version
import fauna.storage._
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import java.util.concurrent.atomic.LongAdder
import org.apache.commons.cli.Options
import scala.concurrent.duration._

/** Read Versions SSTables looking for collections and functions with a given name or
  * alias. Useful to discover FQL v10 global names that are available to claim as
  * builtins. Initial ran took about ~15m to scan over a cluster backup of 3.3 TB
  * (across all CFs) using 64 threads.
  */
object GlobalV10NameSearch extends SSTableApp("GlobalV10NameSearch") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "name", true, "The global name to search for.")
    o.addOption(null, "threads", true, "The number of threads to use.")
  }

  start {
    val name =
      Option(cli.getOptionValue("name")) getOrElse {
        handler.warn("--name is required.")
        sys.exit(1)
      }

    val threads =
      Option(cli.getOptionValue("threads"))
        .flatMap { _.toIntOption }
        .getOrElse(sys.runtime.availableProcessors)

    run(name, threads)
    sys.exit(0)
  }

  private def run(name: String, threads: Int) = {
    val segments = Segment.All.subSegments(threads)
    val executor = new Executor("Global name search", threads)

    handler.output(
      s"Looking for collections or functions called '$name' using $threads threads.")

    // Only collections and functions live at the global scope in FQL v10.
    val selector =
      Selector.Schema(
        Set(
          CollectionID.collID,
          UserFunctionID.collID
        ))

    val versions =
      openColumnFamily(
        Tables.Versions.CFName,
        withSSTables = true
      )

    val iterCounter = new IteratorStatsCounter
    val found = new LongAdder
    val start = Timing.start

    segments foreach { segment =>
      executor.addWorker { () =>
        val iter =
          new CassandraIterator(
            iterCounter,
            versions,
            ScanBounds(segment),
            selector,
            snapTime = Timestamp.MaxMicros
          )

        while (iter.hasNext) {
          val (key, cell) = iter.next()
          val version = Version.decodeCell(key, cell)
          // TODO: use partial cbor codec
          def matchesIdent =
            try {
              SchemaNames.findName(version) == name ||
              SchemaNames.findAlias(version) == Some(name)
            } catch {
              case _: IllegalArgumentException => false
            }

          if (matchesIdent) {
            handler.output(s"!!! Found version: ${version}")
            found.increment()
          }
        }
      }
    }

    def report(): Unit = {
      val stats = iterCounter.snapshot()

      handler.output(
        s"Found: ${found.sum}. " +
          s"Rows: ${stats.rows} " +
          s"Cells: ${stats.cells} total, " +
          s"${stats.liveCells} live, " +
          s"${stats.unbornCells} unborn, " +
          s"${stats.deletedCells} deleted " +
          s"${stats.bytesRead} bytes read " +
          s"(${stats.bytesRead / 1024 / 1024 / start.elapsedSeconds.max(1)} MB/s)"
      )
    }

    Timer.Global.scheduleRepeatedly(10.seconds, true) {
      report()
    }

    executor.waitWorkers()
    report()

    val end = start.elapsed
    handler.output(
      s"Total search time: ${end.toMinutes / 60}h${end.toMinutes % 60}m")
  }
}
