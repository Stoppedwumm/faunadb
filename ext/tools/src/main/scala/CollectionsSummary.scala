package fauna.tools

import fauna.atoms._
import fauna.exec.Timer
import fauna.lang.{ Timestamp, Timing }
import fauna.storage._
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import java.util.concurrent.ConcurrentHashMap
import org.apache.cassandra.utils.StreamingHistogram
import org.apache.commons.cli.Options
import scala.concurrent.duration._

/** Read Versions SSTables gathering histograms on document's history length, version
  * sizes, and total history size for a given scope. Initial run took about ~12m to
  * scan over a cluster backup of 3.3 TB (across all CFs) using 64 threads.
  */
object CollectionsSummary extends SSTableApp("CollectionsSummary") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "scope", true, "The scope ID to summarize collections.")
    o.addOption(null, "threads", true, "The number of threads to use.")
  }

  start {
    val scopeID =
      Option(cli.getOptionValue("scope"))
        .flatMap { _.toLongOption }
        .map { ScopeID(_) }
        .getOrElse {
          handler.warn("--scope is required.")
          sys.exit(1)
        }

    val threads =
      Option(cli.getOptionValue("threads"))
        .flatMap { _.toIntOption }
        .getOrElse(sys.runtime.availableProcessors)

    run(scopeID, threads)
    sys.exit(0)
  }

  private class Component(val title: String) {
    var largestValue = 0L
    var largestDoc = DocID.MinValue
    val histogram = new StreamingHistogram(10)

    def update(docID: DocID, value: Long): Unit =
      synchronized {
        histogram.update(value.toDouble)
        if (value > largestValue) {
          largestValue = value
          largestDoc = docID
        }
      }

    def display(prefix: String): Unit = {
      var count = 0L
      handler.output(s"${prefix}$title:")
      histogram.getAsMap forEach { (p, m) =>
        handler.output(f"${prefix}$p%20.2f $m")
        count += m
      }
      handler.output(s"${prefix}Largest: $largestDoc $largestValue")
      handler.output(s"${prefix}  Count: $count")
    }
  }

  private final class Summary {
    val versionSize = new Component("Version Size")
    val docHistoryLength = new Component("Doc. History Length")
    val docHistorySize = new Component("Doc. History Size")
  }

  private def run(scopeID: ScopeID, threads: Int) = {
    val segments = Segment.All.subSegments(threads)
    val executor = new Executor("Collections summary", threads)

    handler.output(s"Summarizing collections for $scopeID using $threads threads.")

    val versions =
      openColumnFamily(
        Tables.Versions.CFName,
        withSSTables = true
      )

    val summaries = new ConcurrentHashMap[CollectionID, Summary]
    val iterCounter = new IteratorStatsCounter
    val start = Timing.start

    segments foreach { segment =>
      executor.addWorker { () =>
        val iter =
          new CassandraIterator(
            iterCounter,
            versions,
            ScanBounds(segment),
            Selector.UserCollections(scopeID),
            snapTime = Timestamp.MaxMicros
          )

        var prevDoc: DocID = null
        var docHistorySize = 0L
        var docHistoryLen = 0L

        while (iter.hasNext) {
          val (key, cell) = iter.next()
          val (_, currDoc) = Tables.Versions.decodeRowKey(key)
          val cellByteSize = cell.byteSize // it's a def; cache it.

          summaries
            .computeIfAbsent(currDoc.collID, { _ => new Summary() })
            .versionSize
            .update(currDoc, cellByteSize)

          if (prevDoc == currDoc) {
            docHistoryLen += 1
            docHistorySize += cellByteSize
          } else {
            if (prevDoc ne null) {
              val summary = summaries.get(prevDoc.collID)
              summary.docHistoryLength.update(prevDoc, docHistoryLen)
              summary.docHistorySize.update(prevDoc, docHistorySize)
            }
            docHistoryLen = 1
            docHistorySize = cellByteSize
            prevDoc = currDoc
          }
        }

        if (prevDoc ne null) { // last doc seen
          val summary = summaries.get(prevDoc.collID)
          summary.docHistoryLength.update(prevDoc, docHistoryLen)
          summary.docHistorySize.update(prevDoc, docHistorySize)
        }
      }
    }

    def report(): Unit = {
      val stats = iterCounter.snapshot()

      handler.output(
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
      s"Total process time: ${end.toMinutes / 60}h${end.toMinutes % 60}m")

    handler.output("Collection summaries:")
    summaries.forEach { (collID, summary) =>
      val prefix = " " * 4
      handler.output(s"  $collID")
      summary.docHistoryLength.display(prefix)
      summary.docHistorySize.display(prefix)
      summary.versionSize.display(prefix)
    }
  }
}
