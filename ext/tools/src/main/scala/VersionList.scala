package fauna.tools

import fauna.lang.{ Timestamp, Timing }
import fauna.repo.doc.Version
import fauna.storage._
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import java.util.concurrent.atomic.LongAdder

object VersionList extends SSTableApp("VersionList") {

  start {
    run()
    sys.exit(0)
  }

  private def run() = {
    handler.output(s"Reading all the versions.")

    // Only collections and functions live at the global scope in FQL v10.
    val selector = Selector.All

    val versions = openColumnFamily(Tables.Versions.CFName, withSSTables = true)

    val iterCounter = new IteratorStatsCounter
    val found = new LongAdder
    val start = Timing.start

    val iter = new CassandraIterator(
      iterCounter,
      versions,
      ScanBounds.All,
      selector,
      snapTime = Timestamp.MaxMicros)

    while (iter.hasNext) {
      val (key, cell) = iter.next()
      val version = Version.decodeCell(key, cell)

      handler.output(s"Found version: ${version}")
      found.increment()
    }

    val end = start.elapsed
    handler.output(
      s"Total search time: ${end.toMinutes / 60}h${end.toMinutes % 60}m")
  }
}
