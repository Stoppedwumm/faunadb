package fauna.tools

import com.google.common.util.concurrent.AtomicDouble
import fauna.atoms.Segment
import fauna.exec.Timer
import fauna.lang.Timing
import fauna.storage.cassandra.IteratorStatsCounter
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.cassandra.utils.OutputHandler
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object SegmentExecutor {
  val LogInterval = 10.seconds
}

class SegmentExecutor(threads: Int, handler: OutputHandler) {
  import SegmentExecutor._

  // Make a whole bunch of segments, so that we get a more even distrobution
  // between threads.
  private val totalSegments = threads * 256
  private val segments = new ConcurrentLinkedQueue(
    Segment.All.subSegments(totalSegments).asJava)
  private val executor = new Executor("Sorted index scan", threads)

  // Store the progress of each thread.
  private val progress = Seq.fill(threads)(new AtomicDouble(0))
  private val completedSegments = new AtomicLong(0)

  // This is passed to a `CassandraIterator` that is build in `work`, and is used to
  // capture stats for logging and progress.
  val iterCounter = new IteratorStatsCounter

  def skipSegments(n: Int): Unit = {
    (0 until n).foreach { _ =>
      completedSegments.incrementAndGet()
      segments.poll()
    }
  }

  def work(f: (Segment, Double => Unit) => Unit): Unit = {
    (0 until threads).foreach { i =>
      executor.addWorker { () =>
        var segment = segments.poll()
        while (segment ne null) {
          f(segment, progress => this.progress(i).set(progress))

          completedSegments.incrementAndGet()
          segment = segments.poll()
        }
      }
    }
  }

  /** The number of completed segments.
    */
  def completed: Long = completedSegments.get

  /** A fraction from 0.0 to 1.0, which is how much data we've scanned so far.
    */
  def percent: Double = {
    val completed = completedSegments.get
    val total = progress.map(_.get).sum + completed
    total / totalSegments
  }

  def runWithProgress() = {
    var prevBytesRead = 0L
    val timing = Timing.start
    def report(): Unit = {
      val stats = iterCounter.snapshot()
      val bytesRead = stats.bytesRead - prevBytesRead
      prevBytesRead = stats.bytesRead
      val speed = bytesRead.toDouble / LogInterval.toSeconds

      val completed = this.completed
      val percent = this.percent

      // We are `percent` into `total_time`, which is the elapsed time. So, given the
      // following:
      // total_time * percent = elapsed
      // total_time = remaining + elapsed
      //
      // We can re-arrange to get:
      // remaining = total_time - elapsed
      // remaining = (elapsed / percent) - elapsed
      val elapsed = timing.elapsed
      val remaining = elapsed / percent - elapsed

      handler.output(
        s"Rows: ${stats.rows} " +
          s"Cells: ${stats.cells} total, " +
          s"${stats.liveCells} live, " +
          s"${stats.unbornCells} unborn, " +
          s"${stats.deletedCells} deleted " +
          s"${stats.bytesRead} bytes read, " +
          s"at ${((speed / 1024 / 1024) * 1000).round / 1000.0}MB/s " +
          s"progress: ${(percent * 100000).round / 1000.0}% (segments: ${completed}) " +
          s"elapsed: ${CheckSortedIndex.prettyDuration(elapsed)} " +
          s"remaining: ${CheckSortedIndex.prettyDuration(remaining)}"
      )
    }

    handler.output("Scanning...")

    @volatile var searching = true
    Timer.Global.scheduleRepeatedly(LogInterval, searching) {
      report()
    }

    executor.waitWorkers()
    handler.output("Done scanning.")
    report()
    searching = false
  }
}
