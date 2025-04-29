package fauna.model.tasks

import fauna.atoms._
import fauna.exec.{ BackOff, LoopThreadService }
import fauna.flags._
import fauna.lang.{ Timestamp, Timing }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.JournalEntry
import fauna.repo._
import fauna.repo.cassandra._
import fauna.repo.query.Query
import fauna.repo.store._
import fauna.stats.QueryMetrics
import fauna.stats.StatsRecorder
import fauna.storage.doc._
import fauna.storage.ScanBounds
import fauna.trace._
import java.util.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Random, Success, Try }
import scala.util.control.{ NoStackTrace, NonFatal }

object Mapper {

  /**
    * Maximum number of attempts to process a segment in a single
    * iteration.
    *
    * See `Mapper.processSegment()`.
    */
  val DefaultAttempts = 5

  /**
    * Current version of mappers' `JournalEntry` schema.
    */
  val SchemaVersion = 1

  /**
    * If a segment is successfully processed in less than this amount
    * of time, consider the process essentially instantaneous and do
    * not delay in processing the next segment.
    */
  val MinimumRuntime = 1.minute

  final class ProgressException(msg: String) extends Exception(msg) with NoStackTrace

  type Paginator[ID, C] = ScanBounds => PagedQuery[Iterable[(ID, C)]]
  type IterFactory[ID, C] = Timestamp => RangeIteratee[ID, C]

  sealed trait Task {
    def init(snapshotTS: Timestamp, segment: Segment): RangeIteratee.State
  }

  object Task {

    def apply[ID, C](
      paginate: Paginator[ID, C],
      iteratee: IterFactory[ID, C]
    ) =
      new Task {

        def init(snapshotTS: Timestamp, segment: Segment): RangeIteratee.State =
          iteratee(snapshotTS).init(paginate(ScanBounds(segment)))
      }
  }
}

abstract class Mapper(
  _name: String,
  feature: Feature[HostID, BooleanValue],
  totalTime: FiniteDuration,
  timeout: FiniteDuration,
  maxAttempts: Int = Mapper.DefaultAttempts,
  val primarySegmentsOnly: Boolean = true,
  priority: Int = Thread.MIN_PRIORITY,
  clock: Clock = Clock
) extends LoopThreadService(_name, priority = priority)
    with HealthCheck {

  import Mapper._

  val task: Mapper.Task

  val log = getLogger
  val backOff = new BackOff(10.milliseconds, 10.seconds)

  /**
    * The current versioned tag for this mapper's associated `JournalEntry`.
    */
  private[this] val currentTag = s"$name-${SchemaVersion}"

  private[this] val rand = new Random

  /**
    * Returns true if this Mapper's Feature is true.
    */
  def isEnabled: Boolean =
    repo.runSynchronously(repo.hostFlag(feature), timeout).value

  /** Returns the locally-owned segments of the token ring which this
    * mapper will process.
    */
  def localSegments(): Vector[Segment] =
    if (primarySegmentsOnly) {
      repo.keyspace.localPrimarySegments.toVector
    } else {
      repo.keyspace.localSegments.toVector
    }

  def processSegment(segment: Segment): Try[Unit] = {
    val tracer = GlobalTracer.instance
    var state = Option(task.init(clock.time, segment))
    var tries = maxAttempts
    var failure = Option.empty[ProgressException]

    def currFailure = {
      if (failure.isEmpty) {
        failure = Some(new ProgressException(s"Failed after $maxAttempts attempts."))
      }
      failure.get
    }

    while (state.nonEmpty && tries > 0) {
      val span = tracer
        .buildSpan("mapper.execute")
        .ignoreParent()
        .start()

      val scope = tracer.activate(span)

      try {
        // Wait for a feature flag change or shutdown
        // notification. This bounds response to flag changes to the
        // Mapper timeout.
        backOff { wakeup =>
          if (wakeup) { // stop signal sent
            done()
            throw new InterruptedException
          }

          BackOff.Operation.breakIf(isEnabled)
        }

        state =
          try {
            storageHealthCheck()
            val next =
              repo
                .withStats(
                  StatsRecorder.Multi(
                    Seq(
                      repo.stats,
                      repo.stats
                        .scoped(s"Mappers.$name")
                        .filtered(QueryMetrics.BytesReadWrite))))
                .runSynchronously(state.get.step, timeout)
                .value
            tries = maxAttempts
            failure = None
            next
          } catch {
            case e: TimeoutException =>
              repo.stats.incr(s"$name.Timeouts")
              span.setStatus(DeadlineExceeded("timeout"))
              currFailure.addSuppressed(e)
              tries -= 1
              state

            case e: ContentionException =>
              // Since contention is handled in query eval, do not retry. give up
              // here, and let the split happen in the caller.
              throw e

            case e: UnretryableException =>
              // these are not transient exceptions - the mapper must
              // split the segment. give up here, and let the split
              // happen in the caller.
              throw e
            case e: Throwable =>
              logException(e)
              repo.stats.incr(s"$name.Errors")
              span.setStatus(InternalError(e.getMessage))
              if (NonFatal(e)) {
                currFailure.addSuppressed(e)
                tries -= 1
                state
              } else {
                throw e
              }
          }
      } finally {
        scope foreach { _.close() }
      }
    }

    // Presuming the retries were all caused by the same failure mode,
    // return the cause of the last failure to inform the caller.
    if (tries <= 0) {
      Failure(currFailure)
    } else {
      Success(())
    }
  }

  @volatile private var stopSignal: CountDownLatch = _
  @volatile private var doneSignal: CountDownLatch = _

  protected def loop(): Unit = {
    try {
      storageHealthCheck()

      var topo = -1L
      var segments = Vector.empty[Segment]

      backOff { wakeup =>
        if (wakeup) { // stop signal sent
          done()
          throw new InterruptedException
        }

        topo = repo.keyspace.topologyVersion
        segments = localSegments()

        BackOff.Operation.breakIf(
          topo == repo.keyspace.topologyVersion && segments.nonEmpty && isEnabled)
      }

      process()
    } catch {
      case _: NoSegmentsOwnedException => ()

      // Interruptions may occur while waiting in a backoff loop and
      // stop() is called. Return to the loop header.
      case _: InterruptedException => ()

      //this happens when the health check service stops before this thread receives a stop signal.
      //this happened only during debugging, but I think it would be good to let this code to avoid an infinite loop.
      case _: UninitializedException =>
        done()
    }
  }

  override def start(): Unit = {
    stopSignal = new CountDownLatch(1)
    doneSignal = new CountDownLatch(1)

    super.start()
  }

  override def stop(graceful: Boolean): Unit = {
    try {
      //send a stop signal
      stopSignal.countDown()
      backOff.wakeup()

      var done = false

      //wait indefinitely for a done signal
      while (!done) {
        try {
          doneSignal.await()
          done = true
        } catch {
          case _: InterruptedException => ()
        }
      }

      super.stop(graceful)
    } catch {
      case NonFatal(_) =>
        done()
    }
  }

  private def maybeSleep(timeout: FiniteDuration): Unit = {
    val timing = Timing.start
    try {
      //put the thread to sleep until it doesn't receive a stop signal or it times out
      if (stopSignal.await(timeout.length, timeout.unit)) {
        done() //stop signal was sent, mark the job as done
      }
    } catch {
      case _: InterruptedException =>
        // if timeout <= 0 await() will return immediately
        maybeSleep(timeout - timing.elapsedMillis.millis)
      case NonFatal(_) =>
        done()
    }
  }

  private def done() = {
    doneSignal.countDown()
    _continue = false
  }

  private object State {

    /**
      * Example V1 journal entry schema:
      *   {
      *     "state": {
      *        "remaining": [ [ -9223372036854775808, 9223372036854775807 ] ],
      *        "version": 42,
      *     }
      *   }
      */
    object V1 {
      implicit val StateT = FieldType.RecordCodec[State]

      private val StateField = Field[State]("state")

      def decode(data: Data): State = data(StateField)
      def encode(state: State): Data = Data(StateField -> state)
    }

    /**
      * Retrieves the mapper's state from storage to begin processing
      * after the previous successful segment, if any.
      */
    def get(): Query[State] =
      getState() flatMap { state =>
        // if all segments were processed or if the topology has
        // changed, start a new iteration
        if (state.remaining.isEmpty) {
          log.info(s"$name: No segments remaining. Restarting.")
          state.nextIteration()
        } else if (state.version != repo.keyspace.topologyVersion) {
          log.info(
            s"$name: Topology change detected (${state.version} != ${repo.keyspace.topologyVersion}. Restarting.")
          state.nextIteration()
        } else {
          log.info(s"$name: Continuing from previous state.")
          Query.value(state)
        }
      }

    private def getState(): Query[State] =
      CassandraService.instance.localID match {
        case None => throw new IllegalStateException("must be a member of a cluster")
        case Some(id) =>
          val curQ = JournalEntry.latestByHostAndTag(id, currentTag) mapT { entry =>
            State.V1.decode(entry.version.data)
          }

          curQ orElseT {
            val segments = localSegments()
            val state = State(segments, repo.keyspace.topologyVersion)

            JournalEntry.write(id, currentTag, State.V1.encode(state)) map { _ =>
              Some(state)
            }
          } getOrElseT {
            // some entry write error?
            throw new IllegalStateException(s"$name: missing journal entry")
          }
      }
  }

  private case class State(
    remaining: Vector[Segment],
    version: Long) {

    /**
      * Records the first remaining segment as successfully processed
      * in this iteration.
      */
    def nextSegment(): Query[State] = {
      val next = copy(remaining = remaining.tail)
      write(next)
    }

    /**
      * Begins the next iteration through the token ring.
      */
    def nextIteration(): Query[State] =
      Query.repo flatMap { repo =>
        var topo = -1L
        var segments = Vector.empty[Segment]

        // poll for a stable topo version
        while (topo != repo.keyspace.topologyVersion) {
          topo = repo.keyspace.topologyVersion
          segments = localSegments()

          // This may happen if the topology changes such that this
          // host no longer is the primary owner of any
          // segments. Fallback to the main loop and poll there.
          if (segments.isEmpty) {
            throw new NoSegmentsOwnedException
          }
        }

        val next = State(segments, topo)
        write(next)
      }

    def withSplit(split: Seq[Segment]): Query[State] =
      write(copy(remaining = split.toVector ++ remaining.tail))

    private def write(state: State) =
      JournalEntry.writeLocal(currentTag, State.V1.encode(state)) map { _ => state }
  }

  private def process() = {
    var state = repo.runSynchronously(State.get(), timeout).value

    // account for splits during prior executions when waking back up,
    // and adjust target time accordingly.
    @volatile var totalSegments = state.remaining.size max localSegments().size
    @volatile var millisPerSegment = totalTime.toMillis / (totalSegments max 1)

    log.info(
      s"$name: Starting cycle. $totalSegments total segments with a time per segment of ${millisPerSegment / 1000.0} seconds."
    )

    def split(state: State, segment: Segment, cause: Throwable): State = {
      val split = segment.split(segment.midpoint)
      val suppressed = cause.getSuppressed.groupBy { ex =>
        (ex.getClass.getSimpleName, ex.getMessage)
      }
      val causes = suppressed.view mapValues { _.length }

      val msg = if (causes.nonEmpty) {
        s"""${cause.getMessage} (${causes.mkString(", ")})"""
      } else {
        cause.getMessage
      }

      val newState = if (split.head == segment) {
        log.error(s"$name: Failed to split segment $segment! Cause: $msg")
        repo.stats.incr(s"$name.Split.Failed")

        // once a segment can't be split further, give up and move on.
        state.nextSegment()
      } else {
        state.withSplit(split) map { state =>
          log.info(s"$name: Splitting $segment. Cause: $msg")
          repo.stats.incr(s"$name.Split")

          // adjust the per-segment target down to account for an extra
          // segment
          totalSegments += 1
          millisPerSegment = totalTime.toMillis / (totalSegments max 1)

          state
        }

      }

      repo.runSynchronously(newState, timeout).value
    }

    while (
      state.remaining.nonEmpty &&
        isRunning &&
        state.version == repo.keyspace.topologyVersion
    ) {
      storageHealthCheck()

      val timing = Timing.start
      val segment = state.remaining.head

      try {
        log.info(s"$name: Starting segment $segment. ${state.remaining.size} segments remaining.")

        val outcome = processSegment(segment)

        val elapsed = timing.elapsedMillis

        outcome match {
          case Success(_) =>
            state = repo.runSynchronously(state.nextSegment(), timeout).value
            val remaining = state.remaining.size

            repo.stats.incr(s"$name.Range.Processed")
            repo.stats.timing(s"$name.Range.Time", elapsed)
            repo.stats.set(s"$name.Range.Remaining", remaining)

            log.info(
              s"$name: Completed segment $segment. Total time ${elapsed / 1000.0} seconds."
            )

            val millisRemaining = (millisPerSegment - elapsed) max 0

            // avoid spinning in a tight loop on tiny clusters
            // (totalSegments == 1), and unnecessarily delaying after
            // segments with no data
            val shouldDelay = totalSegments == 1 || elapsed > MinimumRuntime.toMillis

            if (shouldDelay && millisRemaining > 0) {
              val fuzzed = fuzz(millisRemaining)
              log.info(s"$name: Delaying next segment by ${fuzzed / 1000.0} seconds.")
              repo.stats.timing(s"$name.Delay.Time", fuzzed)
              maybeSleep(fuzzed.millis)
            }
          case Failure(ex) =>
            // At least one task failed for this segment after
            // MaxRetries. If it's failing in a fast loop, allow the
            // healthcheck to slow it down.
            repo.stats.incr(s"$name.Range.Failed")
            log.info(
              s"$name: Failed segment $segment. Total time ${elapsed / 1000.0} seconds."
            )

            state = split(state, segment, ex)
        }
      } catch {
        case ex: UnretryableException =>
          repo.stats.incr(s"$name.Failures")
          // no healthcheck here; these are not transient issues. hit
          // the healthcheck at the loop header
          state = split(state, segment, ex)

        case ex: ContentionException =>
          repo.stats.incr(s"$name.ContentionExceptions")
          ex match {
            case SchemaContentionException(_, _) =>
              repo.stats.incr(s"$name.SchemaContentionExceptions")
            case _ => ()
          }
          storageHealthCheck()
          state = split(state, segment, ex)

        case ex: TimeoutException =>
          repo.stats.incr(s"$name.Checkpoint.Timeouts")
          storageHealthCheck()
          state = split(state, segment, ex)

        case e: Throwable =>
          logException(e)
          repo.stats.incr(s"$name.Checkpoint.Errors")
          if (NonFatal(e)) {
            storageHealthCheck()
          } else {
            throw e
          }
      }
    }
  }

  private def fuzz(v: Long, factor: Double = 0.1): Long = {
    val min = v * (1 - factor)
    val max = v * (1 + factor)

    (min + rand.nextDouble() * (max - min + 1)).toLong
  }
}

final class SparseDocumentScanner(
  val repo: RepoContext,
  retainTime: FiniteDuration,
  totalTime: FiniteDuration,
  timeout: FiniteDuration)
    extends Mapper(
      "Document.Scan.Sparse",
      EnableSparseDocumentScanner,
      totalTime,
      timeout) {

  val task =
    Mapper.Task(
      // OCC checks are unnecessary in this mapper: the data it affects is not
      // subject to snapshot isolation.
      { bounds =>
        Query.disableConcurrencyChecksForPage(VersionStore.sparseScan(bounds))
      },
      SparseDocumentTask(retainTime)(_)
    )
}

final class SparseSortedIndexScanner(
  val repo: RepoContext,
  retainTime: FiniteDuration,
  totalTime: FiniteDuration,
  timeout: FiniteDuration)
    extends Mapper(
      "SortedIndex.Scan.Sparse",
      EnableSparseSortedIndexScanner,
      totalTime,
      timeout) {

  val task =
    Mapper.Task(
      // OCC checks are unnecessary in this mapper: the data it affects is not
      // subject to snapshot isolation.
      { bounds =>
        Query.disableConcurrencyChecksForPage(SortedIndex.sparseScan(bounds))
      },
      SparseIndexTask(retainTime)(_)
    )
}

final class SparseHistoricalIndexScanner(
  val repo: RepoContext,
  retainTime: FiniteDuration,
  totalTime: FiniteDuration,
  timeout: FiniteDuration)
    extends Mapper(
      "HistoricalIndex.Scan.Sparse",
      EnableSparseHistoricalIndexScanner,
      totalTime,
      timeout) {

  val task =
    Mapper.Task(
      // OCC checks are unnecessary in this mapper: the data it affects is not
      // subject to snapshot isolation.
      { bounds =>
        Query.disableConcurrencyChecksForPage(HistoricalIndex.sparseScan(bounds))
      },
      SparseIndexTask(retainTime)(_)
    )
}

final class FullSortedIndexScanner(
  val repo: RepoContext,
  retainTime: FiniteDuration,
  totalTime: FiniteDuration,
  timeout: FiniteDuration)
    extends Mapper(
      "SortedIndex.Scan.Full",
      EnableFullSortedIndexScanner,
      totalTime,
      timeout,
      primarySegmentsOnly = false) {

  val task =
    Mapper.Task(
      SortedIndex.elementScan,
      FullIndexTask(retainTime, SortedIndex.tombstone)(_))
}

final class FullHistoricalIndexScanner(
  val repo: RepoContext,
  retainTime: FiniteDuration,
  totalTime: FiniteDuration,
  timeout: FiniteDuration)
    extends Mapper(
      "HistoricalIndex.Scan.Full",
      EnableFullHistoricalIndexScanner,
      totalTime,
      timeout,
      primarySegmentsOnly = false) {

  val task =
    Mapper.Task(
      HistoricalIndex.elementScan,
      FullIndexTask(retainTime, HistoricalIndex.tombstone)(_))
}
