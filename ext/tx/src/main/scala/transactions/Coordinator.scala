package fauna.tx.transaction

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec._
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.lang.syntax._
import fauna.net.{ HostInfo, HostService }
import fauna.trace.GlobalTracer
import java.util.concurrent.{
  ConcurrentHashMap,
  RejectedExecutionException,
  ScheduledThreadPoolExecutor,
  ThreadLocalRandom,
  TimeoutException
}
import scala.annotation.tailrec
import scala.collection.concurrent.{ Map => CMap }
import scala.collection.mutable.{ Set => MSet }
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Random, Success }
import scala.util.control.NoStackTrace

object Coordinator {
  // FIXME: local apply delay should be based on the deadline, but
  // 30 seconds is too long for most writes.
  val LocalWaitTime = 5.seconds
  // time to wait if there are no active log segments yet
  val SegmentWaitTime = 1.seconds
  // time to wait between log nodes list refreshes
  val LogNodesRefreshPeriod = 1.second

  sealed trait Leadership

  object Leadership {
    case class Leader(host: HostID, segmentID: SegmentID) extends Leadership
    case object NoLeader extends Leadership
    case object Failed extends Leadership
  }

  object ShutDownException
      extends IllegalStateException("Transaction Coordinator is shut down.")

  object BackoffException
      extends Exception("Write rejected by the coordinator.")
      with NoStackTrace

  sealed trait Outcome[WV]

  object Outcome {
    final case class HasResult[WV](ts: Timestamp, result: WV) extends Outcome[WV]

    final case class KeysCovered[WV](ts: Timestamp, result: WV, allLocals: Boolean)
        extends Outcome[WV]

    final case class AllNodes[WV](ts: Timestamp, result: WV) extends Outcome[WV]

    final case class MismatchedResults[WV](results: Vector[(HostID, Timestamp, WV)])
        extends Outcome[WV]
  }

  object Applies {

    def apply[W, WV](
      partitioner: Partitioner[_, W],
      hostService: HostService,
      expr: W): Applies[WV] = {

      val writers =
        partitioner.hostsForWrite(expr)

      val consideredLocal =
        writers filter { host =>
          (hostService.isLocal(host) || hostService.isNear(host)) &&
          hostService.isLive(host)
        }

      new Applies(
        writers.size,
        consideredLocal,
        host => partitioner.txnCovered(host, expr))
    }
  }

  /**
    * Accumulates incoming applies from writers and determines transaction outcome.
    * NB. This class is not thread-safe.
    */
  final class Applies[WV] private (
    allNodes: Int,
    local: Set[HostID],
    keysCovered: Set[HostID] => Boolean) {
    import Outcome._

    private[this] var applies = Vector.empty[(HostID, Timestamp, WV)]
    private[this] var outcome = Option.empty[Outcome[WV]]

    /** Add a new result to the state and emit a non-pending outcome upon changes */
    def add(
      host: HostID,
      transactionTime: Timestamp,
      result: WV): Option[Outcome[WV]] = {

      val prev = outcome
      applies :+= ((host, transactionTime, result))
      outcome = calculateOutcome(applies)

      prev match {
        case None => outcome
        case Some(p) =>
          if (outcome.get.getClass != p.getClass) {
            outcome
          } else {
            None
          }
      }
    }

    def currentOutcome: Option[Outcome[WV]] = outcome

    private def calculateOutcome(
      aps: Vector[(HostID, Timestamp, WV)]): Option[Outcome[WV]] = {

      val resSeen = MSet.empty[(Timestamp, WV)]
      val res = aps filter {
        case (_, t, r) =>
          if (resSeen.contains(t -> r)) false
          else {
            resSeen += (t -> r)
            true
          }
      }

      res match {
        case Vector((_, ts, r)) =>
          val nodes = aps.iterator.map { _._1 }.toSet
          if (nodes.size == allNodes) {
            Some(AllNodes(ts, r))
          } else if (keysCovered(nodes)) {
            Some(KeysCovered(ts, r, local.nonEmpty && (local forall nodes.contains)))
          } else {
            Some(HasResult(ts, r))
          }
        case Vector() =>
          None
        case _ =>
          Some(MismatchedResults(res))
      }
    }
  }

  def preferredLogNodes(
    selfHostID: HostID,
    topology: Map[SegmentID, SegmentInfo],
    knownLeaders: CMap[SegmentID, HostID],
    hostService: HostService): List[HostID] = {

    val nodes = topology.values flatMap { _.hosts }
    // Only consider live nodes; shuffle equal-preference ones
    val random = new Random(ThreadLocalRandom.current())
    val liveNodes = random.shuffle(nodes.filter(hostService.isLive))
    val leaders = knownLeaders.values.toSet

    // Nodes residing in segments for which we don't know the current leader yet.
    // These get highest preference so the coordinator will send them requests
    // and they'll include their current leader in the response. If we didn't do
    // this, and gave highest preference to known leaders, the logic would get stuck
    // sending to the first leader we discovered and never discover other segments'
    // leaders.
    val leaderUnknownSegmentNodes = (topology collect {
      case (id, si) if !knownLeaders.contains(id) => si.hosts
    } flatten).toSet

    def part(s: Iterable[HostID], p: HostID => Boolean) = {
      val (a, b) = s.partition(p)
      a ++ b
    }

    // The effective algorithm is as this:
    //   * Prefer nodes in segments where we don't know the leader, then
    //   * prefer leaders, finally
    //   * consider the rest (i.e. followers).
    // Within each of the three groups named above, further
    //   * prefer self (if member of the group), then
    //   * prefer other local members,
    //   * prefer members in a neighbor replica, finally
    //   * consider the rest (i.e. remote members).
    // Due to the way the expression list below is constructed, the bottom
    // preference is strongest (partitioning applied last), while the top
    // preference is weakest (partitioning applied first).
    part(part(part(part(part(liveNodes,
      hostService.isNear),       // nearby
      hostService.isLocal),      // locals
      { _ == selfHostID }),      // self
      leaders),                  // leaders
      leaderUnknownSegmentNodes) // nodes in segments for which we don't know the leaders
      .toList
  }
}

final class Coordinator[K, R, RV, W, WV](
  val ctx: PipelineCtx[K, R, RV, W, WV],
  mismatchResetTxn: W => Option[W])
    extends Node[K, R, RV, W, WV] { coordinator =>

  import Coordinator._
  import ctx.codecs._

  private implicit def ec = ctx.executionContext

  private[this] val tracer = GlobalTracer.instance

  private val fineGrainedTimer = new ScheduledThreadPoolExecutor(1)

  def onClose(): Unit =
    fineGrainedTimer.shutdownNow()

  private[this] val knownLogLeaders = new ConcurrentHashMap[SegmentID, HostID]
  @volatile private[this] var logNodes = List.empty[HostID]

  private def recomputeLogNodes(): Unit = {
    val now = Epoch(ctx.epochClock.time)
    val logNodeInfo = ctx.logNodeProvider.getLogNodeInfo
    val liveSegTopo = logNodeInfo.topology filter { case (_, si) => si.isLiveAt(now) }

    logNodes = preferredLogNodes(
      ctx.hostID,
      liveSegTopo,
      knownLogLeaders.asScala,
      ctx.hostService)
  }

  // Periodically recompute the log node list to randomize among
  // equal preference nodes.
  Timer.Global.scheduleRepeatedly(LogNodesRefreshPeriod, !isClosed) {
    recomputeLogNodes()
  }

  def write(expr: W, priority: GlobalID, deadline: TimeBound): Future[(Timestamp, FiniteDuration, WV)] =
    if (isClosed) {
      Future.failed(ShutDownException)
    } else {
      val c = new TxnCoordinator(expr, priority, deadline)
      c.write()
      c.result
    }

  /**
    * Gets the leader for the input [[HostID]].
    */
  def getLeader(hostID: HostID): Future[Leadership] =
    ctx.logSink(hostID).request(LogMessage.GetLeader, 5.seconds.bound) map {
      case Some(Some(response)) =>
        response.leader match {
          case Some(leader) =>
            Leadership.Leader(leader, response.segmentID)

          case None =>
            Leadership.NoLeader
        }

      case Some(None) =>
        Leadership.NoLeader

      case None =>
        Leadership.Failed
    }

  def markSegmentLeader(seg: SegmentID, host: HostID): Unit = {
    // Can't just use "knownLogLeaders.put(seg, host) != host"
    // because HostID extends AnyVal and plays badly with null returns from put.
    if (!knownLogLeaders.containsKey(seg) || knownLogLeaders.get(seg) != host) {
      knownLogLeaders.put(seg, host)
      getLogger().info(s"Updated leader of $seg to $host")
      // Recompute log node list immediately upon change so we move the newly
      // found leader ahead and also formerly unknown-leader segment nodes behind
      // in the list.
      recomputeLogNodes()
    }
  }

  /**
    * Coordinates a single transaction.
    *
    * Effectively this is a transaction-specific stateful actor that updates an
    * outcome state machine based on receiving applies and timeout events.
    *
    * When enough apply results have been received, `rv` is completed with the
    * result, and the coordinator is left hanging around in order to detect and
    * respond to mismatches. Once LocalWaitTime has passed, `rv` is completed if
    * possible and it has not been so already, and the coordinator is closed.
    * Otherwise, the timeout is rescheduled based on time remaining before the
    * deadline.
    */
  private final class TxnCoordinator(
    expr: W,
    priority: GlobalID,
    deadline: TimeBound) {
    private[this] val rv = Promise[(Timestamp, FiniteDuration, WV)]()

    // nullable after Txn.Committed is received
    @volatile private[this] var exprBytes = CBOR.encode(expr).toByteArray

    private[this] var coroutine = Future.unit // use a future to serialize event handling.
    private[this] var nodesToTry = logNodes
    private[this] var applies: Applies[WV] = _
    private[this] var localWaitExpired = false
    private[this] var completed = false
    private[this] var timing: Timing = _

    private[this] val handler = ctx.coordReplyHandler { (from, msg, _) =>
      sequence(handleCoordReply(from, msg))
      Future.unit
    }

    private[this] var timeout = Timer.Global.scheduleTimeout(LocalWaitTime) {
      sequence(onTimeout(true))
    }

    def result = rv.future

    // manages an internal promise chain to serialize event/timeout handling,
    // which eliminates the need to manage concurrency elsewhere in this class.
    // Importantly, all callbacks into TxnCoordinator need to call back into
    // this function before mutating internal state.
    //
    // FIXME: this feels overly general to leave here, and we already have
    // FutureSequence which does something similar, but not quite what we want
    // here.
    private def sequence(f: => Unit): Unit = {
      val done = Promise[Unit]()
      val prev = synchronized {
        val p = coroutine
        coroutine = done.future
        p
      }

      implicit val ec = ImmediateExecutionContext
      prev onComplete { _ => try f finally done.success(()) }
    }

    // Called when we no longer need the coordinator to hang around.
    private def close() = {
      handler.close()
      timeout.cancel()
    }

    @tailrec def write(): Unit = {
      // Refresh the partitioner on each write attempt to catch topology changes.
      applies = Applies(ctx.partitioner, ctx.hostService, expr)

      nodesToTry match {
        case host :: rest =>
          // FIXME: we could retire Batch.Txn.expiry and rely on the deadline
          // communicated in the bus send instead. That'd make Coordinator
          // independent of the epochClock.
          val expiry = ctx.epochClock.expireTime(deadline)
          val occReads = ctx.keyExtractor.readKeysSize(expr)
          val trace = tracer.activeSpan map { _.context }
          val txn = Batch.Txn(exprBytes, handler.id, expiry, trace = trace)
          val msg = LogMessage.AddTxn(ScopeID.RootID, txn, occReads, priority)

          nodesToTry = rest
          ctx.logSink(host).send(msg, deadline) onComplete {
            case Success(true)  => timing = Timing.start // Begin transaction timer. See trySuccess().
            case Success(false) => sequence(retryWrite())
            case Failure(t)     => tryFailure(t)
          }
        case Nil =>
          // Grab fresh logNodes
          nodesToTry = logNodes
          if (nodesToTry.isEmpty) {
            // No active log nodes; most likely log topology didn't initialize yet.
            // Give it a bit of time.
            Timer.Global.scheduleTimeout(SegmentWaitTime) { sequence(retryWrite()) }
          } else {
            // Retry immediately
            write()
          }
      }
    }

    private def retryWrite(): Unit =
      if (deadline.hasTimeLeft) {
        // retry write if we have time.
        write()
      } else {
        tryFailure(
          new TimeoutException(s"Rejected transaction write. Deadline: $deadline"))
      }

    private def handleCoordReply(from: HostInfo, msg: CoordMessage) = {
      msg match {
        case CoordMessage.TxnCommitted(committed, backoff, leader) =>
          for {
            l <- leader
            seg <- ctx.logSegmentFor(l)
          } {
            markSegmentLeader(seg, l)
          }

          if (!committed) {
            if (backoff) {
              exprBytes = null // GC
              tryFailure(BackoffException)
            } else {
              retryWrite()
            }
          } else {
            // release encoded expr to the GC
            exprBytes = null
          }
        case CoordMessage.TxnApplied(ts, wv, _) =>
          applies.add(from.id, ts, CBOR.parse[WV](wv)) foreach {
            onTransactionOutcome(_, false)
          }
      }
    }

    // events

    /**
      * Received when the current timeout fires. The first timeout scheduled is
      * based on LocalWaitTime. If there is no result yet, a second timeout is
      * scheduled based on the deadline.
      */
    private def onTimeout(localWaitTimeout: Boolean): Unit = {
      ctx.stats.incrCoordinatorTimeouts(localWaitTimeout)
      localWaitExpired = true

      if (!completed) {
        Option(applies) flatMap {
          _.currentOutcome
        } foreach {
          onTransactionOutcome(_, true)
        }
      }

      if (!completed) {
        if (localWaitTimeout && deadline.hasTimeLeft) {
          timeout = Timer.Global.scheduleTimeout(deadline.timeLeft) {
            sequence(onTimeout(false))
          }
        } else {
          tryFailure(
            new TimeoutException(
              s"Timed out on transaction write. Deadline $deadline"))
        }
      } else {
        close()
      }
    }

    private def onTransactionOutcome(
      outcome: Outcome[WV],
      onTimeout: Boolean): Unit =
      outcome match {
        case Outcome.HasResult(ts, r) =>
          if (onTimeout || localWaitExpired) {
            trySuccess(ts, r)
            close()
          }

        case Outcome.KeysCovered(ts, r, allLocals) =>
          if (allLocals) {
            // Only sample for read clock based on local acks.
            ctx.readClock.mark(ts)
          }
          // Succeed as soon as every write key is covered by at least one of the responding nodes.
          // Statistically likely to happen at ceil(writers/replicas) responses.
          trySuccess(ts, r)

          if (localWaitExpired) {
            close()
          }

        case Outcome.AllNodes(ts, r) =>
          // this call to trySuccess is probably unnecessary
          trySuccess(ts, r)
          close()

        case Outcome.MismatchedResults(res) =>
          ctx.stats.incrMismatchedResults()

          getLogger.error(s"Mismatched transaction results: $res transaction: $expr")
          // Generate and fire/forget a reset transaction.
          mismatchResetTxn(expr) foreach { reset =>
            getLogger.info(s"Mismatched transaction reset: $reset")
            coordinator.write(reset, priority, 30.seconds.bound)
          }

          tryFailure(
            new IllegalStateException(s"Mismatched transaction results $res"))
      }

    // do not publish success until the local read clock has
    // advanced past the transaction time.
    private def trySuccess(ts: Timestamp, result: WV): Unit = {
      completed = true

      // Register the time taken to successfully transit a transaction
      // through the log. See write().
      val elapsed = Option(timing) match {
        case None    => throw new IllegalStateException("trySuccess called without a timing started")
        case Some(t) => t.elapsed
      }

      def trySuccess0(elapsed: FiniteDuration): Unit = {
        val wait = ts.millis - math.min(ctx.ackDelayClock.millis, ctx.readClock.millis)

        if (wait > 0) {
          try {
            fineGrainedTimer.schedule(
              (() => trySuccess0(elapsed)): Runnable,
              wait, MILLISECONDS)
          } catch {
            case ex: RejectedExecutionException =>
              getLogger().warn(s"Coordinator: Scheduling trySuccess callback failed, completing promise immediately instead. (Are we shutting down?)", ex)
              rv.trySuccess((ts, elapsed, result))
          }
        } else {
          rv.trySuccess((ts, elapsed, result))
        }
      }

      trySuccess0(elapsed)
    }

    private def tryFailure(t: Throwable): Unit = {
      completed = true
      rv.tryFailure(t)
      close()
    }
  }
}
