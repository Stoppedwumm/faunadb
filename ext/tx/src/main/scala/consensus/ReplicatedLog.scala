package fauna.tx.consensus

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.{ ImmediateExecutionContext, Timer }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.logging.ExceptionLogging
import fauna.net.bus._
import fauna.stats.StatsRecorder
import fauna.tx.consensus.log._
import fauna.tx.consensus.messages.{ Message, Sync }
import fauna.tx.consensus.role._
import fauna.tx.log._
import java.nio.file.Path
import java.util.concurrent.{
  LinkedBlockingQueue,
  TimeoutException,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.ref.WeakReference
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

object ReplicatedLogTransport {

  /**
    * Event handler interface used by replicated log. The only
    * implementation is in ReplicatedLog, below.
    */
  sealed trait Handler {

    /**
      * Receive a batch of entries from the entries queue for
      * appending to the log.
      */
    def entries(entries: Vector[Entry], meta: Vector[(TX, Timestamp)]): Unit

    /**
      * Handle a message from a peer.
      */
    def message(sender: HostID, msg: Future[Message]): Boolean

    /**
      * Handle an idle tick.
      */
    def tick(): Unit

    /**
      * Handle a flush event.
      */
    def flush(peers: Set[HostID]): Unit
  }
}

/**
  * A trait encapsulating message sending and receiving between nodes
  * in a replicated log cluster.
  */
trait ReplicatedLogTransport {
  // Type of the message handler function

  type Handler = ReplicatedLogTransport.Handler

  def clock: Clock

  def isClosed: Boolean

  def close(): Unit

  // inbound events

  /**
    * Adds an entry to the inbound entries queue.
    */
  def addEntry(entry: Entry, idx: TX, ts: Timestamp): Unit

  /**
    * Sends a message to the recipient node.
    *
    * @param to recipient node identifier
    * @param message message to send
    */
  def send(to: HostID, message: Message, after: FiniteDuration = Duration.Zero): Unit

  /**
    * Schedules a message on the actor loop in the future.
    */
  def schedule(from: HostID, message: Future[Message]): Unit

  /**
    * Schedules a tick on the actor loop in the future.
    */
  def scheduleTick(after: FiniteDuration): Unit

  /**
    * Schedules log file flush on the event flush loop.
    */
  def scheduleFlush(notify: Set[HostID]): Unit

  /**
    * Sets the event handler.
    *
    * @param handler event handler
    */
  def handle(handler: Handler): Unit
}

/**
  * A replicated log transport using {@link MessageBus}.
  *
  * @param bus the bus instance
  * @param signalID the signalID id for this transport
  * @param ec an execution context for message sending
  */
class MessageBusTransport(
  logName: String,
  bus: MessageBus,
  signalID: SignalID,
  maxPendingMessages: Int,
  val clock: Clock = Clock,
  statsRecorder: StatsRecorder = StatsRecorder.Null,
  statsPrefix: String = "ReplicatedLog",
  dedicatedBusStream: Boolean = false)(implicit sendEC: ExecutionContext)
    extends ReplicatedLogTransport with ExceptionLogging { self =>

  protected val EntriesBatch = 32
  protected val MessageBatch = 32
  protected val MProtocol = Protocol[Message](s"$logName.consensus")

  private[this] val logger = getLogger

  @volatile private[this] var handler: Handler = _

  private[this] val entriesQueue = new LinkedBlockingQueue[(Entry, TX, Timestamp)]
  private[this] val messageQueue = new LinkedBlockingQueue[(HostID, Future[Message])](maxPendingMessages)
  private[this] val pendingFlush = new AtomicReference[Set[HostID]]
  @volatile private[this] var nextTick = TimeBound.Min

  @volatile private[this] var _isClosed = false
  def isClosed = _isClosed

  def close() = {
    bus.unbind(signalID)
    _isClosed = true
  }

  private def loop(name: String)(f: => Unit) = {
    val t = new Thread(name) {
      override def run(): Unit = {
        while (!isClosed) {
          try {
            f
          } catch {
            case NonFatal(e) => logException(e)
          }
        }
      }
    }

    t.setPriority(Thread.MAX_PRIORITY) // optimism.
    t.start()
  }

  private[this] val DroppedMessagesPrefix = s"$statsPrefix.Messages.Inbound.Dropped"

  def schedule(sender: HostID, msgF: Future[Message]) = {
    if (!messageQueue.offer((sender, msgF))) {
      statsRecorder.incr(DroppedMessagesPrefix)
      if (logger.isDebugEnabled) {
        implicit val ec = ImmediateExecutionContext
        msgF foreach { msg =>
          logger.debug(s"ReplicatedLog $signalID receive dropped from $sender: $msg")
        }
      }
    }
  }

  def handle(h: Handler) = {
    handler = h

    if (dedicatedBusStream) {
      bus.useDedicatedStream(signalID)
    }

    bus.asyncHandler(MProtocol, signalID) { (from, msgF, _) =>
      schedule(from.id, msgF)

      // XXX: ugh. Fix this when we fix ownership
      msgF.unit
    }

    loop(s"$statsPrefix-Entries") {
      var e = entriesQueue.poll(50, TimeUnit.MILLISECONDS)
      if (e ne null) {
        val entries = Vector.newBuilder[Entry]
        val meta = Vector.newBuilder[(TX, Timestamp)]
        var count = 0

        while (e ne null) {
          entries += e._1
          meta += ((e._2, e._3))
          count += 1
          e = if (count < EntriesBatch) entriesQueue.poll() else null
        }

        handler.entries(entries.result(), meta.result())
      }
    }

    loop(s"$statsPrefix-Messages") {
      var m = messageQueue.poll(50, TimeUnit.MILLISECONDS)
      if (m ne null) {
        var count = 0

        while (m ne null) {
          val (sender, msg) = m

          if (logger.isTraceEnabled) {
            msg foreach { m =>
              logger.trace(s"ReplicatedLog $signalID receive from $sender: $m")
            }
          }

          val reenqueue = handler.message(sender, msg)
          if (reenqueue) msg foreach { _ => schedule(sender, msg) }
          count += 1
          m = if (count < MessageBatch) messageQueue.poll() else null
        }
      }

      if (nextTick.isOverdue) {
        nextTick = TimeBound.Max
        logger.trace(s"ReplicatedLog $signalID *tick*")
        handler.tick()
      }
    }

    loop(s"$statsPrefix-LogFlush") {
      self.synchronized {
        if (pendingFlush.get() eq null) {
          self.wait(1000)
        }
      }

      val hosts = pendingFlush.getAndSet(null)

      if (hosts ne null) {
        handler.flush(hosts)
      }
    }
  }

  def addEntry(entry: Entry, idx: TX, ts: Timestamp) = {
    entriesQueue.add((entry, idx, ts))
  }

  def send(to: HostID, message: Message, after: FiniteDuration) = {
    logger.trace(s"ReplicatedLog $signalID send to $to: $message")

    def sinkMsg = bus.sink(MProtocol, signalID.at(to)).send(message)

    val sendF = if (after.length <= 0) {
      sinkMsg
    } else {
      Timer.Global.delay(after)(sinkMsg)
    }

    if (logger.isTraceEnabled) {
      sendF onComplete {
        case Failure(ex)    => logger.trace(s"ReplicatedLog $signalID send failed to $to: $message, $ex")
        case Success(false) => logger.trace(s"ReplicatedLog $signalID send dropped to $to: $message")
        case _              => ()
      }
    }
  }

  def scheduleTick(after: FiniteDuration) =
    if (nextTick == TimeBound.Max) {
      nextTick = after.bound
    }

  // this blows away any prior set of hosts to notify. We assume
  // that the actor state machine is always going to give us the
  // latest host set to broadcast Accept to.
  def scheduleFlush(hosts: Set[HostID]) = {
    pendingFlush.set(hosts)
    self.synchronized { self.notify }
  }
}

object ReplicatedLog {
  def apply[V: CBOR.Codec] = new Builder[V]

  class Builder[V: CBOR.Codec] {
    def open(
      transport: ReplicatedLogTransport,
      self: HostID,
      logDir: Path,
      logName: String,
      tickDuration: FiniteDuration,
      statsRecorder: StatsRecorder = StatsRecorder.Null,
      statsPrefix: String = null,
      ringLog: Option[Handle] = None,
      logFileSize: Int = BinaryLogStore.DefaultFileSize,
      leaderStateChangeListener: ReplicatedLog[V] => Unit = { _ => () },
      alwaysActive: Boolean = false,
      txnLogBackupPath: Option[Path]): ReplicatedLog[V] = {

      val log = BinaryLogStore.open[Entry](logDir, txnLogBackupPath, logName, logFileSize)
      // FIXME: read initial term/ring from persistent state, when
      // truncation happens
      val store = ReplicatedLogStore(self, log)
      val stats = new Stats(Option(statsPrefix) getOrElse s"ReplicatedLog-$logName", statsRecorder)

      new ReplicatedLog(transport, tickDuration, store, stats, logName, alwaysActive, ringLog map { _.service }, leaderStateChangeListener)
    }
  }

  class Handle(private[consensus] val service: ReplicatedLog[_])

  def handle(log: ReplicatedLog[_]): Handle = new Handle(log)

  class Stats(prefix: String, recorder: StatsRecorder) {
    private val proposals = s"$prefix.Proposals"
    def incrProposals() = recorder.incr(proposals)

    private val proposalsDropped = s"$prefix.Proposals.Dropped"
    def incrProposalsDropped() = recorder.incr(proposalsDropped)

    private val pending = s"$prefix.Pending.Commits"
    def recordPending(count: Int) = recorder.set(pending, count)

    private val committedIdx = s"$prefix.Committed.Index"
    def recordCommittedIdx(idx: TX) = recorder.set(committedIdx, idx.toLong)

    private val currentTerm = s"$prefix.Term"
    def recordTerm(term: Term) = recorder.set(currentTerm, term.toLong)

    private val ticks = s"$prefix.Tick.Events"
    def incrTicks() = recorder.incr(ticks)

    private val messageRecv = s"$prefix.Messages.Received"
    def incrMessageRecv() = recorder.incr(messageRecv)

    private val messageSend = s"$prefix.Messages.Sent"
    def incrMessageSend() = recorder.incr(messageSend)

    private val messageTimeout = s"$prefix.Messages.Timed.Out"
    def incrMessageTimeout() = recorder.incr(messageTimeout)

    private val adds = s"$prefix.Adds"
    def incrAdds() = recorder.incr(adds)

    private val flushes = s"$prefix.LogFlushes"
    def incrLogFlushes() = recorder.incr(flushes)

    private val entryBytesSize = s"$prefix.Entry.Bytes"
    private val entryBytesThroughput = s"$prefix.Entry.Bytes.Throughput"
    def recordEntryBytesSize(size: Int) = {
      recorder.count(entryBytesThroughput, size)
      recorder.timing(entryBytesSize, size)
    }

    private val commits = s"$prefix.Commits"
    def countCommits(i: Long) = recorder.count(commits, i.toInt)

    private val commitLatency = s"$prefix.Commit.Latency"
    def timeCommit[T](f: => Future[T]) = recorder.timeFuture(commitLatency)(f)

    private val reinits = s"$prefix.Reinits"
    def incrReinits() = recorder.incr(reinits)

    private val elections = s"$prefix.Elections"
    def incrElections() = recorder.incr(elections)

    private val electionCalls = s"$prefix.Election.Calls"
    def incrElectionCalls() = recorder.incr(electionCalls)
  }
}

final class ReplicatedLog[V: CBOR.Codec](
  transport: ReplicatedLogTransport,
  tickDuration: FiniteDuration,
  val store: ReplicatedLogStore,
  val stats: ReplicatedLog.Stats,
  name: String,
  alwaysActive: Boolean,
  ringLog: Option[ReplicatedLog[_]],
  leaderStateChangeListener: ReplicatedLog[V] => Unit) extends Log[TX, V] {

  type E = ReplicatedLogEntry[V]

  private val state = new State((ringLog getOrElse this).store.ring, store, transport, stats, name, alwaysActive)

  @volatile private[this] var role: Role =
    Follower(state)

  @volatile private[this] var ringDependents =
    List.empty[WeakReference[ReplicatedLog[_]]]

  private[this] val lockDelay = tickDuration.toNanos

  private def lock[T](context: => String)(f: => T) = {
    val timing = Timing.start
    try {
      synchronized { f }
    } finally {
      val elapsed = timing.elapsedNanos
      if (elapsed > lockDelay) {
        state.logWarn(
          role.name,
          s"Spent ${elapsed / 1000000} ms in Replog lock. ($context)"
        )
      }
    }
  }

  private def addRingDependent(dep: ReplicatedLog[_]) =
    lock("add dependents") { ringDependents = ringDependents :+ WeakReference(dep) }

  ringLog foreach { rl =>
    require(self == rl.self,
      s"ReplicatedLog must have the same self as its ring-managing log. $self != ${rl.self}")
    rl.addRingDependent(this)
  }

  transport.handle(new ReplicatedLogTransport.Handler {
    def entries(es: Vector[Entry], meta: Vector[(TX, Timestamp)]) =
      stepRole(s"add entries ${es.size}") { role.addProposals(state, None, es, meta) }

    def message(sender: HostID, msgF: Future[Message]) = {
      stats.incrMessageRecv()

      try {
        if (!isLeader) {
          // Block the loop for followers. This may throw a TimeoutException.
          Await.ready(msgF, 10.seconds)
        }

        msgF.value match {
          case None =>
            true
          case Some(r) =>
            r match {
              case Success(msg)                 => stepRole(s"message $msg") { role.step(state, Some((sender, msg))) }
              case Failure(err)                 => throw err
            }
            false
        }
      } catch {
        case _: TimeoutException => stats.incrMessageTimeout()
        false
      }
    }

    def tick() = {
      stepRole("tick") { role.step(state, None) }
      stats.incrTicks()
    }

    def flush(peers: Set[HostID]) = {
      state.flush(peers)
      stats.incrLogFlushes()
    }
  })

  override def toString = s"ReplicatedLog($state, $role)"

  def term = state.term

  def self = state.self
  def ring: Set[HostID] = (ringLog getOrElse this).store.ring.all

  def leader: Option[HostID] =
    role match {
      case _: Leader    => Some(self)
      case _: Candidate => None
      case f: Follower  => f.leader(state)
    }

  def logRoleAndState(): Unit =
    state.logInfo(role.name, s"ReplicatedLog state=$state")

  def isLeader: Boolean = role.isInstanceOf[Leader]

  def isMember: Boolean = state.isMember

  def prevIdx: TX = store.prevIdx

  def lastIdx: TX = store.lastIdx

  def committedIdx: TX = state.globalCommittedIdx

  def init(): Future[Unit] =
    lock("init") {
      state.logInfo(role.name, "Initializing.")
      // set ring to just this node
      state.resetRing()

      // start as a Follower and allow self-election
      setRole(Follower(state, Follower.Null))
      while (!isLeader) stepRole("init") { role.step(state, None) }

      Future.unit
    }

  def join(seed: HostID): Future[TX] = {
    state.logInfo(role.name, s"Joining using seed $seed.")
    lock("join") {
      setRole(Follower(state, Follower.Observed(seed)))
      Future.unit
    }
    addMember(state.self)
  }

  def addMember(host: HostID): Future[TX] = {
    if (ringLog.isDefined) {
      throw new UnsupportedOperationException("Cannot modify membership on dependent log.")
    }
    addProposal(Entry.AddMember(Term.MinValue, state.randomToken, host), TX.MaxValue, Duration.Inf)
  }

  def leave(): Future[TX] = removeMember(state.self)

  def removeMember(member: HostID): Future[TX] = {
    if (ringLog.isDefined) {
      throw new UnsupportedOperationException("Cannot modify membership on dependent log.")
    }
    addProposal(Entry.RemoveMember(Term.MinValue, state.randomToken, member), TX.MaxValue, Duration.Inf)
  }

  def abdicate(reason: String) =
    lock("abdicate") {
      setRole(role match {
        case l: Leader => l.abdicate(state, reason)
        case r         => r
      })
    }

  // read

  private def toReplicatedLogEntry(e: LogEntry[TX, Entry]): ReplicatedLogEntry[V] =
    e.get match {
      case Entry.Value(term, token, v)        => ReplicatedLogEntry.Value(e.idx, term, token, CBOR.parse(v))
      case Entry.Null(term, token)            => ReplicatedLogEntry.Null(e.idx, term, token)
      case Entry.AddMember(term, token, m)    => ReplicatedLogEntry.AddMember(e.idx, term, token, m)
      case Entry.RemoveMember(term, token, m) => ReplicatedLogEntry.RemoveMember(e.idx, term, token, m)
    }

  def entries(after: TX): EntriesIterator[E] =
    store.entries(after) map toReplicatedLogEntry

  def poll(after: TX, within: Duration): Future[Boolean] =
    store.poll(after, within)

  def sync(within: Duration): Future[TX] = sync(within, state.randomToken)

  def sync(within: Duration, token: Long): Future[TX] = {
    val p = Promise[TX]()

    def sync0(): Unit =
      if (isClosed) {
        p.tryFailure(new LogClosedException)
      } else {
        p.completeWith(state.waitForSync(token, within))

        // FIXME: This has to get send() out of synchronized so that
        // the timer thread and test thread don't deadlock on
        // TestExchange and ReplicatedLog. otherwise this could be put
        // in stepRole { role.syncCommitted(state, token) }
        val (peers, term) = lock("sync") {
          if (state.isMember) {
            state.addAccept(state.log.flushedLastPos, state.self, Some(token))
          }

          (state.peers, state.term)
        }

        peers foreach { state.send(_, Sync(term, token)) }

        Timer.Global.scheduleTimeout(tickDuration) {
          if (!p.isCompleted) sync0()
        }
      }

    sync0()
    p.future
  }

  def subscribe(after: TX, idle: Duration)(f: Log.Sink[TX, E])(implicit ec: ExecutionContext): Future[Unit] =
    store.subscribe(after, idle) {
      case Log.Entries(prev, es) => f(Log.Entries(prev, es map toReplicatedLogEntry))
      case Log.Idle(prev)        => f(Log.Idle(prev))
      case Log.Reinit(prev)      => f(Log.Reinit(prev))
      case Log.Closed            => f(Log.Closed)
    }

  // write

  def add(v: V, within: Duration): Future[TX] =
    add(v, within, state.randomToken)

  def add(v: V, within: Duration, token: Long): Future[TX] =
    addValue(v, TX.MaxValue, within, token)

  def add(v: V, within: TX): Future[TX] =
    add(v, within, state.randomToken)

  def add(v: V, within: TX, token: Long): Future[TX] =
    addValue(v, within, Duration.Inf, token)

  def truncate(to: TX): Unit = lock("truncate") {
    store.truncate(to)
  }

  def close(): Unit = lock("close") {
    if (!isClosed) {
      transport.close()
      state.close()
    }
  }

  def isClosed: Boolean = transport.isClosed

  // helpers

  private def addValue(v: V, idx: TX, duration: Duration, token: Long) = {
    val bytes = CBOR.encode(v).toByteArray
    stats.recordEntryBytesSize(bytes.length)
    addProposal(Entry.Value(Term.MinValue, token, bytes), idx, duration)
  }

  private def addProposal(entry: Entry, idx: TX, duration: Duration) =
    if (isClosed) {
      Future.failed(new LogClosedException)
    } else {
      stats.incrProposals()
      stats.timeCommit {
        // If this fails, we leak this future, but it definitely should not be happening.
        try {
          val fut = state.waitForPending(entry.token, duration)
          transport.addEntry(entry, idx, transport.clock.fromNow(duration))
          fut
        } catch {
          case NonFatal(e) => Future.failed(e)
        }
      }
    }

  private def stepRole(ctx: => String)(action: => Role) = {
    val oldRing = store.ring
    lock(s"stepRole: $ctx") { if (!isClosed) setRole(action) }
    val newRing = store.ring

    if ((oldRing ne newRing) && (oldRing.all ne newRing.all) && (oldRing.all != newRing.all)) {
      lock("stepRole update dependents") {
        ringDependents foreach { _.get foreach { _.scheduleTick() } }
      }
    }

    if (!isClosed && role.isActive(state)) {
      scheduleTick()
    }
  }

  private def scheduleTick(): Unit =
    transport.scheduleTick(tickDuration)

  private def setRole(newRole: Role): Unit = {
    val oldRole = role
    role = newRole

    val s = state
    val oldLeader = oldRole.leader(s)
    val newLeader = newRole.leader(s)
    if (oldLeader != newLeader) {
      s.logInfo(role.name, s"Changed leader to $newLeader")
      val self = s.self
      if (oldLeader.contains(self) != newLeader.contains(self)) {
        leaderStateChangeListener(this)
      }
    }
  }
}
