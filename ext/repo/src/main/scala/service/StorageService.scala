package fauna.repo.service

import fauna.atoms.{ GlobalID, HostID }
import fauna.cluster.{ ClusterTopologyException, FailureDetector, Membership }
import fauna.codex.cbor.CBOR
import fauna.codex.cbor.CBOR.BufRetention
import fauna.exec.{ FaunaExecutionContext, ImmediateExecutionContext, Timer }
import fauna.lang.{ FutureSyntax, TimeBound, Timing }
import fauna.lang.syntax._
import fauna.net.{ HostInfo, HostService, PIDTimeoutController, ScoreKeeper }
import fauna.net.bus.{ HandlerID, MessageBus, Protocol, SignalID }
import fauna.repo.StorageCleaner
import fauna.stats.StatsRecorder
import fauna.storage.api.{ Read, Storage }
import fauna.storage.ops.Write
import fauna.storage.TxnRead
import fauna.trace.GlobalTracer
import fauna.tx.transaction.{ KeyLocator, Partitioner }
import io.netty.buffer.ByteBuf
import java.io.Closeable
import java.util.concurrent.TimeoutException
import scala.annotation.nowarn
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Random, Success }

object StorageService {

  /** The protocol for reading data from a remote data node, and
    * returning a reply.
    */
  object Reads {
    final case class Request(
      priority: GlobalID,
      replyTo: SignalID,
      read: Read[_],
      writes: Vector[Write]
    )

    implicit val RequestCodec = CBOR.TupleCodec[Request]

    val RequestProtocol = Protocol[Request]("read.request")

    def ReplyProtocol[R](implicit codec: CBOR.Codec[R]) =
      Protocol[R]("read.reply")
  }

  /** The protocol for administrative queries against a remote data
    * node's storage engine.
    */
  object System {
    sealed trait Request

    /** An RPC for StorageEngine.needsCleanup(). If no column family is
      * supplied, all column families will be checked.
      */
    case class NeedsCleanup(cf: Option[String], replyTo: SignalID) extends Request

    sealed trait Reply

    object CleanupNeeded {
      implicit val codec =
        CBOR.AliasCodec[CleanupNeeded, Boolean](apply, _.value)
    }

    /** A response to a NeedCleanup message. */
    case class CleanupNeeded(value: Boolean) extends Reply

    implicit val RequestCodec =
      CBOR.SumCodec[Request](CBOR.RecordCodec[NeedsCleanup])

    val RequestProtocol = Protocol[Request]("storage.system.request")

    def ReplyProtocol[T <: Reply](implicit codec: CBOR.Codec[T]) =
      Protocol[T]("storage.system.reply")

  }
}

/** The StorageService sends and receives requests for reads from across the
  * cluster.
  */
final class StorageService(
  readSignal: SignalID,
  sysSignal: SignalID,
  bus: MessageBus,
  membership: Membership,
  hostService: HostService,
  fd: FailureDetector,
  partitioner: => Partitioner[TxnRead, _],
  keyLocator: KeyLocator[ByteBuf, TxnRead],
  storage: Storage,
  storageCleaner: StorageCleaner,
  stats: StatsRecorder,
  backupRequestRatio: Double,
  readTimeout: FiniteDuration,
  minDelay: FiniteDuration)
    extends Closeable {

  import StorageService._

  private[this] val scoreKeeper = new ScoreKeeper("StorageService", stats = stats)
  scoreKeeper.start()
  hostService.subscribeStartsAndRestarts { scoreKeeper.reset }
  fd.subscribe { scoreKeeper.reset }

  private[this] val readController =
    new PIDTimeoutController(
      readRatio = backupRequestRatio,
      readTimeout = readTimeout,
      minDelay = minDelay,
      name = "BackupStorageRead",
      stats = stats)

  // FIXME: Figure out coordinator query exec pooled buffer management
  // so the handler doesn't have to copy out of the message buffer.
  private[this] val readHandler =
    bus.handler(Reads.RequestProtocol, readSignal) {
      case (from, Reads.Request(priority, replyTo, read, writes), deadline) =>
        recvRead(priority, replyTo.at(from.id), read, writes, deadline)
    }

  private[this] val sysHandler =
    bus.handler(System.RequestProtocol, sysSignal) {
      case (from, System.NeedsCleanup(cf, replyTo), deadline) =>
        recvNeedsCleanup(cf, replyTo.at(from.id), deadline)
    }

  def close(): Unit = {
    readHandler.close()
    readController.close()
    scoreKeeper.stop()
    sysHandler.close()
  }

  // Guaranteed to return at least one candidate, or throw an exception.
  private def readCandidates[R](
    read: Read[R],
    replicaLocal: Boolean): Seq[HostID] = {
    val replicas = partitioner.hostsForRead(TxnRead.inDefaultRegion(read.rowKey))
    val candidates =
      if (replicaLocal) {
        replicas filter { hostService.isLocal } toSeq
      } else {
        // PreferredOrder is a stable sort: shuffling guarantees equal-weight
        // hosts an equal chance to be chosen.
        val sortedReplicas =
          hostService.preferredOrder(Random.shuffle(replicas), scoreKeeper)
        sortedReplicas take 2
      }

    // There are no defined recipients for this key, which indicates
    // some sort of configuration problem.
    if (candidates.isEmpty) {
      throw ClusterTopologyException(keyLocator.locate(read.rowKey))
    }
    candidates
  }

  /** Performs `read` on behalf of the caller. The operation may be done
    * remotely.
    *
    * `backups` determines whether a remote read can potentially be served by a
    * second remote, if the first one is not responding fast enough.
    *
    * If `replicaLocal` is true, only nodes in the same replica are eligible to
    * perform the read.
    */
  def read[R](
    priority: GlobalID,
    read: Read[R],
    writes: Iterable[Write],
    backups: Boolean,
    replicaLocal: Boolean,
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[R] = {

    val span = GlobalTracer.instance.activeSpan

    val candidates = readCandidates(read, replicaLocal)

    // Launch the read on the preferred candidate.
    readController.incrSample()

    val preferredCandidate = candidates.head
    span foreach { _.addAttribute("preferred", preferredCandidate.toString) }

    val preferred = readFrom(priority, preferredCandidate, read, writes, deadline)
    span foreach { _.onFailure(preferred)(ImmediateExecutionContext) }

    // Prepare the backup read. After a delay determined by the PID controller,
    // launch it if the preferred read has not completed.
    val backupReadAvailable =
      backups && backupRequestRatio > 0.0 && candidates.lengthIs > 1
    val backup =
      if (!backupReadAvailable) {
        Option.empty[Future[R]]
      } else {
        val result = Promise[R]()

        def doBackupRead() = {
          val backupCandidate = candidates(1)
          span foreach { _.addAttribute("backup", backupCandidate.toString) }

          val backup = readFrom(priority, backupCandidate, read, writes, deadline)
          span foreach { _.onFailure(backup)(ImmediateExecutionContext) }
          result.completeWith(backup)
        }

        val timer = readController.scheduleTimeout(Timer.Global, deadline) {
          // The preferred read hasn't completed yet, so launch the backup.
          readController.incrTimeout()
          doBackupRead()
        } {
          result.failure(
            new TimeoutException(
              "deadline passed before backup read PID delay passed"))
        }

        preferred.onComplete({
          case Failure(_) =>
            () // The PID controller delay manages launching the backup read.
          case Success(_) => timer.cancel()
        })(ImmediateExecutionContext)

        Some(result.future)
      }

    val t = Timing.start
    backup
      .fold(preferred) { b =>
        FutureSyntax.firstSuccessOf(Seq(b, preferred))
      }
      .ensure {
        stats.timing("StorageService.Read.Time", t.elapsedMillis)
        stats.timing(s"StorageService.Read.${read.name}.Time", t.elapsedMillis)
      }(ImmediateExecutionContext)
  }

  def readFrom[R](
    priority: GlobalID,
    host: HostID,
    read: Read[R],
    writes: Iterable[Write],
    deadline: TimeBound)(implicit ec: ExecutionContext): Future[R] = {

    val t = Timing.start

    val result = if (bus.hostID == host) {
      readLocal(host, priority, read, writes, deadline)
    } else {
      readFromRemote(priority, host, read, writes, deadline)
    }

    result.onComplete {
      case Success(_) => scoreKeeper.receiveTiming(host, t.elapsedMillis)
      case Failure(_) => scoreKeeper.receiveTiming(host, deadline.duration.toMillis)
    }(ImmediateExecutionContext)

    result
  }

  def allClean(
    cf: Option[String],
    deadline: TimeBound,
    ignore: Set[HostID] = Set.empty)(
    implicit ec: ExecutionContext): Future[Boolean] = {
    val peers =
      membership.hosts &~ membership.leavingHosts &~ membership.leftHosts &~ ignore
    val reqs = peers map { needsCleanup(_, cf, deadline) }

    reqs.sequence map {
      _.forall { _ == false }
    }
  }

  def needsCleanup(host: HostID, cf: Option[String], deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Boolean] = {

    if (host == bus.hostID) { // it me!
      Future.successful(storageCleaner.needsCleanup(cf))
    } else {
      val resultP = Promise[Boolean]()

      @nowarn("cat=unused-params")
      def onReply(
        hi: HostInfo,
        result: System.CleanupNeeded,
        tb: TimeBound): Future[Unit] = {
        resultP.trySuccess(result.value)
        Future.unit
      }

      def onExpiry: Future[Unit] = {
        val cfs = cf.getOrElse("all")
        val msg =
          s"Timed out checking cleanliness of cfs: $cfs. Deadline: $deadline. Recipient: $host."
        resultP.tryFailure(new TimeoutException(msg))
        Future.unit
      }

      val src =
        bus.tempHandler(
          System.ReplyProtocol[System.CleanupNeeded],
          deadline.timeLeft)(onReply, onExpiry)

      val req = System.NeedsCleanup(cf, src.id.signalID)
      bus.sink(System.RequestProtocol, sysSignal.at(host)).send(req, deadline)

      resultP.future.ensure { src.close() }(ImmediateExecutionContext)
    }
  }

  private def readFromRemote[R](
    priority: GlobalID,
    host: HostID,
    read: Read[R],
    writes: Iterable[Write],
    deadline: TimeBound
  )(implicit ec: ExecutionContext): Future[R] = {
    import read.codec

    val resultP = Promise[R]()

    @nowarn("cat=unused-params")
    def onReply(hi: HostInfo, result: R, tb: TimeBound): Future[Unit] = {
      resultP.trySuccess(result)
      Future.unit
    }

    def onExpiry: Future[Unit] = {
      val msg =
        s"Read Timeout. Deadline: $deadline. Read: $read. Recipient: $host."
      resultP.tryFailure(new TimeoutException(msg))
      Future.unit
    }

    val src =
      bus.tempHandler(
        Reads.ReplyProtocol[R],
        deadline.timeLeft,
        bufRetention = BufRetention.Copy)(onReply, onExpiry)
    val req = Reads.Request(priority, src.id.signalID, read, writes.toVector)

    bus.sink(Reads.RequestProtocol, readSignal.at(host)).send(req, deadline)

    resultP.future.ensure { src.close() }(ImmediateExecutionContext)
  }

  private def readLocal[R](
    source: HostID,
    priority: GlobalID,
    read: Read[R],
    writes: Iterable[Write],
    deadline: TimeBound)(implicit ec: ExecutionContext): Future[R] = {

    def doRead[R](priority: GlobalID, read: Read[R]): Future[R] = {
      val part = partitioner
      // Must cover key.
      if (!part.coversRead(TxnRead.inDefaultRegion(read.rowKey))) {
        stats.incr("Storage.Orphaned.Read")
        stats.incr(s"Storage.Orphaned.Read.${read.name}")
        Future.failed(new IllegalArgumentException(
          s"Read for key ${CBOR.showBuffer(read.rowKey)} not covered by this node."))
      } else {
        storage.read(source, priority, read, writes, deadline) flatMap { row =>
          if (partitioner.version == part.version) {
            Future.successful(row)
          } else if (deadline.hasTimeLeft) {
            // Retry.
            doRead(priority, read)
          } else {
            Future.failed(
              new TimeoutException("Timed out waiting for topology to stabilize."))
          }
        }
      }
    }

    doRead(priority, read)
  }

  private def recvRead[R](
    priority: GlobalID,
    replyTo: HandlerID,
    read: Read[R],
    writes: Iterable[Write],
    deadline: TimeBound): Future[Unit] = {
    import read.codec

    val t = Timing.start
    val source = replyTo.host.toEither.fold(Function.const(HostID.NullID), identity)
    val readF = readLocal(source, priority, read, writes, deadline)(
      FaunaExecutionContext.Implicits.global)

    implicit val iec = ImmediateExecutionContext
    readF flatMap { res =>
      val sink = bus.sink(Reads.ReplyProtocol[R], replyTo)
      sink.send(res, deadline).unit
    } recover { case _: TimeoutException =>
      ()
    } ensure {
      // The service could also track a served time for each host, but one
      // should be able to see if A->B is slow based on the
      // StorageService.Read.{op}.Host{host id of B}.Time metric on A.
      stats.timing("StorageService.Read.Served.Time", t.elapsedMillis)
      stats.timing(s"StorageService.Read.Served.${read.name}.Time", t.elapsedMillis)
    }
  }

  private def recvNeedsCleanup(
    cf: Option[String],
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {
    implicit val iec = ImmediateExecutionContext

    val needed = storageCleaner.needsCleanup(cf)
    val reply = System.CleanupNeeded(needed)
    val sink = bus.sink(System.ReplyProtocol[System.CleanupNeeded], replyTo)

    sink.send(reply, deadline).unit
  }
}
