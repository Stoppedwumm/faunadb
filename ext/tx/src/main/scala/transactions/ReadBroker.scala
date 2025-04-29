package fauna.tx.transaction

import com.github.benmanes.caffeine.cache.{ Cache, Caffeine, RemovalCause }
import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.{ ImmediateExecutionContext, Timer }
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.lang.clocks.DeadlineClock
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.stats.StatsRecorder
import io.netty.util.Timeout
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._

/**
  * The ReadBroker is responsible for managing transaction reads on behalf of
  * the DataNode. The important methods are `read()` and `recvResult()`.
  *
  * On transaction apply, the ScopeBroker calls `read()` for each of the
  * transaction's read dependencies, and leaves to this class the work of
  * dispatching reads to remote nodes, apply backup request policy, manage
  * retry, etc.
  *
  * When called, `read()` calls `registerRead()`, which puts a promise into
  * `pendingReads`. If a result has already been received from another node's
  * read broadcast, it will be read from the `cache` and used to immediately
  * complete the promise. If not, then `read()` uses the latest partitioner to
  * issue a backup request and subsequently retry on timeout, until the read is
  * successful.
  *
  * When a result is received via `recvResult()`, if an entry already exists in
  * `pendingReads`, it is completed with the result. If not, the value is stored
  * in the cache, which may be used in the future, expire, or be evicted.
  */
class ReadBroker[K, R, RV](service: DataNode[K, R, RV, _, _])
  (implicit deadlineClock: DeadlineClock) extends ExceptionLogging {

  import DataMessage._
  import ctx.codecs._

  private[this] val ctx = service.ctx

  // The IEC should be appropriate because the compute is trivial in
  // this class.
  private[this] implicit val ec = ImmediateExecutionContext

  val ReadCacheExpiry = ctx.config.roundTripTime * 10
  val ReadTimeout = ctx.config.roundTripTime * 2
  val MaxBackoffTime = ctx.config.roundTripTime * 16
  val ForwardReadTimeout = ReadTimeout / 2

  private type ReadCacheKey = (Timestamp, R)
  private type ReadResult = (R, RV)

  // init

  private[this] val pendingReads =
    new ConcurrentHashMap[ReadCacheKey, Promise[ReadResult]]

  // Must use Option[RV] instead of RV to force the cache value type to be a reference...
  private[this] val cache: Cache[ReadCacheKey, RV] = Caffeine.newBuilder
    .expireAfterWrite(ReadCacheExpiry.length, ReadCacheExpiry.unit)
  // It's important that this not be too small. Evicting older entries before they're used will cause
  // huge performance issues.
    .maximumSize(1_000_000)
    .removalListener({ (k: ReadCacheKey, _: RV, cause) =>
      val (ts, r) = k
      cause match {
        case RemovalCause.EXPIRED =>
          ctx.logger.debug(s"DataNode READ at $ts: Cached read expired. $r")
        case RemovalCause.SIZE =>
          ctx.logger.debug(s"DataNode READ at $ts: Cached read evicted due to size constraints. $r")
        case RemovalCause.EXPLICIT =>
          ctx.logger.debug(s"DataNode READ at $ts: Cached read invalidated. $r")
        case _ =>
      }
    })
    .recordStats()
    .build()

  @volatile private[this] var stats = cache.stats()

  private def reportStats() = {
    val snap = cache.stats()
    val prev = stats
    stats = snap

    val delta = snap.minus(prev)

    ctx.stats.recorder.count("Cache.RemoteRead.Hits", delta.hitCount.toInt)
    ctx.stats.recorder.count("Cache.RemoteRead.Misses", delta.missCount.toInt)
    ctx.stats.recorder.count("Cache.RemoteRead.Evictions", delta.evictionCount.toInt)
    ctx.stats.recorder.count("Cache.RemoteRead.Loads", delta.loadCount.toInt)
    ctx.stats.recorder.count("Cache.RemoteRead.Exceptions", delta.loadFailureCount.toInt)
    ctx.stats.recorder.count("Cache.RemoteRead.Objects", cache.estimatedSize.toInt)
  }

  StatsRecorder.polling(10.seconds) {
    reportStats()
  }

  // methods

  @volatile private[this] var closed = false
  def close() = closed = true

  def recvResult(
    host: HostID,
    ts: Timestamp,
    read: R,
    value: RV,
    token: Option[Long],
    version: Long): Future[Unit] = {
    if (token.isEmpty) {
      ctx.logger.debug(
        s"DataNode READ at $ts: Received forwarded read from $host $read (version=$version)")
    }

    val part = ctx.partitioner
    if (!part.hostsForRead(read).contains(host)) {
      // Drop this read result - it comes from a different topology.
      ctx.logger.warn(
        s"DataNode READ at $ts: Dropping forwarded read from $host $read. " +
          s"(${part.version} != $version)")
      return Future.unit
    }

    // pass the result to waiters
    if (!putResult(ts, read, value)) {
      // if there is no waiter, cache the result
      ctx.logger.debug(s"DataNode READ at $ts: Caching forwarded read from $host $read")
      cache.put((ts, read), value)

      // try the put again in case a waiter showed up between our initial put
      // and cache population.
      //FIXME: invalidate cache here if this returns true
      if (putResult(ts, read, value)) {
        ctx.logger.debug(s"DataNode READ at $ts: Late arrival for $read from $host")
      }
    } else {
      ctx.logger.debug(s"DataNode READ at $ts: Waiter found for $read from $host")
    }

    Future.unit
  }

  private def putResult(ts: Timestamp, read: R, value: RV): Boolean = {
    val p = pendingReads.get((ts, read))
    if (p ne null) {
      p.trySuccess((read, value))
      true
    } else {
      false
    }
  }

  def readRegistered(ts: Timestamp, read: R): Unit = {
    val rp = pendingReads.get((ts, read))
    if (rp ne null) {
      readRegistered(ts, read, rp)
    }
  }

  private[this] def readRegistered(
    ts: Timestamp,
    read: R,
    rp: Promise[ReadResult]): Unit = {

    val timing = Timing.start
    @volatile var readSent = false
    @volatile var timeout = Option.empty[Timeout]
    lazy val encoded = CBOR.encode(read).toByteArray

    def loop(i: Int, isRetry: Boolean): Unit = {
      if (isRetry) {
        ctx.stats.incrRemoteReadStalls()
      }

      val part = ctx.partitioner
      val deadline = ReadTimeout.bound
      val pool = part.hostsForRead(read)
      val hosts = ctx.hostService.preferredOrder(pool)

      if (hosts.isEmpty) {
        ctx.stats.incrRemoteReadUncovered()
        ctx.logger.warn(s"DataNode READ at $ts: No hosts covered read $read.")
      }

      def sendRead0(h: HostID): Unit =
        sendRead(h, ts, encoded, part.version, deadline) foreach { sent =>
          if (sent) readSent = true
        }

      def sendBackupReadAndScheduleRetry(h: HostID): Option[Timeout] =
        Option.when(!rp.isCompleted) {
          if (!isRetry) {
            // Log failure
            val primary = hosts.head
            if (ctx.hostService.isLocal(primary)) {
              ctx.stats.incrRemoteReadForwardedTimeout()
              ctx.logger.debug(
                s"DataNode READ at $ts: Timed out waiting for forwarded read from $primary $read")
            } else {
              ctx.stats.incrRemoteReadRequestedTimeout()
              ctx.logger.debug(
                s"DataNode READ at $ts: Timed out on remote read request from $primary $read")
            }
          }
          sendRead0(h)
          scheduleRetry()
        }

      def scheduleRetry(): Timeout =
        Timer.Global.scheduleTimeout((ReadTimeout * i) min MaxBackoffTime) {
          if (!rp.isCompleted) loop(i + 1, isRetry = true)
        }

      // The primary request is skipped where we can determine that the result is
      // already on its way. This logic exists in registerRead. On retry iterations,
      // send read requests to other hosts, but not localhost.
      //
      // NOTE: Every read is scheduled into the read broker by the scope broker, even
      // reads served by the local host. Local host reads never timeout at the scope
      // broker, therefore, their results are guaranteed to arrive during normal
      // operation. In these cases, we should not be issuing remote read requests.
      // However, we still must remain in the retry loop until its completion so that
      // changes in topology can be detected and remote read requests can be issued
      // if this read becomes no longer owned by the local host.
      if (isRetry && !pool.contains(ctx.hostID)) {
        hosts.headOption.foreach(sendRead0)
      }

      // NOTE: Backup reads workaround failures in the local replica by requesting
      // read results from the second most preferred host in the list, which is
      // likely a host in a non-local replica that is not forwarding its read results
      // to this host unless explicitly requested. Moreover, races during topology
      // changes may cause a stale partitioner at the scope broker to miss scheduling
      // local reads. Thus, backup reads can patch over it by still requesting read
      // results from some other host.
      hosts.lift(1) match {
        case Some(h) =>
          ctx.logger.debug(
            s"DataNode READ at $ts: Waiting $deadline for $read from ${hosts.head}")

          if (isRetry) {
            timeout = sendBackupReadAndScheduleRetry(h)
          } else {
            timeout = Some {
              Timer.Global.scheduleTimeout(ForwardReadTimeout) {
                timeout = sendBackupReadAndScheduleRetry(h)
              }
            }
          }
        case None =>
          // If a backup read did not get scheduled, directly schedule the retry.
          timeout = Some(scheduleRetry())
      }
    }

    // we may have already received the result, either from local processing or
    // preemptively from a peer. If so, no need to bother scheduling anything.
    if (!rp.isCompleted) {
      rp.future ensure {
        timeout foreach { _.cancel() }
        if (readSent) {
          ctx.stats.incrRemoteReadCacheMisses()
        } else {
          ctx.stats.incrRemoteReadCacheHits()
        }
        ctx.stats.recordRemoteReadLatency(timing.elapsed)
      }
      loop(1, isRetry = false)
    } else {
      ctx.stats.incrRemoteReadCacheHits()
    }
  }

  def registerRead(
    ts: Timestamp,
    read: R,
    isRecovery: Boolean): Future[ReadResult] = {

    val key = (ts, read)
    val promise = Promise[ReadResult]()
    ctx.logger.debug(s"DataNode READ at $ts: Register read promise for $read")

    // every (ts, read) combination is unique (or unique per region w/ data locality)
    assert(pendingReads.put(key, promise) eq null)

    promise.future onComplete { _ =>
      pendingReads.remove(key)
      cache.invalidate(key) // remove from the map once it has been consumed
    }

    cache.getIfPresent(key) match {
      case null =>
        val part = ctx.partitioner
        val pool = part.hostsForRead(read)
        val hosts = ctx.hostService.preferredOrder(pool)
        def preferredIsLocal = hosts.headOption.fold(true)(ctx.hostService.isLocal)

        if (!pool.contains(ctx.hostID)) { // not a localhost read
          // we should proactively send a read request in two cases:
          // 1. if the preferred host is not in the local replica, this means the
          //    local forwarding peer is likely down.
          // 2. We are in recovery mode, and should not expect forwarded reads
          def shouldSendReadRequest = isRecovery || !preferredIsLocal

          if (shouldSendReadRequest) {
            hosts.headOption foreach { hostID =>
              val encoded = CBOR.encode(read).toByteArray
              sendRead(hostID, ts, encoded, part.version, ReadTimeout.bound)
            }
          }
        }
      case result =>
        ctx.logger.debug(s"DataNode READ at $ts: Found read promise for $read")
        promise.trySuccess((read, result))
    }

    promise.future
  }

  private def sendRead(
    to: HostID,
    ts: Timestamp,
    encoded: Array[Byte],
    version: Long,
    deadline: TimeBound) = {
    val token = deadlineClock.millis
    ctx
      .dataSink(to)
      .send(Read(ScopeID.RootID, encoded, ts, Some(token), version), deadline)
  }
}
