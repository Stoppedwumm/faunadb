package fauna.repo.cassandra

import fauna.atoms._
import fauna.cluster._
import fauna.cluster.topology._
import fauna.cluster.workerid._
import fauna.exec.{ NamedForkJoinExecutionContext, Timer }
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.flags.{
  AccountProperties,
  CachedResult,
  HostFlags,
  HostProperties,
  Properties,
  Service => FFClient,
  Value => FFValue
}
import fauna.lang.{ AdminControl, Timestamp }
import fauna.lang.clocks._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.{
  ArrivalRate,
  ArrivalRateService,
  BuildVersion,
  BurstLimiter,
  HostAddress,
  HostDest,
  HostService
}
import fauna.net.bus._
import fauna.net.gossip.{ GossipAdaptor, ReadClockDelay }
import fauna.net.security.Encryption
import fauna.net.util.FutureSequence
import fauna.repo.{
  Executor,
  HealthChecker,
  RepoContext,
  StorageCleaner,
  UninitializedException
}
import fauna.repo.data.{ InitializeConfig, InitializeResult }
import fauna.repo.query.{ Query, StreamOp }
import fauna.repo.service._
import fauna.repo.service.stream.{ Coordinator => StreamCoordinator }
import fauna.scheduler._
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.api.{ Storage => StorageAPI }
import fauna.storage.cassandra.{
  CassandraKeyExtractor,
  CassandraKeyLocator,
  CassandraStorageEngine
}
import fauna.tx.transaction._
import io.netty.buffer.ByteBuf
import java.lang.{ Boolean => JBoolean }
import java.nio.file.{ Files, Path, Paths }
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.TimeoutException
import scala.annotation.unused
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

// giant singleton
object CassandraService extends ExceptionLogging {
  // @formatter:off
  val MembershipAddressSignal = SignalID(1001)
  val MembershipSignal        = SignalID(1002)
  val MembershipStateSignal   = SignalID(1003)
  val WorkerIDStateSignal     = SignalID(1004)
  val WorkerIDSignal          = SignalID(1005)
  // 1006-1007 were used by HostStates.
  val TopologiesSignal        = SignalID(1008)
  val TopologiesStateSignal   = SignalID(1009)
  val LogTopologySignal       = SignalID(1010)
  val LogTopologyStateSignal  = SignalID(1011)
  val TxnReplogSignal         = SignalID(7)
  val TxnBatchlogSignal       = SignalID(8)
  val TxnDataSignal           = SignalID(9)
  // 10 was the old RowRead service
  // 11 was the old RowService signal
  // 12 was the old Segment service
  // 13 was the old TransferSignal
  // 14 was the old ValidationSignal
  // 15 was the old RepairSignal
  val FailureSignal           = SignalID(16)
  val RestoreSignal           = SignalID(17)
  val GossipSignal            = SignalID(18)
  // 19 was the old HostInfoGossipSignal
  val PingSignal              = SignalID(20)
  val ReadClockDelaySignal    = SignalID(21)
  val VersionGossipSignal     = SignalID(22)
  // 23 was the old ClockSyncSignal
  // 24 was the old SegmentSignal2
  // 25 was the old ServiceDiscoverySignal
  // 26 was used by legacy data node stream service
  val TransferSignal2         = SignalID(27)
  val TaskSignal              = SignalID(28)
  val BackupSignal            = SignalID(29)
  val RepairSignal            = SignalID(30)
  val ValidationSignal2       = SignalID(31)
  // 32 was the old SegmentSignal3
  val StreamProtocolSignal    = SignalID(33)
  val StorageReadSignal       = SignalID(34)
  val StorageSysSignal        = SignalID(35)
  val ArrivalRateSignal       = SignalID(36)
  val StreamResultsSignal     = SignalID(37)
  // @formatter:on

  val StatsSigs = Set(
    TxnReplogSignal,
    TxnBatchlogSignal,
    TxnDataSignal,
    StorageReadSignal,
    StreamProtocolSignal,
    StreamResultsSignal
  )

  private[CassandraService] val GossipAddressesUpdateInterval = 1.second
  // TopologyReconciler's streamer will wait this long for DataNode to
  // catch up with the required topology partitioner version before logging
  // a warning (and then retrying.)
  private[CassandraService] val DataNodeTopologyWaitPeriod = 10.seconds

  private[this] val startedPromise = Promise[CassandraService]()
  val started = startedPromise.future

  @volatile private[this] var _shutdownRequested: Boolean = false
  def shutdownRequested = _shutdownRequested

  @volatile private[this] var _instance: CassandraService = _
  def instanceOpt = Option(_instance)

  def instance = instanceOpt getOrElse {
    throw UninitializedException("FaunaDB Service")
  }

  def fatal(msg: String, ex: Option[Throwable] = None): Nothing = {
    getLogger.error(msg)
    System.err.println(msg)

    ex foreach { e =>
      logException(e)
      e.printStackTrace(System.err)
    }

    System.exit(1)
    throw null // make a 'nothing' in the absence of @noreturn
  }

  private def terminateIfInitialized(): Unit =
    if (_instance ne null) {
      fatal("FaunaDB Service has already been initialized.")
    }

  private def initCassandra(config: CassandraConfig): Unit = {
    // FIXME: move to singleton initialization
    Cassandra.init(config)

    // Check for shutdown snapshots and validate them.
    Cassandra.snapshots foreach { case (cf, snaps) =>
      snaps foreach { snapshot =>
        if (snapshot.startsWith("shutdown-")) {
          Cassandra.validateSnapshot(cf, snapshot)
        }
      }
    }
  }

  def initialize(
    config: CassandraConfig,
    encryption: Encryption.Level,
    stats: StatsRecorder,
    prioritizer: PriorityProvider,
    tmpDirectory: Path,
    txnLogBackupPath: Option[Path],
    storagePath: Path,
    backupDir: Path,
    mkExecutor: Option[CassandraService => Future[Executor]],
    flagsClient: FFClient,
    arrivalsService: ArrivalRateService): Unit =
    synchronized {
      InitializeConfig.getOrCreate(
        config = config,
        encryption = encryption,
        stats = stats,
        prioritizer = prioritizer,
        tmpDirectory = tmpDirectory,
        txnLogBackupPath = txnLogBackupPath,
        storagePath = storagePath,
        backupDir = backupDir,
        mkExecutor = mkExecutor,
        flagsClient = flagsClient,
        arrivalsService = arrivalsService
      )
      initCassandra(config)
    }

  /** If replicaName is already set then start!
    */
  def maybeStart(): Option[Unit] =
    Cassandra.getReplicaName map start

  /** Used to start CassandraService with a replicaName if the replicaName
    * was unknown on boot-up.
    *
    * Pre-condition: [[Cassandra]] is expected to already be initialized.
    */
  def start(replicaName: String): InitializeResult =
    InitializeConfig.get match {
      case Some(config) =>
        initializeAndStart(
          replicaName = replicaName,
          prepareInitConfig = config
        )
      case None =>
        fatal(
          s"${InitializeConfig.getClass.getSimpleName} is not set. Make sure to invoke " +
            "initialize before calling this function."
        )
    }

  private def setInstance(cassandraService: CassandraService) = {
    _instance = cassandraService
    startedPromise.completeWith(cassandraService.started map { _ =>
      cassandraService
    })
  }

  /** Used to start CassandraService with a replicaName if the replicaName
    * was unknown on boot-up.
    *
    * Pre-condition: [[Cassandra]] is expected to already be initialized.
    */
  private def initializeAndStart(
    replicaName: String,
    prepareInitConfig: InitializeConfig): InitializeResult =
    synchronized {
      if (instanceOpt.isDefined) {
        // send a response based on the state of replica_name.
        Cassandra.getReplicaName match {
          case Some(existingReplicaName) =>
            if (existingReplicaName == replicaName) {
              InitializeResult.Initialized
            } else {
              InitializeResult.ReplicaNameAlreadySet(existingReplicaName)
            }

          case None =>
            // CassandraService instance is never created if replica name is not set
            // so
            // the following should never really occur.
            fatal("Replica name is not set")
        }
      } else {
        Cassandra.setReplicaNameIfEmpty(replicaName) match {
          case SetReplicaNameResult.Success =>
            setInstance {
              new CassandraService(
                replicaName,
                config = prepareInitConfig.config,
                encryption = prepareInitConfig.encryption,
                stats = prepareInitConfig.stats,
                prioritizer = prepareInitConfig.prioritizer,
                tmpDirectory = prepareInitConfig.tmpDirectory,
                txnLogBackupPath = prepareInitConfig.txnLogBackupPath,
                storagePath = prepareInitConfig.storagePath,
                backupDir = prepareInitConfig.backupDir,
                mkExecutor = prepareInitConfig.mkExecutor,
                flagsClient = prepareInitConfig.flagsClient,
                arrivalsService = prepareInitConfig.arrivalsService
              )
            }

            InitializeResult.Initialized

          case SetReplicaNameResult.InvalidReplicaName =>
            InitializeResult.InvalidReplicaName
          case SetReplicaNameResult.ReplicaNameAlreadySet(oldReplicaName) =>
            InitializeResult.ReplicaNameAlreadySet(oldReplicaName)
        }
      }
    }

  def initializeAndStart(
    replicaName: String,
    config: CassandraConfig,
    encryption: Encryption.Level,
    stats: StatsRecorder,
    prioritizer: PriorityProvider,
    tmpDirectory: Path,
    txnLogBackupPath: Option[Path],
    storagePath: Path,
    backupDir: Path,
    mkExecutor: Option[CassandraService => Future[Executor]],
    flagsClient: FFClient,
    arrivalsService: ArrivalRateService) =
    synchronized {
      terminateIfInitialized()
      initCassandra(config)

      setInstance {
        new CassandraService(
          replicaName,
          config,
          encryption,
          stats,
          prioritizer,
          tmpDirectory,
          txnLogBackupPath,
          storagePath,
          backupDir,
          mkExecutor,
          flagsClient,
          arrivalsService
        )
      }
    }

  def stop(grace: Duration): Future[Unit] = {
    _shutdownRequested = true

    // Generate a snapshot before shutdown for validation after
    // restart.
    if (JBoolean.getBoolean("fauna.shutdown-snapshot")) {
      Cassandra.snapshot(s"shutdown-${Clock.time.millis}")
    }

    // This is needed to shut down the heartbeat, so that other hosts remove
    // this host from their list of live nodes during the shutdown grace period.
    instance.failureService.close()
    instance.txnPipeline.close(grace)
    instance.logTopology.prepareStop()
    Timer.Global.delay(grace) { Future.successful(stop()) }
  }

  def stop() =
    synchronized {
      instance.stop()
      Cassandra.stop()

      _instance = null
    }
}

class CassandraService(
  val replicaName: String,
  val config: CassandraConfig,
  val encryption: Encryption.Level,
  val stats: StatsRecorder,
  val prioritizer: PriorityProvider,
  val tmpDirectory: Path,
  val txnLogBackupPath: Option[Path],
  val storagePath: Path,
  val backupDir: Path,
  val mkExecutor: Option[CassandraService => Future[Executor]],
  val flagsClient: FFClient,
  val arrivalsService: ArrivalRateService)
    extends ExceptionLogging {

  import CassandraService._

  val log = getLogger

  private val startedPromise = Promise[Unit]()
  val started = startedPromise.future

  @volatile private[this] var running = false
  def isRunning = running

  private[this] def txnMinPersistedTimestamp =
    txnPipeline.minPersistedTimestamp

  private[this] def txnHintGlobalPersistedTimestamp(ts: Timestamp) =
    txnPipeline.ctx.hintGlobalPersistedTimestamp(ts)

  private[this] val systemDir = Paths.get(config.rootPath, "system")
  Files.createDirectories(systemDir)

  private val readScheduler =
    new IOScheduler(
      "StorageRead",
      config.processors * 2,
      config.enableQoS,
      stats,
      config.concurrentReads)

  // FIXME: CassandraService currently manages the bus, because it
  // controls the HostID (UUID) -> HostAddress (IP:port) mapping.
  // Once we control it, bus instantiation should be pulled out.
  val bus = {
    val ssl = encryption.sslConfig
    val zone = encryption match {
      case Encryption.Cleartext | _: Encryption.All => None
      case _: Encryption.Replica                    => Some(replicaName)
    }

    MessageBus(
      config.cluster,
      Cassandra.self,
      config.broadcastAddress,
      Option(config.listenAddress),
      zone = zone,
      ssl = ssl,
      port = config.busPort,
      sslPort = config.busSSLPort,
      connectionsPerHost = config.connectionsPerHost,
      connectTimeout = config.busConnectTimeout,
      handshakeTimeout = config.busHandshakeTimeout,
      idleTimeout = config.busIdleTimeout,
      keepaliveInterval = config.busKeepaliveInterval,
      keepaliveTimeout = config.busKeepaliveTimeout,
      maxMessageSize = config.busMaxMessageSize,
      stats = stats,
      statsSigs = CassandraService.StatsSigs
    )
  }

  bus.start()

  // identify ourselves in the log and console for archaeological
  // purposes
  val identity = s"Identified as ${Cassandra.self} at ${config.broadcastAddress}."
  log.info(identity)
  System.err.println(identity)

  txnLogBackupPath map { Files.createDirectories(_) }
  // To backup all raft logs change the None to txnLogBackupPath
  private val clusterServiceConfig =
    ClusterServiceConfig(
      bus,
      config.busMaxPendingMessages,
      tmpDirectory,
      systemDir,
      config.roundTripTime / 2,
      None)

  private val membership = Membership(
    clusterServiceConfig,
    MembershipSignal,
    MembershipStateSignal,
    MembershipAddressSignal,
    replicaName,
    stats)

  membership.start()

  private def updateAddress(): Future[Unit] =
    if (membership.isMember && !membership.busAddressMatches) {
      val knownAddress = membership.localHostName getOrElse "<unknown>"

      log.warn(
        s"This node's address ${bus.hostAddress.hostName} does not match its address " +
          s"$knownAddress known by the rest of the cluster. New address will be " +
          "made known to the cluster.")

      membership.updateAddress() recoverWith {
        // Keep trying, because what else will you do?
        case _: TimeoutException => updateAddress()
      }
    } else {
      Future.unit
    }

  updateAddress()

  private val failureDetector = FailureDetector(membership, stats)

  val failureService =
    FailureService(FailureSignal, bus, membership, failureDetector, stats)
  FailureService.run(failureService)

  val hostService = new HostService {
    def isLive(host: HostID): Boolean = failureDetector.isAlive(host)
    def isLocal(host: HostID): Boolean = membership.isLocal(host)

    def isNear(host: HostID): Boolean =
      config.neighborReplica exists { neighbor =>
        membership.getReplica(host) contains neighbor
      }

    def subscribeStartsAndRestarts(f: HostID => Unit): Unit =
      failureService.subscribeStartsAndRestarts(f)
  }

  private val workerIDs =
    WorkerIDs(
      clusterServiceConfig,
      WorkerIDSignal,
      WorkerIDStateSignal,
      membership,
      stats,
      config.minWorkerID,
      config.maxWorkerID)
  workerIDs.start()

  private val topologies = Topologies(
    clusterServiceConfig,
    TopologiesSignal,
    TopologiesStateSignal,
    membership,
    stats)
  topologies.start()

  private val logTopology = LogTopology(
    clusterServiceConfig,
    LogTopologySignal,
    LogTopologyStateSignal,
    topologies,
    membership,
    stats)

  logTopology.start()

  private val repartitioner = {
    val r = new Repartitioner(membership, workerIDs, topologies)
    r.start()
    r
  }

  val partitioner =
    new TopologyPartitionerProvider[ByteBuf, TxnRead, Txn](topologies) {
      def makePartitioner(tstate: TopologyState) =
        new TopologyPartitioner[ByteBuf, TxnRead, Txn](
          tstate,
          membership,
          CassandraKeyLocator,
          CassandraKeyExtractor
        )
    }

  val storage = new CassandraStorageEngine(
    StorageEngine.Config(
      membership.self,
      Paths.get(config.rootPath),
      CassandraKeyLocator,
      { () => partitioner.partitioner },
      config.concurrentReads,
      config.concurrentTxnReads,
      readScheduler,
      config.syncPeriod,
      StorageEngine.Stats(stats),
      syncOnShutdown = config.syncOnShutdown,
      dualWriteIndexCFs = config.dualWriteIndexCFs,
      openFileReserve = config.openFileReserve,
      unleveledSSTableLimit = config.unleveledSSTableLimit,
      latWaitThreshold = config.latWaitThreshold
    ))

  val pconf = PipelineConfig(
    replicationSignal = TxnReplogSignal,
    logSignal = TxnBatchlogSignal,
    dataSignal = TxnDataSignal,
    rootPath = Paths.get(config.rootPath),
    maxBytesPerBatch = config.transactionLogBufferSize,
    busMaxPendingMessages = config.busMaxPendingMessages,
    roundTripTime = config.roundTripTime,
    txnLogBackupPath = config.txnLogBackupPath,
    maxConcurrentTransactions = config.concurrentApplyingTransactions,
    maxOCCReadsPerSecond = config.maxOCCReadsPerSecond,
    occBackoffThreshold = config.occReadsBackoffThreshold
  )

  private val _readClock = new WeightedDelayTrackingClock(Clock)
  private val ackDelayClock = new DirectDelayTrackingClock(Clock, pconf.maxReadDelay)

  private[this] val dataNodeArbitrator =
    new DataNodeArbitrator(membership, workerIDs, topologies, storage)

  private val txnEC =
    NamedForkJoinExecutionContext("Transaction.Pipeline", config.processors)

  val txnPipeline =
    TxnPipeline(
      pconf,
      hostService,
      logTopology.logNodeProvider,
      membership.hosts,
      dataNodeArbitrator.isDataNode,
      CassandraKeyExtractor,
      mismatchResetTxn,
      partitioner,
      StreamOp.DataFilterCodec,
      storage,
      txnEC,
      bus,
      _readClock,
      ackDelayClock,
      Clock,
      config.backupReadRatio,
      config.consensusStallRestartPeriod,
      stats
    )

  txnPipeline.ctx.markEpochSynced()

  private val topologyMutex = FutureSequence()

  val storageCleaner =
    new StorageCleaner(storage, membership, topologies, topologyMutex, stats = stats)

  val newStorage = {
    val ctx = new StorageAPI.Context(
      prioritizer,
      storage,
      () => cachedHostFlags(),
      stats
    )
    StorageAPI(ctx)
  }

  val storageService = new StorageService(
    StorageReadSignal,
    StorageSysSignal,
    bus,
    membership,
    hostService,
    failureDetector,
    partitioner.partitioner,
    CassandraKeyLocator,
    newStorage,
    storageCleaner,
    stats,
    config.backupReadRatio,
    config.readTimeout,
    config.minBackupReadDelay
  )

  val streamService =
    new StreamService(
      StreamProtocolSignal,
      StreamResultsSignal,
      bus,
      txnPipeline,
      CassandraKeyLocator,
      StreamCoordinator.Config(
        maxOpenStreams = config.streamMaxOpenStreams,
      ),
      stats
    )

  val validationService = new ValidationService(
    ValidationSignal2,
    bus,
    partitioner.partitioner,
    storage,
    () => txnMinPersistedTimestamp)

  val recvLimiter = BurstLimiter
    .newBuilder()
    .withName("Transfer")
    .withPermitsPerSecond(config.recvMaxBytesPerSecond)
    .withMaxBurstSeconds(config.recvMaxBurstSeconds)
    .build()

  val transferService = new TransferService(
    TransferSignal2,
    bus,
    partitioner.partitioner,
    storage,
    tmpDirectory,
    config.transferChunkSize,
    config.enableDataTransferDigests,
    config.streamPlanThreads,
    recvLimiter,
    stats
  )

  val repairService = new RepairService(
    RepairSignal,
    bus,
    partitioner.partitioner,
    membership,
    CassandraKeyLocator,
    validationService,
    transferService,
    storage,
    config.repairSyncParallelism,
    config.repairHashDepth
  )

  private var _schemaCacheInvalidationService: SchemaCacheInvalidationService = _

  // Set of collection IDs that should invalidate schema cache.
  private val versionedCollIDs =
    Set(
      DatabaseID.collID,
      CollectionID.collID,
      IndexID.collID,
      UserFunctionID.collID,
      RoleID.collID,
      KeyID.collID,
      TokenID.collID,
      AccessProviderID.collID
    )

  // Set of collection IDs that do not invalidate schema cache.
  private val unversionedCollIDs =
    Set(
      KeyID.collID,
      TokenID.collID
    )

  def initSchemaCacheInvalidationService(
    repo: RepoContext,
    invalidateCache: (ScopeID, DocID, Timestamp) => Query[Unit]): Unit = {

    if (_schemaCacheInvalidationService != null) {
      _schemaCacheInvalidationService.stop()
    }

    _schemaCacheInvalidationService = new SchemaCacheInvalidationService(
      streamService,
      stats,
      repo,
      versionedCollIDs,
      unversionedCollIDs,
      invalidateCache)
    _schemaCacheInvalidationService.start()
  }

  def schemaCacheInvalidationService = Option(_schemaCacheInvalidationService)

  @volatile private var gossipAddresses: Array[HostDest] = Array.empty[HostDest]

  val MaxGossipBytes = 65536
  val gossipService =
    new GossipAdaptor(
      PingSignal,
      GossipSignal,
      bus,
      Clock,
      MaxGossipBytes,
      gossipAddresses.toIndexedSeq,
      stats)

  gossipService.start()

  val buildVersionService = new BuildVersion(
    Build.identifier,
    VersionGossipSignal,
    bus.hostID,
    gossipService)

  Timer.Global.scheduleRepeatedly(
    CassandraService.GossipAddressesUpdateInterval,
    gossipService.isRunning) {
    // Only gossip among members of the tribe.
    (localID, membership.localReplica) match {
      case (None, _) => ()
      case (_, None) => ()
      case (Some(_), Some(local)) =>
        if (computeReplicas contains local) {
          gossipAddresses = computeHosts map { id => HostDest(Right(id)) } toArray
        } else {
          // XXX: Log replicas currently always contain
          // data. Should that change in the future, this may need
          // updating.
          gossipAddresses = dataHosts map { id => HostDest(Right(id)) } toArray
        }
    }
  }

  val readClockDelayService = new ReadClockDelay(
    bus.hostID,
    ReadClockDelaySignal,
    gossipService,
    membership.isLocal,
    config.ackDelayPercentile,
    readClock,
    stats)(ackDelayClock.mark)

  val arrivalRates = new ArrivalRate(
    ArrivalRateSignal,
    config.limiterGossipInterval,
    arrivalsService,
    gossipService)

  arrivalRates.start()

  val topologyReconciler = {
    val streamer = new Streamer {
      // We could always use the latest storage.appliedTimestamp as a
      // conservative timestamp. This would likely force many syncs and apply
      // timestamp waits in the nodes we request the transfers from. Instead, we
      // pin the timestamp at the storage.appliedTimestamp observed after we
      // know the DataNode has fully transitioned to the specified (or later)
      // topology version.
      val vlat = new AtomicReference((0L, Timestamp.Epoch))

      def streamSegment(host: HostID, seg: Segment, version: Long): Future[Boolean] =
        txnPipeline.dataNode.fold(FutureFalse) { dn =>
          val svlat = vlat.get()
          val (lversion, lat) = svlat
          if (version > lversion) {
            dn.waitForTopologyVersion(version, DataNodeTopologyWaitPeriod) flatMap {
              caughtUp =>
                if (caughtUp) {
                  vlat.compareAndSet(svlat, (version, storage.appliedTimestamp))
                } else {
                  getLogger.warn(
                    s"DataNode is taking long to catch up to topology version $version")
                }
                streamSegment(host, seg, version)
            }
          } else {
            val tf = transferService.request(
              host,
              None,
              Vector(seg),
              lat,
              config.streamTimeout.bound)
            tf transformWith {
              case Success(segs) =>
                if (segs contains seg) {
                  dataNodeArbitrator.beforeTakingOwnership()
                  FutureTrue
                } else {
                  FutureFalse
                }
              case Failure(t) =>
                getLogger.warn(s"Failed to stream segment $seg from $host")
                logException(t)
                FutureFalse
            }
          }
        }
    }

    StatsRecorder.polling(10.seconds) {
      stats.set(
        "Topology.Current",
        ClusterStatus.ownership(
          membership.self,
          replicaName,
          topologies,
          { _.current }))

      stats.set(
        "Topology.Pending",
        ClusterStatus.ownership(
          membership.self,
          replicaName,
          topologies,
          { _.pending }))
    }

    new TopologyReconciler(
      topologies,
      membership,
      workerIDs,
      streamer,
      hostService,
      topologyMutex,
      stats)
  }

  val healthChecker = new HealthChecker(
    membership.self,
    membership,
    stats,
    txnMinPersistedTimestamp,
    dataReplicas,
    txnHintGlobalPersistedTimestamp)

  val executor: Future[Option[Executor]] = started flatMap { _ =>
    mkExecutor match {
      case Some(mk) => mk(this) map { Some(_) }
      case None     => Future.successful(None)
    }
  }

  val taskService = executor mapT { exec =>
    val svc = new TaskService(TaskSignal, bus, exec, streamService, stats)

    svc.start()
    svc
  }

  if (membership.isMember) {
    if (!membership.replicaNameMatches(replicaName)) {
      val knownReplica = membership.localReplica getOrElse "<unknown>"

      fatal(
        s"This node's configured data center name '$replicaName'" +
          "does not match the data center name known by the rest of the " +
          s"cluster '$knownReplica'.")
    } else {
      Await.result(startDaemon(true), Duration.Inf)
    }
  }

  private def startDaemon(blocking: Boolean): Future[Unit] = {
    val waitables = Seq(waitForWorkerID, waitUntilRegionHasWholeReplicaAndLog)
    val fut = Future.sequence(waitables) flatMap { _ =>
      if (blocking) {
        txnPipeline.onStart map { _ =>
          running = true
        }
      } else {
        running = true
        Future.unit
      }
    } recoverWith { case NonFatal(e) =>
      logException(e)
      Future.unit
    }

    startedPromise.completeWith(fut)

    fut.failed foreach {
      case ex: ExceptionInInitializerError =>
        fatal(s"Failed to start: ${ex.getCause}")
      case ex =>
        fatal(s"Failed to start with an unknown exception: $ex", Some(ex))
    }

    fut
  }

  private def localRegionReplica(@unused replicaName: String): Boolean = true

  private def waitUntilRegionHasWholeReplicaAndLog = {
    def hasWholeReplicaAndLog = {
      val t = topologies.snapshot
      t.hasWholeReplicaAmong(t.replicaNames.toSet, membership.hosts) &&
      (t.logReplicaNames exists localRegionReplica)
    }

    val p = Promise[Unit]()

    ClusterService.subscribeWithLogging(membership, topologies) {
      if (p.isCompleted) {
        FutureFalse
      } else if (hasWholeReplicaAndLog) {
        p.setDone()
        FutureFalse
      } else if (!membership.isMember) {
        // Don't try to initialize topology until we see ourselves as a member
        // While not strictly necessary, it'll save us from trying to
        // (unsuccessfully) initialize topology based on an observed intermediate
        // old membership state.
        FutureTrue
      } else {
        initializeTopology flatMap { _ =>
          // Retry even after this, as it can be unsuccessful if we
          // acted on an older topology state
          FutureTrue
        }
      }
    }

    p.future
  }

  // If there's no data+log replica in this cluster, but there's
  // exactly one replica, mark it data+log. This ensures that the
  // first replica in every cluster starts out as a data+log replica.
  private def initializeTopology: Future[Unit] = {
    val t = topologies.snapshot
    val hasLogReplicaInRegion = t.logReplicaNames exists localRegionReplica

    if (hasLogReplicaInRegion) {
      Future.unit
    } else {
      membership.replicaNames.toSeq match {
        case rn :: Nil =>
          val dataReplicas = t.replicaNames
          val logReplicas = t.logReplicaNames
          val newDataReplicas = dataReplicas :+ rn
          val newLogReplicas = logReplicas :+ rn
          try {
            topologies.setReplicas(
              newDataReplicas.toVector,
              newLogReplicas.toVector,
              dataReplicas.toVector,
              logReplicas.toVector)
          } catch {
            case e: IllegalArgumentException =>
              // Even though we waited until membership Raft has shown this node as a
              // member,
              // we might still be operating on a stale topology state (as the two
              // Raft logs
              // evolve independently.)
              logException(e)
              getLogger.warn(s"Attempted to initialize topology too early", e)
              Future.unit
          }
        case _ =>
          // Most likely, this node is bootstrapping and hasn't gotten the current
          // membership and/or topology state yet, so it observes it as empty. This
          // should work itself out as soon as it receives the current topology
          // state.
          getLogger.debug(
            s"Cannot select a single replica to automatically configure. " +
              s"Currently known replicas in cluster: ${membership.replicaNames}.")
          Future.unit
      }
    }
  }

  private def waitForWorkerID: Future[Unit] =
    workerIDs.subscribeWithLogging {
      Future.successful(workerIDs.workerID == UnassignedWorkerID)
    } map { _ =>
      if (workerIDs.workerID == UnavailableWorkerID) {
        new IllegalStateException("No free worker IDs are available.")
      }
    }

  private[this] val nodeRemoval =
    new NodeRemoval(membership, topologies, hostService)

  def stop(): Unit = {
    synchronized { if (isRunning) running = false }

    executor foreach { _ foreach { _.stop(graceful = false) } }
    nodeRemoval.stop()
    repartitioner.stop()
    logTopology.stop()
    ringAvailabilityService.stop()
    topologies.stop()
    workerIDs.stop()
    membership.stop()
    gossipService.stop()
    buildVersionService.stop()
    readClockDelayService.close()
    arrivalRates.stop()

    bus.stop()

    schemaCacheInvalidationService foreach { _.stop() }
    streamService.close()
    storageService.close()
    validationService.close()
    transferService.close()
    repairService.close()
    barService.close()
    topologyReconciler.close()
    storage.close()
    taskService foreach { _ foreach { _.stop() } }
  }

  def workerID = {
    workerIDs.workerID match {
      case AssignedWorkerID(wid) =>
        wid
      case UnassignedWorkerID =>
        throw new IllegalStateException("Worker ID not available yet.")
      case UnavailableWorkerID =>
        throw new IllegalStateException("No free worker IDs are available.")
    }
  }

  /* Backup And Restore Services */
  val barService = new BarService(
    RestoreSignal,
    BackupSignal,
    bus,
    storage,
    tmpDirectory,
    config.transferChunkSize,
    config.enableSnapshotTransferDigests,
    failureService,
    membership,
    partitioner.partitioner,
    recvLimiter,
    stats,
    storagePath,
    backupDir,
    workerID
  )

  val ringAvailabilityService = new RingAvailabilityService(
    failureDetector = failureDetector,
    membership = membership,
    partitioner = partitioner.partitioner,
    stats = stats,
    topologies = topologies,
    flagsProperties = () => hostProperties(),
    flagsClient = flagsClient
  )

  started foreach { _ =>
    ringAvailabilityService.start()
  }

  @volatile private[this] var cachedHostFlags =
    null.asInstanceOf[CachedResult[Option[HostFlags]]]

  def cachedHostFlags(): Future[Option[HostFlags]] =
    if (cachedHostFlags ne null) {
      cachedHostFlags.get
    } else {
      localID match {
        case None =>
          flagsClient.getUncached[HostID, HostProperties, HostFlags](
            hostProperties())
        case Some(_) =>
          cachedHostFlags = flagsClient.getCached[HostID, HostProperties, HostFlags](
            hostProperties())
          cachedHostFlags.get
      }
    }

  def hostProperties(): HostProperties = {
    localID match {
      case None =>
        // We have no identity, and therefore no flags. Make something up.
        HostProperties(HostID.NullID, Map.empty)
      case Some(id) =>
        HostProperties(
          id,
          Map(
            Properties.HostID -> id.toString,
            Properties.Environment -> config.environment,
            Properties.RegionGroup -> config.regionGroup,
            Properties.ReplicaName -> replicaName,
            Properties.ClusterName -> bus.clusterID
          )
        )
    }
  }

  /** Return the decorated properties for a given account. */
  def accountProperties(
    acct: AccountID,
    base: Map[String, FFValue]): AccountProperties = {

    val namespaced = base.map { case (k, v) =>
      s"${Properties.AccountPrefix}.$k" -> v
    }
    var props = namespaced

    // add account ID to props
    props += (Properties.AccountID -> acct.toLong)

    // If we have an identity/real host context, add in env-specific props
    localID.foreach { _ =>
      props += (Properties.RegionGroup -> config.regionGroup)
    }

    AccountProperties(acct, props)
  }

  def status(announcements: Map[HostID, Timestamp]) =
    ClusterStatus.status(
      membership,
      workerIDs,
      topologies,
      failureDetector,
      announcements)

  def hostVersions =
    NodeVersion.versions(buildVersionService.hostVersions, membership)

  def logLeaders: Future[Set[HostID]] =
    txnPipeline.logLeaders

  def compressionRatio(cf: String) = storage.compressionRatio(cf)

  // Schema config and topology

  /** Returns the set of host IDs of current cluster members.
    */
  def hosts: Set[HostID] = membership.hosts

  def runningHosts =
    membership.hosts &~ membership.leavingHosts &~ membership.leftHosts

  /** Returns the set of host IDs of recently left cluster members.
    */
  def leftHosts: Set[HostID] = membership.leftHosts

  /** Returns the set of departed hosts remaining in the graveyard, and
    * the timestamp of their departure.
    */
  def graveyard: Map[HostID, Timestamp] = membership.graveyard

  /** Returns the set of host IDs of currently leaving cluster members.
    */
  def leavingHosts: Set[HostID] = membership.leavingHosts

  def hostsInReplica(replica: String): Set[HostID] =
    membership.hostsInReplica(replica)

  def localID: Option[HostID] =
    if (membership.isMember) Some(membership.self) else None

  def initCluster(): Future[Boolean] = {
    val init =
      for {
        _ <- membership.init()
        f <- initializeTopology
      } yield f

    if (running || membership.isMember) {
      FutureFalse
    } else {
      init flatMap { _ =>
        synchronized {
          startDaemon(false) map { _ =>
            true
          }
        }
      }
    }
  }

  def joinCluster(seed: String): Future[Boolean] =
    startDaemonAfter(membership.join(seedHostAddress(seed)))

  private def startDaemonAfter(f: => Future[Unit]): Future[Boolean] =
    if (running || membership.isMember) {
      FutureFalse
    } else {
      doAndThenStartDaemon(f) map { _ =>
        true
      }
    }

  private def doAndThenStartDaemon(f: => Future[Unit]): Future[Unit] =
    f flatMap { _ =>
      synchronized { startDaemon(true) }
    }

  private def seedHostAddress(seed: String) =
    // FIXME: check if seedHostAddress is live?
    seed.split(":") match {
      case Array(host, portStr) => HostAddress(host, portStr.toInt)
      case _                    => HostAddress(seed, bus.hostAddress.port)
    }

  /** Removes a node from the cluster.
    *
    * @param hostID the host id of the node to be removed
    * @param force when true, consider the host permanently gone
    * @param allowUnclean when true, skip checking cluster storage cleanliness
    * @return a Future that represents the completion of the removal operation
    * @throws IllegalArgumentException if the operation can not be completed for the node
    */
  def removeNode(
    hostID: HostID,
    force: Boolean,
    allowUnclean: Boolean): Future[Unit] =
    if (allowUnclean) {
      nodeRemoval.removeNode(hostID, force)
    } else {
      storageService.allClean(None, 5.minutes.bound, Set(hostID)) flatMap { clean =>
        if (clean) {
          nodeRemoval.removeNode(hostID, force)
        } else {
          Future.failed(
            new IllegalStateException(
              "Storage is not clean. Run cleanup before attempting removal."))
        }
      }
    }

  def updateReplication(
    activeReplicaNames: Set[String],
    activeLogReplicaNames: Set[String],
    prevActiveReplicaNames: Set[String],
    prevActiveLogReplicaNames: Set[String]): Future[Unit] =
    topologies.setReplicas(
      activeReplicaNames.toVector,
      activeLogReplicaNames.toVector,
      prevActiveReplicaNames.toVector,
      prevActiveLogReplicaNames.toVector)

  /** Returns the set of replicas announced by current cluster
    * members. Includes compute-only, data, and data-and-log replicas.
    */
  def allReplicas: Set[String] =
    membership.replicaNames

  /** Returns the set of replicas within the current cluster
    * membership which do not replicate data or participate in the
    * log.
    */
  def computeReplicas: Set[String] =
    // Future proofing: currently, log always contains data, but that
    // may not always be so.
    allReplicas -- dataReplicas -- logReplicas

  def computeHosts: Set[HostID] = {
    val compute = computeReplicas flatMap { hostsInReplica(_) }
    compute & runningHosts
  }

  /** Returns the set of data replicas within the current cluster
    * membership.
    */
  def dataReplicas: Set[String] =
    topologies.snapshot.replicaNames.toSet

  def dataHosts: Set[HostID] = {
    val data = dataReplicas flatMap { hostsInReplica(_) }
    data & runningHosts
  }

  /** Returns the set of log replicas within the current cluster
    * membership.
    */
  def logReplicas: Set[String] =
    topologies.snapshot.logReplicaNames.toSet

  def movementStatus = topologies.pendingStatus

  def balanceReplica(replicaName: String) =
    storageService.allClean(None, 5.minutes.bound) flatMap { clean =>
      if (clean) {
        val optFut = for {
          rt              <- topologies.snapshot.getTopology(replicaName)
          balancedPending <- topologies.balancedTopology(replicaName)
        } yield {
          topologies.proposeTopology(
            replicaName,
            balancedPending,
            rt.pending,
            Vector.empty)
        }

        optFut getOrElse {
          throw new IllegalArgumentException(s"Replica $replicaName does not exist.")
        }
      } else {
        Future.failed(
          new IllegalStateException(
            "Storage is not clean. Run cleanup before attempting balance."))
      }
    }

  // Utilities for observing and altering log topology.
  def getLogSegment(hostID: HostID): Option[Int] =
    txnPipeline.ctx.logSegmentFor(hostID) map { _.toInt }

  // Orders the log segment `segmentID` to be closed.
  // Returns the epoch by which the segment will be closed.
  def closeLogSegment(segmentID: SegmentID): Epoch = {
    val version = logTopology.logNodeProvider.getLogNodeInfo.version
    logTopology.logNodeProvider.closeSegment(segmentID, version)
  }

  // Orders the log segment `segmentID` to be moved from `from` to `to`.
  // The nodes must be in the same replica.
  def moveLogSegment(segmentID: SegmentID, from: HostID, to: HostID) = {
    val version = logTopology.logNodeProvider.getLogNodeInfo.version
    logTopology.logNodeProvider.moveSegment(segmentID, from, to, version)
  }

  def readClock = _readClock

  /** *** DANGER ***
    */
  def skipTransactions()(implicit ctl: AdminControl) =
    txnPipeline.skipTransactions()

  def unlockStorage() =
    storage.unlock()
}
