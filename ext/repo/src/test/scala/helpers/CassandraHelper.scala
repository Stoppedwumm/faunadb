package fauna.repo.test

import fauna.atoms._
import fauna.codex.cbor._
import fauna.flags.{
  AccountProperties,
  EnableIndexBuildPriorities,
  Feature,
  Flags,
  FlagsCompanion,
  HostProperties,
  Properties,
  Service => FFService,
  Value => FFValue
}
import fauna.lang.Timestamp
import fauna.logging.test._
import fauna.net.{ ArrivalRateService, LimiterStats }
import fauna.net.security.{ Encryption, JWK, JWKProvider }
import fauna.repo._
import fauna.repo.cache.{ Cache, SchemaCache }
import fauna.repo.cassandra._
import fauna.repo.query.Query
import fauna.repo.service.rateLimits._
import fauna.scheduler._
import fauna.snowflake._
import fauna.stats.{ DelegatingStatsRecorder, StatsRecorder }
import fauna.storage._
import fauna.storage.cassandra._
import java.io.{ File, IOException }
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

object CassandraHelper {

  // For testing cluster health response

  @volatile private[this] var iAmHealthy = true

  def isHealthy = iAmHealthy

  var DisableSchemaCacheInvalidationService = false

  def markHealthy() = iAmHealthy = true

  def markUnhealthy() = iAmHealthy = false

  /** Returns a valid [[CassandraConfig]] object
    */
  def testConfig(
    rootPath: String = "test",
    busPort: Int = 8080,
    busSSLPort: Int = 80): CassandraConfig =
    CassandraConfig(
      cluster = "test",
      regionGroup = "local",
      environment = "dev",
      rootPath = rootPath,
      busPort = busPort,
      busSSLPort = busSSLPort,
      busConnectTimeout = 5.seconds,
      busHandshakeTimeout = 5.seconds,
      busIdleTimeout = 60.seconds,
      busKeepaliveInterval = 10.seconds,
      busKeepaliveTimeout = 60.seconds,
      connectionsPerHost = 1,
      busMaxMessageSize = Int.MaxValue,
      busMaxPendingMessages = 1024,
      broadcastAddress = "127.0.0.1",
      listenAddress = "127.0.0.1",
      keyCacheSizeMB = 32,
      enableQoS = true,
      processors = 4,
      concurrentReads = 128,
      concurrentTxnReads = 16,
      concurrentWrites = 128,
      concurrentCompactors = 8,
      compactionThroughputMBPerSec = 0,
      streamTimeout = Duration(30, SECONDS),
      indexSummaryInterval = 1.hour,
      indexSummarySizeMB = 1,
      memtableSizeMB = 32,
      memtableFlushWriters = 1,
      commitLogSizeMB = 8,
      concurrentApplyingTransactions = 12,
      roundTripTime = 50.millis,
      syncPeriod = 1000.millis,
      syncOnShutdown = false,
      backupReadRatio = 0.1,
      minBackupReadDelay = 10.millis,
      readTimeout = 5.seconds,
      newReadTimeout = 5.seconds,
      repairHashDepth = 8,
      repairSyncParallelism = 16,
      ackDelayPercentile = 99.99,
      txnLogBackupPath = None,
      transactionLogBufferSize = 512 * 1024,
      backupDir = rootPath + File.pathSeparator + "snapshots",
      dualWriteIndexCFs = false,
      index2CFValidationRatio = 0.0,
      consensusStallRestartPeriod = Duration.Inf,
      minWorkerID = 0,
      maxWorkerID = 1023,
      neighborReplica = None,
      enableDataTransferDigests = true,
      enableSnapshotTransferDigests = true,
      streamMaxOpenStreams = 10_000,
      openFileReserve = 1.0,
      unleveledSSTableLimit = Int.MaxValue,
      transferChunkSize = Int.MaxValue,
      recvMaxBytesPerSecond = Double.MaxValue,
      recvMaxBurstSeconds = Double.MaxValue,
      streamPlanThreads = 1,
      latWaitThreshold = 1.second,
      maxOCCReadsPerSecond = Int.MaxValue,
      occReadsBackoffThreshold = Int.MaxValue,
      limiterGossipInterval = 1.second,
      sstableDiskAccessMode = "auto"
    )

  // Scope is used to coordinate with CassandraHelper running other
  // processes.
  private var _scope: Option[String] = None

  object stats extends DelegatingStatsRecorder {
    @volatile private var stats: StatsRecorder = StatsRecorder.Null

    def delegates = Seq(stats)

    def set(s: StatsRecorder) = stats = s

    def reset() = stats = StatsRecorder.Null
  }

  def withStats[T](s: StatsRecorder)(action: => T): T =
    try {
      stats.set(s)
      action
    } finally stats.reset()

  object ArrivalRates extends ArrivalRateService {
    def poll() = Map.empty
    def register(arrivals: Map[AccountID, LimiterStats]) = ()
  }

  // Enable user-defined schema APIs by default.
  val AccountFlagDefaults: Map[Feature[AccountID, FFValue], FFValue] =
    Map.empty

  val HostFlagDefaults: Map[Feature[HostID, FFValue], FFValue] =
    Map(EnableIndexBuildPriorities -> true)

  object ffService extends FFService {

    private[this] var accountFlags: Map[String, FFValue] = Map.empty
    private[this] var hostFlags: Map[String, FFValue] = Map.empty
    @volatile private[this] var _version = 0
    reset()

    def version = _version

    def getAllUncached[ID, P <: Properties[ID], F <: Flags[ID]](props: Vector[P])(
      implicit propCodec: CBOR.Encoder[Vector[P]],
      flagsCodec: CBOR.Decoder[Vector[F]],
      companion: FlagsCompanion[ID, F]): Future[Vector[F]] =
      synchronized {
        Future.successful {
          props map {
            case AccountProperties(id, _) => companion(id, accountFlags)
            case HostProperties(id, _)    => companion(id, hostFlags)
          }
        }
      }

    def setHost(ff: Feature[HostID, _], value: FFValue): this.type =
      synchronized {
        hostFlags += ff.key -> value
        _version += 1
        this
      }

    def setAccount(ff: Feature[AccountID, _], value: FFValue): this.type =
      synchronized {
        accountFlags += ff.key -> value
        _version += 1
        this
      }

    def reset(): Unit =
      synchronized {
        hostFlags = HostFlagDefaults.map { case (f, value) => f.key -> value }
        accountFlags = AccountFlagDefaults.map { case (f, value) => f.key -> value }
        _version += 1
      }

    def withAccount[T](ff: Feature[AccountID, _], value: FFValue)(f: => T): T = {
      setAccount(ff, value)
      try {
        f
      } finally {
        reset()
      }
    }
  }

  val QueryTimeout = 30.seconds
  val ReadTimeout = 5.seconds
  val RangeTimeout = 30.seconds
  val WriteTimeout = 30.seconds

  def startServer(scope: String): Unit = synchronized {
    _scope match {
      case None =>
        _scope = Some(scope)
      case Some(prev) =>
        if (prev != scope)
          sys.error(
            s"C* was already started with scope $prev, but $scope was requested.")
    }

    ffService.reset()
    if (CassandraService.instanceOpt.fold(false)(_.isRunning)) return

    println(s"Starting C* services for the first time. (Scope: $scope)")

    val busPort = findFreePort()
    val defaultRoot = Option(System.getenv("FAUNA_TEST_ROOT"))
    val baseDir = System.getProperty("os.name") match {
      case "Linux"    => defaultRoot getOrElse "/dev/shm"
      case "Mac OS X" => defaultRoot getOrElse "/Volumes"
      case x if x.startsWith("Windows") =>
        defaultRoot getOrElse System.getProperty("java.io.tmpdir")
      case _ => defaultRoot getOrElse System.getProperty("java.io.tmpdir")
    }
    val root = Paths.get(baseDir, "fauna-api-test", scope)

    if (Files.exists(root)) {
      Files.walkFileTree(
        root,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes) = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException) = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }

    Files.createDirectories(root)

    val config =
      testConfig(rootPath = root.toString, busPort = busPort, busSSLPort = busPort)

    val logfile = s"$root/cassandra.log"
    new File(logfile).getParentFile.mkdirs()

    val tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir"))

    enableFileLogging(logfile)

    CassandraService.initializeAndStart(
      "DC",
      config,
      Encryption.Default,
      stats,
      ConstantPriorityProvider,
      tmpDirectory,
      config.txnLogBackupPath,
      Paths.get(config.rootPath),
      Paths.get(config.backupDir),
      None,
      ffService,
      ArrivalRates
    )
    Await.ready(CassandraService.instance.initCluster(), Duration.Inf)

    var txStarted = false
    while (!txStarted) {
      try {
        CassandraService.instance.txnPipeline
        txStarted = true
      } catch {
        case _: UninitializedException => Thread.sleep(200)
      }
    }
  }

  def context(
    scope: String,
    schema: Seq[ColumnFamilySchema] = Tables.FullSchema,
    backupRequests: Boolean = false,
    invalidateOpt: Option[(ScopeID, DocID, Timestamp) => Query[Unit]] = None,
    jwkProvider: Option[JWKProvider] = None,
    internalJWK: Option[(JWK, String)] = None,
    fqlxMaxStackFrames: Int = RepoContext.DefaultFQLXMaxStackFrames,
    mvtProvider: MVTProvider = MVTProvider.Default
  ) = {
    startServer(scope)

    val service = CassandraService.instance

    // Keep the refresh rate long so tests will run into
    // issues if they rely on a stale cache, but keep it
    // less than the MVTOffset. See Database.addMVTPin.
    val caches = CacheContext(
      Cache(name = "Schema", maxSizeMB = 2, refreshRate = 10.minutes),
      SchemaCache(maxScopes = 1024, maxSchemaSizeMB = 2),
      Cache(name = "Keys", maxSizeMB = 2, refreshRate = 10.minutes)
    )

    val workerID = () => 0

    val ctx = RepoContext(
      service,
      Storage(schema),
      service.readClock,
      PriorityGroup.Default,
      stats,
      PermissiveAccountOpsRateLimitsService,
      true, // retry-on-contention
      QueryTimeout,
      ReadTimeout,
      ReadTimeout,
      RangeTimeout,
      WriteTimeout,
      new IDSource(workerID), // id source,
      caches,
      Query.repo map { _ => iAmHealthy },
      Query.repo map { _ => iAmHealthy },
      Query.repo map { _ => iAmHealthy },
      0.1,
      1.minute,
      1.day,
      16 * 1024 * 1024,
      Int.MaxValue,
      true, // enforceIndexEntriesSizeLimit
      2.5e5.toInt,
      Long.MaxValue,
      32_000,
      taskReprioritizerLimit = 100,
      backupReads = backupRequests,
      internalJWK = internalJWK,
      jwkProvider = jwkProvider,
      maxPerQueryParallelism = 1024,
      queryMaxConcurrentReads = 1024,
      queryMaxWidth = Int.MaxValue,
      queryMaxGlobalWidth = Int.MaxValue,
      queryEvalStepsPerYield = 1024,
      queryAccumulatePageWidth = 1024,
      fqlxMaxStackFrames = fqlxMaxStackFrames,
      mvtProvider = mvtProvider
    )

    caches.setRepo(ctx)
    ctx.keyspace.storage.removeAll()
    ctx.keyspace.storage.init()

    initInvalidationService(ctx, invalidateOpt)

    ctx
  }

  def initInvalidationService(
    ctx: RepoContext,
    invalidateOpt: Option[(ScopeID, DocID, Timestamp) => Query[Unit]]): Unit = {

    @annotation.nowarn("cat=unused-params")
    def invalidate(scope: ScopeID, doc: DocID, ts: Timestamp): Query[Unit] =
      if (DisableSchemaCacheInvalidationService) {
        Query.unit
      } else {
        Query.deferred {
          invalidateCaches(ctx)
        }
      }

    CassandraService.instance.initSchemaCacheInvalidationService(
      ctx,
      invalidateOpt getOrElse invalidate)
  }

  def setCacheContext(ctx: RepoContext) =
    ctx.cacheContext.setRepo(ctx)

  def invalidateCaches(ctx: RepoContext) =
    ctx.cacheContext.invalidateAll()
}
