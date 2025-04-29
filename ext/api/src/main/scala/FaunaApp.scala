package fauna.api

import fauna.atoms._
import fauna.config._
import fauna.exec.{ FaunaExecutionContext, Timer }
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.exec.Timer.Global
import fauna.flags.{ FileService => FFFileService, Service => FFService }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model.{ Cache => ModelCache, Collection, Prioritizer, SampleData }
import fauna.model.gc.{ MVTProvider => GCProvider }
import fauna.model.stream.StreamContext
import fauna.model.tasks.{ TaskExecutor, TaskReprioritizer, TaskRouter }
import fauna.net.http.HttpServer
import fauna.net.statsd.DogStatsDClient
import fauna.net.ArrivalRateService
import fauna.repo._
import fauna.repo.cache.Cache
import fauna.repo.cache.SchemaCache
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.service.rateLimits._
import fauna.scheduler.{ ConstantPriorityProvider, PriorityGroup }
import fauna.snowflake._
import fauna.stats._
import fauna.storage.{ Cassandra, Storage, Tables }
import fauna.storage.cassandra.GCGrace
import fauna.trace._
import fauna.trace.datadog.{ Client => DDClient }
import fauna.util.BCrypt
import io.netty.buffer.PooledByteBufAllocator
import io.netty.util.NettyRuntime
import java.io.{ File, PrintWriter, StringWriter }
import java.net.{ BindException, InetSocketAddress }
import java.nio.file.{ AccessDeniedException, Files, Path, Paths }
import java.util.concurrent.{ CopyOnWriteArraySet, TimeUnit, TimeoutException }
import org.apache.commons.cli.{
  CommandLine,
  DefaultParser,
  HelpFormatter,
  Option => _,
  Options
}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.Logger
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

abstract class FaunaApp(val name: String) {

  /**  Mutates the supplied  `Options` with any additional per-app command line options.
    */
  protected def setPerAppCLIOptions(o: Options) = o

  /**  configDefaults are per-app default values for certain configuration file parameters.
    */
  protected def configDefaults: Map[String, Any] = Map.empty[String, Any]

  /** emits how to use the FaunaApp on standard out.  By default, simply emits
    * definitions of the App's CLI options, but subclasses may override in order
    * to give additional info (such as commands for the Admin tool.)
    */
  protected def usage(): Unit = {
    val sw = new StringWriter
    (new HelpFormatter).printOptions(new PrintWriter(sw), 80, cliOptions(), 2, 2)
    System.err.println("usage: faunadb [OPTIONS]\n")
    System.err.println(sw.toString)
  }

  /** Emits a friendly welcome message.
    */
  protected def printBanner(): Unit = {
    val build = Build.identifier
    val title = s"Fauna $name $build"
    logAndPrint(title)
    System.err.println("=" * title.length)
    logAndPrint(s"Starting...")
    logOnly {
      val javaVendor = System.getProperty("java.vendor")
      val javaVersion = System.getProperty("java.vendor.version")
      s"Running on $javaVersion JVM from $javaVendor"
    }
  }

  /** emits the message to the log (if it has been initialized at this point).
    */
  protected final def logOnly(msg: => String): Unit =
    log foreach { _.info(msg) }

  /** emits the message both to standard out and the log (if the latter has been
    * initialized at this point).
    */
  protected def logAndPrint(msg: String): Unit = {
    logOnly(msg)
    System.err.println(msg)
  }

  /** emits the message to the log, and then throws a fatal exception.
    */
  protected def logThenDie(msg: String): Nothing = {
    log foreach { _.error(msg) }
    sys.error(msg)
  }

  /* lowest common denominator requirements for a FaunaApp. Any start hooks may
   * assume these have been initialized within `main` already. */

  private var _cli: CommandLine = null
  private var _config: CoreConfig = null
  private var _rootKeys: List[String] = null
  private var _stats: StatsRecorder = null

  protected final def cli = _cli
  protected final def config = _config
  protected final def rootKeys = _rootKeys
  protected final def stats = _stats

  /** log is optional; it will be initialized to the value returned by setupLogging().
    */
  protected final var log: Option[Logger] = None

  /** Set up loggers. Should return Some(rootLogger) or None for no logger.
    */
  protected def setupLogging(): Option[Logger] = None

  /** Cleanly stop all loggers, allowing them to flush before shutting down.
    */
  protected def stopLogging(): Unit = {
    val ctx = LoggerContext.getContext(false)
    val grace = config.shutdownGracePeriod
    ctx.stop(grace.length, grace.unit)
  }

  private val preinits = ListBuffer.empty[(CommandLine, CoreConfig) => Unit]
  private val starts = ListBuffer.empty[() => Unit]
  private val stops = ListBuffer.empty[() => Unit]

  /** preinit callbacks are invoked before initialization.  By this point, the
    * FaunaApp state may not have been initialized, with the exception of
    * the config file and command line arguments being parsed.  Hence,
    * those get passed explicitly as arguments to the callback.
    */
  protected final def preinit(f: (CommandLine, CoreConfig) => Unit): Unit = {
    preinits += f
  }

  /** start thunks are invoked first on startup.  By this point, all of cli,
    * config, stats, and rootKey should be initialized (e.g. non-null).
    */
  protected final def start(f: => Unit): Unit = {
    starts += (() => f)
  }

  /** stop thunks are invoked on shutdown.
    */
  protected final def stop(f: => Unit): Unit = {
    stops += (() => f)
  }

  preinit { case (cli, config) =>
    // CLI overrides
    Option(cli.getOptionValue("bind-address")) foreach { addr =>
      config.network_coordinator_http_address = addr
    }

    Option(cli.getOptionValue("port")) foreach { port =>
      val asInt = Try { port.toInt } match {
        case Failure(_) =>
          System.err.println("Invalid port.")
          System.exit(1); -1 // FIXME: this is rather unceremonious.
        case Success(i) => i
      }
      config.network_coordinator_http_port = asInt
    }
  }

  preinit { case (_, config) =>
    FaunaExecutionContext.setAvailableProcessors(config.runtime_processors)
    NettyRuntime.setAvailableProcessors(config.runtime_processors)
  }

  final def main(args: Array[String]) = {
    _cli = parseArgs(args)
    val cr = configFromArgs(_cli)

    cr.warnings foreach { w =>
      logAndPrint(w.toString)
    }

    _config = cr.toException match {
      case Some(e) => logThenDie(e.toString)
      case None    => cr.config
    }

    FaunaApp.registerRootExceptionHandlers()
    log = setupLogging()

    preinits foreach { _(_cli, _config) }

    // Welcome to Fauna!
    printBanner()
    logAndPrint(cr.loadingMsg)

    _rootKeys = setupRootKeys()

    _stats = setupStats()

    stats.incr("Process.Starts")

    starts foreach { _() }

    getReplicaNameMessage() foreach { logAndPrint(_) }
    println("FaunaDB is ready.")

    awaitShutdown {
      stops foreach { _() }
      stopLogging()
      stats.incr("Process.Stops")
    }
  }

  /** Builds a message to log/print the current state of replica_name.
    */
  private def getReplicaNameMessage(): Option[String] =
    Cassandra.getReplicaName map { replicaName =>
      s"Replica name: $replicaName"
    }

  private def configFromArgs(cli: CommandLine): ConfigResult[CoreConfig] = {
    FaunaApp.loadConfig(cli, configDefaults)
  }

  /** cliOptions are command-line options common to all Fauna applications.
    */
  protected def cliOptions() = {
    val options = new Options

    options.addOption("c", "conf", true, "Path to configuration file.")
    options.addOption(
      "b",
      "bind-address",
      true,
      "IP address to bind to. (default: 127.0.0.1)")
    options.addOption("p", "port", true, "Port to listen on. (default: 8443)")
    options.addOption("h", "help", false, "Print this message.")

    setPerAppCLIOptions(options)
  }

  /** Extracts an a.c.cli from the specified argv list.
    */
  private def parseArgs(args: Array[String]): CommandLine = {
    val cli = (new DefaultParser).parse(cliOptions(), args)

    if (cli.hasOption("help")) {
      usage()
      System.exit(0)
    }

    cli
  }

  private def setupRootKeys(): List[String] = {
    // XXX: This god-awful aberration temporarily allows
    // either (or both) hashed key fields to be defined during the
    // migration from auth_root_key_hash to auth_root_key_hashes.
    val hashed =
      (
        Option(config.auth_root_key_hashes),
        Option(config.auth_root_key_hash)) match {
        case (Some(a), Some(b)) => Some(a :+ b)
        case (a, None)          => a
        case (None, b)          => b map { List(_) }
      }

    hashed
      .orElse(
        Option(config.auth_root_key) map { clear =>
          List(BCrypt.hash(clear))
        }
      )
      .getOrElse {
        logThenDie("Error: auth_root_key or auth_root_key_hash must be set.")
      }
  }

  private def setupStats(): StatsRecorder = {
    if (config.dogstatsd_host ne null) {
      logAndPrint(
        s"Stats endpoint: ${config.dogstatsd_host}:${config.dogstatsd_port}")
    } else if (config.stats_csv_path ne null) {
      logAndPrint(s"Stats csv path: ${config.stats_csv_path}")
    }

    val stats =
      Option(config.dogstatsd_host) orElse Option(config.stats_host) flatMap {
        host =>
          Option.when(config.dogstatsd_port > 0) {
            val dogstatsd = DogStatsDClient(host, config.dogstatsd_port)
            dogstatsd.start()
            new BufferedRecorder(
              config.stats_poll_seconds.seconds,
              config.stats_timings_per_second,
              dogstatsd)
          }
      } orElse {
        Option(config.stats_csv_path) map { new CSVStats(_) }
      } getOrElse {
        StatsRecorder.Null
      }

    StatsRecorder.polling(config.stats_poll_seconds.seconds) {
      FaunaExecutionContext.report((name, value) => stats.set(name, value.toDouble))

      val m = PooledByteBufAllocator.DEFAULT.metric
      stats.set("Allocation.Caches", m.numThreadLocalCaches.toDouble)
      stats.set("Allocation.HeapUsed", m.usedHeapMemory.toDouble)
      stats.set("Allocation.NonHeapUsed", m.usedDirectMemory.toDouble)
      stats.set(
        "Allocation.Direct.Active",
        m.directArenas.asScala.map { _.numActiveAllocations }.sum.toDouble)
      stats.set(
        "Allocation.Heap.Active",
        m.heapArenas.asScala.map { _.numActiveAllocations }.sum.toDouble)
    }

    MetricsReporter(stats).start(config.stats_poll_seconds, TimeUnit.SECONDS)

    stats
  }

  private def awaitShutdown(onShutdown: => Unit): Unit = {
    val shutdownP = Promise[Unit]()
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        logAndPrint(s"FaunaDB is shutting down after ${config.shutdownGracePeriod}.")
        val stopF = CassandraService.stop(config.shutdownGracePeriod) map { _ =>
          logAndPrint("FaunaDB Service has been shut down.")
          onShutdown
          logAndPrint("Shutdown complete.")
          shutdownP.success(())
        }
        Await.ready(stopF, Duration.Inf)
      }
    })

    Await.ready(shutdownP.future, Duration.Inf)
  }
}

object FaunaApp {

  val DefaultConfigFilePaths = Seq(
    "./faunadb.yml",
    "./etc/faunadb.yml",
    "/usr/etc/faunadb.yml",
    "/etc/faunadb.yml")

  private def registerRootExceptionHandlers() = {
    // Exit if the main thread throws an exception.
    Thread.currentThread.setUncaughtExceptionHandler(
      new Thread.UncaughtExceptionHandler {
        def uncaughtException(t: Thread, e: Throwable): Unit = {
          // We don't necessarily have logging configured yet. Stderr is the only
          // safe place.
          System.err.println("Uncaught Exception on Main Thread. Terminating: ")
          e.printStackTrace(System.err)
          System.exit(1)
        }
      })
  }

  private def loadConfig(cli: CommandLine, appDefaults: Map[String, Any]) = {
    val provided =
      Option(cli.getOptionValue('c')) orElse Option(System.getenv("FAUNADB_CONFIG"))
    val path = (provided map { Paths.get(_) }) orElse {
      (DefaultConfigFilePaths map { Paths.get(_) } find { Files.isRegularFile(_) })
    }

    CoreConfig.load(path, appDefaults)
  }
}

/**  APIServer contains the pieces necessary to initialise a Fauna application that
  * exposes an API endpoint to serve traffic.
  */
trait APIServer { self: FaunaApp =>

  @volatile protected var apiInst: Option[API] = None

  @volatile protected var apiS: Option[HttpServer] = None

  protected var adminS: Option[HttpServer] = None

  protected val tracedSecrets = new CopyOnWriteArraySet[String]

  preinit { case (_, config) =>
    setupTempDir(config.tempPath, config.storagePath)
  }

  start {
    setupTracing(config)

    // Quick sanity check for a dangerous misconfiguration.
    checkCachePeriodAndMVTOffset(config)

    val storage = setupStorage(config)
    val flags = setupFlags(config, stats)
    val limiters = setupRateLimits(config, stats)

    val repoF = CassandraService.started map { cs =>
      setupRepo(cs, storage, limiters, stats, config)
    }

    setupCassandraService(repoF, flags, limiters, config)

    val apiF = repoF map { repo =>
      val streamCtx = setupStreamContext(repo, stats, config)
      setupAPI(repo, streamCtx)
    }

    val admin = {
      setupAdmin(storage, repoF, config, stats, rootKeys, Build.identifier)
    }

    // Endpoint setup

    adminS = Some(adminServer(config, admin))

    // API endpoints come up once the service is ready
    apiF foreach { api =>
      // populate the sample data
      initSampleData(api)

      apiInst = Some(api)
      apiS = Some(apiServer(config, api))

      println("FaunaDB is ready.")
    }

    // APIServer will invoke CassandraService.initializeAndRun in its start;
    // this happens before this start block. If the node is a member of a
    // cluster, that method blocks until CassandraService fully starts so when
    // it returns, the CassandraService.started will be completed. As a
    // corollary to that, if we reach this point in the execution, and
    // CassandraService didn't start, it means the node is not member of a
    // cluster yet. We also suppress this message if the node becomes a member
    // within 2 seconds, typically if some external automation is driving its
    // init/join.
    Global.scheduleTimeout(2.second) {
      if (!apiF.isCompleted) {
        logAndPrint(
          "This node is not part of the cluster yet. You should use faunadb-admin to " +
            "have it either join an existing cluster or initialize a new one.")
      }
    }
  }

  stop {
    apiInst foreach { _.stop() }
    apiS foreach { _.stop() }
    adminS foreach { _.stop() }
  }

  private def setupTempDir(tmpPath: String, storagePath: String) = {
    val storageP = Paths.get(storagePath).toAbsolutePath
    val tmpP = Paths.get(tmpPath)

    // Only clean temp if it is stored under the base fauna directory
    if (tmpP.startsWith(storageP)) {
      tmpP.deleteRecursively()
    }
    val f = new File(tmpPath)
    if (
      (f.exists && !(f.canRead && f.canWrite && f.canExecute)) ||
      (!f.exists && !f.mkdirs)
    ) {
      throw new AccessDeniedException(
        f.getAbsolutePath,
        null,
        "Insufficient permissions.")
    }
    System.setProperty("java.io.tmpdir", tmpPath)
  }

  protected def apiServer(config: CoreConfig, api: API): HttpServer = {
    val addr = config.networkCoordinatorHttpAddress
    val port = config.network_coordinator_http_port
    logAndPrint(s"API endpoint: $addr:$port")

    val sock = new InetSocketAddress(addr, port)
    val server = api.server(
      sock,
      config.httpReadTimeout,
      config.httpKeepAliveTimeout,
      config.httpSSL,
      config.network_http_max_initial_line_length,
      config.network_http_max_header_size,
      config.network_http_max_chunk_size,
      config.http_max_concurrent_streams
    )

    try {
      server.start()
    } catch {
      case _: BindException =>
        logThenDie(
          s"Unable to bind to $addr:$port. Verify network_coordinator_http_address is set to a valid local network address.")
    }
    server
  }

  // Export tasks must be able to pin collection MVT between the MVT
  // offset and snapshot time with enough time to spare for all nodes
  // to refresh the cache and see the MVT pin before MVT has an
  // opportunity to pass the pinned time.
  private def checkCachePeriodAndMVTOffset(config: CoreConfig) = {
    val mvtOffset = Collection.MVTOffset
    val refreshRate = config.cache_schema_ttl_seconds.seconds
    if (refreshRate > mvtOffset) {
      logThenDie(
        s"Configured cache refresh rate ${refreshRate} is too infrequent: it must be less than the MVT offset ${mvtOffset}")
    }
  }

  private def setupStorage(config: CoreConfig): Storage = {
    val schema =
      if (config.index2cf_disable_extended_gc_grace) {
        Tables.FullSchema map { cfs =>
          cfs.name match {
            case Tables.SortedIndex.CFName2 | Tables.HistoricalIndex.CFName2 =>
              cfs.withGCGrace(GCGrace.Default)
            case _ => cfs
          }
        }
      } else {
        Tables.FullSchema
      }

    Storage(schema)
  }

  private def setupRepo(
    cs: CassandraService,
    storage: Storage,
    limiters: AccountOpsRateLimitsService,
    stats: StatsRecorder,
    config: CoreConfig): RepoContext = {

    val caches = CacheContext(
      schema = Cache(
        name = "Schema",
        maxSizeMB = config.cache_schema_size_mb,
        refreshRate = config.cache_schema_ttl_seconds.seconds
      ),
      schema2 = SchemaCache(
        maxSchemaSizeMB = config.cache_schema_size_mb,
        refreshRate = config.cache_schema_ttl_seconds.seconds
      ),
      keys = Cache(
        name = "Keys",
        maxSizeMB = config.cache_key_size_mb,
        refreshRate = config.cache_key_ttl_seconds.seconds
      )
    )

    StatsRecorder.polling(config.statsPollSeconds) {
      caches.reportStats(stats)
      BCrypt.report(stats)
    }

    val workerID = () => CassandraService.instance.workerID
    val localCheckQ =
      Query.deferred(CassandraService.instance.healthChecker) flatMap {
        _.isLocalHealthy
      }
    val clusterCheckQ =
      Query.deferred(CassandraService.instance.healthChecker) flatMap {
        _.isClusterHealthy
      }
    val storageCheckQ =
      Query.deferred(CassandraService.instance.healthChecker) flatMap {
        _.isStorageHealthy
      }

    val ctx = RepoContext(
      cs,
      storage,
      cs.readClock,
      PriorityGroup.Default,
      stats,
      limiters,
      false, // retry-on-contention
      RepoContext.DefaultQueryTimeout,
      config.network_read_timeout_ms.millis,
      config.storage_new_read_timeout_ms.millis,
      config.network_range_timeout_ms.millis,
      config.network_write_timeout_ms.millis,
      new IDSource(workerID), // id source,
      caches,
      localCheckQ,
      clusterCheckQ,
      storageCheckQ,
      config.storage_backup_request_ratio,
      config.repairTimeout,
      config.schema_retention_days.days,
      config.transaction_max_size_bytes,
      config.transaction_max_occ_reads,
      config.index_enforce_entries_max_size,
      config.tasks_reindex_cancel_limit,
      config.reindexCancelBytesSize,
      config.tasks_reindex_docs_limit,
      config.task_reprioritizer_limit,
      config.health_check_timeout_ms.millis,
      config.health_check_healthy_period_ms.millis,
      config.health_check_unhealthy_period_ms.millis,
      internalJWK = config.internalJWK,
      jwkProvider = Some(config.jwkProvider),
      mvtProvider = GCProvider,
      queryMaxConcurrentReads = config.queryMaxConcurrentReads,
      queryEvalStepsPerYield = config.queryEvalStepsPerYield,
      queryAccumulatePageWidth = config.queryAccumulatePageWidth,
      fqlxMaxStackFrames = config.fqlx_max_stack_frames,
      exportPath = config.exportPath
    )

    caches.setRepo(ctx)
    ctx
  }

  private def setupStreamContext(
    repo: RepoContext,
    stats: StatsRecorder,
    config: CoreConfig): StreamContext = {

    new StreamContext(
      repo = repo,
      stats = stats,
      logEvents = config.log_streams,
      idGen = new IDSource(() => CassandraService.instance.workerID),
      streamService = CassandraService.instance.streamService,
      eventReplayLimit = config.stream_event_replay_limit
    )
  }

  private def setupCassandraService(
    repoF: Future[RepoContext],
    flags: FFService,
    limiters: ArrivalRateService,
    config: CoreConfig): Unit = {

    logAndPrint(s"Data path: ${config.storagePath}")
    logAndPrint(s"Temp path: ${config.tempPath}")

    val prio =
      if (config.storage_enable_qos) {
        Prioritizer(repoF)
      } else {
        ConstantPriorityProvider
      }

    val mkExec = if (config.background_enable_task_execution) {
      Some({ cs: CassandraService =>
        repoF map { repo =>
          val sampler = new ProbabilitySampler(config.traceProbabilityTaskExecutor)

          new TaskExecutor(
            repo,
            TaskRouter,
            cs,
            new TaskReprioritizer(
              repo,
              cs,
              sampler,
              config.background_task_reprioritize_sleep_time_seconds.seconds,
              config.background_task_reprioritize_backoff_time_seconds.seconds),
            sampler,
            yieldTime = config.background_task_exec_yield_time_seconds.seconds,
            initSleepTime = config.background_task_exec_sleep_time_seconds.seconds,
            maxSleepTime = config.background_task_exec_backoff_time_seconds.seconds,
            batchSize = config.background_task_exec_batch_size,
            stealSize = config.background_task_steal_size,
            unhealthyStealSize = config.background_task_unhealthy_steal_size,
            enableStealing = config.background_task_stealing_enabled
          )
        }
      })
    } else {
      None
    }

    // Do not start CassandraService on boot. Only Cassandra is initialised.
    // Enough information is cached about the configuration so that CassandraService
    // can be
    // initialized when 'init' or 'join' is invoked from the admin.
    CassandraService.initialize(
      config = config.cassandra,
      encryption = config.encryption,
      stats = stats,
      prioritizer = prio,
      tmpDirectory = Paths.get(config.tempPath),
      txnLogBackupPath = config.txnLogBackupPath,
      storagePath = Paths.get(config.storagePath),
      backupDir = Paths.get(config.snapshotPath),
      mkExecutor = mkExec,
      flagsClient = flags,
      arrivalsService = limiters
    )

    CassandraService.maybeStart()
  }

  private def setupAPI(repo: RepoContext, streamCtx: StreamContext): API = {
    logAndPrint(s"Network Host ID: ${config.networkHostID}")
    logAndPrint(s"Cluster name: ${config.cluster_name}")

    val workerID = () => CassandraService.instance.workerID

    val api = new API(
      config,
      repo.withRetryOnContention,
      streamCtx,
      rootKeys,
      false,
      repo.stats,
      config.jwkProvider,
      new IDSource(workerID))

    // Start the API only when we have joined a cluster, and insert the
    // sample data for ping consistency checks if not already present
    CassandraService.started foreach { service =>
      api.start()

      // Ouroboros: Executor needs CassandraService, but cannot start
      // until API enables health checking after CassandraService
      // starts...
      service.executor foreach { _ foreach { _.start() } }

      service.initSchemaCacheInvalidationService(repo, ModelCache.invalidate)
    }

    api
  }

  private def setupTracing(config: CoreConfig) = {

    GlobalTracer.registerIfAbsent {
      val resource = Resource(
        Resource.Labels.CloudAccountID -> config.cloud_account_id,
        Resource.Labels.CloudProvider -> config.cloud_provider,
        Resource.Labels.CloudRegion -> config.cloud_region,
        Resource.Labels.CloudZone -> config.cloud_zone,
        Resource.Labels.HostID -> config.cloud_instance_id,
        Resource.Labels.HostImageID -> config.cloud_image_id,
        Resource.Labels.HostImageName -> config.cloud_image_name,
        Resource.Labels.HostImageVersion -> config.cloud_image_version,
        Resource.Labels.HostName -> config.networkHostID,
        Resource.Labels.HostType -> config.cloud_instance_type,
        Resource.Labels.ServiceName -> "faunadb",
        Resource.Labels.ServiceNamespace -> config.cluster_name,
        Resource.Labels.ServiceVersion -> Build.identifier
      )

      val tracer = SamplingTracer(
        config.traceProbability,
        resource,
        Tracer.Stats(stats),
        tracedSecrets)

      val b = Vector.newBuilder[Exporter]

      if (config.log_trace) {
        b += new LogExporter(tracer.stats)
      }

      config.trace_apm.toLowerCase match {
        case "datadog" =>
          b += new DDClient(
            config.datadog_host,
            config.datadog_port,
            config.datadog_version,
            config.datadog_endpoint,
            config.datadog_connect_timeout_ms.millis,
            config.datadog_response_timeout_ms.millis,
            config.datadog_connections,
            tracer.stats
          )
        case _ => ()
      }

      val exporters = b.result()

      if (exporters.nonEmpty) {
        val queue =
          new QueueExporter(config.trace_buffer_size, exporters, tracer.stats)

        queue.start()
        stop {
          queue.stop()
        }

        tracer.addExporter(queue)
      } else {
        logAndPrint("Trace sampling enabled without exporters?")
      }

      (config.traceSecret, tracer)
    }
  }

  private def setupFlags(config: CoreConfig, stats: StatsRecorder): FFService = {
    val baseSvc = if (config.flags_path ne null) {
      val file = new FFFileService(Path.of(config.flags_path), stats)
      // Start watching for updates
      file.start()
      logAndPrint(s"Feature Flags File: ${file.path}")
      file
    } else {
      logThenDie("flags_path must be defined in faunadb.yml")
    }

    stop {
      baseSvc.stop()
    }

    baseSvc.withFallbacks(account = Map.empty)
  }

  private def setupRateLimits(
    config: CoreConfig,
    stats: StatsRecorder): AccountOpsRateLimitsService =
    if (!config.queryOpsRateLimitsEnabled) {
      PermissiveAccountOpsRateLimitsService
    } else {
      new CachingAccountOpsRateLimitsService(
        config.opsLimitCacheMaxSize,
        config.opsLimitCacheExpireAfter,
        config.maxReadOpsPerSecond,
        config.maxWriteOpsPerSecond,
        config.maxComputeOpsPerSecond,
        config.maxStreamsPerSecond,
        config.opsLimitBurstSeconds,
        stats,
        config.log_limiters
      )
    }

  protected def initSampleData(api: API): Unit = {
    // NB. Ensure init can retry quickly in order to unlock the host initialization.
    val repo = api.repo.withQueryTimeout(5.seconds)

    def init: Future[Unit] = {
      val ts = Clock.time
      val initF = repo.runNow(SampleData.check(ts)) flatMap {
        case (_, true)  => Future.unit
        case (_, false) => repo.runNow(SampleData.init()).unit
      }
      initF recoverWith { case _: UninitializedException | _: TimeoutException =>
        Timer.Global.delay(100.milliseconds)(init)
      }
    }

    Await.result(init, Duration.Inf)
  }

  // Admin server components

  private def adminServer(config: CoreConfig, admin: Application): HttpServer = {
    val addr = config.network_admin_http_address
    val port = config.network_admin_http_port
    logAndPrint(s"Admin endpoint: $addr:$port")

    val sock = new InetSocketAddress(addr, port)
    val server = admin.server(
      sock,
      config.httpReadTimeout,
      config.httpKeepAliveTimeout,
      config.adminSSL)

    try {
      server.start()
    } catch {
      case _: BindException =>
        logThenDie(
          s"Unable to bind to $addr:$port. Verify network_admin_http_address is set to a valid local network address.")
    }

    server
  }

  private def setupAdmin(
    storage: Storage,
    repo: Future[RepoContext],
    config: CoreConfig,
    stats: StatsRecorder,
    rootKeys: List[String],
    version: String): Application = {
    logAndPrint(s"Snapshot path: ${config.snapshotPath}")

    AdminApplication(
      storage,
      rootKeys,
      config.networkHostID,
      stats,
      tracedSecrets,
      repo map { _.withRetryOnContention },
      config.storagePath,
      version,
      config
    )
  }
}
