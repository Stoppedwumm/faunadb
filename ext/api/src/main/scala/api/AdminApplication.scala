package fauna.api

// FIXME: move to admin-specific variants?
import fauna.api.fql1.{ APIRequest, APIResponse, PlainResponse, ResponseError }
import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.cluster._
import fauna.cluster.workerid.{
  AssignedWorkerID,
  UnassignedWorkerID,
  UnavailableWorkerID
}
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.config._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.flags._
import fauna.lang.{ AdminControl, Timestamp }
import fauna.lang.syntax._
import fauna.model.{ Collection, Database, Index, RenderContext, SampleData, Task }
import fauna.model.account.AccountSettings
import fauna.model.schema.index.CollectionIndexManager
import fauna.model.schema.SchemaCollection
import fauna.model.tasks.{
  IndexBuild,
  IndexRebuild,
  IndexSwap,
  MigrationTask,
  Repair,
  TaskRouter
}
import fauna.model.tasks.Reindex
import fauna.model.Cache
import fauna.net.http._
import fauna.repair.{ LookupsParser, LookupsResolver }
import fauna.repo._
import fauna.repo.cassandra._
import fauna.repo.data.InitializeResult
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.service.{
  IllegalBackupNameException,
  UnexpectedBackupClusterException
}
import fauna.repo.store.{
  CacheStore,
  DatabaseTree,
  DumpEntry,
  HealthCheckStore,
  LookupStore
}
import fauna.scheduler.{ Priority, PriorityGroup }
import fauna.snowflake.IDSource
import fauna.stats.StatsRecorder
import fauna.storage.{ Cassandra, Resolved, Storage, Tables }
import fauna.storage.cassandra.CollectionStrategy
import fauna.storage.doc.{ Data, Diff, MaskTree }
import fauna.storage.ir._
import fauna.tx.transaction.{ Epoch, SegmentID }
import fauna.util.{ BCrypt, Base64 }
import fauna.util.router._
import io.netty.handler.codec.http.HttpMethod
import java.io.{ FileInputStream, PrintWriter }
import java.net.NetworkInterface
import java.nio.file.FileAlreadyExistsException
import java.security.MessageDigest
import java.util.{ Set => JSet }
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

case class UnknownReplicaException(msg: String) extends Exception(msg)

object ReplicaTypeNames {
  val Compute = "compute"
  val Data = "data"
  val Log = "data+log"
}

object AdminApplication {

  /** Converts input [[InitializeResult]] to a response JSObject.
    *
    * @return Returns a response (for early request termination) if there was an error
    *         else None (for further initialization).
    */
  private[api] def toResponse(initResult: InitializeResult): Option[PlainResponse] =
    initResult match {
      case InitializeResult.Initialized =>
        None // return None so that request can be further processed to initialised the cluster.

      case InitializeResult.InvalidReplicaName =>
        Some(APIResponse.BadRequest("Replica name is invalid."))

      case InitializeResult.ReplicaNameAlreadySet(oldReplicaName) =>
        Some(
          APIResponse.BadRequest(
            s"Replica name is already set to '$oldReplicaName'."))
    }

  private[api] val noSeedResponse = APIResponse.BadRequest("No seed in the request.")
}

case class AdminApplication(
  storage: Storage,
  rootKeys: List[String],
  networkHostID: String,
  stats: StatsRecorder,
  tracedSecrets: JSet[String],
  repo: Future[RepoContext],
  path: String,
  version: String,
  config: CoreConfig)
    extends Application
    with RouteDSL[APIRequest, Future[PlainResponse]] {

  val workerID = () =>
    try {
      CassandraService.instance.workerID
    } catch {
      // WorkerID hasn't yet been determined; these logs are low
      // value, so use a value outside of the ID space. -1 chosen by
      // random dice roll.
      case _: IllegalStateException | _: UninitializedException => -1
    }

  val messageIDSource = new IDSource(workerID)

  val logger = getLogger()

  // enable admin-only functionality
  implicit val ctl = new AdminControl

  // FIXME: dig out of repo or provide via construction
  private def services = CassandraService.instance

  def replicaName = CassandraService.instanceOpt map { _.replicaName }

  /** Starts [[CassandraService]] with the replicaName.
    */
  private def initCassandraService(replicaName: String): InitializeResult =
    CassandraService.start(replicaName)

  private def OK(json: JSObject) = APIResponse.JSONResponse(200, json)

  private def bodyJSON(req: APIRequest) =
    req.body flatMap {
      JSON.tryParse[JSValue](_).toOption
    } flatMap {
      _.asOpt[JSObject]
    } getOrElse {
      JSObject.empty
    }

  private def withRepo(f: => Query[PlainResponse]): Future[PlainResponse] =
    if (repo.isCompleted) {
      for {
        ctx <- repo
        res <- ctx.withStats(stats).withPriority(PriorityGroup.Root).result(f) map {
          _.value
        }
      } yield res
    } else {
      Future.successful(
        APIResponse.InternalError("Host is not a member of a cluster")
      ) // XXX: better message?
    }

  GET / "admin" / "identity" / { _ =>
    try {
      services.localID match {
        case Some(uuid) =>
          Future.successful(OK(JSObject("identity" -> JSString(uuid.toString))))
        case None =>
          Future.successful(
            APIResponse.InternalError("Host is not a member of a cluster"))
      }
    } catch {
      case _: UninitializedException =>
        Future.successful(APIResponse.InternalError("Host is not initialized"))
    }

  }

  val hostVersionInfo = JSObject("version" -> version)

  val buildInfo = JSObject(
    "product" -> BuildInfo.product,
    "faunaVersion" -> BuildInfo.version,
    "revision" -> BuildInfo.revision,
    "scalaVersion" -> BuildInfo.scalaVersion,
    "sbtVersion" -> BuildInfo.sbtVersion,
    "buildTimeStamp" -> BuildInfo.buildTimestamp,
    "buildFullHash" -> BuildInfo.gitFullHash,
    "buildBranch" -> BuildInfo.gitBranch
  )

  GET / "admin" / "host-version" / { _ =>
    Future.successful(OK(hostVersionInfo))
  }

  GET / "admin" / "host-versions" / { _ =>
    val versions = services.hostVersions map { v =>
      JSObject(
        "host_id" -> v.hostID.toString,
        "replica" -> v.replica,
        "host_name" -> v.hostName,
        "build_version" -> v.buildVersion
      )
    }

    Future.successful(
      OK(JSObject("nodes" -> versions))
    )
  }

  def getHostIP(): JSArray = {
    val b = JSArray.newBuilder
    val netint = NetworkInterface.getNetworkInterfaces.asScala.toList

    netint.foreach { ni =>
      val inetaddr = ni.getInetAddresses.asScala.toList
      inetaddr.foreach { i => b += i.getHostAddress }
    }
    b.result()
  }

  def getComputerInfo(): JSObject = {
    val maxMem =
      if (Runtime.getRuntime.maxMemory == Long.MaxValue) {
        0
      } else {
        Runtime.getRuntime.maxMemory
      }
    JSObject(
      "OSName" -> System.getProperty("os.name"),
      "OSType" -> System.getProperty("os.arch"),
      "OSVersion" -> System.getProperty("os.version"),
      "JavaVersion" -> System.getProperty("java.version"),
      "AvailableCores" -> Runtime.getRuntime.availableProcessors,
      "JVMFreeMemory" -> Runtime.getRuntime.freeMemory,
      "JVMMaxMemory" -> maxMem,
      "JVMInUseMemory" -> Runtime.getRuntime.totalMemory,
      "HostIP" -> getHostIP()
    )
  }

  GET / "admin" / "host" / "info" / { _ =>
    Future.successful(OK(getComputerInfo()))
  }

  GET / "admin" / "host" / "config" / { _ =>
    Future.successful(OK(JSObject("config" -> config.toJSON)))
  }

  GET / "admin" / "host" / "build" / { _ =>
    Future.successful(OK(buildInfo))
  }

  GET / "admin" / "host" / { _ =>
    val uuid = services.localID match {
      case Some(uuid) => JSString(uuid.toString)
      case None       => JSString("Host is not a member of a cluster")
    }

    Future.successful(
      OK(
        JSObject(
          "identity" -> uuid,
          "version" -> hostVersionInfo,
          "computer" -> getComputerInfo(),
          "config" -> config.toJSON,
          "build" -> buildInfo
        )
      ))
  }

  GET / "admin" / "host-version" / { _ =>
    val info = JSObject("version" -> version)
    Future.successful(OK(info))
  }

  POST / "admin" / "host-id" / { req =>
    val hostID = for {
      node <- (bodyJSON(req) / "node").asOpt[String]
      status <- services.status(Map.empty) find {
        _.hostName exists { _ == node }
      }
    } yield status.hostID

    hostID match {
      case Some(uuid) =>
        Future.successful(OK(JSObject("identity" -> JSString(uuid.toString))))
      case None =>
        Future.successful(APIResponse.BadRequest("Host is unknown."))
    }
  }

  private def getLogSegment(hostID: HostID) = {
    /* The getLogSegment can be called before the system is completely initialized,
     * do not let this break the json formatting, but just return we are not setup
     * yet */
    try {
      services.getLogSegment(hostID) match {
        case Some(s) => s.toString
        case None    => "none"
      }
    } catch {
      case _: UninitializedException => "Uninitialized"
    }
  }

  GET / "admin" / "status" / { _ =>
    val announcements = if (repo.isCompleted) {
      repo flatMap { rp =>
        rp.result(HealthCheckStore.getAll, Timestamp.Min) map { _.value }
      }
    } else {
      Future.successful(Map.empty[HostID, Timestamp])
    }

    announcements flatMap { as =>
      services.logLeaders map { logSegmentLeaders =>
        val statuses = services.status(as) map { s =>
          val status = s.status match {
            case HostStatus.Up   => "up"
            case HostStatus.Down => "down"
          }

          val workerID = s.workerID match {
            case AssignedWorkerID(id) => id.toString
            case UnassignedWorkerID   => "n/a"
            case UnavailableWorkerID  => "error"
          }

          val persistedTimestamp = s.persistedTimestamp match {
            case Some(Timestamp.MaxMicros) => "none"
            case Some(ts)                  => ts.toString
            case None                      => "not reported"
          }

          JSObject(
            "status" -> status,
            "state" -> s.state,
            "ownership" -> s.ownership,
            "ownership_goal" -> s.ownershipGoal,
            "host_id" -> s.hostID.toString,
            "address" -> JSString(s.hostName getOrElse "n/a"),
            "replica" -> s.replica,
            "worker_id" -> workerID,
            "log_segment" -> getLogSegment(s.hostID),
            "log_segment_leader" -> logSegmentLeaders.contains(s.hostID),
            "persisted_timestamp" -> persistedTimestamp
          )
        }
        if (statuses.isEmpty) {
          APIResponse.ErrorResponse(
            503,
            "cluster uninitialized",
            "This node is not part of a cluster.")
        } else {
          val (repInfo, _) = replicaInfo
          OK(JSObject("nodes" -> statuses, "replica_info" -> repInfo))
        }
      }
    }
  }

  private def replicaInfo = {
    val dataReplicas = services.dataReplicas
    val logReplicas = services.logReplicas
    val allReplicas = services.allReplicas

    val replicaInfo = services.allReplicas.toSeq.sorted map { name =>
      val replicaType = if (logReplicas.contains(name)) {
        ReplicaTypeNames.Log
      } else if (dataReplicas.contains(name)) {
        ReplicaTypeNames.Data
      } else {
        ReplicaTypeNames.Compute
      }
      JSObject("name" -> name, "type" -> replicaType)
    }
    val etag = replicaInfoEtag(allReplicas, dataReplicas, logReplicas)
    (replicaInfo, etag)
  }

  private def replicaInfoEtag(
    allReplicas: Set[String],
    dataReplicas: Set[String],
    logReplicas: Set[String]) = {
    val log = logReplicas.toVector.sorted.mkString(",")
    val data = (dataReplicas -- logReplicas).toVector.sorted.mkString(",")
    val compute = (allReplicas -- dataReplicas).toVector.sorted.mkString(",")
    val s = s"compute=$compute;data=$data;log=$log"
    val digest = MessageDigest.getInstance("SHA1").digest(s.getBytes("utf-8"))
    Base64.encodeUrlSafe(digest)
  }

  private def initOrJoin(
    req: APIRequest,
    isJoin: Boolean = false): Option[PlainResponse] =
    (bodyJSON(req) / "replica_name").asOpt[String] match {
      case Some(replicaName) =>
        storage.init(extendedGCGrace = isJoin)
        AdminApplication.toResponse(initCassandraService(replicaName))

      case None =>
        // if the replicaName is not set - return response.
        Some(APIResponse.BadRequest("Replica name is required."))
    }

  POST / "admin" / "init" / { req =>
    initOrJoin(req) map { error =>
      Future.successful(error)
    } getOrElse {
      services.initCluster() map { b =>
        OK(JSObject("changed" -> JSBoolean(b)))
      }
    }
  }

  POST / "admin" / "join" / { req =>
    // read the seed node first and respond "no seed" if absent early.
    // initOrJoin should only be invoked if seed node is set.
    // there is no need to init cassandra if the request is bad.
    readSeedNode(req) match {
      case Some(seedNode) =>
        {
          initOrJoin(req, isJoin = true) match {
            case Some(error) =>
              Future.successful(error)

            case None =>
              joinOrMove(seedNode) { seed =>
                services.joinCluster(seed)
              }
          }
        } recover { case timeout: TimeoutException =>
          APIResponse.HelloTimeout(timeout.getMessage)
        }

      case None =>
        Future.successful(AdminApplication.noSeedResponse)
    }
  }

  private def readSeedNode(req: APIRequest) =
    (bodyJSON(req) / "seed").asOpt[String]

  private def joinOrMove(seedNode: String)(f: String => Future[Boolean]) =
    f(seedNode) map { b =>
      OK(JSObject("changed" -> JSBoolean(b)))
    }

  POST / "admin" / "remove_node" / { req =>
    (bodyJSON(req) / "nodeID").asOpt[String] match {
      case Some(nodeID) =>
        val host = HostID(nodeID)
        val force = (bodyJSON(req) / "force").asOpt[Boolean]
        val allowUnclean = (bodyJSON(req) / "allow_unclean").asOpt[Boolean]
        services.removeNode(
          host,
          force.getOrElse(false),
          allowUnclean.getOrElse(
            services.computeHosts.contains(host))) transformWith {
          case Success(_) => Future.successful(APIResponse.NoContent)
          case Failure(e) => Future.successful(APIResponse.BadRequest(e.getMessage))
        }
      case None =>
        Future.successful(APIResponse.BadRequest("No nodeID in the request"))
    }
  }

  GET / "admin" / "restore" / "status" / { _ =>
    val restoresJson =
      services.barService.getRestoreStatuses.map { res =>
        val hostStatusesJson =
          res.hostProgress.foldLeft(JSObject.empty) { case (acc, (hostID, state)) =>
            val cfsJson = state.foldLeft(JSObject.empty) {
              case (acc, (name, state)) =>
                acc :+ (name -> JSObject(
                  "status" -> state.status,
                  "retries" -> state.retries))
            }
            acc :+ (hostID -> cfsJson)
          }
        JSObject(
          "start_time" -> res.startTime.toString,
          "end_time" -> res.endTime.map(_.toString),
          "status" -> res.status,
          "host_status" -> hostStatusesJson)
      }
    Future.successful(OK(JSObject("restores" -> restoresJson)))
  }

  GET / "admin" / "movement_status" / { _ =>
    Future.successful(
      OK(JSObject("resource" -> JSObject(
        "movement_status" -> JSString(services.movementStatus)))))
  }

  GET / "admin" / "replication" / { _ =>
    val (repInfo, etag) = replicaInfo
    Future.successful(
      OK(
        JSObject("resource" -> JSObject(
          "replicas" -> new JSArray(repInfo)
        ))).copy(headers = Seq(HTTPHeaders.ETag -> etag)))
  }

  PUT / "admin" / "replication" / { req =>
    try {
      val allReplicas = services.allReplicas
      val dataReplicas = services.dataReplicas
      val logReplicas = services.logReplicas

      val matches = req.getHeader(HTTPHeaders.IfMatch) match {
        case None | Some("*") => true
        case Some(tag) =>
          tag == replicaInfoEtag(allReplicas, dataReplicas, logReplicas)
      }

      if (!matches) {
        Future.successful(APIResponse.PreconditionFailed)
      } else {
        val replicas = (bodyJSON(req) / "replicas").as[JSArray].value
        val activeBuilder = Set.newBuilder[String]
        val logBuilder = Set.newBuilder[String]

        val missingReplicas = replicas.foldLeft(allReplicas) {
          case (replicas, repInfo) =>
            val name = (repInfo / "name").as[String]
            val rtyp = (repInfo / "type").as[String]

            if (!replicas.contains(name)) {
              // returns Bad Request
              throw UnknownReplicaException(s"Unknown replica $name")
            }

            rtyp match {
              case ReplicaTypeNames.Log =>
                logBuilder += name
                activeBuilder += name
                replicas - name
              case ReplicaTypeNames.Data =>
                activeBuilder += name
                replicas - name
              case ReplicaTypeNames.Compute =>
                replicas - name
              case _ =>
                // returns Bad Request
                throw UnknownReplicaException(s"Unknown replica type $rtyp")
            }
        }

        if (missingReplicas.nonEmpty) {
          val strMissingReplicas = missingReplicas.mkString(",")
          val repWord = if (missingReplicas.size == 1) {
            "replica"
          } else {
            "replicas"
          }
          Future.successful(
            APIResponse.BadRequest(
              s"Missing specification for $repWord $strMissingReplicas"))
        } else {
          val newActive = activeBuilder.result()
          val newLog = logBuilder.result()
          services.updateReplication(
            newActive,
            newLog,
            dataReplicas,
            logReplicas) map { _ =>
            if (
              services.dataReplicas != newActive || services.logReplicas != newLog
            ) {
              // topology changed between us snapshotting dataReplicas and
              // logReplicas on method entry and here. It's safe for the client to
              // retry.
              APIResponse.PreconditionFailed
            } else {
              APIResponse.NoContent
            }
          }
        }
      }
    } catch {
      case e: UnknownReplicaException =>
        Future.successful(APIResponse.BadRequest(e.getMessage))
    }
  }

  POST / "admin" / "balance_replica" / { req =>
    (bodyJSON(req) / "replicaName").asOpt[String] match {
      case Some(replicaName) =>
        try {
          if (
            storage.getGCGrace() exists { case (_, grace) =>
              grace.seconds < Storage.ExtendedGracePeriod
            }
          ) {
            Future.failed(new IllegalStateException(
              s"GC grace period is too short. Use set-extended-gc-grace to set to ${Storage.ExtendedGracePeriod}"))
          } else {
            services.balanceReplica(replicaName) map { _ =>
              APIResponse.NoContent
            }
          }
        } catch {
          case e: IllegalArgumentException =>
            Future.successful(APIResponse.BadRequest(e.getMessage))
        }
      case None =>
        Future.successful(APIResponse.BadRequest("No replicaName in the request"))
    }
  }

  private def doReplicaBackup(backupName: Option[String]) = {
    val backupTimeout = 60.seconds.bound
    services.barService.createBackupReplica(backupName, backupTimeout) map { ack =>
      val status = ack forall { _.ok }
      val hosts = ack map { _.host.toString }
      val msgs = ack map { _.msg }
      OK(
        JSObject("backup" ->
          JSObject("successful" -> status, "hosts" -> hosts, "msgs" -> msgs)))
    } recover {
      case e @ (_: FileAlreadyExistsException | _: IllegalBackupNameException |
          _: UnexpectedBackupClusterException) =>
        APIResponse.BadRequest(e.getMessage)
      case e: Throwable =>
        APIResponse.InternalError(s"Snapshot interrupted with unknown error ($e).")
    }
  }

  POST / "admin" / "backup" / "replica" / { _ =>
    doReplicaBackup(None)
  }

  POST / "admin" / "backup" / "replica" / stringP / { (name, _) =>
    doReplicaBackup(Some(name))
  }

  POST / "admin" / "snapshots" / stringP / { (name, _) =>
    services.barService.createBackup(Cassandra.KeyspaceName, name) map {
      case (ts, path) =>
        OK(
          JSObject("snapshot" ->
            JSObject("path" -> path.toString, "persisted_ts" -> ts.toString)))
    } recover {
      case _: FileAlreadyExistsException =>
        APIResponse.BadRequest(s"Snapshot [$name] already exists.")
      case e: IllegalBackupNameException => APIResponse.BadRequest(e.getMessage)
      case e: Throwable =>
        APIResponse.InternalError(s"Snapshot interrupted with unknown error ($e).")
    }
  }

  GET / "admin" / "snapshots" / { _ =>
    services.barService.listBackup() map { snapshots =>
      val snaps = snapshots.iterator.to(Seq) map { JSString(_) }
      OK(JSObject("resource" -> JSObject("snapshots" -> snaps)))
    }
  }

  POST / "admin" / "load" / { req =>
    (bodyJSON(req) / "path").asOpt[String] match {
      case Some(path) =>
        val target = (bodyJSON(req) / "target").asOpt[String]
        services.barService
          .restoreBackup(
            path,
            target,
            config.loadSnapshotThreads,
            config.loadSnapshotTimeout.bound)
          .map { _ =>
            OK(JSObject("resource" -> "ok"))
          }
          .andThen {
            case Success(_) =>
              getLogger().info(
                s"Restore succeeded for path $path and target $target.")
            case Failure(e) =>
              getLogger().error(
                s"Restore failed for path $path and target $target.",
                e)
          }
      case None =>
        Future.successful(APIResponse.BadRequest("Invalid snapshot parameters."))
    }
  }

  POST / "admin" / "repair" / "lookups" / { req =>
    val dryRun =
      (bodyJSON(req) / "dryRun").asOpt[Boolean].getOrElse(true)
    (
      (bodyJSON(req) / "path").asOpt[String],
      (bodyJSON(req) / "outputPath").asOpt[String]) match {
      case (Some(path), Some(outputPath)) =>
        val outputWriter = new PrintWriter(outputPath)
        val lookupsInput = LookupsParser.parseLookupsFromFile(path)
        withRepo {
          Query.repo.flatMap { repo =>
            val lookupsResolver =
              LookupsResolver(
                repo,
                services.storageService,
                services.partitioner.partitioner)

            Query.future(
              lookupsResolver
                .resolveLookups(
                  lookupsInput.lookups,
                  lookupsInput.snapshotTS,
                  outputWriter,
                  dryRun = dryRun)
                .map { _ => OK(JSObject("resource" -> outputPath)) }
            )
          }
        } ensure { outputWriter.close() }
      case _ =>
        Future.successful(APIResponse.BadRequest("Invalid fix-lookups parameters."))
    }
  }

  GET / "admin" / "storage" / "version" / { _ =>
    Future.successful(
      OK(JSObject("resource" ->
        JSObject("version" -> storage.version.toString))))
  }

  GET / "admin" / "storage" / "clean" / stringP / { (cf, _) =>
    Future {
      OK(
        JSObject("resources" ->
          JSObject("clean" -> services.storageCleaner.needsCleanup(Some(cf)))))
    }
  }

  GET / "admin" / "storage" / "clean" / { _ =>
    Future {
      OK(
        JSObject("resources" ->
          JSObject("clean" -> services.storageCleaner.needsCleanup(None))))
    }
  }

  PUT / "admin" / "storage" / "clean" / { _ =>
    Future {
      services.storageCleaner.scheduleCleanup()
      OK(JSObject("resource" -> "ok"))
    }
  }

  PUT / "admin" / "storage" / "version" / { _ =>
    Future {
      storage.update()
      storage.cleanup()
      APIResponse.NoContent
    }
  }

  POST / "admin" / "storage" / "unlock" / { _ =>
    Future {
      services.unlockStorage()
      APIResponse.NoContent
    }
  }

  DELETE / "admin" / "storage" / "cache" / { _ =>
    Future {
      CollectionStrategy.hints.clear()
      APIResponse.NoContent
    }
  }

  GET / "admin" / "storage" / "get-gc-grace" / { _ =>
    Future.successful(OK(JSObject(storage.getGCGrace() map {
      case (cf, graceSeconds) =>
        cf -> JSObject(
          "days" -> JSLong(graceSeconds.seconds.toDays),
          "seconds" -> JSLong(graceSeconds)
        )
    }: _*)))
  }

  POST / "admin" / "storage" / "set-standard-gc-grace" / { _ =>
    storage.updateGCGraceAll(Storage.StandardGCGrace)
    Future.successful(APIResponse.NoContent)
  }

  POST / "admin" / "storage" / "set-extended-gc-grace" / { _ =>
    storage.updateGCGraceAll(Storage.ExtendedGracePeriod)
    Future.successful(APIResponse.NoContent)
  }

  POST / "admin" / "reload" / { _ =>
    Future {
      storage.reload()
      APIResponse.NoContent
    }
  }

  POST / "admin" / "repair_task" / { req =>
    withRepo {
      val effect = if (req.params.contains("dry_run")) {
        Repair.NoCommit
      } else {
        Repair.Commit
      }

      // TODO: add replication-only
      val mode = if (req.params.contains("data_only")) {
        Repair.Model
      } else {
        Repair.Full
      }

      val filterScope = req.params.get("filter_scope") map { scope =>
        ScopeID(scope.toLong)
      }

      // FIXME: pass delayed timestamp?
      Query.snapshotTime flatMap { snapTime =>
        Repair.RootTask.createOpt(snapTime, effect, mode, filterScope) map {
          case Some(_) => APIResponse.NoContent
          case None    => APIResponse.TooManyRequests // repair already in progress?
        }
      }
    }
  }

  POST / "admin" / "repair_task" / "cancel" / { _ =>
    withRepo {
      Repair.RootTask.cancelAll(TaskRouter) map { _ =>
        APIResponse.NoContent
      }
    }
  }

  GET / "admin" / "repair_task" / "status" / { _ =>
    withRepo {
      Query.snapshotTime flatMap { snapTime =>
        Repair.RootTask.pendingState(snapTime).headValueT map {
          case Some((stage, remaining)) =>
            JSObject("pending" -> true, "stage" -> stage, "remaining" -> remaining)
          case _ => JSObject("pending" -> false)
        } map { js =>
          OK(JSObject("resource" -> js))
        }
      }
    }
  }

  POST / "admin" / "index_rebuild" / "start" / { req =>
    val maxID = req.params.get("max_task_id") map { id =>
      Try(id.toLong)
    } getOrElse Success(Long.MaxValue)
    val phases =
      req.params.get("phases").fold(Vector.empty[String]) { _.split(',').toVector }

    withRepo {
      maxID match {
        case Success(maxID) =>
          IndexRebuild.RootTask.create(maxID, phases) map { _ =>
            APIResponse.NoContent
          }
        case Failure(_: NumberFormatException) =>
          Query.value(APIResponse.BadRequest("max_task_id is not a valid number."))
        case Failure(ex) =>
          Query.value(APIResponse.InternalError(ex))
      }
    }
  }

  POST / "admin" / "force-migrate" / { req =>
    val scope = ScopeID(req.params.get("scope").get.toLong)
    val collection = CollectionID(req.params.get("collection").get.toLong)

    withRepo {
      for {
        task <- MigrationTask.Root.create(scope, collection, isOperational = true)
      } yield {
        OK(JSObject("task" -> JSLong(task.id.toLong)))
      }
    }
  }

  POST / "admin" / "reset-migrations" / { req =>
    val scope = ScopeID(req.params.get("scope").get.toLong)
    val collection = CollectionID(req.params.get("collection").get.toLong)
    val rebuild = req.params.get("rebuild").exists(_ == "true")

    // This is a bit gross, but the schema validation hooks appear to work, so we can
    // really just demolish this collection, and the post-eval hooks will pick up the
    // change.
    withRepo {
      for {
        _ <- SchemaCollection
          .Collection(scope)
          .internalUpdate(
            collection,
            Diff(
              MapV(
                // Force this collection back into the dark ages of `*: Any`.
                Collection.DefinedFields.path.head -> NullV,
                Collection.WildcardField.path.head -> StringV("Any"),
                Collection.MigrationsField.path.head -> NullV,
                Collection.InternalMigrationsField.path.head -> ArrayV()
              ))
          )

        // Delete and re-add all the user-defined indexes.
        _ <-
          if (rebuild) {
            CollectionIndexManager.rebuildAll(scope, collection)
          } else {
            CollectionIndexManager.markNotQueryable(scope, collection)
          }
      } yield APIResponse.NoContent
    }
  }

  /** *** DANGER ***
    */
  POST / "admin" / "skip_transactions" / { _ =>
    withRepo {
      val lat = services.skipTransactions()
      Query.value(OK(JSObject("lat" -> lat.toString)))
    }
  }

  // Sample data

  POST / "admin" / "reseed" / { _ =>
    withRepo {
      SampleData.init() map { _ =>
        APIResponse.NoContent
      }
    }
  }

  // Task administration

  POST / "admin" / "tasks" / "steal" / { _ =>
    withRepo {
      val orphans = Task.getAllRunnable() rejectT { t =>
        services.hosts contains t.host
      }

      val mine = orphans foreachValueT { t =>
        Task.steal(t).join
      }

      mine map { _ =>
        APIResponse.NoContent
      }
    }
  }

  def taskToJson(task: Task): Query[JSObject] =
    stateToJson(task.scopeID, task.state) map { state =>
      JSObject(
        "ts" -> task.validTS.micros,
        "id" -> task.id.toLong,
        "parent" -> task.parent.map(_.toLong),
        "name" -> task.name,
        "scope" -> task.scopeID.toLong,
        "account" -> task.accountID.toLong,
        "priority" -> task.priority,
        "host" -> task.host.toString,
        "state" -> state
      )
    }

  // Like taskToJson, but does not render the state, avoiding
  // cache lookups and potential schema contention. This has
  // been a factor in list-tasks because listing can take a long
  // time.
  def taskSummaryToJson(task: Task): JSObject =
    JSObject(
      "ts" -> task.validTS.micros,
      "id" -> task.id.toLong,
      "parent" -> task.parent.map(_.toLong),
      "name" -> task.name,
      "scope" -> task.scopeID.toLong,
      "account" -> task.accountID.toLong,
      "priority" -> task.priority,
      "host" -> task.host.toString,
      "state" -> Task.State.name(task.state)
    )

  val entryFieldMask = MaskTree(
    Task.StateField.path ++ Task.DataField.path ++ Repair.EntryField.path)

  def stateToJson(scopeID: ScopeID, state: Task.State): Query[JSValue] = {
    val data = entryFieldMask.reject(Data(Task.StateField -> state).fields)
    val literal = Literal(scopeID, data.get(Task.StateField.path) getOrElse NullV)
    RenderContext.render(
      RootAuth,
      APIVersion.Default,
      Timestamp.Epoch,
      literal) map { buf =>
      JSRawValue(buf.toUTF8String)
    }
  }

  def withTask(req: APIRequest, ts: Option[Timestamp] = None)(
    taskFn: Task => Query[PlainResponse]): Future[PlainResponse] = {
    withRepo {
      val taskID = req.params.get("task_id") map { id => Try(TaskID(id.toLong)) }

      taskID match {
        case Some(Success(id)) =>
          Query.snapshotTime flatMap { snapTime =>
            Task.get(id, ts getOrElse snapTime) flatMap {
              case Some(task) =>
                taskFn(task)

              case None =>
                Query.value(APIResponse.BadRequest(s"Task $id doesn't exist."))
            }
          }
        case Some(Failure(_: NumberFormatException)) =>
          Query.value(APIResponse.BadRequest("task_id is not a valid number."))
        case Some(Failure(ex)) =>
          Query.value(APIResponse.InternalError(ex))
        case None =>
          Query.value(APIResponse.BadRequest("task_id parameter required."))
      }
    }
  }

  def withTaskHistory(req: APIRequest)(
    taskFn: Seq[Task] => Query[PlainResponse]): Future[PlainResponse] = {
    withRepo {
      val taskID = req.params.get("task_id") map { id => Try(TaskID(id.toLong)) }

      taskID match {
        case Some(Success(id)) =>
          Task.TaskColl.versions(id).flattenT flatMap { versions =>
            taskFn(versions map { version =>
              Task(version)
            })
          }
        case Some(Failure(_: NumberFormatException)) =>
          Query.value(APIResponse.BadRequest("task_id is not a valid number."))
        case Some(Failure(ex)) =>
          Query.value(APIResponse.InternalError(ex))
        case None =>
          Query.value(APIResponse.BadRequest("task_id parameter required."))
      }
    }
  }

  GET / "admin" / "tasks" / "list" / { req =>
    case class TaskFilter(
      scopeFilter: Option[ScopeID],
      collectionFilter: Option[CollectionID],
      indexFilter: Option[IndexID]) {
      def apply(task: Task): Boolean = task.name match {
        // If no filters are set, show everything.
        case _
            if scopeFilter.isEmpty &&
              collectionFilter.isEmpty &&
              indexFilter.isEmpty =>
          true

        // If a collection ID is set, then we're implicitly filtering for
        // only migration tasks and index build tasks.
        case MigrationTask.Root.name | MigrationTask.ByIndex.Root.name |
            MigrationTask.ByIndex.Leaf.name | MigrationTask.Scan.name =>
          val scope = task.data(MigrationTask.ScopeField)
          val coll = task.data(MigrationTask.CollectionField)

          scopeFilter.forall { _ == scope } &&
          collectionFilter.forall { _ == coll }

        case IndexBuild.RootTask.name | IndexBuild.ScanTask.name |
            IndexBuild.IndexTask.name =>
          val scope = task.data(IndexBuild.ScopeField)
          val index = task.data(IndexBuild.IndexField)

          scopeFilter.forall { _ == scope } &&
          indexFilter.forall { _ == index }

        case Reindex.Root.name =>
          val scope = task.data(Reindex.ScopeField)
          val index = task.data(Reindex.Root.DestinationField)

          scopeFilter.forall { _ == scope } &&
          indexFilter.forall { _ == index }

        case Reindex.ReindexDocs.name =>
          val scope = task.data(Reindex.ScopeField)
          val index = task.data(Reindex.ReindexDocs.IndexField)

          scopeFilter.forall { _ == scope } &&
          indexFilter.forall { _ == index }

        case _ => false
      }
    }

    val filter = TaskFilter(
      req.params.get("scope").map { s => ScopeID(s.toLong) },
      req.params.get("collection").map { s => CollectionID(s.toLong) },
      req.params.get("index").map { s => IndexID(s.toLong) }
    )

    withRepo {
      val jsonQ =
        Task.getRunnableHierarchy() map { tasks =>
          tasks
            .filter { task => filter(task) }
            .map { taskSummaryToJson(_) }
            .toSeq
        }

      jsonQ map { tasks =>
        OK(JSObject("tasks" -> JSArray(tasks: _*)))
      }
    }
  }

  POST / "admin" / "tasks" / "dry-run" / { req =>
    withTask(req) { task =>
      Task.dryRun(task.id) flatMap { state =>
        val oldStateQ = stateToJson(task.scopeID, task.state)
        val newStateQ = stateToJson(task.scopeID, state)

        (oldStateQ, newStateQ) par { (oldState, newState) =>
          Query.value(OK(JSObject("old_state" -> oldState, "new_state" -> newState)))
        }
      } recoverWith { ex =>
        Query.value(APIResponse.InternalError(ex))
      }
    }
  }

  GET / "admin" / "tasks" / "get" / { req =>
    val ts = req.params.get("ts") map { ts => Timestamp.ofMicros(ts.toLong) }
    val showHistory = req.params.get("history") flatMap {
      _.toBooleanOption
    } getOrElse { false }

    if (showHistory) {
      withTaskHistory(req) { versions =>
        val versionsJson = versions map { taskToJson(_) }

        versionsJson.sequence map { versions =>
          OK(JSObject("versions" -> versions.reverse))
        }
      }
    } else {
      withTask(req, ts) { task =>
        taskToJson(task) map OK
      }
    }
  }

  POST / "admin" / "tasks" / "move" / { req =>
    withTask(req) { task =>
      val hostID = req.params.get("host") map { HostID(_) }

      if (hostID forall services.hosts) {
        Task.steal(task, hostID) map { _ => APIResponse.NoContent }
      } else {
        Query.value(
          APIResponse.BadRequest(
            s"Host ${hostID.get} doesn't belong to the cluster."))
      }
    }
  }

  POST / "admin" / "tasks" / "repartition-index-scan" / { req =>
    withTask(req) { task =>
      val targetHost = req.params("target_host")
      IndexBuild.ScanTask.repartitionTo(task, HostID(targetHost)) map { _ =>
        APIResponse.NoContent
      }
    }
  }

  POST / "admin" / "tasks" / "repartition-migration-scan" / { req =>
    withTask(req) { task =>
      val targetHost = req.params("target_host")
      MigrationTask.Scan.repartitionTo(task, HostID(targetHost)) map { _ =>
        APIResponse.NoContent
      }
    }
  }

  POST / "admin" / "tasks" / "cancel" / { req =>
    withTask(req) { task =>
      TaskRouter.cancel(task, None) map { _ => APIResponse.NoContent }
    }
  }

  POST / "admin" / "tasks" / "complete" / { req =>
    withTask(req) { task =>
      TaskRouter.complete(task, None, 0) map { _ => APIResponse.NoContent }
    }
  }

  POST / "admin" / "tasks" / "reprioritize" / { req =>
    withTask(req) { task =>
      req.params.get("priority") map { priority =>
        task.reprioritize(priority.toInt) map { _ =>
          APIResponse.NoContent
        }
      } getOrElse {
        Query.value(APIResponse.BadRequest("priority parameter is required."))
      }
    }
  }

  private def logPauseOrUnpause(task: Task, pause: Boolean): Query[Unit] =
    (IndexBuild.index(task), Database.forScope(task.scopeID)) par {
      case (idxOpt, dbOpt) =>
        // The message assembly is goofy because not all tasks are index tasks.
        val pauseInfo = if (pause) "paused" else "unpaused"
        val idxInfo = idxOpt map { idx =>
          s"for index ${idx.name} (${idx.id}) "
        } getOrElse ("")
        val dbInfo = dbOpt map { db =>
          s"${db.name} (${db.id})"
        } getOrElse ("<not available>")
        logger.info(
          s"$pauseInfo task ${task.name} (${task.id}) ${idxInfo}in database $dbInfo for account ${task.accountID}")
        Query.unit
    }

  POST / "admin" / "tasks" / "pause" / { req =>
    withTask(req) { task =>
      task.pause("paused by an operator") flatMap { _ =>
        logPauseOrUnpause(task, true)
      } map { _ => APIResponse.NoContent }
    }
  }

  POST / "admin" / "tasks" / "unpause" / { req =>
    withTask(req) { task =>
      task.unpause() flatMap { _ =>
        logPauseOrUnpause(task, false)
      } map { _ => APIResponse.NoContent }
    }
  }

  GET / "admin" / "compactions" / "list" / { _ =>
    val cm = org.apache.cassandra.db.compaction.CompactionManager.instance
    Future.successful(
      OK(JSObject("compactions" -> JSArray(
        cm.getCompactionSummary.asScala.toSeq.map(JSString): _*))))
  }

  GET / "admin" / "tracing" / "list" / { _ =>
    Future.successful(
      OK(
        JSObject(
          "tracedSecrets" -> JSArray(
            tracedSecrets.asScala.toSeq map { JSString(_) }: _*
          )
        ))
    )
  }

  POST / "admin" / "tracing" / "enable" / { req =>
    val secret = req.params("secret")
    tracedSecrets.add(secret)
    Future.successful(OK((JSObject())))
  }

  POST / "admin" / "tracing" / "disable" / { req =>
    val secret = req.params("secret")
    tracedSecrets.remove(secret)
    Future.successful(OK((JSObject())))
  }

  POST / "admin" / "indexes" / "force-build" / { req =>
    val scope = req.params.get("scope") map { id =>
      ScopeID(id.toLong)
    }

    val index = req.params.get("index") map { id =>
      IndexID(id.toLong)
    }

    (scope, index) match {
      case (Some(scope), Some(index)) =>
        withRepo {
          Index.build(scope, index, isOperational = true) map { id =>
            OK(JSObject("task" -> JSLong(id.toLong)))
          }
        }

      case (s, i) =>
        Future.successful(
          APIResponse.BadRequest(s"scope and index required. scope=$s index=$i"))
    }
  }

  POST / "admin" / "indexes" / "swap" / { req =>
    val scope = req.params.get("scope") map { id =>
      ScopeID(id.toLong)
    }

    val index = req.params.get("index") map { id =>
      IndexID(id.toLong)
    }

    (scope, index) match {
      case (Some(scope), Some(index)) =>
        withRepo {
          IndexSwap.Root.create(scope, index) map { task =>
            OK(JSObject("task" -> JSLong(task.id.toLong)))
          }
        }

      case (s, i) =>
        Future.successful(
          APIResponse.BadRequest(s"scope and index required. scope=$s index=$i"))
    }
  }

  /** Drop a hidden index. This is intended for use after a swap has
    * occurred, such that the index being dropped here is the old,
    * probably broken index prior to the swap. Use the same
    * scope/index IDs as used in the swap.
    */
  DELETE / "admin" / "indexes" / stringP / stringP / { (scopeID, indexID, _) =>
    withRepo {
      val scope = ScopeID(scopeID.toLong)
      val index = IndexID(indexID.toLong)

      Index.get(scope, index) flatMap {
        case Some(idx) if idx.isHidden =>
          SchemaCollection.Index(scope).internalDelete(index) map { _ =>
            APIResponse.NoContent
          }
        case Some(idx) =>
          Query.value(
            APIResponse.BadRequest(
              s"Index ${idx.name} at scope $scope and id $index is not hidden!"))
        case None =>
          Query.value(APIResponse.NotFound)
      }
    }
  }

  GET / "admin" / "index2cf-gc-grace" / { _ =>
    val (h_days, s_days) = storage.getIndex2CFGcGrace()

    Future.successful(
      OK(
        JSObject(
          "HistoricalIndex_2_gc_grace_days" -> h_days,
          "SortedIndex_2_gc_grace_days" -> s_days)))
  }

  POST / "admin" / "index2cf-gc-grace" / { req =>
    val d = req.params("days").toInt.days

    storage.updateGCGrace(Tables.HistoricalIndex.Schema2, d)
    storage.updateGCGrace(Tables.SortedIndex.Schema2, d)

    Future.successful(OK(JSObject()))
  }

  GET / "admin" / "scope-from-account" / { req =>
    val accountID = req.params("account-id").toLong

    withRepo {
      Database.scopeByAccountID(AccountID(accountID)) map {
        case Some(id) => OK(JSObject("scope" -> JSLong(id.toLong)))
        case None     => APIResponse.NotFound
      }
    }
  }

  GET / "admin" / "accounts" / stringP / "settings" / { (accStr, _) =>
    withRepo {
      val account = AccountID(accStr.toLong)

      Database.liveVersionForAccount(account).mapT { vers =>
        val props = vers.data(Database.AccountField).getOrElse(Data.empty)
        val settings = AccountSettings.fromData(props)
        OK(JSObject("resource" -> settings.toJSON))
      } map {
        case None       => APIResponse.NotFound
        case Some(resp) => resp
      }
    }
  }

  GET / "admin" / "accounts" / stringP / "limits" / { (accStr, _) =>
    withRepo {
      Query.repo.flatMap { repo =>
        val account = AccountID(accStr.toLong)

        Database.liveVersionForAccount(account).flatMapT { vers =>
          val accData = vers.data(Database.AccountField).getOrElse(Data.empty)
          val settings = AccountSettings.fromData(accData)

          repo.accountFlags(account, settings.ffProps).flatMap { flags =>
            // Static configuration from YAML.
            val systemJSON = JSObject(
              "read_ops" -> config.maxReadOpsPerSecond,
              "write_ops" -> config.maxWriteOpsPerSecond,
              "compute_ops" ->
                config.maxComputeOpsPerSecond,
              "burst_seconds" -> config.opsLimitBurstSeconds
            )

            // Dynamic configuration from flags.
            val flagJSON = JSObject(
              "read_ops" -> flags.get(new RateLimitsReads(Double.MaxValue)),
              "write_ops" -> flags.get(new RateLimitsWrites(Double.MaxValue)),
              "compute_ops" -> flags.get(new RateLimitsCompute(Double.MaxValue))
            )

            // Effective limits, provided the three inputs.
            repo.limiters.get(account, flags, settings.limits).flatMap { limiter =>
              Query.some(OK(JSObject("resource" -> JSObject(
                "host_id" -> services.localID.get.toString,
                "account_id" -> settings.id.fold("N/A")(_.toLong.toString),
                "settings" -> settings.limits.toJSON,
                "system" -> systemJSON,
                "flags" -> flagJSON,
                "effective_limits" -> limiter.toJSON
              ))))
            }
          }
        } map {
          case None       => APIResponse.NotFound
          case Some(resp) => resp
        }
      }
    }
  }

  POST / "admin" / "accounts" / "reprioritize" / { req =>
    val json = bodyJSON(req)
    val accountID = (json / "account_id").asOpt[Long]
    val priority = (json / "priority").asOpt[Int]

    (accountID, priority) match {
      case (Some(id), Some(prio)) =>
        withRepo {
          Database.forAccount(AccountID(id)) flatMap {
            case None => Query.value(APIResponse.NotFound)
            case Some(db) =>
              Query.snapshotTime flatMap { ts =>
                // this is equiv. to an admin key on the target
                // database's immediate parent
                val auth = SystemAuth(db.parentScopeID, db, AdminPermissions)

                val ec = EvalContext.write(auth, ts, req.version)
                val diff = Diff(Database.PriorityField -> Some(Priority(prio)))
                val collID = db.id.toDocID.collID
                val subID = SubID(db.id.toLong)

                WriteAdaptor(collID).update(
                  ec,
                  subID,
                  diff,
                  isPartial = true,
                  pos = RootPosition) map {
                  case Right(_) => APIResponse.NoContent
                  case Left(errs) =>
                    val json = errs map { Error.toJSValue(req.version, _) }
                    APIResponse.JSONResponse(400, JSObject("errors" -> json))
                }
              }
          }
        }
      case _ =>
        Future.successful(
          APIResponse.BadRequest("account_id and priority params required."))
    }
  }

  /** Get the database represented by this id.
    * The id can be either a scopeID, if it's all numeric digits,
    * or a globalID, if it's encoded using zbase32.
    *
    * Case the id represents a globalID and we call this method after
    * load snapshot with globalID preservation (restoring in place),
    * we need disambiguate which database we want to return, the actual
    * live database (disabled) or the recently loaded.
    */
  private def getDatabase(id: String, disambiguate: Boolean = false) = {
    id.toLongOption match {
      case Some(id) =>
        Database.getUncached(ScopeID(id)) map { _.toRight(APIResponse.NotFound) }

      case None =>
        // TODO: deprecate global_id resolution in favour of
        // global_id => scope_id mapping
        Database.decodeGlobalID(id) match {
          case Some(globalID) =>
            def filterDB(db: Database): Query[Boolean] =
              if (disambiguate) {
                Database.isDisabled(db.scopeID)
              } else {
                Query.value(!db.isDeleted)
              }

            for {
              dbIDs <- Store.databases(globalID).flattenT
              dbs <- dbIDs
                .map { case (parentScope, db) =>
                  Database.getUncached(parentScope, db).map { _.toSeq }
                }
                .sequence
                .map(_.flatten)
              dbsFiltered <- dbs
                .map { db => filterDB(db).map { keep => (db, keep) } }
                .sequence
                .map(_.filter(_._2).map(_._1))
            } yield {
              dbsFiltered.headOption
                .orElse {
                  // if no disabled database was found, likely the database is
                  // deleted, in this case return the most recent one.
                  dbs.toSeq sortBy { _.deletedTS } lastOption
                }
                .toRight(APIResponse.NotFound)
            }
          case None =>
            Query.value(Left(APIResponse.BadRequest(s"Invalid global id '$id'")))
        }
    }
  }

  POST / "admin" / "database" / "global-to-scope" / { req =>
    val json = bodyJSON(req)

    withRepo {
      (json / "global_id").asOpt[String] match {
        case Some(globalID) =>
          Database.decodeGlobalID(globalID) match {
            case Some(globalID) =>
              Store.getDatabaseID(globalID) flatMap {
                // If no live database maps to this global ID, check
                // for deleted databases. This happens when a user
                // wants to restore a backup of a database they have
                // deleted.
                case None        => Store.getLatestDatabaseID(globalID)
                case v @ Some(_) => Query.value(v)
              } flatMapT { case (parent, id) =>
                Database.getUncached(parent, id)
              } flatMap {
                case Some(db) =>
                  Query.value(OK(JSObject("scope_id" -> db.scopeID.toLong)))
                case None => throw ResponseError(APIResponse.NotFound)
              }

            case None =>
              throw ResponseError(
                APIResponse.BadRequest(s"Invalid global id '$globalID'"))
          }

        case None =>
          Query.value(APIResponse.BadRequest("Missing 'global_id' field."))
      }
    }
  }

  POST / "admin" / "database" / "move" / { req =>
    val json = bodyJSON(req)
    val from = (json / "from").asOpt[JSValue]
    val to = (json / "to").asOpt[JSValue]
    val name = (json / "name").asOpt[String]
    val restore = (json / "restore").asOpt[Boolean] getOrElse false

    withRepo {
      val moveQ = (from, to, name) match {
        // account id version
        case (Some(JSLong(_)), Some(JSLong(_)), None) =>
          Query.value(
            Left(APIResponse.BadRequest(
              "Name field is mandatory when used with Account ID.")))

        case (Some(JSLong(from)), Some(JSLong(to)), Some(name)) =>
          val fromDB = Database.forAccount(AccountID(from)) map {
            _.toRight(APIResponse.NotFound)
          }
          val toDB = Database.forAccount(AccountID(to)) map {
            _.toRight(APIResponse.NotFound)
          }

          (fromDB, toDB) parT { case (from, to) =>
            val path = name.split('/').toList
            val srcQ = Database.getSubScope(from.scopeID, path) map {
              _.toRight(APIResponse.NotFound)
            }

            srcQ flatMapT {
              Database.moveDatabase(_, to, restore = false, RootPosition) map {
                case Right(_) => Right(APIResponse.NoContent)
                case Left(_) =>
                  throw ResponseError(APIResponse.BadRequest("cannot move"))
              }
            }
          }

        // global/scope id version
        case (Some(JSString(_)), Some(JSString(_)), Some(_)) =>
          Query.value(
            Left(APIResponse.BadRequest(
              "Name field cannot be used along with Global ID.")))

        case (Some(JSString(from)), Some(JSString(to)), None) if from == to =>
          Query.value(
            Left(APIResponse.BadRequest(
              "Target and destination databases cannot be the same.")))

        case (Some(JSString(from)), Some(JSString(to)), None) =>
          val fromDB = getDatabase(from)
          val toDB = getDatabase(to, disambiguate = true)

          (fromDB, toDB) parT { case (from, to) =>
            Database.moveDatabase(from, to, restore, RootPosition) map {
              case Right(_) =>
                Right(APIResponse.NoContent)
              case Left(errs) =>
                val json = errs map { Error.toJSValue(req.version, _) }
                throw ResponseError(
                  APIResponse.JSONResponse(400, JSObject("errors" -> json)))
            }
          }

        // invalid fields
        case _ =>
          Query.value(
            Left(APIResponse.BadRequest(
              "Invalid combination of 'from', 'to' and 'name' fields.")))
      }

      moveQ map { _.merge }
    }
  }

  POST / "admin" / "database" / "recover" / { req =>
    import APIResponse.BadRequest

    val body = bodyJSON(req)
    val dryRun = (body / "dry_run").asOpt[Boolean].getOrElse(false)
    val scopeID =
      (body / "scope_id")
        .asOpt[String]
        .flatMap { _.toLongOption }
        .map { ScopeID(_) }

    def oops(msg: String) =
      Query.fail(new IllegalStateException(msg))

    def canRecover(snapTS: Timestamp, deletedTS: Timestamp) = {
      val retention = config.schema_retention_days.days
      snapTS.difference(deletedTS) < retention - 15.minutes // some buffer for safety
    }

    def undelete(
      scope: ScopeID,
      id: DatabaseID,
      deletedTS: Timestamp): Query[Boolean] =
      SchemaCollection.Database(scope) flatMap { dbs =>
        dbs.getVersion(id) flatMap {
          case None => oops(s"Database version for $scope and $id not found.")
          case Some(_: Version.Live) => oops(s"Database $scope $id is not deleted.")
          case Some(v: Version.Deleted) =>
            if (v.ts == Resolved(deletedTS)) {
              if (dryRun) {
                Query.True // pretend to undelete...
              } else {
                for {
                  _ <- dbs.removeVersion(id, v.versionID)
                  // Bump the schema version for the parent and child scopes.
                  _ <- CacheStore.updateSchemaVersion(scope)
                  childScope = v.prevVersion.get.data(Database.ScopeField)
                  _ <- CacheStore.updateSchemaVersion(childScope)
                } yield true
              }
            } else {
              Query.False
            }
        }
      }

    def recover(db: Database, deletedTS: Timestamp): Query[PlainResponse] =
      Database.foldDescendants(
        db.scopeID,
        List.empty[String],
        validTS = deletedTS.prevNano // list descendants prior to delete
      ) { case (acc, scope, id) =>
        Database.getUncached(scope, id) flatMap {
          case None => oops(s"Database $scope $id not found.")
          case Some(db) =>
            val path = db.namePath.mkString("/")
            db.deletedTS match {
              case None => oops(s"Database $path is not deleted.")
              case Some(ts) if ts != deletedTS => Query.value(acc)
              case _ =>
                undelete(scope, id, deletedTS) map {
                  case true  => path :: acc
                  case false => acc
                }
            }
        }
      } map { recoveredDBs =>
        OK(JSObject("recovered_dbs" -> recoveredDBs))
      }

    scopeID match {
      case None =>
        Future.successful(
          BadRequest(
            "Invalid or missing 'scope_id' fields."
          ))
      case Some(scopeID) =>
        withRepo {
          Query.snapshotTime flatMap { snapTS =>
            Cache
              .guardFromStalenessIf(scopeID, Database.latestForScope(scopeID)) {
                case None     => true
                case Some(db) => db.deletedTS.isEmpty
              }
              .flatMap {
                case None => Query.value(BadRequest("Database not found."))
                case Some(db) =>
                  db.deletedTS match {
                    case Some(delTS) if canRecover(snapTS, delTS) =>
                      recover(db, delTS)
                    case Some(_) =>
                      Query.value(BadRequest("Schema retention passed."))
                    case None => Query.value(BadRequest("Database is not deleted."))
                  }
              }
          }
        }
    }
  }

  GET / "admin" / "backup" / "dump-tree" / { req =>
    withRepo {
      Query.snapshotTime flatMap { snapTS =>
        val ts = req.params.get("snapshot_ts") map {
          Timestamp.parse(_)
        } getOrElse snapTS

        LookupStore.dumpDatabases(ts) map { entries =>
          OK(DumpEntry.toJSON(entries))
        }
      }
    }
  }

  POST / "admin" / "backup" / "check-tree" / { req =>
    (bodyJSON(req) / "path").asOpt[String] match {
      case Some(path) =>
        val fis = new FileInputStream(path)
        val entries = DumpEntry.fromStream(fis)

        Try(DatabaseTree.build(entries)) match {
          case Success(_) => Future.successful(OK(JSObject.empty))
          case Failure(ex) =>
            Future.successful(APIResponse.BadRequest(ex.getMessage))
        }
      case None =>
        Future.successful(APIResponse.BadRequest("Missing 'path' parameter."))
    }
  }

  POST / "admin" / "backup" / "allocate-scopes" / { req =>
    val globalIDOpt = req.params.get("global_id") flatMap {
      Database.decodeGlobalID(_)
    }

    val preserveGlobalIDs = req.params.get("preserve_global_ids") flatMap {
      _.toBooleanOption
    } getOrElse {
      false
    }

    def allocateScopes(node: DatabaseTree)
      : Query[Seq[(ScopeID, ScopeID, GlobalDatabaseID, GlobalDatabaseID)]] = {
      val oldScopeID = node.scopeID
      val oldGlobalID = node.globalID

      val newGlobalIDQ = if (preserveGlobalIDs) {
        Query.value(oldGlobalID)
      } else {
        Query.nextID map { GlobalDatabaseID(_) }
      }

      (Query.nextID, newGlobalIDQ) par { case (newScopeID, newGlobalID) =>
        val ids = (oldScopeID, new ScopeID(newScopeID), oldGlobalID, newGlobalID)

        val childrenQ = node.children map {
          allocateScopes(_)
        } sequence

        childrenQ map { c => ids +: c.flatten }
      }
    }

    (globalIDOpt, Try(DatabaseTree.fromJSON(bodyJSON(req)))) match {
      case (Some(globalID), Success(rootNode)) =>
        withRepo {
          Database.forGlobalID(globalID) flatMap {
            case None =>
              Query.value(APIResponse.BadRequest(s"global_id '$globalID' not found"))

            case Some(db) if db.globalID == rootNode.globalID =>
              Query.value(APIResponse.BadRequest("cannot import a tree on itself."))

            case Some(newRootDB) =>
              allocateScopes(rootNode) map { ids =>
                // remap the parent scope of root node
                val oldScopeID = rootNode.parentScopeID
                val newScopeID = newRootDB.scopeID

                val globalID = newRootDB.globalID

                val parentIDs = (oldScopeID, newScopeID, globalID, globalID)

                val mapping = (parentIDs +: ids) map {
                  case (oldScopeID, newScopeID, oldGlobalID, newGlobalID) =>
                    JSObject(
                      "old_scope_id" -> oldScopeID.toLong.toString,
                      "new_scope_id" -> newScopeID.toLong.toString,
                      "old_global_id" -> Database.encodeGlobalID(oldGlobalID),
                      "new_global_id" -> Database.encodeGlobalID(newGlobalID)
                    )
                }

                OK(JSObject("mapping" -> mapping))
              }
          }
        }

      case (globalID, tree) =>
        val b = Seq.newBuilder[String]

        if (globalID.isEmpty) {
          b += "global_id param required"
        }

        if (tree.isFailure) {
          b += "invalid dump-tree"
        }

        Future.successful(
          APIResponse.BadRequest(b.result().mkString("", " or ", ".")))
    }
  }

  POST / "admin" / "database" / "enable" / { req =>
    val globalIDOpt = req.params.get("global_id")

    val enableOpt = req.params.get("enable") flatMap {
      _.toBooleanOption
    }

    (globalIDOpt, enableOpt) match {
      case (Some(globalID), Some(enable)) =>
        withRepo {
          getDatabase(globalID) flatMap {
            case Left(_) =>
              Query.value(APIResponse.BadRequest(s"global_id '$globalID' not found"))

            case Right(db) if db.isDeleted =>
              Query.value(APIResponse.BadRequest(s"Database '$globalID' is deleted"))

            case Right(db) =>
              val auth = EvalAuth(db.parentScopeID, AdminPermissions)
              val ec =
                EvalContext.write(auth, Timestamp.MaxMicros, APIVersion.Default)

              DatabaseWriteConfig.Default.enable(ec, db.id.toDocID, enable) flatMap {
                case Right(_) => Query.value(())
                case Left(_) =>
                  Query.fail(
                    new IllegalStateException("Failed to enable/disable database"))
              } map { _ =>
                OK(
                  JSObject(
                    "message" -> s"Database '$globalID' updated successfully"))
              }
          }
        }
      case (_, _) =>
        Future.successful(
          APIResponse.BadRequest("global_id & enable parameters are required."))
    }
  }

  GET / "admin" / "document" / "location" / { req =>
    val scopeIDOpt = req.params.get("scope_id").flatMap(_.toLongOption)
    val collectionIDOpt = req.params.get("collection_id").flatMap(_.toLongOption)
    val docIDOpt = req.params.get("doc_id").flatMap(_.toLongOption)

    scopeIDOpt.zip(collectionIDOpt).zip(docIDOpt) match {
      case Some(((scope, collectionId), docId)) =>
        val scopeID = ScopeID(scope)
        withRepo {
          Database.forScope(scopeID) flatMap {
            case None =>
              Query.value(APIResponse.BadRequest(s"scope_id '$scopeID' not found"))
            case Some(_) =>
              Query.repo flatMap { repo =>
                val docID = DocID(SubID(docId), CollectionID(collectionId))
                Query.future(
                  repo.keyspace.locateDocumentSegment(scopeID, docID)) map {
                  case None =>
                    APIResponse.BadRequest(
                      "could not locate the document's segment.")
                  case Some((_, hostIDs)) =>
                    OK(
                      JSObject("hostIDs" -> new JSArray(
                        hostIDs.map(h => JSString(h.toString)).toList)))
                }
              }
          }
        }
      case _ =>
        Future.successful(
          APIResponse.BadRequest(
            "scope_id, collection_id and doc_id params required."))
    }

  }

  POST / "admin" / "txn-log" / "truncate" / { req =>
    (bodyJSON(req) / "epoch").asOpt[Long] match {
      case Some(ep) =>
        Try(services.txnPipeline.unsafeTruncateLog(Epoch(ep))) match {
          case Success(_) => Future.successful(APIResponse.NoContent)
          case Failure(e) => Future.successful(APIResponse.BadRequest(e.getMessage))
        }
      case None => Future.successful(APIResponse.BadRequest("missing epoch"))
    }
  }

  POST / "admin" / "log-topology" / "close-segment" / { req =>
    (bodyJSON(req) / "segmentID").asOpt[Int] match {
      case Some(sid) =>
        Try(services.closeLogSegment(SegmentID(sid))) match {
          case Success(closeAt) =>
            Future.successful(APIResponse.JSONResponse(
              200,
              JSObject(
                "status" -> s"segment $sid will be closed no later than ${closeAt.ceilTimestamp}")))
          case Failure(e) =>
            Future.successful(APIResponse.BadRequest(e.getMessage))
        }
      case None =>
        Future.successful(APIResponse.BadRequest("missing segment ID"))
    }
  }

  POST / "admin" / "log-topology" / "move-segment" / { req =>
    val json = bodyJSON(req)
    (
      (json / "segmentID").asOpt[Int],
      (json / "fromID").asOpt[String],
      (json / "toID").asOpt[String]) match {
      case (Some(sid), Some(from), Some(to)) =>
        Try(
          services.moveLogSegment(SegmentID(sid), HostID(from), HostID(to))) match {
          case Success(moveBy) =>
            Future.successful(APIResponse.JSONResponse(
              200,
              JSObject(
                "status" -> s"segment $sid will move hosts no later than ${moveBy.ceilTimestamp}")))
          case Failure(e) => Future.successful(APIResponse.BadRequest(e.getMessage))
        }
      case _ =>
        Future.successful(
          APIResponse.BadRequest("must specify segment ID and from and to host IDs"))
    }
  }

  def handleRequest(
    info: HttpRequestChannelInfo,
    httpReq: HttpRequest): Future[HttpResponse] = {

    auth(httpReq.authorization) match {
      case Some(auth) =>
        wrap(httpReq, auth) flatMap { req =>
          route(req) map { _.toHTTPResponse }
        } recoverWith {
          case ResponseError(r) =>
            repo flatMap { repo =>
              repo.result(r.toRenderedHTTPResponse(Timestamp.Epoch)) map { res =>
                val resp = res.value
                logRequest(None, APIEndpoint.Response(resp), info)
                resp
              }
            }

          case e =>
            Future.successful(
              fql1.TMPEndpoint.exceptionToResponse(e, stats).toHTTPResponse)
        } map { res =>
          if (config.log_queries) {
            logRequest(Some(httpReq), APIEndpoint.Response(res), info)
          }
          res
        }
      case None =>
        Future.successful(APIResponse.Unauthorized.toHTTPResponse)
    }
  }

  private def auth(auth: AuthType): Option[Auth] =
    auth match {
      case BasicAuth(secret, _) if rootKeys exists { BCrypt.check(secret, _) } =>
        Some(RootAuth)

      case _ =>
        None
    }

  private def wrap(req: HttpRequest, auth: Auth): Future[APIRequest] = {
    val version = req.getHeader(HTTPHeaders.FaunaDBAPIVersion) match {
      case Some(APIVersion(v)) => v
      case _                   => APIVersion.Default
    }

    val body = if (req.hasBody) {
      req.body.data map { Some(_) }
    } else {
      FutureNone
    }

    body map {
      APIRequest(req, _, Some(auth), version)
    }
  }

  private def route(req: APIRequest): Future[PlainResponse] = {
    val endpoint = req.method match {
      case HttpMethod.HEAD => endpoints(HttpMethod.GET, req.path)
      case method          => endpoints(method, req.path)
    }

    val response = endpoint match {
      case NotFound =>
        Future.successful(APIResponse.NotFound)
      case MethodNotAllowed(ms) =>
        Future.successful(APIResponse.MethodNotAllowed(ms))
      case Handler(args, handler) =>
        req.getAuth match {
          case Some(_) => handler(args, req)
          case None    => Future.successful(APIResponse.Unauthorized)
        }
    }

    response recover { case _: RoutingException =>
      APIResponse.NotFound
    } map { res =>
      if (req.method == HttpMethod.HEAD) {
        APIResponse.EmptyResponse(res.code, res.headers)
      } else {
        res
      }
    }
  }
}
