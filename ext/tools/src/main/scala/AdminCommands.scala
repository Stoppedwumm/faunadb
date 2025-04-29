package fauna.tools

import fauna.api.ReplicaTypeNames
import fauna.atoms._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.tasks.IndexRebuild
import fauna.net.http._
import fauna.net.util.URIEncoding
import fauna.repo.service.BarService._
import fauna.stats.QueryMetrics
import fauna.storage.{ CassandraHelpers, Storage }
import fauna.tools.error.{ Error, ErrorMessage }
import fauna.util.BCrypt
import java.io.{ FileInputStream, FileOutputStream, PrintStream }
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.cli.{ HelpFormatter, Options }
import org.apache.commons.text.WordUtils
import scala.annotation.tailrec
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{ Success, Try }

object AdminCommands {

  private val commands = Seq(
    "status" -> "Show cluster status.",
    "config [values|basic]" -> "Show FaunaDB configuration.",
    "movement-status" -> "Show status of data movements in cluster.",
    "show-identity" -> "Show host ID.",
    "host-id HOST" -> "Show the host ID of the provided HOST.",
    "host-version" -> "Show FaunaDB version information.",
    "host-versions" -> "Show FaunaDB version for all nodes in the cluster.",
    "host-info" -> "Show system information.",
    "host-build" -> "Show build information.",
    "init" -> "Initialize the first node in a cluster",
    "join HOST" -> s"Join a cluster through an active node specified in HOST. Sets GC grace period to ${Storage.ExtendedGracePeriod}.",
    "remove HOST_ID" -> "Remove a node identified by HOST_ID from the cluster. Removes which are not safe, such as those resulting in data loss, will return an error.",
    "show-replication" -> "Display replication configuration.",
    s"update-replica {${ReplicaTypeNames.Compute}|${ReplicaTypeNames.Data}|${ReplicaTypeNames.Log}} DC[ DC[ ...]]" -> "Set type of one or more named replicas.",
    "balance-replica DC" -> "Balance the ownership goal evenly across hosts in the specified replica.",
    "show-storage-version" -> "Display this node's storage version.",
    "update-storage-version" -> "Apply any pending updates to the cluster's storage layer.",
    "repair lookups PATH OUTPUT_PATH" -> "fixes lookups",
    "repair-task create [SCOPE_ID]" -> "Run data repair task. If optional SCOPE_ID is present, runs it for the specified scope.",
    "repair-task cancel" -> "Cancel data repair task.",
    "repair-task status" -> "Current repair task status.",
    "index-rebuild" -> s"Run data repair. Optionally specify one or more of [${String
        .join(", ", IndexRebuild.AllPhases map { p => s"--$p" } asJava)}] to limit the number of phases to be executed",
    "index-rebuild-promote-index2" -> "Promote the index2 CFs on this node. This should only be ran on a stopped node after the full index rebuild finished.",
    "backup-show" -> "Show all replica backups.",
    "backup-replica [NAME]" -> "Creates a backup of a replica with name NAME, if not specified to the node's replica name.",
    "create-snapshot NAME" -> "Creates a snapshot with name NAME.",
    "load-snapshot PATH [TARGET]" -> "Loads an existing snapshot from PATH. Optionally restricts loading only to a single TARGET host.",
    "show-snapshots" -> "Shows available snapshots.",
    "hash-key KEY" -> "Generates a hash of KEY.",
    "reseed-samples" -> "Re-seeds sample data for health checks.",
    "security-check" -> "Sanity check the security of the FaunaDB server files.",
    "security-repair" -> "Repair security issues with the FaunaDB server files.",
    "steal-tasks" -> "Steal pending tasks from removed nodes.",
    "list-tasks" -> "List all running tasks",
    "run-task TASK_ID" -> "Run a step of the task TASK_ID.",
    "get-task TASK_ID" -> "Prints detailed information for TASK_ID. Can optionally show history with --history.",
    "move-task TASK_ID [HOST]" -> "Moves the task TASK_ID to the host HOST or if not specified to the current host.",
    "cancel-task TASK_ID" -> "Cancel the task TASK_ID and any sub-tasks.",
    "complete-task TASK_ID" -> "Complete the task TASK_ID.",
    "reprioritize-task TASK_ID PRIORITY" -> "Re-prioritize the task TASK_ID to new PRIORITY. Negative numbers can be passed in, but require -- before the number. For example, -10 would be passed with `-- -10`",
    "pause-task TASK_ID" -> "Pause the execution of the task TASK_ID.",
    "unpause-task TASK_ID" -> "Un-pause the execution of the task TASK_ID.",
    "index-force-build SCOPE_ID COLLECTION_ID" -> "Starts a background index build on a collection.",
    "index-drop SCOPE_ID INDEX_ID" -> "Drops the hidden index identified by SCOPE_ID and INDEX_ID.",
    "index-swap SCOPE_ID INDEX_ID" -> "Rebuilds the index identified by SCOPE_ID and INDEX_ID.",
    "repartition-index-scan SOURCE_TASK_ID TARGET_HOST" -> "Repartition the index-build-scan task, assigning parts of segments to the target host.",
    "repartition-migration-scan SOURCE_TASK_ID TARGET_HOST " -> "Repartition the migration-scan task, assigning parts of segments to the target host.",
    "enable-traced-secret SECRET" -> "Enable tracing of the specified SECRET on this node.",
    "disable-traced-secret SECRET" -> "Disable tracing of the specified SECRET on this node.",
    "list-traced-secrets" -> "List the secrets traced by this node.",
    "get-index2cf-gc-grace-period" -> "Get the C* gc grace period for tombstones on the SortedIndex_2 and HistoricalIndex_2 CFs.",
    "set-index2cf-gc-grace-period DAYS" -> "Dynamically set the C* gc grace period for tombstones on the SortedIndex_2 and HistoricalIndex_2 CFs.",
    "scope-from-account ACCOUNT_ID" -> "Retrieve the root scope ID for an account",
    "move-database SRC_ACCOUNT NAME DEST_ACCOUNT" -> "Moves a database named NAME from account SRC_ACCOUNT to account DEST_ACCOUNT. NAME may be a slash-separated path to move a sub-scope.",
    "move-database TARGET_ID DESTINATION_ID [--restore]" -> "Moves the database TARGET_ID to DESTINATION_ID. If TARGET_ID is logically the same database as DESTINATION_ID, --restore must be provided.",
    "global-to-scope GLOBAL_ID" -> "Translates the GLOBAL_ID of a database to its SCOPE_ID.",
    "reprioritize-account ACCOUNT_ID PRIORITY" -> "Re-prioritize the account ACCOUNT_ID to new PRIORITY.",
    "locate-document SCOPE_ID COLLECTION_ID DOC_ID" -> "Locate the hostIDs which contains the document identified by SCOPE_ID, COLLECTION_ID & DOC_ID.",
    "export [REQUEST_FILE]" -> "Execute data export, if REQUEST_FILE is omitted the command will read from standard input.",
    "export-status" -> "Current data export status.",
    "dump-tree [OUTPUT]" -> "Dumps the tree hierarchy of the current node.",
    "check-tree FILE" -> "Checks that the given file is a valid dump-tree.",
    "allocate-scopes [GLOBAL_ID] [TREE_FILE]" -> "Allocate new IDs for the tree described in TREE_FILE.",
    "enable-database [GLOBAL_ID]" -> "Enable the GLOBAL_ID database.",
    "disable-database [GLOBAL_ID]" -> "Disable the GLOBAL_ID database.",
    "truncate-txn-log [EPOCH]" -> "Truncate the local transaction log up to and including EPOCH. This is an unsafe operation. Only use it if you know what you are doing.",
    "close-log-segment [SEGMENT_ID]" -> "Close the log segment. A new one will be opened in its place.",
    "move-log-segment [SEGMENT_ID] [FROM_ID] [TO_ID]" -> "Move the log segment between two hosts in the same replica.",
    "account-limits ACCOUNT_ID" -> "Get the rate limit configuration for account ACCOUNT_ID.",
    "clear-mvt-cache" -> "Invalidates all cached MVT hints.",
    "get-gc-grace" -> "Gets the node's GC grace period.",
    "set-standard-gc-grace" -> "Sets the node's GC grace period to the standard value.",
    "set-extended-gc-grace" -> "Sets the node's GC grace period to the extended value.",
    "force-migrate SCOPE_ID COLLECTION_ID" -> "Starts a background migration task on a collection.",
    "reset-migrations SCOPE_ID COLLECTION_ID rebuild|no-rebuild" -> "Resets the migrations on a collection.",
    "recover-database SCOPE_ID [--dry-run]" -> "Recover a deleted database.",
    "list-compactions" -> "List in-progress compactions on this node."
  )

  private val envvars = Seq(
    "FAUNADB_CONFIG" -> "Path to FaunaDB config file.",
    "FAUNADB_ROOT_KEY" -> "FaunaDB cluster root key.",
    "FAUNADB_HOST" -> "Host to connect to.")

  private def printMap(mp: Seq[(String, String)]): Unit = {
    // If the typical unix COLUMNS env var is set then let's be nice
    // and adjust our usage width to the admins screen. If it is not set
    // then use the current behavior of wrapping
    val maxWidth = Option(System.getenv("COLUMNS")) flatMap { _.toIntOption }
    val maxLen = mp.foldLeft(0) { case (acc, (a, _)) => acc max a.length }
    mp foreach { case (a, b) => printMsg(a, b, maxWidth, maxLen) }
  }

  private def printMsg(
    hdr: String,
    desc: String,
    maxWidth: Option[Int],
    indent: Int) = {
    val fmt = s"%-${indent}s  %s"
    if (maxWidth.isDefined) {
      val nl = "\n" + " " * (indent + 4)
      val tmp = WordUtils.wrap(desc, maxWidth.get)
      println(fmt.format(hdr, tmp.replace("\n", nl)))
    } else {
      println(fmt.format(hdr, desc))
    }
  }

  def printUsage(options: Options): Unit = {
    val formatter = new HelpFormatter
    formatter.printHelp("faunadb-admin COMMAND [args]", options)

    println("\nCOMMANDS:")
    printMap(commands)

    println("\nENVIRONMENT:")
    printMap(envvars)
    println()
  }

  type OutputHandler = JSValue => Unit

  sealed abstract class AdminCommand(
    api: HttpClient,
    key: String,
    asJson: Boolean,
    outStream: PrintStream = System.out,
    errStream: PrintStream = System.err)
      extends (() => Boolean) {

    val outputHandler = if (asJson) jsonOutput else textOutput

    def apply(): Boolean =
      execute map { outputHandler(_) } map { _ => true } getOrElse false

    protected def jsonOutput: OutputHandler =
      AdminCommand.DefaultJsonOutput(outStream)

    protected def textOutput: OutputHandler =
      AdminCommand.DefaultTextOutput(outStream)

    protected def postAndValidate(
      path: String,
      body: HttpBody = NoBody,
      query: String = null,
      message: ErrorMessage = ErrorMessage.Common) = {
      waitForAndValidate(api.post(path, body, key, query), message)
    }

    protected def getAndValidate(
      path: String,
      query: String = null,
      message: ErrorMessage = ErrorMessage.Common) = {
      waitForAndValidate(api.get(path, key, query), message)
    }

    protected def putAndValidate(
      path: String,
      body: HttpBody = NoBody,
      query: String = null,
      message: ErrorMessage = ErrorMessage.Common) = {
      waitForAndValidate(api.put(path, body, key, query), message)
    }

    protected def deleteAndValidate(
      path: String,
      query: String = null,
      message: ErrorMessage = ErrorMessage.Common) = {
      waitForAndValidate(api.delete(path, key, query), message)
    }

    private def waitForAndValidate(
      future: Future[HttpResponse],
      message: ErrorMessage) = {
      Try {
        Await.result(future, Duration.Inf)
      } filter { rsp =>
        validateHttpResponse(rsp, message, errStream)
      }
    }

    protected def execute: Try[JSValue]
  }

  /** AdminCommand helpers.
    */
  object AdminCommand {
    def DefaultJsonOutput(out: PrintStream): OutputHandler = _.writeTo(out, true)

    def DefaultTextOutput(out: PrintStream): OutputHandler = {
      case JSString(s) => out.println(s)
      case js          => out.println(js.toString)
    }
  }

  /** *** DANGER ***
    * SkipTransactions.
    */
  final case class SkipTransactions(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val lat = (js / "lat").as[String]
      println(
        s"Skipped transactions up to $lat. Restart FaunaDB to complete the recovery process.")
    }

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/skip_transactions") map { extractJsonResponse(_) }
  }

  final case class UnlockStorage(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/storage/unlock") map { _ =>
        JSString("Storage unlocked. Restart this process to continue.")
      }
  }

  final case class CreateReplicaBackup(
    api: HttpClient,
    key: String,
    backupName: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val (name, path) = backupName match {
      case Some(n) => (n, s"/admin/backup/replica/$n")
      case None    => ("backup", s"/admin/backup/replica")
    }

    override protected def textOutput: OutputHandler = { js =>
      val backupDetail = (js / "backup" / "msgs").as[Seq[String]]
      val backupOK = (js / "backup" / "successful").as[Boolean]
      if (backupOK) {
        println(s"Backup $name created successfully.")
        backupDetail.foreach { x => println(s"    $x") }
      } else {
        System.err.println(s"Error in creating backup $name.")
        backupDetail.foreach { x => System.err.println(s"    $x") }
      }
    }

    protected def execute: Try[JSValue] =
      postAndValidate(path) map { extractJsonResponse(_) }
  }

  final case class CreateSnapshot(
    api: HttpClient,
    key: String,
    name: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate(s"/admin/snapshots/$name") map {
        extractJsonResponse(_)
      }
  }

  final case class FixLookups(
    api: HttpClient,
    key: String,
    path: String,
    outputPath: String,
    dryRun: Boolean,
    asJson: Boolean
  ) extends AdminCommand(api, key, asJson) {
    protected def execute: Try[JSValue] =
      postAndValidate(
        path = s"/admin/repair/lookups",
        body = JSObject(
          "path" -> path,
          "outputPath" -> outputPath,
          "dryRun" -> dryRun).toString) map {
        extractJsonResponse(_)
      }
  }

  final case class LoadSnapshot(
    api: HttpClient,
    key: String,
    path: String,
    target: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate(
        path = "/admin/load",
        body = JSObject("path" -> path, "target" -> target).toString
      ) map { _ =>
        JSString("OK")
      }
  }

  /** This function will take a name on disk and pretty print it
    * Replica backups have a name with two parts a name and a timestamp in seconds
    * Simple snapshots have a name/label only
    *
    * @param backupList    The list of backup name to format
    * @param clusterBackupsOnly  Only show cluster backups
    * @return               The formated list of names
    */
  private def prettyPrintBackupList(
    backupList: Seq[String],
    clusterBackupsOnly: Boolean = true): Seq[String] = {
    val s2 = if (clusterBackupsOnly) {
      backupList filter { _.contains(ClusterBackupSep) }
    } else {
      backupList
    }
    val df = new SimpleDateFormat("MM-dd-yy HH:mm:ss")
    val formatedList = s2 map { s =>
      s.split(ClusterBackupSep) match {
        case Array(name, timestampSeconds, _) =>
          val d = new Date(timestampSeconds.toLong.seconds.toMillis)
          s"${name} taken at ${df.format(d)}"
        case _ => s
      }
    }
    formatedList
  }

  final case class ShowClusterBackups(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val snapshots = (js / "resource" / "snapshots").as[Seq[String]]
      println(prettyPrintBackupList(snapshots).mkString("\n"))
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/snapshots") map { extractJsonResponse(_) }
  }

  final case class ShowSnapshots(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val snapshots = (js / "resource" / "snapshots").as[Seq[String]]
      println(prettyPrintBackupList(snapshots, false).mkString("\n"))
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/snapshots") map { extractJsonResponse(_) }
  }

  final case class RestoreStatus(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {
    override protected def execute: Try[JSValue] =
      getAndValidate("/admin/restore/status") map { extractJsonResponse(_) }
  }

  final case class TakeBackup(
    api: HttpClient,
    key: String,
    name: String,
    from: Option[Long],
    to: Option[Long],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = Map {
      "from" -> from
      "to" -> to
    } flatMap { case (key, value) =>
      value map { ts =>
        s"$key=$ts"
      }
    } mkString "&"

    protected def execute: Try[JSValue] =
      postAndValidate(s"/admin/backup/$name", query = params) map { _ =>
        JSString(s"Backup $name created.")
      }
  }

  final case class UpdateReplicas(
    api: HttpClient,
    replicaType: String,
    replicas: Seq[String],
    key: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override def apply(): Boolean = {
      if (replicas.isEmpty) {
        outputHandler(
          JSString(s"No replicas were given. Please specify at least one replica."))
        false
      } else {
        if (updateReplicasNoPrint(api, replicaType, replicas, key)) {
          val strReplicas = replicas.mkString(", ")
          val s = if (replicas.size > 1) "s" else ""
          outputHandler(
            JSString(s"Updated replica$s $strReplicas to type $replicaType."))
          true
        } else {
          false
        }
      }
    }

    protected def execute: Try[JSValue] = Success(JSNull)
  }

  private def updateReplicasNoPrint(
    api: HttpClient,
    replicaType: String,
    replicas: Seq[String],
    key: String): Boolean =
    changeReplication(api, key) { rs =>
      rs ++ (replicas map { r =>
        r -> replicaType
      })
    }

  @tailrec
  private def changeReplication(api: HttpClient, key: String)(
    f: Map[String, String] => Map[String, String]): Boolean =
    getReplication(api, key) match {
      case Some((rep, etag)) =>
        putReplication(api, f(rep), etag, key) match {
          case UpdatedSucceeded => true
          case UpdatedFailed    => false
          case UpdateRetry      => changeReplication(api, key)(f)
        }
      case None =>
        false
    }

  private def getReplication(
    api: HttpClient,
    key: String): Option[(Map[String, String], Option[String])] = {
    val uResp = Await.result(api.get("/admin/replication", key), Duration.Inf)
    val success = validateHttpResponse(uResp)

    if (success) {
      val json = extractJsonResponse(uResp)
      val jsReplicas = (json / "resource" / "replicas").as[JSArray]
      val repMap = jsReplicas.value map { r =>
        (r / "name").as[String] -> (r / "type").as[String]
      } toMap

      Some((repMap, uResp.getHeader(HTTPHeaders.ETag)))
    } else {
      None
    }
  }

  sealed trait UpdateResult
  case object UpdatedSucceeded extends UpdateResult
  case object UpdatedFailed extends UpdateResult
  case object UpdateRetry extends UpdateResult

  private def putReplication(
    api: HttpClient,
    replicas: Map[String, String],
    etag: Option[String],
    key: String): UpdateResult = {
    val jsReplicas = new JSArray(replicas map { case (name, rtype) =>
      JSObject("name" -> name, "type" -> rtype)
    } toSeq)
    val resp = Await.result(
      api.put(
        "/admin/replication",
        JSObject("replicas" -> jsReplicas).toString,
        key,
        headers = etag map { t =>
          HTTPHeaders.IfMatch -> t
        } toSeq),
      Duration.Inf)
    if (resp.status.code == 412) {
      UpdateRetry
    } else if (validateHttpResponse(resp)) {
      UpdatedSucceeded
    } else {
      UpdatedFailed
    }
  }

  final case class ShowReplication(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override def apply(): Boolean = {
      getReplication(api, key) match {
        case Some((repInfo, _)) =>
          // FIXME: use adjustable width
          outputHandler(JSString("\treplica\ttype"))
          repInfo.toSeq.sorted.zipWithIndex.foreach { case ((name, rtype), i) =>
            outputHandler(JSString(s"${i + 1}\t$name\t$rtype"))
          }

          true
        case None => false
      }
    }

    protected def execute: Try[JSValue] = Success(JSNull)
  }

  final case class BalanceReplica(
    api: HttpClient,
    key: String,
    replicaName: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/balance_replica",
        JSObject("replicaName" -> replicaName).toString) map { _ =>
        JSString(s"Rebalanced replica $replicaName.")
      }
  }

  final case class NeedsCleanup(
    api: HttpClient,
    key: String,
    cf: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      val ext = cf match {
        case None     => ""
        case Some(cf) => s"/$cf"
      }
      getAndValidate(s"/admin/storage/clean$ext") map { extractJsonResponse(_) }
    }
  }

  final case class CleanStorage(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      putAndValidate("/admin/storage/clean") map { _ =>
        JSString("Storage cleanup scheduled.")
      }
  }

  final case class ShowStorageVersion(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/storage/version") map { extractJsonResponse(_) }
  }

  final case class UpdateStorageVersion(
    api: HttpClient,
    key: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      putAndValidate("/admin/storage/version") map { _ =>
        JSString("Storage version updated.")
      }
  }

  final case class Repair(
    api: HttpClient,
    key: String,
    dryRun: Boolean,
    dataOnly: Boolean,
    scopeID: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = Seq.newBuilder[String]

    if (dryRun) {
      params += "dry_run=true"
    }

    if (dataOnly) {
      params += "data_only=true"
    }

    scopeID foreach { scope =>
      params += s"filter_scope=$scope"
    }

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/repair_task",
        query = params.result().mkString("&")) map { _ =>
        JSString("Repair started.")
      }
  }

  final case class CancelRepair(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/repair_task/cancel") map { _ =>
        JSString("Repair cancelled.")
      }
  }

  final case class RepairStatus(
    api: HttpClient,
    key: String,
    asJson: Boolean,
    out: PrintStream = System.out)
      extends AdminCommand(api, key, asJson, outStream = out) {

    override protected def textOutput: OutputHandler = { js =>
      val pending = (js / "resource" / "pending").as[Boolean]
      val stage = (js / "resource" / "stage").asOpt[String]
      (pending, stage) match {
        case (true, Some(stage)) => println(s"Repair running. Current step: $stage")
        case _                   => println("Repair idle.")
      }
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/repair_task/status") map { extractJsonResponse(_) }
  }

  final case class IndexRebuildStart(
    api: HttpClient,
    key: String,
    phases: Vector[String],
    maxTaskID: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = Seq.newBuilder[String]

    if (phases.nonEmpty) {
      val value = String.join(",", phases.asJava)
      params += s"phases=$value"
    }

    maxTaskID foreach { id =>
      params += s"max_task_id=$id"
    }

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/index_rebuild/start",
        query = params.result().mkString("&")) map { _ =>
        JSString("Repair started.")
      }
  }

  final case class IndexRebuildCancel(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/index_rebuild/cancel") map { _ =>
        JSString("Repair cancelled.")
      }
  }

  final case class IndexRebuildStatus(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val pending = (js / "resource" / "pending").as[Boolean]
      val stage = (js / "resource" / "stage").asOpt[String]
      (pending, stage) match {
        case (true, Some(stage)) =>
          println(s"Index rebuild running. Current step: $stage")
        case _ => println("Index rebuild idle.")
      }
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/index_rebuild/status") map { extractJsonResponse(_) }
  }

  def indexRebuildPromoteIndex2CFs(storagePath: String): Boolean =
    try {
      CassandraHelpers.promoteIndex2CFs(storagePath)
      true
    } catch {
      case e: Throwable =>
        System.err.println(s"Index file promotion failed: $e")
        false
    }

  final case class IndexRebuildRepartitionSegments(
    api: HttpClient,
    key: String,
    taskID: String,
    targetHost: String,
    targetTaskID: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params =
      Seq(s"task_id=$taskID", s"target_host=$targetHost") ++
        (targetTaskID map { t => s"target_task_id=$t" }).toSeq

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/index_rebuild/repartition-segments",
        query = params.mkString("&")) map { _ =>
        JSString(s"Index rebuild local-index-build task repartitioned.")
      }
  }

  final case class ShowIdentity(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val id = (js / "identity").as[String]
      println(id)
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/identity") map { extractJsonResponse(_) }
  }

  final case class GetHostID(
    api: HttpClient,
    key: String,
    hostIP: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val id = (js / "identity").as[String]
      println(id)
    }

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/host-id", JSObject("node" -> hostIP).toString) map {
        extractJsonResponse(_)
      }
  }

  def hashKey(key: String): Boolean = {
    println(BCrypt.hash(key))
    true
  }

  final case class ReseedSamples(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/reseed") map { _ =>
        JSString("Sample data re-seeded.")
      }
  }

  final case class StealTasks(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/steal") map { _ =>
        JSString("Tasks stolen.")
      }
  }

  case class TaskInfo(
    id: Long,
    parent: Option[Long],
    name: String,
    scope: Long,
    account: Long,
    priority: Int,
    state: String,
    host: String)

  object TaskInfo {

    def apply(json: JSValue): TaskInfo =
      TaskInfo(
        (json / "id").as[Long],
        (json / "parent").asOpt[Long],
        (json / "name").as[String],
        (json / "scope").as[Long],
        (json / "account").as[Long],
        (json / "priority").as[Int],
        (json / "state").as[String],
        (json / "host").as[String]
      )
  }

  final case class ListTasks(
    api: HttpClient,
    key: String,
    scope: Option[ScopeID],
    collection: Option[CollectionID],
    index: Option[IndexID],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val tasks = (js / "tasks").as[JSArray]

      val grouped = tasks.value map { TaskInfo(_) } groupBy {
        _.parent
      } withDefaultValue Seq.empty

      def calculateWidths(
        parent: Option[Long],
        level: Int,
        actual: List[Int]): List[Int] = {
        val children = grouped(parent)

        children.foldLeft(actual) {
          case (id :: name :: account :: priority :: state :: host :: Nil, child) =>
            calculateWidths(
              Some(child.id),
              level + 1,
              List(
                id max child.id.toString.length + 2 * level,
                name max child.name.length,
                account max child.account.toString.length,
                priority max child.priority.toString.length,
                state max child.state.length,
                host max child.host.length
              )
            )

          case _ =>
            throw new IllegalArgumentException
        }
      }

      val headers = List("TaskId", "Name", "Account", "Priority", "State", "Host")

      val widths = calculateWidths(None, 0, headers.map { _.length })

      val fmt = (widths map { w =>
        s"%-${w}s"
      }).mkString("  ")

      def printChildTasks(parent: Option[Long], level: Int): Unit = {
        val children = grouped(parent) sortBy { _.id }

        children.foreach { child =>
          val str = fmt.format(
            (" " * level * 2) + child.id,
            child.name,
            child.account,
            child.priority,
            child.state,
            child.host
          )

          println(str)

          printChildTasks(Some(child.id), level + 1)
        }
      }

      println(fmt.format(headers: _*))
      printChildTasks(None, 0)
    }

    protected def execute: Try[JSValue] =
      getAndValidate(
        "/admin/tasks/list",
        query = (
          Nil
            ++ scope.map { s => s"scope=${s.toLong}" }
            ++ collection.map { c => s"collection=${c.toLong}" }
            ++ index.map { i => s"index=${i.toLong}" }
        ).mkString("&")
      ) map {
        extractJsonResponse(_)
      }
  }

  final case class DryRunTask(
    api: HttpClient,
    key: String,
    taskID: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val oldState = js / "old_state"
      val newState = js / "new_state"

      println("Old")
      println(s"State: ${oldState / "name"}")
      println(s"Data : ${oldState / "data"}")

      println()
      println("New")
      println(s"State: ${newState / "name"}")
      println(s"Data : ${newState / "data"}")
    }

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/dry-run", query = s"task_id=$taskID") map {
        extractJsonResponse(_)
      }
  }

  final case class GetTask(
    api: HttpClient,
    key: String,
    taskID: String,
    ts: Option[String],
    history: Boolean,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = List.newBuilder[String]
    params += s"task_id=$taskID"
    params += s"history=$history"
    ts foreach { ts => params += s"ts=$ts" }

    override protected def textOutput: OutputHandler = { js =>
      def printTask(js: JSValue) = {
        println(s"TaskID  : ${js / "id"}")
        println(s"ParentID: ${js / "parent"}")
        println(s"Name    : ${js / "name"}")
        println(s"Priority: ${js / "priority"}")
        println(s"Host    : ${js / "host"}")
        println(s"State   : ${js / "state" / "name"}")
        println(s"Data    : ${(js / "state" / "data").asOpt[JSValue]}")
      }
      if (history) {
        val versions = (js / "versions").as[Seq[JSObject]]
        versions foreach { ver =>
          println(
            s">>>> at ${Timestamp.ofMicros((ver / "ts").as[Long])} or ${ver / "ts"}")
          printTask(ver)
        }
      } else {
        printTask(js)
      }
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/tasks/get", query = params.result().mkString("&")) map {
        extractJsonResponse(_)
      }
  }

  final case class MoveTask(
    api: HttpClient,
    key: String,
    taskID: String,
    host: Option[String],
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = List.newBuilder[String]
    params += s"task_id=$taskID"
    host foreach { host => params += s"host=$host" }

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/tasks/move",
        query = params.result().mkString("&")) map { _ =>
        JSString(s"Task $taskID successfully moved")
      }
  }

  final case class CancelTask(
    api: HttpClient,
    key: String,
    taskID: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/cancel", query = s"task_id=$taskID") map { _ =>
        JSString(s"Task $taskID successfully cancelled")
      }
  }

  final case class CompleteTask(
    api: HttpClient,
    key: String,
    taskID: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/complete", query = s"task_id=$taskID") map { _ =>
        JSString(s"Task $taskID successfully completed")
      }
  }

  final case class ReprioritizeTask(
    api: HttpClient,
    key: String,
    taskID: String,
    priority: Int,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = s"task_id=$taskID&priority=$priority"

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/reprioritize", query = params) map { _ =>
        JSString(s"Task $taskID successfully re-prioritized")
      }
  }

  final case class PauseTask(
    api: HttpClient,
    key: String,
    taskID: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = s"task_id=$taskID"

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/pause", query = params) map { _ =>
        JSString(s"Task $taskID successfully paused")
      }
  }

  final case class UnpauseTask(
    api: HttpClient,
    key: String,
    taskID: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = s"task_id=$taskID"

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/tasks/unpause", query = params) map { _ =>
        JSString(s"Task $taskID successfully un-paused")
      }
  }

  final case class RepartitionIndexScan(
    api: HttpClient,
    key: String,
    taskID: String,
    targetHost: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = Seq(s"task_id=$taskID", s"target_host=$targetHost")

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/tasks/repartition-index-scan",
        query = params.mkString("&")) map { _ =>
        JSString(s"Index scan task repartitioned.")
      }
  }

  final case class RepartitionMigrationScan(
    api: HttpClient,
    key: String,
    taskID: String,
    targetHost: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    private val params = Seq(s"task_id=$taskID", s"target_host=$targetHost")

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/tasks/repartition-migration-scan",
        query = params.mkString("&")) map { _ =>
        JSString(s"Migration scan task repartitioned.")
      }
  }

  final case class EnableTracedSecret(
    api: HttpClient,
    key: String,
    secret: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/tracing/enable",
        query = s"secret=${URIEncoding.encode(secret)}") map { _ =>
        JSString(s"Enabled tracing for secret=$secret")
      }
  }

  final case class DisableTracedSecret(
    api: HttpClient,
    key: String,
    secret: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/tracing/disable",
        query = s"secret=${URIEncoding.encode(secret)}") map { _ =>
        JSString(s"Disabled tracing for secret=$secret")
      }
  }

  final case class ListTracedSecrets(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/tracing/list") map { extractJsonResponse(_) }
  }

  case class GetIndex2CfGcGracePeriod(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/index2cf-gc-grace") map { extractJsonResponse(_) }
  }

  case class SetIndex2CfGcGracePeriod(
    api: HttpClient,
    key: String,
    days: Int,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      postAndValidate("/admin/index2cf-gc-grace", query = s"days=$days") map {
        extractJsonResponse(_)
      }
  }

  final case class GetFaunaVersion(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      println("Fauna %s".format((js / "version").as[String]))
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/host-version") map { extractJsonResponse(_) }
  }

  case class NodeVersion(
    hostID: String,
    replica: String,
    hostName: String,
    buildVersion: String)

  object NodeVersion {

    def apply(obj: JSObject): NodeVersion =
      NodeVersion(
        (obj / "host_id").as[String],
        (obj / "replica").asOpt[String].getOrElse("n/a"),
        (obj / "host_name").asOpt[String].getOrElse("n/a"),
        (obj / "build_version").asOpt[String].getOrElse("n/a")
      )
  }

  final case class GetHostVersions(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val nodes = (js / "nodes").as[Seq[JSObject]] map {
        NodeVersion(_)
      }

      val headers = List("HostID", "Address", "Version")

      val widths = nodes.foldLeft(headers map { _.length }) {
        case (hostID :: address :: buildVersion :: Nil, n) =>
          List(
            hostID max n.hostID.length,
            address max n.hostName.length,
            buildVersion max n.buildVersion.length
          )

        case _ =>
          throw new IllegalArgumentException
      }
      val fmt = (widths map { w =>
        s"%-${w}s"
      }).mkString("  ")

      val nlist = (nodes groupBy { _.replica } toList) sortBy { _._1 }
      nlist foreach { case (replica, nodes) =>
        val dcStr = s"Replica: $replica"
        val rows = nodes.sortBy { _.hostName } map { n =>
          fmt.format(n.hostID, n.hostName, n.buildVersion)
        }

        println("")
        println(dcStr)
        println("=" * dcStr.length)
        println(fmt.format(headers: _*))
        rows foreach println
      }
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/host-versions") map { extractJsonResponse(_) }
  }

  final case class GetHostInfo(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      def formatSize(info: JSValue, name: String): String = {
        val value = ((info / name).as[Long])
        value.humanReadableSize()
      }

      val maxmem = (js / "JVMMaxMemory").as[Long] match {
        case 0 => "unlimited"
        case x => x.humanReadableSize()
      }

      println(
        "%-20s %s\n".format("OS Name", (js / "OSName").as[String]) +
          "%-20s %s\n".format("OS Type", (js / "OSType").as[String]) +
          "%-20s %s\n".format("OS Version", (js / "OSVersion").as[String]) +
          "%-20s %d\n".format("Cores", (js / "AvailableCores").as[Long]) +
          "%-20s %s\n".format("JVM Version", (js / "JavaVersion").as[String]) +
          "%-20s %s\n".format("JVM Maximum Memory", maxmem) +
          "%-20s %s\n".format("JVM Free Memory", formatSize(js, "JVMFreeMemory")) +
          "%-20s %s\n"
            .format("JVM Used Memory", formatSize(js, "JVMInUseMemory")) +
          "%-20s %s\n".format(
            "Node IP(s)",
            (js / "HostIP").as[Seq[String]] mkString (", "))
      )
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/host/info") map { extractJsonResponse(_) }
  }

  final case class GetBuildInfo(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      println(
        "%-20s %s\n".format("Fauna Version", (js / "faunaVersion").as[String]) +
          "%-20s %s\n".format("Fauna Revision", (js / "revision").as[String]) +
          "%-20s %s\n".format("Scala Compiler", (js / "scalaVersion").as[String]) +
          "%-20s %s\n".format("SBT Version", (js / "sbtVersion").as[String]) +
          "%-20s %s\n".format(
            "Build Timestamp",
            (js / "buildTimeStamp").as[String]) +
          "%-20s %s\n".format("Branch Name", (js / "buildBranch").as[String]) +
          "%-20s %s\n".format("Git Hash", (js / "buildFullHash").as[String])
      )
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/host/build") map { extractJsonResponse(_) }
  }

  final case class SecureFiles(
    api: HttpClient,
    key: String,
    repair: Boolean,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val unchecked = (js / "unchecked_perms").as[Int]
      val checked = (js / "checked_perms").as[Int]
      val corrected = (js / "repaired_perms").as[Int]
      val found = (js / "insecure_perms").as[Int]

      println("File Permissions Check\n===============")
      println(s"$checked / ${unchecked + checked} files checked")
      println(s"$found files with world-accessible mode bits found")
      if (repair) {
        println(s"$corrected files with world-accessible mode bits fixed")
      }
      if (found > 0 || unchecked > 0) {
        println(
          s"Please check core.log on ${api.host} for additional information.\n")
      }
    }

    protected def execute: Try[JSValue] = {
      val res = if (repair) {
        postAndValidate("/admin/security/repair-files")
      } else {
        getAndValidate("/admin/security/check-files")
      }
      res map { extractJsonResponse(_) }
    }
  }

  case class ConfigItem(
    name: Option[String],
    value: Option[JSValue],
    default: Option[JSValue],
    required: Option[Boolean],
    deprecated: Option[Boolean],
    basic: Option[Boolean])

  object ConfigItem {

    def apply(obj: JSObject): ConfigItem =
      ConfigItem(
        (obj / "name").asOpt[String],
        (obj / "value").asOpt[JSValue],
        (obj / "default").asOpt[JSValue],
        (obj / "required").asOpt[Boolean],
        (obj / "deprecated").asOpt[Boolean],
        (obj / "basic").asOpt[Boolean]
      )
  }

  // Report sizes for the Config information
  val configScreenSize = 80
  val configWidthCol1 = (configScreenSize * 0.4375).toInt // 35
  val configWidthCol2 = (configScreenSize * 0.3125).toInt // 25
  val configWidthCol3 = (configScreenSize * 0.125).toInt // 10

  case class ConfigLine(
    name: String,
    value: String,
    depreicated: String,
    required: String,
    isDefault: Boolean)

  private def printConfig(cfg: ConfigLine): Unit = {

    val col1 = configWidthCol1
    // Slide for the previous column overflowing
    val col2 = configWidthCol2 - Math.max(0, cfg.name.length - col1)
    val col3 = configWidthCol3 - Math.max(0, cfg.value.length - col2)

    println(
      (s"| %-${col1}s | %-${col2}s | %${col3}s |")
        .format(cfg.name, cfg.value, (cfg.depreicated + " " + cfg.required).trim))
  }

  final case class NodeConfig(
    api: HttpClient,
    key: String,
    opt: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val cfg = (js / "config").as[Seq[JSObject]] map {
        ConfigItem(_)
      }
      // Sanitize the data and create list of the config
      val cfglist = cfg filter { _.name.isDefined } map { c =>
        val (item, dodef) = (c.value, c.default) match {
          case (Some(_: JSNull), _)   => ("", false)
          case (Some(v: JSString), _) => (v.value, false)
          case (Some(v: JSValue), _)  => (v.toString, false)
          case (_, Some(_: JSNull))   => ("", true)
          case (_, Some(d: JSString)) => (d.value, true)
          case (_, Some(d: JSValue))  => (d.toString, true)
          case (_, _)                 => ("", false)
        }

        val req = c.required match {
          case Some(true)  => "Required"
          case Some(false) => ""
          case None        => "Required  << Not Set >>"
        }

        val dep = c.deprecated match {
          case Some(true)  => "Deprecated"
          case Some(false) => ""
          case None        => "Deprecated << Not Set>>"
        }

        ConfigLine(c.name.get, item, dep, req, dodef)
      }

      // Print the header to the table
      println("=" * configScreenSize)
      println(
        s"| %-${configWidthCol1}s ".format("Name") +
          s"| %-${configWidthCol2}s ".format("Value") +
          s"| %-${configWidthCol3}s |".format("Options"))
      println("=" * configScreenSize)

      // Print the body based on command line options
      opt.toLowerCase match {
        //  All configuration
        case "all" =>
          cfglist sortBy { _.name } foreach { printConfig }
        //  Default: Configuration tag as basic or has been set
        case _ =>
          cfglist filter { _.value.nonEmpty } sortBy { _.name } foreach {
            printConfig
          }
      }

      println("=" * configScreenSize)
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/host/config") map { extractJsonResponse(_) }
  }

  case class NodeStatus(
    dc: String,
    status: String,
    state: String,
    workerID: String,
    log_segment: String,
    address: String,
    owns: String,
    ownsGoal: String,
    hostID: String,
    persistedTimestamp: String)

  object NodeStatus {

    def leaderText(obj: JSObject) =
      if ((obj / "log_segment_leader").as[Boolean]) {
        " (leader)"
      } else {
        ""
      }

    def apply(obj: JSObject): NodeStatus =
      NodeStatus(
        (obj / "replica").as[String],
        (obj / "status").as[String],
        (obj / "state").as[String],
        (obj / "worker_id").as[String],
        (obj / "log_segment").as[String] + leaderText(obj),
        (obj / "address").as[String],
        "%4.1f%%".format((obj / "ownership").as[Float] * 100),
        "%4.1f%%".format((obj / "ownership_goal").as[Float] * 100),
        (obj / "host_id").as[String],
        (obj / "persisted_timestamp").as[String]
      )
  }

  final case class Status(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val nodes = (js / "nodes").as[Seq[JSObject]] map {
        NodeStatus(_)
      }

      val replicaInfo =
        (js / "replica_info").as[Seq[JSObject]] map { obj =>
          (obj / "name").as[String] -> (obj / "type").as[String]
        } toMap

      // The cluster status is a table of NodeStatuses, collated by replica, whose
      // non-DC
      // fields are in columns whose width is the maximum of all their entries.
      val headers =
        List(
          "Status",
          "State",
          "WorkerID",
          "Log Segment",
          "Address",
          "Owns",
          "Goal",
          "HostID",
          "Persisted Timestamp")

      val widths = nodes.foldLeft(headers map { _.length }) {
        case (
              status :: state :: wid :: log_segment :: address :: owns :: goal :: host :: ts :: Nil,
              n) =>
          List(
            status max n.status.length,
            state max n.state.length,
            wid max n.workerID.length,
            log_segment max n.log_segment.length,
            address max n.address.length,
            owns max n.owns.length,
            goal max n.ownsGoal.length,
            host max n.hostID.length,
            ts max n.persistedTimestamp.length
          )

        case _ =>
          throw new IllegalArgumentException
      }

      val fmt = (widths map { w =>
        s"%-${w}s"
      }).mkString("  ")

      // Group by replica and then sort the replica names to create a stable order
      val nlist = (nodes groupBy { _.dc } toList) sortBy { _._1 }
      nlist foreach { case (dc, nodes) =>
        val repType = replicaInfo.getOrElse(dc, "unknown type")
        val dcStr = s"Replica: $dc ($repType)"
        val rows = nodes.sortBy { _.workerID } map { n =>
          fmt.format(
            n.status,
            n.state,
            n.workerID,
            n.log_segment,
            n.address,
            n.owns,
            n.ownsGoal,
            n.hostID,
            n.persistedTimestamp)
        }

        println("")
        println(dcStr)
        println("=" * dcStr.length)
        println(fmt.format(headers: _*))
        rows foreach println
      }
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/status") map { extractJsonResponse(_) }
  }

  case class MapperStatus(
    hostID: String,
    name: String,
    iteration: String,
    percent: String,
    lastUpdate: String)

  object MapperStatus {

    def apply(obj: JSObject): MapperStatus =
      MapperStatus(
        (obj / "host_id").as[String],
        (obj / "name").as[String],
        "%9d".format((obj / "iteration").as[Long]),
        "%4.1f%%".format((obj / "percent").as[Float] * 100),
        (obj / "last_update").as[String]
      )
  }

  final case class RemoveNode(
    api: HttpClient,
    key: String,
    nodeID: String,
    force: Boolean,
    allowUnclean: Boolean,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      val body = JSObject(
        "nodeID" -> JS(nodeID),
        "force" -> force,
        "allow_unclean" -> allowUnclean).toString

      postAndValidate("/admin/remove_node", body) map { _ =>
        JSString(s"Node $nodeID is now being removed.")
      }
    }
  }

  final case class Init(
    api: HttpClient,
    key: String,
    replicaName: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler =
      interpretClusterOperationResponse("initialized")

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/init",
        JSObject("replica_name" -> JS(replicaName)).toString) map {
        extractJsonResponse(_)
      }

  }

  final case class Join(
    api: HttpClient,
    key: String,
    seed: String,
    replicaName: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler =
      interpretClusterOperationResponse("joined")

    protected def execute: Try[JSValue] = {
      val body =
        JSObject("seed" -> JS(seed), "replica_name" -> JS(replicaName)).toString
      postAndValidate("/admin/join", body, message = ErrorMessage.Join) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class MovementStatus(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val id = (js / "resource" / "movement_status").as[String]
      println(id)
    }

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/movement_status") map { extractJsonResponse(_) }
  }

  final case class IndexDrop(
    api: HttpClient,
    key: String,
    scope: Long,
    index: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] =
      deleteAndValidate(s"/admin/indexes/$scope/$index") map {
        extractJsonResponse(_)
      }
  }

  final case class IndexSwap(
    api: HttpClient,
    key: String,
    scope: Long,
    index: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val id = (js / "task").as[String]
      println(s"Index Swap task $id created.")
    }

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/indexes/swap",
        query = s"scope=$scope&index=$index") map {
        extractJsonResponse(_)
      }
  }

  final case class IndexForceBuild(
    api: HttpClient,
    key: String,
    scope: Long,
    index: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val id = (js / "task").as[String]
      println(s"Index build task $id created.")
    }

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/indexes/force-build",
        query = s"scope=$scope&index=$index") map {
        extractJsonResponse(_)
      }
  }

  final case class ForceMigrate(
    api: HttpClient,
    key: String,
    scope: Long,
    collection: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      val id = (js / "task").as[String]
      println(s"Migration task $id created.")
    }

    protected def execute: Try[JSValue] =
      postAndValidate(
        "/admin/force-migrate",
        query = s"scope=$scope&collection=$collection") map {
        extractJsonResponse(_)
      }
  }

  final case class ScopeFromAccount(
    api: HttpClient,
    key: String,
    accountID: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    override protected def textOutput: OutputHandler = { js =>
      println((js / "scope").as[String])
    }

    protected def execute: Try[JSValue] =
      getAndValidate(
        "/admin/scope-from-account",
        query = s"account-id=$accountID") map { extractJsonResponse(_) }
  }

  final case class ReprioritizeAccount(
    api: HttpClient,
    key: String,
    accountID: Long,
    priority: Int,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      val body =
        JSObject("account_id" -> JSLong(accountID), "priority" -> JSLong(priority))

      postAndValidate("/admin/accounts/reprioritize", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class MoveDatabase(
    api: HttpClient,
    key: String,
    from: Long,
    name: String,
    to: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      val body = JSObject(
        "from" -> JSLong(from),
        "name" -> JSString(name),
        "to" -> JSLong(to))

      postAndValidate("/admin/database/move", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class MoveDatabase2(
    api: HttpClient,
    key: String,
    target: String,
    destination: String,
    restore: Boolean,
    asJson: Boolean,
    out: PrintStream = System.out,
    err: PrintStream = System.err)
      extends AdminCommand(api, key, asJson, out, err) {

    protected def execute: Try[JSValue] = {
      val body =
        JSObject("from" -> target, "to" -> destination, "restore" -> restore)

      postAndValidate("/admin/database/move", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class RecoverDatabase(
    api: HttpClient,
    key: String,
    scope: String,
    dryRun: Boolean,
    asJson: Boolean,
    out: PrintStream = System.out,
    err: PrintStream = System.err)
      extends AdminCommand(api, key, asJson, out, err) {

    protected def execute: Try[JSValue] = {
      val body = JSObject("scope_id" -> scope, "dry_run" -> dryRun)
      postAndValidate("/admin/database/recover", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class GlobalID2ScopeID(
    api: HttpClient,
    key: String,
    globalID: String,
    asJson: Boolean,
    out: PrintStream = System.out,
    err: PrintStream = System.err)
      extends AdminCommand(api, key, asJson, out, err) {

    protected def execute: Try[JSValue] = {
      val body = JSObject("global_id" -> globalID).toString
      postAndValidate("/admin/database/global-to-scope", body) map {
        extractJsonResponse
      }
    }
  }

  final case class DumpTree(
    api: HttpClient,
    key: String,
    snapshotTS: Option[String],
    output: Option[String] = None,
    pretty: Boolean = true)
      extends AdminCommand(api, key, true) {

    override protected def jsonOutput: OutputHandler = { js =>
      output match {
        case Some(file) =>
          val output = new FileOutputStream(file)

          try {
            js.writeTo(output, pretty)
          } finally {
            output.close()
          }

        case None =>
          js.writeTo(System.out, pretty)
      }
    }

    override protected def execute: Try[JSValue] = {
      val params = snapshotTS match {
        case None     => ""
        case Some(ts) => s"snapshot_ts=$ts"
      }

      getAndValidate("/admin/backup/dump-tree", query = params) map {
        extractJsonResponse
      }
    }
  }

  final case class CheckTree(
    api: HttpClient,
    key: String,
    path: String,
    output: PrintStream)
      extends AdminCommand(api, key, true, output) {

    override protected def textOutput: OutputHandler = { js =>
      js.writeTo(output, pretty = true)
    }

    override protected def execute: Try[JSValue] = {
      val body = JSObject("path" -> path).toString
      postAndValidate("/admin/backup/check-tree", body = body) map {
        extractJsonResponse
      }
    }
  }

  final case class AllocateScopes(
    api: HttpClient,
    key: String,
    globalID: String,
    input: Option[String],
    preserveGlobalID: Boolean,
    output: PrintStream,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson, output) {

    override protected def textOutput: OutputHandler = { js =>
      val mapping = (js / "mapping").as[Seq[JSValue]]

      mapping foreach { s =>
        val oldScope = (s / "old_scope_id").as[String]
        val newScope = (s / "new_scope_id").as[String]
        val oldGlobal = (s / "old_global_id").as[String]
        val newGlobal = (s / "new_global_id").as[String]

        output.println(s"$oldScope $newScope $oldGlobal $newGlobal")
      }
    }

    override protected def execute: Try[JSValue] = {
      val inputStream = input match {
        case Some(file) =>
          new FileInputStream(file)

        case None =>
          System.in
      }

      val body = Body(Source.fromInputStream(inputStream).mkString, ContentType.JSON)

      postAndValidate(
        "/admin/backup/allocate-scopes",
        body,
        query = s"global_id=$globalID&preserve_global_ids=$preserveGlobalID") map {
        extractJsonResponse
      }
    }
  }

  final case class EnableDatabase(
    api: HttpClient,
    key: String,
    globalID: String,
    enable: Boolean,
    asJson: Boolean,
    out: PrintStream = System.out,
    err: PrintStream = System.err)
      extends AdminCommand(api, key, asJson, out, err) {

    override protected def textOutput: OutputHandler = { js =>
      out.println((js / "message").as[String])
    }

    override protected def execute: Try[JSValue] = {
      postAndValidate(
        "/admin/database/enable",
        query = s"global_id=$globalID&enable=$enable") map {
        extractJsonResponse
      }
    }
  }

  final case class LocateDocument(
    api: HttpClient,
    key: String,
    scopeID: Long,
    collectionID: Long,
    docID: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    val params =
      s"scope_id=$scopeID" :: s"collection_id=$collectionID" :: s"doc_id=$docID" :: Nil

    protected def execute: Try[JSValue] =
      getAndValidate("/admin/document/location", query = params.mkString("&")) map {
        extractJsonResponse(_)
      }
  }

  final case class TruncateTxnLog(
    api: HttpClient,
    key: String,
    epoch: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {
    protected def execute: Try[JSValue] = {
      val body = JSObject("epoch" -> JSLong(epoch))
      postAndValidate("/admin/txn-log/truncate", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class CloseLogSegment(
    api: HttpClient,
    key: String,
    segmentID: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      val body = JSObject("segmentID" -> JSLong(segmentID))
      postAndValidate("/admin/log-topology/close-segment", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class MoveLogSegment(
    api: HttpClient,
    key: String,
    segmentID: Long,
    fromID: String,
    toID: String,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      val body = JSObject(
        "segmentID" -> JSLong(segmentID),
        "fromID" -> fromID,
        "toID" -> toID)
      postAndValidate("/admin/log-topology/move-segment", body.toString) map {
        extractJsonResponse(_)
      }
    }
  }

  final case class AccountLimits(
    api: HttpClient,
    key: String,
    accountID: Long,
    asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      getAndValidate(s"/admin/accounts/$accountID/limits") map {
        extractJsonResponse(_)
      }
    }

    override protected def textOutput: OutputHandler = { js =>
      val hostID = (js / "host_id")
      val system = (js / "system")
      val flags = (js / "flags")
      val effective = (js / "effective_limits")

      val cols = Seq("System", "Flags", "Effective Limits")
      val limits = Map(
        "Read Ops" -> "read_ops",
        "Write Ops" -> "write_ops",
        "Compute Ops" -> "compute_ops")

      val prefix = limits.keys.map { _.length } max

      val header = s"Rate Limits for Account $accountID on $hostID"

      println(header)
      println("=" * header.length)
      print(" " * prefix)
      print("\t")
      println(cols.mkString("\t"))

      limits foreach { case (col, field) =>
        print(col)
        print(" " * (prefix - col.length))
        print("\t")
        print(system / field)
        print("\t")
        print(flags / field)
        print("\t")

        // Divide out the multiplier to recover comparable compute
        // ops.
        if (field == "compute_ops") {
          val net = (effective / field).as[Double]

          println(net / QueryMetrics.BaselineCompute)
        } else {
          println(effective / field)
        }
      }
    }
  }

  final case class ClearMVTCache(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      deleteAndValidate("/admin/storage/cache") map {
        extractJsonResponse(_)
      }
    }
  }

  final case class GetGCGrace(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      getAndValidate("/admin/storage/get-gc-grace") map {
        extractJsonResponse(_)
      }
    }
  }

  final case class SetStandardGCGrace(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      postAndValidate("/admin/storage/set-standard-gc-grace") map {
        extractJsonResponse(_)
      }
    }
  }

  final case class SetExtendedGCGrace(api: HttpClient, key: String, asJson: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      postAndValidate("/admin/storage/set-extended-gc-grace") map {
        extractJsonResponse(_)
      }
    }
  }

  final case class ResetMigrations(
    api: HttpClient,
    key: String,
    asJson: Boolean,
    scopeID: Long,
    collectionID: Long,
    rebuild: Boolean)
      extends AdminCommand(api, key, asJson) {

    protected def execute: Try[JSValue] = {
      postAndValidate(
        "/admin/reset-migrations",
        query = s"scope=$scopeID&collection=$collectionID") map {
        extractJsonResponse
      }
    }
  }

  final case class ListCompactions(
    api: HttpClient,
    key: String,
    asJson: Boolean
  ) extends AdminCommand(api, key, asJson) {
    protected def execute: Try[JSValue] = {
      getAndValidate("/admin/compactions/list") map extractJsonResponse
    }
  }

  // Helpers

  private def validateHttpResponse(
    resp: HttpResponse,
    message: ErrorMessage = ErrorMessage.Common,
    errStream: PrintStream = System.err): Boolean =
    resp.status.code() match {
      case 200 | 204  => true
      case statusCode =>
        // convert the json error message to a user readable String to print to
        // console.
        val error = Error.toString(extractJsonResponse(resp), statusCode, message)
        errStream.println(error)
        false
    }

  private def extractJsonResponse(resp: HttpResponse) =
    resp.body match {
      case _: NoBody => JSObject.empty
      case c: Body =>
        JSON.tryParse[JSValue](Await.result(c.data, Duration.Inf)) getOrElse JSNull
      case c: Chunked =>
        JSON.tryParse[JSValue](Await.result(c.data, Duration.Inf)) getOrElse JSNull
    }

  private def interpretClusterOperationResponse(actionDesc: String)(js: JSValue) = {
    (js / "changed").asOpt[Boolean] match {
      case Some(true) =>
        println(s"Node has $actionDesc the cluster.")
      case Some(false) =>
        println(
          s"Node has NOT $actionDesc the cluster. It is already a member of a cluster.")
      case None =>
        println("Unrecognized response from server.")
    }
  }
}
