package fauna.tools

import fauna.api.FaunaApp
import fauna.atoms._
import fauna.model.tasks.IndexRebuild
import fauna.net.http._
import fauna.tools.error.ErrorMessage
import java.net.{ ConnectException, InetAddress, UnknownHostException }
import org.apache.commons.cli.Options
import scala.concurrent.duration._

object Admin extends FaunaApp("Admin Interface") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption("k", "key", true, "FaunaDB root key to authenticate with.")
    o.addOption("H", "host", true, "FaunaDB host to connect to.")
    o.addOption("D", "data-path", true, "Root storage path.")
    o.addOption(
      "n",
      "dry-run",
      false,
      "When used with 'repair-task', do not modify index data, only log what would happen.")
    o.addOption(
      "d",
      "data-only",
      false,
      "When used with 'repair-task', do not repair replication, skip to data-model repair.")
    o.addOption(
      "r",
      "replica",
      true,
      "The replica that this node should join by default.")
    o.addOption(
      "t",
      "timestamp",
      true,
      "When used with 'dump-tree', 'get-task', and 'cancel-task', change the snapshot timestamp."
    )
    o.addOption(
      null,
      "history",
      false,
      "When used with 'get-task', outputs the history of the task."
    )
    o.addOption("f", "force", false, "Force the operation, if possible.")
    o.addOption("j", "json", false, "Output as JSON.")
    o.addOption(
      null,
      "preserve-global-ids",
      false,
      "When used with 'allocate-scopes' indicates if the Databases Global ID should be preserved or not.")
    o.addOption(
      null,
      "restore",
      false,
      "When used with 'move-database', restores a database in place.")

    IndexRebuild.AllPhases foreach { phase =>
      o.addOption(
        null,
        phase,
        false,
        s"Option for 'index-rebuild' to include the '$phase' phase.")
    }

    o.addOption(
      null,
      "max-task-id",
      true,
      s"When used with 'index-rebuild' excludes tasks greater than the specified ID from rebuild.")

    o.addOption(
      null,
      "allow-unclean",
      false,
      "When used with 'remove', skips checking cluster storage cleanliness.")

    o.addOption(null, "no-pretty", false, "Disable output formatting.")
    o.addOption(
      null,
      "perform-updates",
      false,
      "When used with fix-lookups will perform the writes needed to correct the lookup entries.")

    // NB: Need `filter-` prefix, to avoid conflicting with `--conf`.
    o.addOption(null, "filter-scope", true, "Set a scope to filter by.")
    o.addOption(null, "filter-index", true, "Set an index to filter by.")
    o.addOption(null, "filter-collection", true, "Set a collection to filter by.")

    o
  }

  override def usage() = AdminCommands.printUsage(cliOptions())

  override def configDefaults: Map[String, Any] = {
    val lo = InetAddress.getLoopbackAddress.getHostAddress

    val envkey = Option(cli.getOptionValue('k')) orElse
      Option(System.getenv("FAUNADB_ROOT_KEY")) getOrElse ""

    // These options allow the admin tool to run without a config
    // file, yet still use the same config parsing logic as the service.
    Map("auth_root_key" -> envkey, "network_broadcast_address" -> lo)
  }

  /** For the Admin tool, do not print the startup banner. */
  override def printBanner() = {}

  /** Replica can only be defined once. Either in config, or via the option.
    * If it is not present in either place or is set in both, that should be an error.
    *
    * See PR 5726.
    */
  def getReplicaName(): Either[String, String] =
    Option(cli.getOptionValue("replica")) match {
      case Some(r) => Right(r)
      case None    => Left(ErrorMessage.replicaNameIsRequired)
    }

  start {
    // TODO: Defaulting auth_root_key in configDefaults with any CLI values,
    // only to stomp over it again here (after config parsing), is an
    // unfortunate vestigial bit, because it is required in the config for the
    // service, and in that application it is not allowed to be overridden on
    // the CLI.
    val cliOpt = Option(cli.getOptionValue('k'))
    val envOpt = Option(System.getenv("FAUNADB_ROOT_KEY"))
    (cliOpt orElse envOpt) foreach {
      config.auth_root_key = _
    }

    val asJson = cli.hasOption("json")

    val storagePath = (Option(System.getenv("FAUNDB_DATA_PATH")) orElse
      Option(cli.getOptionValue('D')) getOrElse
      config.storage_data_path)

    val PortOnly = "^:([0-9]+$)".r
    val HostPair = "^([^:]+):([0-9]+$)".r

    val (host, port: Int) = {
      val flag = Option(cli.getOptionValue('H')) orElse
        Option(System.getenv("FAUNADB_ADMIN_HOST"))

      // If epoll is enabled, we can't connect to the server via 0.0.0.0. If the
      // config has Admin bound to 0.0.0.0 then we can pretend it's bound to
      // 127.0.0.1 and try to connect there.
      val cfgHost =
        Option(config.network_admin_http_address) filter {
          _ != "0.0.0.0"
        } getOrElse "127.0.0.1"

      flag match {
        case Some(PortOnly(port)) =>
          (cfgHost, Integer.valueOf(port))
        case Some(HostPair(host, port)) =>
          (host, Integer.valueOf(port))
        case Some(host) if host.endsWith(":") =>
          (host, config.network_admin_http_port)
        case Some(host) =>
          (host, config.network_admin_http_port)
        case None =>
          (cfgHost, config.network_admin_http_port)
      }
    }

    try {
      val api = Http2Client(
        host,
        port,
        ssl = Some(config.adminSSL),
        responseTimeout = Duration.Inf,
        enableEndpointIdentification = false)

      val handler: () => Boolean = cli.getArgs.toList match {
        case "debug" :: "read-log" :: logName :: Nil =>
          () => BinaryLogReader.read(logName, config.storagePath)

        case "debug" :: "skip-transactions" :: Nil =>
          AdminCommands.SkipTransactions(api, config.auth_root_key, asJson)

        case "debug" :: "unlock-storage" :: Nil =>
          AdminCommands.UnlockStorage(api, config.auth_root_key, asJson)

        case "debug" :: "needs-cleanup" :: Nil =>
          AdminCommands.NeedsCleanup(api, config.auth_root_key, None, asJson)

        case "debug" :: "needs-cleanup" :: cf :: Nil =>
          AdminCommands.NeedsCleanup(api, config.auth_root_key, Some(cf), asJson)

        case "debug" :: "clean-storage" :: Nil =>
          AdminCommands.CleanStorage(api, config.auth_root_key, asJson)

        case "status" :: Nil =>
          AdminCommands.Status(api, config.auth_root_key, asJson)

        case "config" :: Nil =>
          AdminCommands.NodeConfig(
            api,
            config.auth_root_key,
            "BasicOrValues",
            asJson)

        case "config" :: cfgopt :: Nil =>
          AdminCommands.NodeConfig(api, config.auth_root_key, cfgopt, asJson)

        case "security-check" :: Nil =>
          AdminCommands.SecureFiles(api, config.auth_root_key, false, asJson)

        case "security-repair" :: Nil =>
          AdminCommands.SecureFiles(api, config.auth_root_key, true, asJson)

        case "movement-status" :: Nil =>
          AdminCommands.MovementStatus(api, config.auth_root_key, asJson)

        case "remove" :: nodeID :: Nil =>
          AdminCommands.RemoveNode(
            api,
            config.auth_root_key,
            nodeID,
            cli.hasOption("force"),
            cli.hasOption("allow-unclean"),
            asJson)

        case "init" :: Nil =>
          getReplicaName() match {
            case Right(replicaName) =>
              AdminCommands.Init(api, config.auth_root_key, replicaName, asJson)
            case Left(error) =>
              () => {
                println(error)
                false
              }
          }

        case "join" :: seed :: Nil =>
          getReplicaName() match {
            case Right(replicaName) =>
              AdminCommands.Join(
                api,
                config.auth_root_key,
                seed,
                replicaName,
                asJson)
            case Left(error) =>
              () => {
                println(error)
                false
              }
          }

        case "repair-task" :: "create" :: Nil =>
          val dryRun = cli.hasOption("dry-run")
          val dataOnly = cli.hasOption("data-only")
          AdminCommands.Repair(
            api,
            config.auth_root_key,
            dryRun,
            dataOnly,
            None,
            asJson)

        case "repair-task" :: "create" :: scopeID :: Nil =>
          val dryRun = cli.hasOption("dry-run")
          val dataOnly = cli.hasOption("data-only")
          AdminCommands.Repair(
            api,
            config.auth_root_key,
            dryRun,
            dataOnly,
            Some(scopeID),
            asJson)

        case "repair-task" :: "cancel" :: Nil =>
          AdminCommands.CancelRepair(api, config.auth_root_key, asJson)

        case "repair-task" :: "status" :: Nil =>
          AdminCommands.RepairStatus(api, config.auth_root_key, asJson)

        case "index-rebuild" :: Nil =>
          val maxTaskID = Option(cli.getOptionValue("max-task-id"))
          val phases = Vector.newBuilder[String]
          IndexRebuild.AllPhases foreach { phase =>
            if (cli.hasOption(phase)) phases.addOne(phase)
          }
          AdminCommands.IndexRebuildStart(
            api,
            config.auth_root_key,
            phases.result(),
            maxTaskID,
            asJson)

        case "index-rebuild-promote-index2" :: Nil =>
          () => AdminCommands.indexRebuildPromoteIndex2CFs(storagePath)

        case "show-replication" :: Nil =>
          AdminCommands.ShowReplication(api, config.auth_root_key, asJson)

        case "update-replica" :: replicaType :: dcs =>
          AdminCommands.UpdateReplicas(
            api,
            replicaType,
            dcs,
            config.auth_root_key,
            asJson)

        case "update-replicas" :: replicaType :: dcs =>
          AdminCommands.UpdateReplicas(
            api,
            replicaType,
            dcs,
            config.auth_root_key,
            asJson)

        case "update-storage-version" :: Nil =>
          AdminCommands.UpdateStorageVersion(api, config.auth_root_key, asJson)

        case "show-storage-version" :: Nil =>
          AdminCommands.ShowStorageVersion(api, config.auth_root_key, asJson)

        case "show-identity" :: Nil =>
          AdminCommands.ShowIdentity(api, config.auth_root_key, asJson)

        case "host-id" :: hostIP :: Nil =>
          AdminCommands.GetHostID(api, config.auth_root_key, hostIP, asJson)

        case "host-version" :: Nil =>
          AdminCommands.GetFaunaVersion(api, config.auth_root_key, asJson)

        case "host-versions" :: Nil =>
          AdminCommands.GetHostVersions(api, config.auth_root_key, asJson)

        case "host-info" :: Nil =>
          AdminCommands.GetHostInfo(api, config.auth_root_key, asJson)

        case "host-build" :: Nil =>
          AdminCommands.GetBuildInfo(api, config.auth_root_key, asJson)

        case "backup-replica" :: name :: Nil =>
          AdminCommands.CreateReplicaBackup(
            api,
            config.auth_root_key,
            Some(name),
            asJson)

        case "backup-replica" :: Nil =>
          AdminCommands.CreateReplicaBackup(api, config.auth_root_key, None, asJson)

        case "backup-show" :: Nil =>
          AdminCommands.ShowClusterBackups(api, config.auth_root_key, asJson)

        case "create-snapshot" :: name :: Nil =>
          AdminCommands.CreateSnapshot(api, config.auth_root_key, name, asJson)

        case "repair" :: "lookups" :: path :: outputPath :: Nil =>
          AdminCommands.FixLookups(
            api,
            config.auth_root_key,
            path,
            outputPath,
            dryRun = !cli.hasOption("perform-updates"),
            asJson)

        case "load-snapshot" :: path :: maybeTarget =>
          require(maybeTarget.lengthIs <= 1)
          val target = maybeTarget.headOption
          AdminCommands.LoadSnapshot(api, config.auth_root_key, path, target, asJson)

        case "show-snapshots" :: Nil =>
          AdminCommands.ShowSnapshots(api, config.auth_root_key, asJson)

        case "restore" :: "status" :: Nil =>
          AdminCommands.RestoreStatus(api, config.auth_root_key, asJson)

        case "hash-key" :: key :: Nil =>
          () => AdminCommands.hashKey(key)

        case "reseed-samples" :: Nil =>
          AdminCommands.ReseedSamples(api, config.auth_root_key, asJson)

        case "steal-tasks" :: Nil =>
          AdminCommands.StealTasks(api, config.auth_root_key, asJson)

        case "list-tasks" :: Nil =>
          AdminCommands.ListTasks(
            api,
            config.auth_root_key,
            Option(cli.getOptionValue("filter-scope")).map { s =>
              ScopeID(s.toLong)
            },
            Option(cli.getOptionValue("filter-collection")).map { s =>
              CollectionID(s.toLong)
            },
            Option(cli.getOptionValue("filter-index")).map { s =>
              IndexID(s.toLong)
            },
            asJson
          )

        case "run-task" :: taskID :: Nil =>
          if (cli.hasOption("dry-run")) {
            AdminCommands.DryRunTask(api, config.auth_root_key, taskID, asJson)
          } else { () =>
            {
              println("Only dry-run is supported, please add --dry-run to command.")
              false
            }
          }

        case "get-task" :: taskID :: Nil =>
          val ts = Option(cli.getOptionValue("timestamp"))
          val showHistory = cli.hasOption("history")
          AdminCommands.GetTask(
            api,
            config.auth_root_key,
            taskID,
            ts,
            showHistory,
            asJson)

        case "move-task" :: taskID :: host :: Nil =>
          AdminCommands.MoveTask(
            api,
            config.auth_root_key,
            taskID,
            Some(host),
            asJson)

        case "move-task" :: taskID :: Nil =>
          AdminCommands.MoveTask(api, config.auth_root_key, taskID, None, asJson)

        case "cancel-task" :: taskID :: Nil =>
          AdminCommands.CancelTask(api, config.auth_root_key, taskID, asJson)

        case "complete-task" :: taskID :: Nil =>
          AdminCommands.CompleteTask(api, config.auth_root_key, taskID, asJson)

        case "reprioritize-task" :: taskID :: priority :: Nil =>
          AdminCommands.ReprioritizeTask(
            api,
            config.auth_root_key,
            taskID,
            priority.toInt,
            asJson)

        case "pause-task" :: taskID :: Nil =>
          AdminCommands.PauseTask(api, config.auth_root_key, taskID, asJson)

        case "unpause-task" :: taskID :: Nil =>
          AdminCommands.UnpauseTask(api, config.auth_root_key, taskID, asJson)

        case "index-drop" :: scope :: index :: Nil =>
          AdminCommands.IndexDrop(
            api,
            config.auth_root_key,
            scope.toLong,
            index.toLong,
            asJson)

        case "index-swap" :: scope :: index :: Nil =>
          AdminCommands.IndexSwap(
            api,
            config.auth_root_key,
            scope.toLong,
            index.toLong,
            asJson)

        case "index-force-build" :: scope :: index :: Nil =>
          AdminCommands.IndexForceBuild(
            api,
            config.auth_root_key,
            scope.toLong,
            index.toLong,
            asJson)

        case "repartition-index-scan" :: taskID :: targetHost :: Nil =>
          AdminCommands.RepartitionIndexScan(
            api,
            config.auth_root_key,
            taskID,
            targetHost,
            asJson)

        case "repartition-migration-scan" :: taskID :: targetHost :: Nil =>
          AdminCommands.RepartitionMigrationScan(
            api,
            config.auth_root_key,
            taskID,
            targetHost,
            asJson)

        case "enable-traced-secret" :: secret :: Nil =>
          AdminCommands.EnableTracedSecret(api, config.auth_root_key, secret, asJson)

        case "disable-traced-secret" :: secret :: Nil =>
          AdminCommands.DisableTracedSecret(
            api,
            config.auth_root_key,
            secret,
            asJson)

        case "list-traced-secrets" :: Nil =>
          AdminCommands.ListTracedSecrets(api, config.auth_root_key, asJson)

        case "balance-replica" :: replicaName :: Nil =>
          AdminCommands.BalanceReplica(
            api,
            config.auth_root_key,
            replicaName,
            asJson)

        case "get-index2cf-gc-grace-period" :: Nil =>
          AdminCommands.GetIndex2CfGcGracePeriod(api, config.auth_root_key, asJson)

        case "set-index2cf-gc-grace-period" :: days :: Nil =>
          AdminCommands.SetIndex2CfGcGracePeriod(
            api,
            config.auth_root_key,
            days.toInt,
            asJson)

        case "account-limits" :: accountID :: Nil =>
          AdminCommands.AccountLimits(
            api,
            config.auth_root_key,
            accountID.toLong,
            asJson)

        case "scope-from-account" :: accountID :: Nil =>
          AdminCommands.ScopeFromAccount(
            api,
            config.auth_root_key,
            accountID.toLong,
            asJson)

        case "reprioritize-account" :: accountID :: priority :: Nil =>
          AdminCommands.ReprioritizeAccount(
            api,
            config.auth_root_key,
            accountID.toLong,
            priority.toInt,
            asJson)

        case "move-database" :: from :: name :: to :: Nil =>
          AdminCommands.MoveDatabase(
            api,
            config.auth_root_key,
            from.toLong,
            name,
            to.toLong,
            asJson)

        case "move-database" :: target :: destination :: Nil =>
          AdminCommands.MoveDatabase2(
            api,
            config.auth_root_key,
            target,
            destination,
            cli.hasOption("restore"),
            asJson)

        case "recover-database" :: scope :: Nil =>
          AdminCommands.RecoverDatabase(
            api,
            config.auth_root_key,
            scope,
            cli.hasOption("dry-run"),
            asJson)

        case "global-to-scope" :: globalID :: Nil =>
          AdminCommands.GlobalID2ScopeID(api, config.auth_root_key, globalID, asJson)

        case "dump-tree" :: output :: Nil =>
          val ts = Option(cli.getOptionValue("timestamp"))
          AdminCommands.DumpTree(
            api,
            config.auth_root_key,
            ts,
            Some(output),
            !cli.hasOption("no-pretty"))

        case "dump-tree" :: Nil =>
          val ts = Option(cli.getOptionValue("timestamp"))
          AdminCommands.DumpTree(
            api,
            config.auth_root_key,
            ts,
            None,
            !cli.hasOption("no-pretty"))

        case "check-tree" :: path :: Nil =>
          AdminCommands.CheckTree(api, config.auth_root_key, path, System.out)

        case "allocate-scopes" :: globalID :: Nil =>
          val preserveGlobalIDs = cli.hasOption("preserve-global-ids")

          AdminCommands.AllocateScopes(
            api,
            config.auth_root_key,
            globalID,
            None,
            preserveGlobalIDs,
            System.out,
            asJson)

        case "allocate-scopes" :: globalID :: input :: Nil =>
          val preserveGlobalIDs = cli.hasOption("preserve-global-ids")

          AdminCommands.AllocateScopes(
            api,
            config.auth_root_key,
            globalID,
            Some(input),
            preserveGlobalIDs,
            System.out,
            asJson)

        case "enable-database" :: globalID :: Nil =>
          AdminCommands.EnableDatabase(
            api,
            config.auth_root_key,
            globalID,
            enable = true,
            asJson)

        case "disable-database" :: globalID :: Nil =>
          AdminCommands.EnableDatabase(
            api,
            config.auth_root_key,
            globalID,
            enable = false,
            asJson)

        case "locate-document" :: scopeID :: collectionID :: docID :: Nil =>
          AdminCommands.LocateDocument(
            api,
            config.auth_root_key,
            scopeID.toLong,
            collectionID.toLong,
            docID.toLong,
            asJson)

        case "truncate-txn-log" :: epoch :: Nil =>
          AdminCommands.TruncateTxnLog(
            api,
            config.auth_root_key,
            epoch.toLong,
            asJson)

        case "close-log-segment" :: segmentID :: Nil =>
          AdminCommands.CloseLogSegment(
            api,
            config.auth_root_key,
            segmentID.toLong,
            asJson)

        case "move-log-segment" :: segmentID :: fromID :: toID :: Nil =>
          AdminCommands.MoveLogSegment(
            api,
            config.auth_root_key,
            segmentID.toLong,
            fromID,
            toID,
            asJson)

        case "clear-mvt-cache" :: Nil =>
          AdminCommands.ClearMVTCache(api, config.auth_root_key, asJson)

        case "get-gc-grace" :: Nil =>
          AdminCommands.GetGCGrace(api, config.auth_root_key, asJson)

        case "set-standard-gc-grace" :: Nil =>
          AdminCommands.SetStandardGCGrace(api, config.auth_root_key, asJson)

        case "set-extended-gc-grace" :: Nil =>
          AdminCommands.SetExtendedGCGrace(api, config.auth_root_key, asJson)

        case "force-migrate" :: scope :: collection :: Nil =>
          AdminCommands.ForceMigrate(
            api,
            config.auth_root_key,
            scope.toLong,
            collection.toLong,
            asJson)

        case "reset-migrations" :: scopeID :: collectionID :: rebuildStr :: Nil =>
          val rebuild = rebuildStr match {
            case "rebuild"    => Some(true)
            case "no-rebuild" => Some(false)
            case _            => None
          }

          rebuild match {
            case Some(rebuild) =>
              AdminCommands.ResetMigrations(
                api,
                config.auth_root_key,
                asJson,
                scopeID.toLong,
                collectionID.toLong,
                rebuild)
            case None =>
              () => {
                usage()
                System.err.println("Invalid rebuild option.")
                false
              }
          }

        case "list-compactions" :: Nil =>
          AdminCommands.ListCompactions(api, config.auth_root_key, asJson)

        case cmd =>
          () => {
            usage()
            System.err.println(
              if (cmd.isEmpty) "Command not specified." else "Invalid command.")
            false
          }
      }

      val success = handler()

      if (success) {
        System.exit(0)
      } else {
        System.exit(1)
      }
    } catch {
      case _: ConnectException =>
        printError(s"Could not connect to FaunaDB at $host")
      case e: UnknownHostException =>
        printError(s"Unknown host name ${e.getMessage}")
      case e: Throwable =>
        printError(e.getMessage)
    }
  }

  private def printError(message: String): Unit = {
    println(s"ERROR: $message")
    System.exit(1)
  }
}
