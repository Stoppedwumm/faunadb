package fauna.tools

import fauna.api.FaunaApp
import fauna.atoms._
import fauna.exec.Timer
import fauna.lang.{ Timestamp, Timing }
import fauna.model.Database
import fauna.repo.doc.Version
import fauna.repo.RepoContext
import fauna.storage._
import fauna.storage.cassandra.{ CassandraIterator, IteratorStatsCounter }
import java.nio.file.Path
import java.util.concurrent.atomic.{ AtomicBoolean, LongAdder }
import java.util.concurrent.ConcurrentHashMap
import org.apache.cassandra.db.compaction.CompactionManager
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.cli.Options
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Read Versions SSTables looking for collections and functions with a given name or
  * alias. Useful to discover FQL v10 global names that are available to claim as
  * builtins. Initial ran took about ~15m to scan over a cluster backup of 3.3 TB
  * (across all CFs) using 64 threads.
  */
object TypecheckEverything extends FaunaApp("TypecheckEverything") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "threads", true, "The number of threads to use.")
  }

  override def setupLogging() =
    Some(config.logging.configure(None, debugConsole = true))

  lazy val handler = new OutputHandler.LogOutput
  val tc = TypecheckEverythingConfig.load(Path.of(sys.env("TYPECHECK_CONFIG")))
  val typecheckConfig = tc.toException match {
    case Some(e) => logThenDie(e.toString)
    case None    => tc.config
  }

  start {
    val threads =
      Option(cli.getOptionValue("threads"))
        .flatMap { _.toIntOption }
        .getOrElse(sys.runtime.availableProcessors * 2)

    run(threads)
  }

  private def run(threads: Int) = {
    val start = Timing.start

    handler.output("Setting up repo...")
    val repo = RepoTool.setupRepoWithoutCompactions(config, stats)
    handler.output("Repo is setup.")

    disableCompaction(repo)

    val databases = searchForDatabases(repo, threads)
    val failed = validateDatabases(repo, threads, databases)

    val end = start.elapsed
    handler.output(
      s"Total search time: ${end.toMinutes / 60}h${end.toMinutes % 60}m${end.toSeconds % 60}s")

    sys.exit(if (failed > 0) 1 else 0)
  }

  private def disableCompaction(repo: RepoContext) = {
    val ks = Keyspace.open(repo.storage.keyspaceName)

    repo.storage.schema.foreach { cf =>
      val cfs = ks.getColumnFamilyStore(cf.name)
      val strategy = cfs.getCompactionStrategy
      strategy.disable()
    }

    CompactionManager.instance.forceShutdown()
  }

  private def searchForDatabases(repo: RepoContext, threads: Int): Set[ScopeID] = {
    // Find all the databases.
    val selector = Selector.Schema(Set(DatabaseID.collID))

    val ks = Keyspace.open(repo.storage.keyspaceName)
    val versions = ks.getColumnFamilyStore(Tables.Versions.CFName)
    val iterCounter = new IteratorStatsCounter
    val ids = new ConcurrentHashMap[ScopeID, Boolean]()

    val segments = Segment.All.subSegments(threads)
    val executor = new Executor("Typecheck database search", threads)
    segments foreach { segment =>
      executor.addWorker { () =>
        val iter = new CassandraIterator(
          iterCounter,
          versions,
          ScanBounds(segment),
          selector,
          snapTime = Timestamp.MaxMicros
        )

        var prevId = DocID(SubID(0), CollectionID(0))
        while (iter.hasNext) {
          val (key, cell) = iter.next()
          val version = Version.decodeCell(key, cell)

          // Latest version will come first, so we can just check if thats live.
          if (version.id != prevId) {
            try {
              if (!version.isDeleted) {
                ids.put(Database.ScopeField(version.data.fields), true)
              }
            } catch {
              case NonFatal(e) =>
                handler.warn(s"Error processing database version: $version", e)
            }
          }

          prevId = version.id
        }
      }
    }

    def report(): Unit = {
      val stats = iterCounter.snapshot()

      handler.output(
        s"Found: ${ids.size()}. " +
          s"Rows: ${stats.rows} " +
          s"Cells: ${stats.cells} total, " +
          s"${stats.liveCells} live, " +
          s"${stats.unbornCells} unborn, " +
          s"${stats.deletedCells} deleted " +
          s"${stats.bytesRead} bytes read "
      )
    }

    handler.output("Searching for databases...")

    @volatile var searching = true
    Timer.Global.scheduleRepeatedly(10.seconds, searching) {
      report()
    }

    executor.waitWorkers()
    handler.output("Done searching for databases.")
    report()
    searching = false

    ids.keySet.asScala.toSet
  }

  private def validateDatabases(
    repo: RepoContext,
    threads: Int,
    databases: Set[ScopeID]): Long = {
    val checked = new LongAdder
    val failed = new LongAdder

    typecheckConfig.invalid_scopes.foreach { scope =>
      if (!databases.contains(ScopeID(scope))) {
        handler.output(s"Scope $scope was not found in the dataset.")
        failed.increment()
      }
    }

    def validate(scope: ScopeID): Unit = {
      val checker = new fauna.model.schema.TypecheckEverything(
        typecheckConfig.invalid_scopes.view.map(ScopeID(_)).toSet)
      val f = new AtomicBoolean(false)

      try {
        repo.runSynchronously(
          checker.check(scope) { msg =>
            handler.output(s"Unexpected result for scope $scope: $msg")
            // This may be called multiple times, but we only want to increment once.
            if (f.compareAndSet(false, true)) {
              failed.increment()
            }
          },
          90.seconds
        )
      } catch {
        case NonFatal(e) =>
          if (!typecheckConfig.invalid_scopes.contains(scope.toLong)) {
            handler.output(s"Validating failed for $scope!!!\nerror: $e")
            failed.increment()
          }
      }
      checked.increment()
    }

    handler.output("Validating databases...")

    def report(): Unit = {
      handler.output(
        s"Failed: ${failed.longValue}. " +
          s"Checked: ${checked.longValue}.")
    }

    if (databases.sizeIs < threads) {
      databases.foreach { scope => validate(scope) }
    } else {
      val executor = new Executor("Typecheck validator", threads)
      databases.grouped(databases.size / threads).foreach { scopes =>
        executor.addWorker { () =>
          scopes.foreach { scope => validate(scope) }
        }
      }

      @volatile var validating = true
      Timer.Global.scheduleRepeatedly(10.seconds, validating) {
        report()
      }

      executor.waitWorkers()
      validating = false
    }

    report()

    failed.longValue
  }
}
