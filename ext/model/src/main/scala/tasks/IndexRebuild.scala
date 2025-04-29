package fauna.model.tasks

import fauna.atoms._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model._
import fauna.model.cache.BatchSchemaCache
import fauna.model.schema.SchemaCollectionID
import fauna.repo.{ ColIteratee, RangeIteratee }
import fauna.repo.cassandra.CassandraService
import fauna.repo.doc.{ Indexer, Version }
import fauna.repo.query.{ Query, State }
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import fauna.stats.StatsRecorder
import fauna.storage.{ ComponentTooLargeException, Selector, Storage, Tables }
import fauna.storage.doc._
import fauna.storage.ops.{ CassandraMutation, SetAdd }
import java.util.concurrent.TimeoutException
import java.util.UUID
import org.apache.cassandra.db.{ ColumnFamilyStore, Keyspace }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Runs through all index rebuild tasks, starting with the canonical data in Versions.
  */
object IndexRebuild {

  // FIXME: this global singleton is terrible. Move to RepoContext

  lazy val Schema = new BatchSchemaCache

  case class RebuildException(version: Version, cause: Throwable)
      extends Exception(s"Error rebuilding indexes for $version", cause)

  val TaskVersion = 0

  // FIXME: This has been tuned for the test cluster. Read pressure shows up
  // as datanode read stalls on the local node.
  val ParallelSegments = 8

  val RangesField = Field[Vector[Segment]]("ranges")
  val StateField = Field[String]("state")

  // Only include task documents with an ID LTE this value.
  val MaxTaskIDField = Field[Long]("max_task_id")
  val PhasesField = Field[Vector[String]]("phases")

  val AllPhases: Vector[String] = Vector(
    "versions-replication",
    "lookups",
    "build-and-distribute-metadata-index",
    "build-local-index",
    "distribute-local-index"
  )

  case object RootTask extends Type("index-rebuild-join", TaskVersion) {

    override def isStealable(t: Task) = false

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] = {

      val phases = task.data(PhasesField)
      def nextState(phase: String): Query[Task.State] = {
        def maybeState(next: String) = {
          if (phases.contains(next)) {
            val newState = next match {
              case "versions-replication" => "versions-replication"
              case "lookups"              => "lookups"
              case "build-and-distribute-metadata-index" =>
                "local-metadata-index-build"
              case "build-local-index"      => "local-index-build"
              case "distribute-local-index" => "distribute-index2-cfs"
            }
            Query.value(
              Task.Runnable(task.data.update(StateField -> newState), None))
          } else {
            nextState(next)
          }
        }
        phase match {
          case "start"                => maybeState("versions-replication")
          case "versions-replication" => maybeState("lookups")
          case "lookups" => maybeState("build-and-distribute-metadata-index")
          case "build-and-distribute-metadata-index" =>
            maybeState("build-local-index")
          case "build-local-index"      => maybeState("distribute-local-index")
          case "distribute-local-index" => Query.value(Task.Completed())
        }
      }

      def joinThenNextState(next: String) =
        task.joinThen() {
          case Left(_)  => Query.value(Task.Cancelled())
          case Right(_) => nextState(next)
        }

      def joinThenUpdateState(state: String) =
        task.joinThen() {
          case Left(_) => Query.value(Task.Cancelled())
          case Right(data) =>
            Query.value(Task.Runnable(data.update(StateField -> state), None))
        }

      def forkAndUpdateState(q: Query[Iterable[Task]], state: String) =
        task.fork {
          q map {
            (_, task.data.update(StateField -> state))
          }
        }

      task.data(StateField) match {
        case "start" =>
          nextState("start")

        case "versions-replication" =>
          forkAndUpdateState(
            Repair.RestoreReplicationIncremental
              .create(task.id, Repair.Commit, None),
            "join-replication"
          )

        case "join-replication" =>
          joinThenNextState("versions-replication")

        case "lookups" =>
          forkAndUpdateState(
            Repair.RepairLookups.create(task.id, Repair.Commit, None),
            "join-lookups")

        case "join-lookups" =>
          joinThenUpdateState("reverse-lookups")

        case "reverse-lookups" =>
          forkAndUpdateState(
            Repair.RepairReverseLookups.create(task.id, Repair.Commit, None),
            "join-reverse-lookups")

        case "join-reverse-lookups" =>
          joinThenNextState("lookups")

        case "local-metadata-index-build" =>
          forkAndUpdateState(
            BuildLocalMetadataIndexes.create(task.id),
            "join-local-metadata-index-build")

        case "join-local-metadata-index-build" =>
          joinThenUpdateState("distribute-local-metadata-index-build")

        case "distribute-local-metadata-index-build" =>
          forkAndUpdateState(
            DistributeLocalMetadataIndexes.create(task.id),
            "join-distribute-local-metadata-index-build")

        case "join-distribute-local-metadata-index-build" =>
          joinThenNextState("build-and-distribute-metadata-index")

        case "local-index-build" =>
          forkAndUpdateState(
            BuildLocalDocumentIndexes.create(task.data(MaxTaskIDField), task.id),
            "join-local-index-build")

        case "join-local-index-build" =>
          joinThenNextState("build-local-index")

        case "distribute-index2-cfs" =>
          forkAndUpdateState(
            DistributeLocalDocumentIndexes.create(task.id),
            "join-distribute-index2-cfs")

        case "join-distribute-index2-cfs" =>
          joinThenNextState("distribute-local-index")
      }
    }

    /** Create a new index rebuild task, if none exists as of snapshotTS.
      */
    def create(maxTaskID: Long, phases: Vector[String]): Query[Option[Task]] = {
      val phases_ = if (phases.isEmpty) {
        AllPhases
      } else {
        phases
      }
      val data = Data(
        StateField -> "start",
        MaxTaskIDField -> maxTaskID,
        PhasesField -> phases_)

      Task.createLocal(
        Database.RootScopeID,
        name,
        data,
        None,
        isOperational = true) map { Some(_) }
    }
  }

  object BuildLocalMetadataIndexes
      extends BuildLocalIndexes("local-metadata-index-build") {

    def create(parent: TaskID): Query[Iterable[Task]] =
      Task.createPrimarySegments(Database.RootScopeID, name, Some(parent)) {
        (segs, _) =>
          val psegs = if (segs.size < IndexRebuild.ParallelSegments) {
            segs.flatMap(_.subSegments(IndexRebuild.ParallelSegments))
          } else {
            segs
          }
          Data(RangesField -> psegs.toVector)
      }

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] =
      Query.stats flatMap { stats =>
        val iter = LocalIndexBuildTask(
          Storage.LocalMetadataIndexBuildKeyspace,
          stats,
          snapshotTS,
          TaskID.MaxValue,
          buildMetadataIndexes = true)
        val q = paginate2(
          task,
          stats,
          "LocalMetadataIndex.Build",
          iter.keyspace,
          iter,
          SchemaCollectionID.Selector)
        backgrounded(q)
      }
  }

  object BuildLocalDocumentIndexes extends BuildLocalIndexes("local-index-build") {

    def create(maxTaskID: Long, parent: TaskID): Query[Iterable[Task]] =
      Task.createPrimarySegments(Database.RootScopeID, name, Some(parent)) {
        (segs, _) =>
          val psegs = if (segs.size < IndexRebuild.ParallelSegments) {
            segs.flatMap(_.subSegments(IndexRebuild.ParallelSegments))
          } else {
            segs
          }
          Data(
            MaxTaskIDField -> maxTaskID,
            RangesField -> psegs.toVector
          )
      }

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] = {
      Query.stats flatMap { stats =>
        val iter = LocalIndexBuildTask(
          Storage.LocalIndexBuildKeyspace,
          stats,
          snapshotTS,
          TaskID(task.data(MaxTaskIDField)),
          buildMetadataIndexes = false)
        val q = paginate2(
          task,
          stats,
          "LocalIndex.Build",
          iter.keyspace,
          iter,
          Selector.All)
        backgrounded(q)
      }
    }

    def repartitionTaskSegments(
      taskID: TaskID,
      targetHost: HostID,
      targetTask: Option[TaskID]): Query[Unit] = {
      Query.snapshotTime flatMap { snapshotTS =>
        Task.get(taskID, snapshotTS) flatMap {
          case None =>
            Query.fail(
              new IllegalArgumentException(
                s"Task $taskID not found, nothing to reassign."))
          case Some(sourceTask) if !sourceTask.isPaused =>
            Query.fail(
              new IllegalArgumentException(
                s"Task $taskID found, but is not paused."))
          case Some(sourceTask) =>
            Query.repo flatMap { repo =>
              val taskSegments = sourceTask.data(RangesField).sorted
              val targetSegments = repo.keyspace.segmentsForHost(targetHost)

              val remaining = Segment.diff(taskSegments, targetSegments)
              val intersections = Segment.diff(taskSegments, remaining)

              def updateData(data: Data, segments: Vector[Segment]): Data = {
                val d =
                  data
                    .update(RangesField -> segments)
                    .update(CursorField -> None)
                CursorFields.foldLeft(d) { case (data, f) =>
                  data.update(f -> None)
                }
              }

              val updateOriginalTaskQ = {
                val updatedData =
                  updateData(sourceTask.state.data, remaining.toVector)
                Task.write(
                  sourceTask.copy(state = sourceTask.state.withData(updatedData)))
              }

              def updateExistingTask(targetTaskID: TaskID) =
                Task.get(targetTaskID, snapshotTS) flatMap {
                  case None =>
                    Query.fail(
                      new IllegalArgumentException(
                        s"Target task $targetTaskID not found."))
                  case Some(targetTask) if !targetTask.isPaused =>
                    Query.fail(
                      new IllegalArgumentException(
                        s"Target task $targetTaskID found, but is not paused."))
                  case Some(targetTask) =>
                    val targetRanges = Segment.normalize(
                      targetTask.state.data(RangesField) ++ intersections)
                    val updatedData = updateData(targetTask.state.data, targetRanges)
                    Task.write(
                      targetTask.copy(state =
                        targetTask.state.withData(updatedData)))
                }

              def createNewTargetTask() = {
                Task.create(
                  ScopeID.RootID,
                  name,
                  targetHost,
                  Data(RangesField -> intersections.toVector),
                  parent = sourceTask.parent
                )
              }

              def updateParentWithNewChild(childID: TaskID) = {
                sourceTask.parent.fold(Query.unit) { parentID =>
                  Task.get(parentID, snapshotTS) flatMap {
                    case None =>
                      Query.fail(new IllegalArgumentException(
                        s"Parent task $parentID not found."))
                    case Some(parentTask) =>
                      Task.addChild(parentTask, childID)
                  }
                }
              }

              val createOrUpdateTargetTaskQ =
                targetTask match {
                  case Some(targetTaskID) =>
                    updateExistingTask(targetTaskID)
                  case None =>
                    createNewTargetTask() flatMap { t =>
                      updateParentWithNewChild(t.id)
                    }
                }

              Seq(updateOriginalTaskQ, createOrUpdateTargetTaskQ).join
            }
        }
      }
    }
  }

  abstract class BuildLocalIndexes(taskName: String)
      extends PaginateType[Segment](taskName, TaskVersion)
      with VersionSegmentMVTPaginator
      with ExceptionLogging {
    override def isStealable(task: Task): Boolean = false

    val StartPageSize = 256

    val MaxTaskRuntime = 60.seconds

    // FIXME: remove backwards compat initial "cursor" label
    val CursorFields = 1 to ParallelSegments map { i =>
      val suffix = if (i == 1) "" else s"_$i"
      Field[Option[Cursor]](s"cursor$suffix")
    }

    def backgrounded[T](q: Query[T]): Query[T] =
      for {
        snapshotTS <- Query.snapshotTime
        repoCtx    <- Query.repo
        result <- Query.future {
          repoCtx.forRepair.result(q, snapshotTS)
        }
      } yield result.value

    def paginate2(
      task: Task,
      stats: StatsRecorder,
      gauge: String,
      flushKeyspace: Keyspace,
      iter: RangeIteratee[RowID, Col],
      selector: Selector): Query[Task.State] = {

      val (segs, segsTail) = task.data(RangesField) splitAt ParallelSegments
      val cursors = CursorFields map { task.data(_) }

      if (segs.isEmpty) {
        Query.value(Task.Completed())
      } else {
        def runIter(state: RangeIteratee.State): Query[Unit] =
          state.step flatMap { _.fold(Query.unit)(runIter _) }

        def processSegment(
          seg: Segment,
          pageSize: Int,
          cursor: Option[Cursor],
          deadline: TimeBound): Query[Either[Option[Cursor], Unit]] =
          getPage(seg, None, cursor, pageSize, selector) flatMap {
            case (versions, next) =>
              runIter(iter.init(Query(Page(versions)))) flatMap { _ =>
                checkpointSegment(seg, pageSize, Right(next), deadline)
              }
          } recoverWith {
            case _: TimeoutException =>
              stats.incr(s"$gauge.Segment.Timeouts")
              checkpointSegment(seg, pageSize, Left(cursor), deadline)
            case NonFatal(e) =>
              // if a segment fails on a logic error, it'll get into a tight
              // loop and spam peers with reads. if that happens, log the
              // exception and drop the current iteration. NB there is some
              // redundancy with the per-version recovery handler below, but
              // this branch will catch unforseen logic errors with the rebuild
              // itself.

              logException(e)
              Query(Left(cursor))
          }

        def checkpointSegment(
          seg: Segment,
          pageSize: Int,
          next: Either[Option[Cursor], Option[Cursor]],
          deadline: TimeBound): Query[Either[Option[Cursor], Unit]] =
          next match {
            case Right(next) =>
              if (next.isDefined) {
                if (deadline.hasTimeLeft) {
                  processSegment(seg, pageSize + 32 min 1024, next, deadline)
                } else {
                  Query(Left(next))
                }
              } else {
                Query(Right(()))
              }

            case Left(next) =>
              if (deadline.hasTimeLeft) {
                processSegment(seg, pageSize / 2 + 1, next, deadline)
              } else {
                Query(Left(next))
              }
          }

        Query.repo flatMap { repo =>
          val runTime = repo.queryTimeout match {
            case duration: FiniteDuration => duration min MaxTaskRuntime
            case _: Duration.Infinite     => MaxTaskRuntime
          }
          val checkpointSegmentTimeBound = (runTime - 5.seconds).bound
          val runTimeBound = runTime.bound

          Query.snapshotTime flatMap { snapshotTime =>
            val futs = segs zip cursors map { case (seg, cursor) =>
              repo.run(
                processSegment(
                  seg,
                  StartPageSize,
                  cursor,
                  checkpointSegmentTimeBound),
                snapshotTime,
                runTimeBound,
                AccountID.Root,
                ScopeID.RootID,
                PermissiveOpsLimiter
              ) map { _._2 }
            }

            Query.future(futs.sequence map { segs zip _ }) flatMap { states =>
              var segs = Vector.empty[Segment]
              var cursors = Vector.empty[Option[Cursor]]

              states collect { case (seg, Left(next)) => (seg, next) } foreach {
                case (seg, next) =>
                  segs :+= seg
                  cursors :+= next
              }

              val newSegs = segs ++ segsTail
              val rngs = task.data.update(RangesField -> newSegs)
              val cs =
                CursorFields zip (cursors.iterator ++ Iterator.continually(None))

              stats.incr(s"$gauge.Pages.Processed")
              stats.set(s"$gauge.Remaining", newSegs.size.toDouble)

              // clear the accumulated schema state since we're done.
              if (newSegs.isEmpty) {
                Schema.clearState()
              }

              // report the current schema sizes.
              // FIXME: This is a weird place to be reporting this, but
              // considering current usage is coupled/localized with rebuild, it
              // is what it is for now. A more general batch framework should
              // address.
              Schema.recordSizeStats(stats)

              Query.updateState {
                _.addLocalKeyspaceFlush(flushKeyspace)
              } map { _ =>
                Task.Runnable((cs foldLeft rngs) { _ update _ }, task.parent)
              }
            }
          }
        }
      }
    }
  }

  final case class LocalIndexBuildTask(
    keyspace: Keyspace,
    stats: StatsRecorder,
    snapshotTS: Timestamp,
    maxTaskID: TaskID,
    buildMetadataIndexes: Boolean)
      extends RangeIteratee[(ScopeID, DocID), Version]
      with ExceptionLogging {

    def apply(rowID: (ScopeID, DocID)): Query[Option[ColIteratee[Version]]] =
      rowID match {
        case (_, TaskID(id)) if id > maxTaskID => Query.none
        case (scope, id)                       =>
          // Look up database state first, then collection, then indexer. This
          // order prevents unnecessary collection and indexer state from being
          // cached when it wouldn't be used.
          Schema.database.isDeleted(scope) flatMap {
            if (_) Query.none
            else {
              Schema.collection.isDeleted(scope, id.collID) flatMap {
                if (_) Query.none
                else {
                  Schema.index.getIndexer(scope, id.collID) map { indexer =>
                    Some(colIter(keyspace, indexer))
                  }
                }
              }
            }
          }
      }

    private def colIter(keyspace: Keyspace, indexer: Indexer): ColIteratee[Version] =
      ColIteratee {
        case None => Query.none
        case Some(page) =>
          val adds = page map { version =>
            // reset metrics after row generation in order to avoid hitting the
            // compute limit. This is the most generous interpretation of the
            // compute limit, as this is pretty close to a single version write
            // within a transaction. If a version's rows cannot be computed
            // within the limit, then this most definitely would fail in a live
            // transaction.
            val resetQ = Query.updateState { _.copy(metrics = new State.Metrics) }
            val rowsQ = indexer.rows(version) flatMap { rows =>
              resetQ map { _ => rows }
            }

            rowsQ foreach { indexRows =>
              indexRows foreach { row =>
                try {
                  val op = SetAdd(row.key, row.value)
                  val mut = CassandraMutation(
                    keyspace.getName,
                    op.rowKey.nioBuffer(),
                    index1CF = buildMetadataIndexes,
                    index2CF = !buildMetadataIndexes)

                  // We need to pass a timestamp to the C* mutation to ensure
                  // that when we load the snapshot, it will merge cleanly with
                  // the live write stream to the staging CFs. The
                  // straightforward choice is to use the transactionTS of the
                  // row. However the existence of legacy `changeID`s in the
                  // transaction ts slot complicates matters. Heuristically
                  // detect this case and report when it happens.
                  val txnTS = row.value.ts.transactionTS
                  val mutateTS = if (txnTS < snapshotTS) {
                    txnTS
                  } else {
                    stats.incr("LocalIndex.Build.Rows.LegacyChangeID")
                    if (version.isChange) {
                      getLogger.info(
                        s"Encountered legacy 'changeID' rebuilding ${version.id}")
                    } else {
                      getLogger.info(
                        s"Encountered legacy 'changeID' rebuilding ${version.id} (not flagged by Version.isChange)")
                    }
                    // fall back to the epoch if this is a possible change.
                    Timestamp.Epoch
                  }

                  op.mutateAt(mut, mutateTS)
                  keyspace.apply(mut.cmut, false /* writeCommitLog */ )

                  // incr stat only when processing is successful
                  stats.incr(s"LocalIndex.Build.Rows.Processed")
                } catch {
                  case _: ComponentTooLargeException =>
                    stats.incr(s"LocalIndex.Build.Rows.Invalid")
                }
              }

              stats.incr(s"LocalIndex.Build.Versions.Processed")
            } recover {
              // we need to propagate timeouts, which are retried above
              case e: TimeoutException => throw e
              case NonFatal(e) =>
                // in the case the exception is not known, we still want to move
                // on, but need to log more information.
                stats.incr(s"LocalIndex.Build.Versions.Failed")
                logException(RebuildException(version, e))
            }
          }

          adds.join map { _ =>
            Some(colIter(keyspace, indexer))
          }
      }
  }

  case object DistributeLocalMetadataIndexes
      extends DistributeIndexCFs("index-rebuild-distribute-metadata-index-cfs") {

    def keyspace = Storage.LocalMetadataIndexBuildKeyspace

    val sortedIndexCFName: String = Tables.SortedIndex.CFName
    val historicalIndexCFName: String = Tables.HistoricalIndex.CFName
  }

  object DistributeLocalDocumentIndexes
      extends DistributeIndexCFs("index-rebuild-distribute-index2cfs") {

    def keyspace = Storage.LocalIndexBuildKeyspace

    val sortedIndexCFName: String = Tables.SortedIndex.CFName2
    val historicalIndexCFName: String = Tables.HistoricalIndex.CFName2
  }

  abstract class DistributeIndexCFs(taskName: String)
      extends Type(taskName, TaskVersion)
      with ExceptionLogging {

    val sortedIndexCFName: String
    val historicalIndexCFName: String

    def keyspace: Keyspace

    override def isStealable(t: Task) = false

    private val log = getLogger

    def step(task: Task, snapshotTS: Timestamp): Query[Task.State] = {
      for {
        _ <- loadCF(sortedIndexCFName)
        _ <- loadCF(historicalIndexCFName)
      } yield {
        Task.Completed()
      }
    }

    def loadCF(cfName: String): Query[Unit] =
      Query.future {
        val view = getSSTableView(cfName)

        val barService = CassandraService.instance.barService
        val session = UUID.randomUUID()
        barService.restoreOne(
          restoreID = UUID.randomUUID(),
          session,
          cfName,
          view.sstables.asScala.toSeq,
          None,
          TimeBound.Max) transform { rv =>
          if (rv.isSuccess) {
            log.info(
              s"Transfer [$session] Load of ${keyspace.getName}/$cfName complete.")
          } else {
            log.info(
              s"Transfer [$session] Load of ${keyspace.getName}/$cfName failed.")
          }

          view.release()
          rv
        }
      }

    def getSSTableView(cfname: String) = {
      val cf = keyspace.getColumnFamilyStore(cfname)
      cf.selectAndReference(ColumnFamilyStore.CANONICAL_SSTABLES)
    }

    def create(parent: TaskID): Query[Iterable[Task]] =
      Task.createAllSegments(ScopeID.RootID, name, Some(parent)) { (_, _) => Data() }
  }
}
