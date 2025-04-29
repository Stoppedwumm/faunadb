package fauna.model.tasks

import fauna.atoms._
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model.{ Collection, Database, RuntimeEnv, Task }
import fauna.model.runtime.fql2.serialization.ValueFormat
import fauna.model.schema.NativeIndex
import fauna.model.tasks.export._
import fauna.model.Task.Children
import fauna.repo.{
  ColIteratee,
  IndexConfig,
  RangeIteratee,
  SchemaContentionException
}
import fauna.repo.doc.Version
import fauna.repo.query.{ QFail, Query }
import fauna.storage.doc.{ Data, Field }
import fauna.storage.doc.FieldType
import fauna.storage.index.IndexTerm
import fauna.storage.ir.{ DocIDV, StringV }
import fauna.storage.Selector
import java.nio.file.{ Files, Path, Paths }
import scala.util.control.NonFatal

object ExportDataTask extends ExceptionLogging {

  private val log = getLogger

  implicit val PathFieldType = FieldType[Path]("Path") {
    _.toString
  } { case StringV(s) =>
    Paths.get(s)
  }

  val ExportIDField = Field[String]("export_id")
  val RootTaskIDField = Field[Option[TaskID]]("internal_id")
  val ScopeField = Field[ScopeID]("database")
  val CollIDsField = Field.ZeroOrMore[CollectionID]("coll_ids")
  val CollNamesField = Field.ZeroOrMore[String]("coll_names")
  val SnapshotTSField = Field[Timestamp]("snapshot_ts")
  val DocFormatField = Field[ValueFormat]("doc_format")
  val DatafileFormatField = Field[DatafileFormat]("datafile_format")
  val DatafileCompressionField = Field[Boolean]("datafile_compression")

  val TotalPartitionsField = Field[Option[Int]]("total_partitions")
  val PartitionField = Field[Option[Int]]("partition")
  val FilesCountField = Field[Option[Int]]("files_count")
  val FilesBytesField = Field[Option[Long]]("files_bytes")

  val OutfilesField = Field.ZeroOrMore[Path]("outfiles")
  val FragmentIdxField = Field[Option[Fragment.Idx]]("fragment_idx")

  val RangesField = Field.ZeroOrMore[Segment]("ranges")
  val StateField = Field[String]("state")
  val ErrorField = Field[Option[String]]("error")

  class ExportDataIter(manager: FilesManager)
      extends RangeIteratee[(ScopeID, DocID), Version] {

    private[this] val formatter = manager.formatter

    @volatile private[this] var docsExported: Long = 0
    @volatile private[this] var renderTime: Long = 0
    @volatile private[this] var writeTime: Long = 0

    def totalDocsExported: Long = docsExported
    def totalRenderTime: Long = renderTime / 1_000_000
    def totalWriteTime: Long = writeTime / 1_000_000

    def apply(rowID: (ScopeID, DocID)): Query[Option[ColIteratee[Version]]] = {
      val (scope, id) = rowID

      if (manager.shouldExport(id)) {
        Query.some(colIter(scope, id))
      } else {
        Query.none
      }
    }

    private def colIter(scope: ScopeID, id: DocID): ColIteratee[Version] =
      ColIteratee {
        case Some(page) =>
          val snapTS = manager.info.snapshotTS
          val writeQ =
            page
              .collect { case v: Version.Live if v.ttl.forall(_ > snapTS) => v }
              .map { version =>
                docsExported += 1

                val render = Timing.start
                formatter.formatVersion(version) map { case (id, line) =>
                  renderTime += render.elapsedNanos

                  val write = Timing.start
                  manager.writeEntry(id, line)
                  writeTime += write.elapsedNanos
                }
              }

          writeQ.join flatMap { _ =>
            Query.some(colIter(scope, id))
          }

        case None =>
          Query.none
      }
  }

  private def withExportInfo(task: Task)(fn: ExportInfo => Query[Task.State]) = {
    Query.repo flatMap { repo =>
      repo.exportPath match {
        case Some(path) => fn(ExportInfo(path, task))
        // fail the task if export_path is not configured.
        case None =>
          failTaskWithException(task) {
            sys.error("export_path not configured")
          }
      }
    }
  }

  private def checkMVTOrFail(task: Task)(fn: => Query[Task.State]) =
    task.state match {
      case Task.Runnable(_, _) =>
        val collIDs = task.data(CollIDsField)
        val snapshotTS = task.data(SnapshotTSField)
        val database = task.data(ScopeField)

        collIDs
          .map(Collection.deriveMinValidTime(database, _))
          .sequence
          .flatMap { mvts =>
            if (mvts.exists(_ > snapshotTS)) {
              mvts.zip(collIDs) foreach { case (mvt, coll) =>
                if (mvt > snapshotTS) {
                  log.info(
                    s"MVT $mvt is above snapshot_ts $snapshotTS on collection ($database -> $coll).")
                }
              }

              Query.value(failTask(task, "mvt is above snapshot_ts"))
            } else {
              fn
            }
          }

      case _ =>
        fn
    }

  private def maybeFailTask(task: Task, error: Option[String]) = {
    if (error.isDefined) {
      Task.Cancelled(Some(task.data.update(ErrorField -> error)))
    } else {
      Task.Cancelled()
    }
  }

  private def failTask(task: Task, ex: Throwable) = {
    logException(ex)
    maybeFailTask(task, Some(s"internal error"))
  }

  private def failTask(task: Task, error: String) =
    maybeFailTask(task, Some(error))

  private def failTaskWithException(task: Task)(
    f: => Query[Task.State]): Query[Task.State] =
    QFail.guard(f) recover {
      case NonFatal(ex: SchemaContentionException) =>
        throw ex
      case NonFatal(ex) =>
        failTask(task, ex)
    }

  private def cancelChildren(task: Task): Query[Unit] = {
    task match {
      case Children(tasks) =>
        tasks map { id =>
          Task.get(id) flatMapT { task =>
            if (task.isCancelled) {
              Query.some(task)
            } else {
              TaskRouter.cancel(task, None) map {
                Some(_)
              }
            }
          }
        } join

      case _ =>
        Query.unit
    }
  }

  def indexScan(
    exportID: String,
    scope: ScopeID,
    collIDs: Vector[CollectionID],
    collNames: Vector[String],
    snapshotTs: Timestamp,
    docFormat: ValueFormat,
    datafileFormat: DatafileFormat,
    datafileCompression: Boolean,
    parent: Option[TaskID] = None,
    isOperational: Boolean = false): Query[Task] =
    Root.create(
      exportID,
      scope,
      collIDs,
      collNames,
      snapshotTs,
      docFormat,
      datafileFormat,
      datafileCompression,
      "start-index-scan",
      parent,
      isOperational)

  def tableScan(
    exportID: String,
    scope: ScopeID,
    collIDs: Vector[CollectionID],
    collNames: Vector[String],
    snapshotTs: Timestamp,
    docFormat: ValueFormat,
    datafileFormat: DatafileFormat,
    datafileCompression: Boolean,
    parent: Option[TaskID] = None,
    isOperational: Boolean = false): Query[Task] =
    Root.create(
      exportID,
      scope,
      collIDs,
      collNames,
      snapshotTs,
      docFormat,
      datafileFormat,
      datafileCompression,
      "start-table-scan",
      parent,
      isOperational)

  case object Root extends AccountCustomPriorityType("export-data-root") {

    def step(task: Task, ts: Timestamp): Query[Task.State] =
      withExportInfo(task) { info =>
        task.data(StateField) match {
          // start export using table scan.
          case "start-table-scan" =>
            task.fork {
              TableScan.create(info, task) map { subtasks =>
                (
                  subtasks,
                  task.data.update(
                    StateField -> "join-table-scan",
                    TotalPartitionsField -> Some(subtasks.size)))
              }
            }

          // start export using index.
          case "start-index-scan" =>
            task.fork {
              ByIndex.Root.create(info, task) map { root =>
                (Seq(root), task.data.update(StateField -> "join-index-scan"))
              }
            }

          // join index tasks.
          case "join-index-scan" =>
            task.joinThen() {
              case Left(subs) =>
                // fall back to table scan if index did not error
                subs.head.data(ErrorField) match {
                  case Some(err) => Query.value(failTask(task, err))

                  case None =>
                    Query.value(
                      Task.Runnable(
                        task.data.update(StateField -> "start-table-scan"),
                        task.parent))
                }

              case Right(_) =>
                // index task completed successfully.
                Query.value(Task.Completed(Some(task.data)))
            }

          // join table scan tasks.
          case "join-table-scan" =>
            task.joinThen() {
              case Left(subs) =>
                val err = subs.view.flatMap(_.data(ErrorField)).headOption
                Query.value(maybeFailTask(task, err))
              case Right(_) => Query.value(Task.Completed(Some(task.data)))
            }
        }
      }

    def create(
      exportID: String,
      scope: ScopeID,
      collIDs: Vector[CollectionID],
      collNames: Vector[String],
      snapshotTS: Timestamp,
      docFormat: ValueFormat,
      datafileFormat: DatafileFormat,
      datafileCompression: Boolean,
      initialState: String,
      parent: Option[TaskID],
      isOperational: Boolean): Query[Task] = {
      val data = Data(
        ScopeField -> scope,
        CollIDsField -> collIDs,
        CollNamesField -> collNames,
        ExportIDField -> exportID,
        SnapshotTSField -> snapshotTS,
        DocFormatField -> docFormat,
        DatafileFormatField -> datafileFormat,
        DatafileCompressionField -> datafileCompression,
        StateField -> initialState
      )

      Database.addMVTPin(scope, snapshotTS) flatMap { _ =>
        Task.createRandom(scope, name, data, parent, isOperational)
      }
    }

    private def tryRemoveMVTPin(scope: ScopeID): Query[Unit] =
      Database.removeMVTPin(scope).recover { case NonFatal(e) =>
        logException(e)
      }

    override def onComplete(task: Task): Query[Unit] =
      tryRemoveMVTPin(task.scopeID)

    override def onCancel(task: Task): Query[Unit] =
      cancelChildren(task) andThen tryRemoveMVTPin(task.scopeID)
  }

  sealed trait ExportScanType extends Type {

    override val isLoggable: Boolean = true

    // cannot be stolen. Task emits local data and table scans perform local reads.
    override def isStealable(task: Task): Boolean = false

    def step0(task: Task, info: ExportInfo, manager: FilesManager): Query[Task.State]

    // use a dummy task to force checkpointing
    protected def forkFinalizeTask(task: Task): Query[Task.State] =
      failTaskWithException(task) {
        checkMVTOrFail(task) {
          task.fork {
            Type.DummyTask.create(task) map { dummy =>
              (Seq(dummy), task.data.update(StateField -> "finalize"))
            }
          }
        }
      }

    // preStep is called before step iteration begins
    override def preStep(task: Task): Query[Task.State] =
      withExportInfo(task) { info =>
        failTaskWithException(task) {
          info.validatedCollections.flatMap {
            case Left(reason) => Query.value(failTask(task, reason))

            case Right(collections) =>
              val partition = task.data(PartitionField).getOrElse(0)
              val prevOut = task.data(OutfilesField)
              val prevIdx = task.data(FragmentIdxField).getOrElse(Fragment.NullIdx)
              val isRunning = task.data(StateField) == "running"

              val manager = FilesManager.init(
                task.id,
                info,
                collections,
                partition,
                prevOut,
                prevIdx)

              manager.processOutfiles(prevOut, incremental = isRunning)
              Query.value(task.state)
          }
        }
      }

    final def step(task: Task, snapshotTS: Timestamp): Query[Task.State] =
      withExportInfo(task) { info =>
        failTaskWithException(task) {
          task.data(StateField) match {
            case "running" =>
              val prevIdx = task.data(FragmentIdxField).getOrElse(Fragment.NullIdx)
              val newIdx = prevIdx.incr
              val manager = FilesManager.forIdx(task.id, newIdx)

              step0(task, info, manager).map { state =>
                manager.close()
                // update fragment idx which ensures subsequent processing will
                // see this step's output
                state.withData(state.data.update(FragmentIdxField -> Some(newIdx)))
              }
            case "finalize" =>
              task.joinThen() { _ =>
                if (info.existsTempFiles()) {
                  // recheckpoint to emit remaining files
                  forkFinalizeTask(task)
                } else {
                  info.cleanupTempPaths()

                  val part = task.data(PartitionField).getOrElse(0)
                  val totalParts = task.data(TotalPartitionsField).getOrElse(1)
                  val filesCount = task.data(FilesCountField).getOrElse(0)
                  val filesBytes = task.data(FilesBytesField).getOrElse(0L)

                  info.finalizeExport(
                    task.host,
                    part,
                    totalParts,
                    filesCount,
                    filesBytes)
                  Query.value(Task.Completed())
                }
              }
          }
        }
      }

    // preCheckpoint is called after step iteration has completed, which happens in 3
    // cases:
    //
    // - the task's time slice has expired
    // - the last step call resulted in an exception
    // - the last step call returned a non-runnable state
    override def preCheckpoint(task: Task): Query[Task.State] =
      failTaskWithException(task) {
        checkMVTOrFail(task) {
          val stateQ =
            if (!task.isRunnable) {
              Query.value(task.state)

            } else if (!FilesManager.contains(task.id)) {
              // If we for some reason do not find a FilesManager state, then log
              // and warn. In theory the next task execution will find it assuming
              // the task is still runnable.
              log.warn(s"Did not find FilesManager for ${task.id}")
              Query.value(task.state)

            } else {
              val idx = task.data(FragmentIdxField).getOrElse(Fragment.NullIdx)
              val manager = FilesManager.forIdx(task.id, idx)

              Query.future(manager.processOutfilesResult).map { newOut =>
                val countDelta = newOut.size
                val bytesDelta = newOut.view.map(o => Files.size(o.path)).sum

                val count = task.data(FilesCountField).getOrElse(0) + countDelta
                val bytes = task.data(FilesBytesField).getOrElse(0L) + bytesDelta

                val outfiles = {
                  val prev = task.data(OutfilesField).map(Outfile(_))
                  val prevMax = Outfile.maxByCollection(prev)
                  newOut.concat(prevMax.values).map(_.path)
                }

                val data0 = task.data.update(
                  OutfilesField -> outfiles,
                  FilesCountField -> Some(count),
                  FilesBytesField -> Some(bytes))

                task.state.withData(data0)
              }
            }

          stateQ.ensure {
            Query.deferred(FilesManager.drop(task.id))
          }
        }
      }

    override def onCancel(task: Task): Query[Unit] =
      Query.repo.map { repo =>
        FilesManager.drop(task.id)
        repo.exportPath.map(ExportInfo(_, task)).foreach {
          _.cleanupTempPaths()
        }
      }
  }

  case object TableScan
      extends PaginateType[Segment]("export-data-table-scan", version = 0)
      with ExportScanType
      with SnapshotSegmentMVTPaginator {

    def step0(
      task: Task,
      info: ExportInfo,
      manager: FilesManager): Query[Task.State] = {
      val ranges = task.data(RangesField)

      if (ranges.isEmpty) {
        forkFinalizeTask(task)
      } else {
        Query.stats flatMap { stats =>
          val iter = new ExportDataIter(manager)
          val start = Timing.start

          paginate(
            task,
            RangesField,
            "Export.Data",
            iter,
            Selector.from(info.scope, info.collIDs)).map {

            case Task.Runnable(data, parent) =>
              stats.count(
                s"Task.${task.statName}.DocsExported",
                iter.totalDocsExported.toInt)
              stats.timing(s"Task.${task.statName}.Paginate", start.elapsedMillis)
              stats.timing(s"Task.${task.statName}.Render", iter.totalRenderTime)
              stats.timing(s"Task.${task.statName}.Write", iter.totalWriteTime)

              Task.Runnable(data, parent)

            case state => state
          }
        }
      }
    }

    def create(
      info: ExportInfo,
      ranges: Iterable[Segment],
      partition: Int,
      totalPartitions: Int): Data =
      info.toTaskData.update(
        StateField -> "running",
        PartitionField -> Some(partition),
        TotalPartitionsField -> Some(totalPartitions),
        RangesField -> ranges.toVector
      )

    /** Creates tasks in all hosts of a random data replica.
      */
    def create(info: ExportInfo, parent: Task): Query[Iterable[Task]] =
      Query.repo.flatMap { repo =>
        // This relies on the fact that createPrimarySegments uses
        // service.dataHosts
        // FIXME: have task create methods pass the total subtasks size down.
        val totalParts = repo.service.dataHosts.size
        Task.createPrimarySegments(info.scope, name, Some(parent.id)) {
          (segs, part) => create(info, segs, part, totalParts)
        }
      }
  }

  object ByIndex {
    val ToScanField = Field.ZeroOrMore[CollectionID]("colls_to_scan")
    val DocsField = Field.ZeroOrMore[DocID]("docs")

    /** Scans the NativeIndex.DocumentsByCollection internal index and collects all
      * documents
      */
    case object Root
        extends PaginateType[(IndexConfig, Vector[IndexTerm])](
          "export-data-index-root",
          version = 0)
        with SortedRowPaginator {

      def step(task: Task, snapshotTS: Timestamp): Query[Task.State] =
        withExportInfo(task) { info =>
          failTaskWithException(task) {
            val stepQ = task.data(StateField) match {
              case "start" =>
                Query.repo flatMap { repo =>
                  val toScan = task.data(ToScanField)

                  if (toScan.isEmpty) {
                    task.fork {
                      ByIndex.Leaf.create(
                        info,
                        task.data(DocsField).sorted,
                        task) map { scan =>
                        (Seq(scan), task.data.update(StateField -> "join"))
                      }
                    }
                  } else {
                    val id = toScan.head
                    val idx = NativeIndex.DocumentsByCollection(info.scope)
                    val terms = Vector(IndexTerm(DocIDV(id.toDocID)))
                    val iter = new Reindex.Root.ReindexIter(info.snapshotTS)

                    paginateOnce(
                      task,
                      (idx, terms),
                      "ExportData.ByIndex",
                      iter,
                      Selector.from(info.scope)) flatMap {
                      case Task.Runnable(data, parent) =>
                        val all = iter.result ++ task.data(DocsField)

                        if (all.size > repo.reindexCancelLimit) {
                          Query.value(Task.Cancelled()) // trigger a table scan.
                        } else {
                          // Continue gathering DocIDs.
                          Query.value(
                            Task
                              .Runnable(
                                data.update(DocsField -> all.toVector),
                                parent))
                        }
                      case _: Task.Completed =>
                        val all = iter.result ++ task.data(DocsField)

                        // All DocIDs gathered; fork or fall back to table scan?
                        if (all.size > repo.reindexCancelLimit) {
                          Query.value(Task.Cancelled()) // trigger a table scan.
                        } else {
                          Query.value(
                            Task.Runnable(
                              task.data.update(
                                DocsField -> all.toVector,
                                ToScanField -> toScan.tail,
                                CursorField -> None,
                                PageSizeField -> None,
                                TransactionSizeField -> None),
                              task.parent
                            ))
                        }

                      case state => Query.value(state)
                    }
                  }
                }

              case "join" =>
                task.joinThen() {
                  // propagate subtask cancelled state
                  case Left(subs) =>
                    val err = subs.head.data(ErrorField)
                    Query.value(maybeFailTask(task, err))
                  case Right(_) => Query.value(Task.Completed())
                }
            }

            // This read-only task must take care the MVT has been pinned,
            // to ensure it always sees the correct set of documents.
            // Its children, which read the docs, will then observe the pin
            // as well.
            for {
              // Do an uncached read here because we really want to be sure
              // the pin is in place and we don't have a good way to do the
              // check using the current cache + invalidation semantics.
              // TODO: Use an improved cache to serve this read.
              db <- Database.getUncached(task.scopeID)
              _ = require(
                db.forall(_.minimumValidTimePinnedTime.nonEmpty),
                s"Expected pinned MVT for scope ${task.scopeID}")
              newState <- stepQ
            } yield newState
          }
        }

      /** Creates the task in a random data host. At this point
        * doesn't matter which replica it belongs as this task
        * will be the only participant in the index scan until
        * the end.
        */
      def create(info: ExportInfo, parent: Task): Query[Task] = {
        val data = info.toTaskData.update(
          StateField -> "start",
          ToScanField -> info.collIDs
        )

        Task.createRandom(info.scope, name, data, Some(parent.id))
      }

      override def onCancel(task: Task): Query[Unit] =
        cancelChildren(task)
    }

    case object Leaf
        extends Type("export-data-index-scan", version = 0)
        with ExportScanType {
      // Once a task is born with a page_size it will stay with that value until it's
      // done so changes to DefaultPageSize doesn't affect page boundaries during
      // exports.
      val PageSizeField = Field[Int]("page_size")

      // TODO:: adds a config or FF for this
      val DefaultPageSize = 512

      def step0(
        task: Task,
        info: ExportInfo,
        manager: FilesManager): Query[Task.State] = {
        val docs = task.data(DocsField)

        if (docs.isEmpty) {
          forkFinalizeTask(task)
        } else {
          val formatter = manager.formatter

          val pageSize = task.data(PageSizeField)
          val page = docs.take(pageSize)
          val nextPage = docs.drop(pageSize)

          val linesQ = page map { id =>
            RuntimeEnv.Default
              .Store(info.scope)
              .get(id, info.snapshotTS) flatMapT {
              formatter.formatVersion(_) map { Some(_) }
            }
          } sequence

          val writeQ = Query.withBytesReadDelta(linesQ) flatMap { case (ls, delta) =>
            // TODO:: should we limit this task with
            // repo.reindexCancelBytesLimit and fallback to table scan?
            Query.incrDocuments(delta getOrElse 0) flatMap { _ =>
              Query.stats map { stats =>
                val lines = ls.flatten

                stats.count(s"Task.${task.statName}.DocsExported", lines.size)

                lines foreach { case (id, line) =>
                  manager.writeEntry(id, line)
                }
              }
            }
          }

          writeQ map { _ =>
            Task.Runnable(task.data.update(DocsField -> nextPage), task.parent)
          }
        }
      }

      def create(
        info: ExportInfo,
        docs: Vector[DocID],
        parent: Task): Query[Task] = {
        val data = info.toTaskData.update(
          StateField -> "running",
          DocsField -> docs,
          PageSizeField -> DefaultPageSize
        )
        Task.create(info.scope, name, parent.host, data, Some(parent.id))
      }
    }
  }
}
