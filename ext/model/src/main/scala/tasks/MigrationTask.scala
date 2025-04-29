package fauna.model.tasks

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Database, Task }
import fauna.model.schema.{ CollectionConfig, NativeIndex, SchemaStatus }
import fauna.repo.{ ColIteratee, IndexConfig, RangeIteratee, Store }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.stats.QueryMetrics
import fauna.storage.{ Selector, VersionID }
import fauna.storage.doc.{ Data, Field }
import fauna.storage.index.{ IndexTerm, IndexValue }
import fauna.storage.ir.DocIDV
import fauna.storage.ops.VersionAdd

// A task family that migrates versions of a single collection. The logic for
// starting and stopping tasks is as follows:
//  Append migrations to collection.
//  |
//  v
//  Cancel all migration tasks for the collection.
//  |
//  v
//  Launch new migration root task.
//    - Scan subtasks migrate all versions.
//  |
//  v
//  Task completes.
//  TODO: The migration list could be truncated on completion.
object MigrationTask {

  val ScopeField = Field[ScopeID]("database")
  val CollectionField = Field[CollectionID]("collection")
  // This field parallels the ranges field. For non-transactional tasks, it prevents
  // charging each write separately on each replica.
  // NB: It'd be nice to package this with the segment, but that would require
  //     re-plumbing the paginator code a bit.
  val IsPrimaryField = Field[Vector[Boolean]]("isPrimary")
  val StateField = Field[String]("state")
  val StartedAt = Field[Timestamp]("started_at")
  val DocsDoneField = Field[Int]("docs_done")
  val VersionsDoneField = Field[Int]("versions_done")

  final case class Iteratee(
    schema: CollectionSchema,
    useTxn: Boolean,
    onPrimarySegment: Boolean)
      extends RangeIteratee[(ScopeID, DocID), Version] {

    // Volatile to publish effects from within Query.
    @volatile private[this] var documents = Set.empty[DocID]
    @volatile private[this] var versions = 0

    def totalDocuments: Int = documents.size
    def totalVersions: Int = versions

    def apply(rowID: (ScopeID, DocID)) = {
      val (scope, id) = rowID
      Query.some(colIter(scope, id))
    }

    private def colIter(scope: ScopeID, id: DocID): ColIteratee[Version] =
      ColIteratee {
        case None => Query.none
        case Some(page) =>
          val writes = page.map { v: Version =>
            documents += v.docID
            versions += 1
            migrateQ(useTxn, onPrimarySegment, schema.schemaVersion, v)
          }

          writes.join.map { _ =>
            Some(colIter(scope, id))
          }
      }
  }

  def updateFromCommit(scope: ScopeID, id: CollectionID): Query[Unit] =
    for {
      // Handle data migrations:
      // - Cancel existing tasks.
      // - If the migration task is enabled,
      //    * Migrate the collection synchronously if it's small.
      //    * Otherwise, launch a new background task.
      _ <- MigrationTask.cancelForCollection(scope, id)
      // Sync migrations shouldn't pop rate limits.
      sync <- Query.unlimited(MigrationTask.Sync.migrate(scope, id))
      _ <-
        if (!sync) {
          MigrationTask.Root.create(scope, id).join
        } else {
          Query.unit
        }
    } yield ()

  // Returns the schema relevant for this task. The task always uses the current
  // cached schema, so it always updates documents to the latest schema version
  // (or retries if the transaction fails schema OCC).
  def schema(task: Task): Query[Option[CollectionSchema]] = {
    val scope = task.data(ScopeField)
    val id = task.data(CollectionField)

    val dbQ = Database.isDeleted(scope)
    val schemaQ = CollectionConfig(scope, id).mapT { _.Schema }

    (dbQ, schemaQ) par {
      case (false, Some(schema)) => Query.some(schema)
      case _                     => Query.none
    }
  }

  // Cancels all migration task instances for this collection.
  def cancelForCollection(scope: ScopeID, id: CollectionID): Query[Unit] =
    Database.latestForScope(scope).flatMap {
      case Some(db) =>
        Task.getAllByAccount(db.accountID, MigrationTask.Root.name).foreachValueT {
          task =>
            if (task.data(MigrationTask.CollectionField) == id) {
              task.cancel(None).join
            } else {
              Query.unit
            }
        }
      case None =>
        Query.unit
    }

  // `onPrimarySegment` is irrelevant if `useTxn` is set.
  private def migrateQ(
    useTxn: Boolean,
    onPrimarySegment: Boolean,
    to: SchemaVersion,
    v: Version): Query[Unit] =
    if (v.schemaVersion < to) {
      val w =
        VersionAdd(v.parentScopeID, v.docID, v.ts, v.action, to, v.data, v.diff)
      if (useTxn) {
        // Apply writes transactionally.
        Query.write(w)
      } else {
        // Write directly to the local C*.
        Query.repo.map { repo =>
          if (onPrimarySegment) {
            // Count writes to the primary segment, so replication is factored out.
            repo.stats.count(QueryMetrics.BytesWrite, w.numBytes)
          }
          repo.service.newStorage.overwriteVersion(w)
        }.join
      }
    } else Query.unit

  // A migration scan task. Iterates over all versions in a collection, migrating
  // them according to the current cached schema.
  case object Scan
      extends SegmentPaginateType("migration-scan", version = 0)
      with VersionSegmentMVTPaginator {

    override val isHealthy = Query.True

    override val isLoggable = true

    // Stealing scans has big GC impact. Don't steal.
    override def isStealable(task: Task): Boolean = false

    override def preCheckpoint(task: Task): Query[Task.State] =
      Query.repo.map { repo =>
        // wait until local writes have been flushed.
        repo.service.newStorage.syncVersions()
        task.state
      }

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)

      schema(task) flatMap {
        case None         => Query(Task.Completed())
        case Some(schema) =>
          // NB: Segment boundaries are not crossed in one call to `step`.
          val iter = Iteratee(
            schema,
            useTxn = false,
            task.data
              .getOrElse(IsPrimaryField, Vector.empty)
              .headOption
              .getOrElse(true))
          for {
            state <- paginate(
              task,
              RangesField,
              "Migration",
              iter,
              // NB: We could get a little bit of performance with a selector that
              //     skips up-to-date schema versions, but that's likely very little
              //     of the corpus whenever the corpus is large.
              Selector.from(scope, Seq(schema.collID))
            )
          } yield state match {
            case Task.Runnable(data, parent) =>
              // Drop the isPrimary values for any ranges that were completed.
              val ranges = data(RangesField)
              val isPrimary = data.getOrElse(IsPrimaryField, Vector.empty)
              val newIsPrimary = isPrimary.drop(isPrimary.size - ranges.size)
              Task.Runnable(
                data.update(
                  DocsDoneField -> (data
                    .getOrElse(DocsDoneField, 0) + iter.totalDocuments),
                  VersionsDoneField -> (data
                    .getOrElse(VersionsDoneField, 0) + iter.totalVersions),
                  IsPrimaryField -> newIsPrimary
                ),
                parent
              )
            case state => state
          }
      }
    }

    def create(
      scope: ScopeID,
      collID: CollectionID,
      topologyVersion: Long,
      ranges: Iterable[(Segment, Boolean)]): Data = {
      Data(
        ScopeField -> scope,
        CollectionField -> collID,
        TopologyVersionField -> topologyVersion,
        RangesField -> ranges.map(_._1).toVector,
        IsPrimaryField -> ranges.map(_._2).toVector
      )
    }

    def create(
      scope: ScopeID,
      collID: CollectionID,
      topologyVersion: Long,
      parent: TaskID): Query[Iterable[Task]] =
      // Non-transactional writes apply locally and aren't replicated, so every
      // data node must apply them.
      Task.createLabeledSegments(scope, name, Some(parent)) { (segments, _) =>
        create(scope, collID, topologyVersion, segments)
      }

    // Repartitions `sourceTask`'s segments to `targetHost` by creating a new scan
    // task with the same parent as this task.
    // TODO: The new task's segments are set as non-primary, which means they
    // won't be charged for.
    // This seems OK right now because this function is meant for rare operation
    // situations.
    def repartitionTo(sourceTask: Task, targetHost: HostID) =
      repartitionTaskSegments(sourceTask, targetHost, None) {
        case (topologyVersion, newSegments) =>
          create(
            sourceTask.data(ScopeField),
            sourceTask.data(CollectionField),
            topologyVersion,
            newSegments.map { (_, false) })
      }
  }

  // The root migration task. It forks the scans (state "fork") and waits for
  // them to finish (state "join").
  case object Root extends Type("migration-root") {

    // TODO: Override onComplete? Someday we'll want to record when migration
    //            is done.

    // TODO: Override onCancel?

    override def hasCustomPriority = true

    // Index builds should always have priority over migration tasks, because the
    // outcome of migration is invisible to user queries. Index builds are
    // deprioritized based on the number of builds in the same account, up to a max.
    // This custom priority gives the migration task a priority one notch below the
    // lowest priority possible for an index build.
    override def customPriority(accountID: AccountID): Query[Int] =
      Query.context.map { ctx =>
        (ctx.repo.taskReprioritizerLimit + 1) * -TaskReprioritizer.TaskPriorityDecrement
      }

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val id = task.data(CollectionField)
      val state = task.data(StateField)

      state match {
        case "fork" =>
          schema(task) flatMap {
            case None => Query(Task.Completed())
            case Some(_) =>
              val nextData = task.data.update(StateField -> "join")
              task.fork {
                for {
                  byIndexRoot <- ByIndex.Root.create(scope, id, task.id)
                } yield {
                  (Seq(byIndexRoot), nextData)
                }
              }
          }
        case "join" =>
          task.joinThen() {
            case Left(_) =>
              // Fall back to a table scan. Any surviving by-index tasks will
              // keep migrating, taking some work from the scans.
              Query.repo flatMap { repo =>
                val topologyVersion = repo.service.partitioner.partitioner.version
                task.fork {
                  Scan.create(scope, id, topologyVersion, task.id) map {
                    (_, task.data.update(StateField -> "join"))
                  }
                }
              }
            case Right(_) =>
              for {
                _ <- recordStats(task.data(StartedAt), task.elapsedMillis)
              } yield Task.Completed()
          }
      }
    }

    private def recordStats(startedAt: Timestamp, elapsedMillis: Long) =
      Query.repo map { repo =>
        val finishedAt = Clock.time
        val totalMillis = finishedAt.difference(startedAt).toMillis
        repo.stats.decr("Migrations")
        repo.stats.timing("Migrations.Time", totalMillis)
        repo.stats.set("Migrations.Time.Executable", elapsedMillis / totalMillis)
      }

    def create(
      scope: ScopeID,
      collID: CollectionID,
      parent: Option[TaskID] = None,
      isOperational: Boolean = false): Query[Task] = {
      for {
        repo <- Query.repo
        data = Data(
          StateField -> "fork",
          StartedAt -> Clock.time,
          ScopeField -> scope,
          CollectionField -> collID)
        task <- Task.createRandom(scope, name, data, parent, isOperational)
      } yield {
        repo.stats.incr("Migrations")
        task
      }
    }

    override val isHealthy = Query.True
  }

  // A synchronous migration not-task that will migrate all the versions of
  // a collection at once, if the collection is small enough.
  case object Sync {
    // The maximum number of versions a collection can have before it becomes
    // ineligible for synchronous migration.
    // TODO: I just used the index build number. Zero thought here.
    val MaxVersions = 128

    // This comes from the sync index build. Supposedly we need it because of
    // paging mumbo jumbo?
    // TODO: Investigate.
    private val FetchSize = MaxVersions + 2

    // Helper to keep eyes from bleeding reading complex nested queries. Given a set
    // of index values pointing to docs, returns a query evaluating to true and
    // migrating all versions of all docs if there aren't too many versions;
    // otherwise, evaluates to false and migrates no versions.
    private def migrateVersions(
      schema: CollectionSchema,
      docs: Seq[IndexValue]): Query[Boolean] =
      for {
        versions <- docs
          .map { iv =>
            Store
              .versions(
                schema,
                iv.docID,
                VersionID.MaxValue,
                VersionID.MinValue,
                FetchSize,
                reverse = false)
              .takeT(MaxVersions + 1)
              .flattenT
          }
          .sequence
          .map { _.flatten }
        didMigration <-
          if (versions.size > MaxVersions) {
            Query.False
          } else {
            // !!! IMPORTANT !!!
            // `useTxn = true` is _required_, because we must resolve the schema
            // version through the transaction pipeline before writing the migrated
            // version to storage.
            versions
              .map {
                migrateQ(
                  useTxn = true,
                  onPrimarySegment = false,
                  schema.schemaVersion,
                  _)
              }
              .sequence
              .map { _ =>
                true
              }
          }
        _ <-
          if (didMigration) Query.stats.map { _.incr("Migrations.Synchronous") }
          else Query.unit
      } yield didMigration

    // Attempt to migrate this collection synchronously. Returns a query which
    // evaluates to true and migrates the collection if the collection is small
    // enough to be synchronously migrated. Otherwise, returns false and migrates
    // no versions.
    def migrate(scope: ScopeID, id: CollectionID): Query[Boolean] = {
      val index = NativeIndex.DocumentsByCollection(scope)
      // NB: Count events with sorted index instead of historical index
      //     because historical index truncates below MVT, which could
      //     exclude older documents from the initial build.
      val docsQ = Store
        .sortedIndex(
          index.indexer.config,
          Vector(IndexTerm(id)),
          IndexValue.MaxValue,
          IndexValue.MinValue,
          FetchSize,
          ascending = false
        )
        .takeT(MaxVersions + 1)
        .flattenT

      val hasStagedQ = SchemaStatus.forScope(scope).map(_.hasStaged)
      (hasStagedQ, docsQ).par { (hasStaged, docs) =>
        if (hasStaged || docs.size > MaxVersions) {
          Query.False
        } else {
          // IMPORTANT: The synchronous migration is expected to happen in the same
          // transaction that the schema is updated, so we need to use the schema for
          // inline index builds. This schema will contain all the pending writes. It
          // is only valid to acquire this collection config once all the type env
          // validation has passed!
          CollectionConfig
            .getForSyncIndexBuild(scope, id)
            .flatMap {
              _.fold(Query.False) { config =>
                migrateVersions(config.Schema, docs)
              }
            }
        }
      }
    }
  }

  object ByIndex {

    private val log = getLogger

    case object Leaf
        extends PaginateType[(ScopeID, DocID)]("migration-by-index", version = 0)
        with VersionRowPaginator {

      // The current list of docs to migrate, consumed by paginate and empty when the
      // task is done.
      private[this] val DocsField = Field.OneOrMore[(ScopeID, DocID)]("docs")

      // The number of docs this task will migrate.
      private[this] val DocsTotalField = Field[Int]("docs_total")

      // Accumulates the total number of bytes read.
      private[this] val BytesReadField = Field[Long]("bytes_read")

      override val isLoggable = true

      override val isHealthy = Query.True

      private def createOne(
        scope: ScopeID,
        id: CollectionID,
        docs: Vector[(ScopeID, DocID)],
        parent: TaskID): Query[Task] = {
        val data = Data(
          ScopeField -> scope,
          CollectionField -> id,
          DocsField -> docs,
          DocsTotalField -> docs.size,
          DocsDoneField -> 0,
          VersionsDoneField -> 0)

        Task.createRandom(scope, name, data, Some(parent))
      }

      def createAll(
        scope: ScopeID,
        id: CollectionID,
        docs: Vector[DocID],
        parent: TaskID): Query[Iterable[Task]] =
        Query.repo flatMap { repo =>
          docs
            .grouped(repo.reindexDocsLimit)
            .map { docs =>
              createOne(scope, id, docs map { (scope, _) }, parent)
            }
            .toSeq
            .sequence
        }

      def step(task: Task, ts: Timestamp): Query[Task.State] = {
        schema(task) flatMap {
          case Some(schema) =>
            val iter = Iteratee(schema, useTxn = true, onPrimarySegment = false)
            paginate(
              task,
              DocsField,
              "MigrationByIndex",
              iter,
              Selector.All) flatMap {
              case Task.Runnable(data, parent) =>
                for {
                  repo   <- Query.repo
                  qState <- Query.state
                } yield {
                  val iterDocs = iter.totalDocuments
                  val iterVersions = iter.totalVersions
                  val qBytesRead = qState.metrics.bytesRead

                  val docsDone = data.getOrElse(DocsDoneField, 0) + iterDocs
                  val vsDone = data.getOrElse(VersionsDoneField, 0) + iterVersions
                  val bytesRead = data.getOrElse(BytesReadField, 0L) + qBytesRead

                  if (bytesRead > repo.reindexCancelBytesLimit) {
                    log.trace(
                      s"Migration-by-index (${task.id}): fallback to table scan due to bytes " +
                        s"read limit ${bytesRead} > ${repo.reindexCancelBytesLimit}")
                    Task.Cancelled() // Fall back to table scan.
                  } else {
                    Task.Runnable(
                      data.update(
                        DocsDoneField -> docsDone,
                        VersionsDoneField -> vsDone,
                        BytesReadField -> bytesRead
                      ),
                      parent
                    )
                  }
                }
              case state =>
                Query.value(state)
            }
          case None => Query.value(Task.Completed())
        }
      }
    }

    case object Root
        extends PaginateType[(IndexConfig, Vector[IndexTerm])](
          "migration-by-index-root",
          version = 0)
        with SortedRowPaginator {

      private[this] val DocsField = Field.ZeroOrMore[DocID]("docs")
      private[this] val StateField = Field[String]("state")

      override val isHealthy = Query.True

      def create(scope: ScopeID, id: CollectionID, parent: TaskID): Query[Task] = {
        val data =
          Data(ScopeField -> scope, CollectionField -> id, StateField -> "start")
        Task.createRandom(scope, name, data, Some(parent))
      }

      def step(task: Task, ts: Timestamp): Query[Task.State] = {
        val scope = task.data(ScopeField)

        task.data(StateField) match {
          case "start" =>
            Query.repo flatMap { repo =>
              val id = task.data(CollectionField)
              val idx = NativeIndex.DocumentsByCollection(scope)
              val terms = Vector(IndexTerm(DocIDV(id.toDocID)))
              val iter = new Reindex.Root.ReindexIter
              paginateOnce(
                task,
                (idx, terms),
                "MigrationByIndexRoot",
                iter,
                Selector.All) flatMap {
                case Task.Runnable(data, parent) =>
                  val all = iter.result ++ task.data(DocsField)

                  if (all.size > repo.reindexCancelLimit) {
                    log.trace(
                      s"Migration-by-index (${task.id}: fall back to table scan ${all.size} > ${repo.reindexCancelLimit}")
                    Query.value(Task.Cancelled()) // Trigger a table scan.
                  } else {
                    // Continue gathering DocIDs.
                    Query.value(
                      Task.Runnable(data.update(DocsField -> all.toVector), parent))
                  }
                case _: Task.Completed =>
                  val all = iter.result ++ task.data(DocsField)

                  // All DocIDs gathered; fork or fall back to table scan?
                  if (all.size > repo.reindexCancelLimit) {
                    log.trace(
                      s"Migration-by-index (${task.id}): fall back to table scan ${all.size} > ${repo.reindexCancelLimit}")
                    Query.value(Task.Cancelled()) // Trigger a table scan.
                  } else if (all.isEmpty) {
                    log.trace(
                      s"Migration-by-index (${task.id}): no documents found in $scope $id")
                    Query.value(Task.Completed())
                  } else {
                    task.fork {
                      Leaf.createAll(scope, id, all.toVector, task.id) map {
                        (_, task.data.update(StateField -> "join"))
                      }
                    }
                  }

                case state => Query.value(state)
              }
            }
          case "join" =>
            task.joinThen() {
              case Left(_) => Query.value(Task.Cancelled())
              case _       => Query.value(Task.Completed())
            }
        }
      }
    }
  }
}
