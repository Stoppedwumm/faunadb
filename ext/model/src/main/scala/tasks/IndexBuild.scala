package fauna.model.tasks

import fauna.atoms._
import fauna.lang.{ Page, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.{ CollectionConfig, CollectionSchemaHooks }
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.Selector

final case class IndexBuild(scopeID: ScopeID, idx: IndexConfig, ts: Timestamp)
    extends RangeIteratee[(ScopeID, DocID), Version] {

  /** Accumulates the number of documents built using this iteratee.
    *
    * Volatile to publish effects from within a Query.
    */
  @volatile private[this] var documents = Set.empty[DocID]

  /** Accumulates the number of versions built using this iteratee.
    *
    * Volatile to publish effects from within a Query.
    */
  @volatile private[this] var versions: Int = 0

  // Preconditions:
  //   * scope == scopeID
  //   * IndexSources.All || IndexSources.Limit(id.collectionID)
  //
  // Use `covers` to determine whether a given row is covered by
  // this index.
  def apply(rowID: (ScopeID, DocID)) = {
    val (scope, id) = rowID
    Query.some(colIter(scope, id))
  }

  def totalDocuments: Int = documents.size
  def totalVersions: Int = versions

  private def covers(id: DocID): Boolean =
    idx.binders(id.collID).isDefined

  private def colIter(scope: ScopeID, id: DocID): ColIteratee[Version] =
    ColIteratee {
      case None => Query.none
      case Some(page) =>
        Query.repo flatMap { repo =>
          page filter { c =>
            covers(c.id)
          } map { res =>
            // regardless of whether any entries were emitted, count
            // this version as indexed.
            documents += res.id
            versions += 1

            Store.build(res, idx.indexer) map { added =>
              if (added > 0) repo.stats.count("Index.Build.Events.Added", added)
            }
          } join
        } map { _ =>
          Some(colIter(scope, id))
        }
    }
}

object IndexBuild {

  val ScopeField = Field[ScopeID]("database")
  val IndexField = Field[IndexID]("index")
  val SourcesField = Field[Vector[CollectionID]]("sources")
  val StateField = Field[String]("state")
  val StartedAt = Field[Timestamp]("started_at")
  val ActivatedAt = Field[Timestamp]("activated_at")
  val StrategyField = Field[Option[String]]("strategy")

  // index returns the index relevant for this task.
  def index(task: Task): Query[Option[Index]] = {
    val scope = task.data(ScopeField)
    val idOpt = task.data.getOpt(IndexField)

    val dbQ = Database.isDeleted(scope)
    val idxQ = idOpt match {
      case Some(id) => Index.get(scope, id)
      case _        => Query.none
    }

    (dbQ, idxQ) par {
      case (false, Some(idx)) => Query.some(idx)
      case _                  => Query.none
    }
  }

  case object ScanTask
      extends SegmentPaginateType("index-build-scan", version = 0)
      with VersionSegmentMVTPaginator {

    /** As scanning proceeds, accumulates the total number of documents indexed.
      */
    private[this] val DocsDoneField = Field[Int]("docs_done")

    /** As scanning proceeds, accumulates the total number of versions indexed.
      */
    private[this] val VersionsDoneField = Field[Int]("versions_done")

    override val isHealthy = Query.True

    override val isLoggable = true

    /** ScanTask is not stealable without severe GC impact. Until that
      * is fixed, these tasks may not be moved from their original host.
      *
      * If a host processing a ScanTask goes away, the task must be
      * rebuilt.
      */
    override def isStealable(task: Task): Boolean = false

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)

      index(task) flatMap {
        case None => Query.value(Task.Completed())
        case Some(idx) =>
          val collIDsQ: Query[Set[CollectionID]] = idx.sources match {
            case IndexSources.All =>
              throw new IllegalStateException(
                s"wildcard index $scope ${idx.id} cannot include native collections")
            case IndexSources.Custom =>
              CollectionID
                .getAllUserDefined(scope)
                .foldLeftValuesT(Set.empty[CollectionID]) { case (acc, cid) =>
                  acc + cid
                }

            case IndexSources.Limit(classes) =>
              val pg: PagedQuery[Iterable[CollectionID]] = Query.value(Page(classes))
              pg.selectMT(CollectionConfig(scope, _).map(_.isDefined))
                .flattenT
                .map(_.toSet)
          }

          collIDsQ flatMap { collIDs =>
            if (collIDs.isEmpty) {
              Query.value(Task.Completed())
            } else {
              val iter = IndexBuild(scope, idx, ts)
              paginate(
                task,
                RangesField,
                "Index.Build",
                iter,
                Selector.from(scope, collIDs.toVector)) map {
                case Task.Runnable(data, parent) =>
                  Task.Runnable(
                    data.update(
                      DocsDoneField -> (data
                        .getOrElse(DocsDoneField, 0) + iter.totalDocuments),
                      VersionsDoneField -> (data
                        .getOrElse(VersionsDoneField, 0) + iter.totalVersions)),
                    parent
                  )
                case state => state
              }
            }
          }
      }
    }

    def create(
      scope: ScopeID,
      idx: IndexID,
      topologyVersion: Long,
      ranges: Iterable[Segment]): Data =
      Data(
        ScopeField -> scope,
        IndexField -> idx,
        TopologyVersionField -> topologyVersion,
        RangesField -> ranges.toVector)

    def create(
      scope: ScopeID,
      idx: IndexID,
      topologyVersion: Long,
      parent: TaskID): Query[Iterable[Task]] =
      Task.createPrimarySegments(scope, name, Some(parent)) { (segs, _) =>
        create(scope, idx, topologyVersion, segs)
      }

    def repartitionTo(sourceTask: Task, targetHost: HostID) =
      repartitionTaskSegments(sourceTask, targetHost, None) {
        case (topologyVersion, newSegments) =>
          create(
            sourceTask.data(ScopeField),
            sourceTask.data(IndexField),
            topologyVersion,
            newSegments)
      }
  }

  case object IndexTask extends Type("index-build-index") {

    override val isHealthy = Query.True

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val sources = task.data(SourcesField)

      task.data(StateField) match {
        case "start" =>
          index(task) flatMap {
            case None => Query.value(Task.Completed())
            case Some(idx) =>
              task.fork {
                val tasks = sources map { cls =>
                  val terms = Vector(DocIDV(cls.toDocID))
                  Reindex.Root.create(
                    scope,
                    NativeIndexID.DocumentsByCollection,
                    terms,
                    idx.id,
                    Some(task.id))
                } sequence

                tasks map {
                  (
                    _,
                    task.data.update(
                      StrategyField -> Some("Reindex"),
                      StateField -> "join"))
                }
              }
          }
        case "join" =>
          task.joinThen() {
            case Right(_) => Query.value(Task.Completed())
            case Left(_) =>
              index(task) flatMap {
                case None      => Query.value(Task.Completed())
                case Some(idx) =>
                  // the index is still valid; fall back to a table
                  // scan, and orphan any remaining reindex tasks,
                  // they do no harm and might help make progress
                  Query.repo flatMap { repo =>
                    val currentTopologyVersion =
                      repo.service.partitioner.partitioner.version
                    task.fork {
                      ScanTask.create(
                        scope,
                        idx.id,
                        currentTopologyVersion,
                        task.id) map {
                        (
                          _,
                          task.data.update(
                            StrategyField -> Some("Scan"),
                            StateField -> "join"))
                      }
                    }
                  }
              }
          }
      }
    }

    def create(scope: ScopeID, idx: IndexID, sources: Iterable[CollectionID]): Data =
      Data(
        ScopeField -> scope,
        IndexField -> idx,
        SourcesField -> sources.toVector,
        StateField -> "start")

    def create(
      scope: ScopeID,
      idx: IndexID,
      sources: Iterable[CollectionID],
      parent: TaskID): Query[Task] =
      Task.createRandom(scope, name, create(scope, idx, sources), Some(parent))
  }

  /** Builds an index via the following state machine:
    *
    * 1. Fork build tasks (BUILDING)
    * 2. Join build tasks; mark active (BUILDING -> ACTIVE)
    */
  case object RootTask extends AccountCustomPriorityType("index-build-join") {

    override def onComplete(task: Task): Query[Unit] = {
      index(task) flatMap {
        case Some(index) if index.isCollectionIndex && !task.isOperational =>
          CollectionSchemaHooks.markComplete(index.scopeID, index.id).join

        case _ =>
          Query.unit
      }
    }

    override def onCancel(task: Task): Query[Unit] = {
      index(task) flatMap {
        case Some(index) if index.isCollectionIndex && !task.isOperational =>
          CollectionSchemaHooks.markFailed(index.scopeID, index.id).join

        case _ =>
          Query.unit
      }
    }

    private def markActive(
      scope: ScopeID,
      id: IndexID,
      ts: Timestamp,
      startedAt: Timestamp,
      elapsedMillis: Long,
      strategy: Option[String] = None): Query[Unit] = {

      Index.markActive(scope, id, ts) flatMap { _ =>
        Query.repo map { repo =>
          val activatedAt = Clock.time
          val total = activatedAt.difference(startedAt).toMillis
          repo.stats.decr("Index.Builds")
          repo.stats.timing("Index.Activation.Time", total)

          strategy foreach { strat =>
            repo.stats.timing(s"Index.$strat.Activation.Time", total)
          }

          // time executing a task as a proportion of the total time to reach active
          repo.stats.set("Index.Builds.Executable.Time", elapsedMillis / total)
        }
      }
    }

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val id = task.data(IndexField)
      val state = task.data(StateField)

      state match {
        case "build" =>
          index(task) flatMap {
            case None => Query.value(Task.Completed())
            case Some(idx) =>
              val sourceQ = idx.sources match {
                case IndexSources.All =>
                  throw new IllegalStateException(
                    s"wildcard index $scope $id cannot include native collections")
                case IndexSources.Custom =>
                  Collection.getAll(scope).mapValuesT(_.id).flattenT
                case IndexSources.Limit(ids) =>
                  ids
                    .map(CollectionConfig(scope, _).mapT(_.id))
                    .sequence
                    .map(_.flatten)
              }

              val nextData = task.data.update(StateField -> "activate")
              sourceQ flatMap { sourceIDs =>
                if (sourceIDs.nonEmpty) {
                  task.fork {
                    val idxTask = IndexTask.create(scope, id, sourceIDs, task.id)
                    val tasks = idxTask map { Vector(_) }
                    tasks map { (_, nextData) }
                  }
                } else {
                  // Indexes with deleted sources are not going to
                  // build. This is caused by a race condition where
                  // the collection was deleted at the same time the
                  // index was created. We conservatively mark the
                  // index as active and let the user decide what to
                  // do.
                  markActive(
                    scope,
                    id,
                    ts,
                    task.data(StartedAt),
                    task.elapsedMillis) map { _ =>
                    Task.Completed()
                  }
                }
              }
          }
        case "activate" =>
          task.joinThen() {
            case Left(_) => Query.value(Task.Cancelled())
            case Right(_) =>
              task.previous flatMap {
                case Some(Task.Children(children)) =>
                  val stratsQ = children map { child =>
                    Task.getLatestRunnable(child) flatMapT { t =>
                      Query.value(t.data(StrategyField))
                    }
                  } sequence

                  stratsQ flatMap { strats =>
                    // There should only be one child, but in case
                    // that changes, pick the first child as the
                    // golden boy for the purpose of recording the
                    // strategy that was used for the build.
                    val strategy = strats.flatten.headOption

                    markActive(
                      scope,
                      id,
                      ts,
                      task.data(StartedAt),
                      task.elapsedMillis,
                      strategy)
                  }
                case _ =>
                  // This shouldn't happen, but roll with it.
                  markActive(scope, id, ts, task.data(StartedAt), task.elapsedMillis)
              } map { _ =>
                Task.Completed()
              }
          }
      }
    }

    def create(
      scope: ScopeID,
      idx: IndexID,
      parent: Option[TaskID] = None,
      isOperational: Boolean = false): Query[Task] = {
      val data = Data(
        StateField -> "build",
        StartedAt -> Clock.time,
        ScopeField -> scope,
        IndexField -> idx)

      Query.repo flatMap { repo =>
        Task.createRandom(scope, name, data, parent, isOperational) map { t =>
          repo.stats.incr("Index.Builds")
          t
        }
      }
    }

    override val isHealthy = Query.True
  }

}
