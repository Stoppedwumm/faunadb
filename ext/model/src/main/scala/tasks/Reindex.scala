package fauna.model.tasks

import fauna.atoms._
import fauna.lang.{ ConsoleControl, Timestamp }
import fauna.lang.syntax._
import fauna.model._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage.doc.{ Data, Field }
import fauna.storage.index._
import fauna.storage.ir.{ ArrayV, IRValue }
import fauna.storage.Selector
import scala.annotation.unused
import scala.collection.mutable.{ Set => MSet }

/** Reindexing is the process of processing all documents in one index
  * (the "source") through an Indexer (the "destination") to produce a
  * new index covering the same documents as the source.
  *
  * This process begins with a Reindex.Root task, which counts the
  * number of documents in the source index. If that quantity is
  * greater than the CancelThreshold, the task cancels itself, causing
  * its parent (see IndexBuild.IndexTask) to spawn a table-scan index
  * build (see IndexBuild.ScanTask).
  *
  * If the source index contains fewer documents than the
  * CancelThreshold, Reindex.Root will spawn one Reindex.ReindexDocs
  * task to process the documents in batches of DocsPerTask.
  *
  * Reindexing will continue until one of:
  *
  *   - All documents in the source have been reindexed.
  *   - The destination index is deleted.
  *   - An operator intervenes and changes these tasks' state.
  */
object Reindex {

  // Shared Field
  val ScopeField = Field[ScopeID]("scope")

  private val log = getLogger

  private def index(scope: ScopeID, id: IndexID) = {
    val dbQ = Database.isDeleted(scope)
    val idxQ = Index.getConfig(scope, id)

    (dbQ, idxQ) par {
      case (false, i @ Some(_)) => Query.value(i)
      case _                    => Query.none
    }
  }

  case object ReindexDocs
      extends PaginateType[(ScopeID, DocID)]("reindex-docs", version = 0)
      with VersionRowPaginator {

    /** Contains the ID of the index being built by this task within
      * the scope stored in ScopeField.
      */
    val IndexField = Field[IndexID]("index")

    /** Contains a worklist of documents to index, consumed via calls
      * to paginate(). This task is complete with the list in this
      * field is empty.
      */
    val DocsField = Field.OneOrMore[(ScopeID, DocID)]("docs")

    /** A constant value containing the number of documents this task will index.
      */
    val DocsTotalField = Field[Int]("docs_total")

    /** As reindexing proceeds, accumulates the total number of documents indexed.
      */
    val DocsDoneField = Field[Int]("docs_done")

    /** As reindexing proceeds, accumulates the total number of versions indexed.
      */
    val VersionsDoneField = Field[Int]("versions_done")

    /** As reindexing proceeds, accumulates the total number of bytes read. */
    val BytesReadField = Field[Long]("bytes_read")

    override val isLoggable = true

    override val isHealthy = Query.True

    private def createOne(
      scope: ScopeID,
      id: IndexID,
      docs: Vector[(ScopeID, DocID)],
      parent: Option[TaskID]): Query[Task] = {
      val data = Data(
        ScopeField -> scope,
        IndexField -> id,
        DocsField -> docs,
        DocsTotalField -> docs.size,
        DocsDoneField -> 0,
        VersionsDoneField -> 0)

      Task.createRandom(scope, name, data, parent)
    }

    // This has proven useful to work around scans paused by topology changes.
    // If you use this to build an index, you'll need to mark the build complete
    // manually as well.
    def createOneForConsole(
      scope: ScopeID,
      id: IndexID,
      docs: Vector[(ScopeID, DocID)])(implicit @unused ctl: ConsoleControl) =
      createOne(scope, id, docs, None)

    def createAll(
      scope: ScopeID,
      id: IndexID,
      docs: Vector[DocID],
      parent: Option[TaskID]): Query[Iterable[Task]] =
      Query.repo flatMap { repo =>
        docs
          .grouped(repo.reindexDocsLimit)
          .map { docs => createOne(scope, id, docs map { (scope, _) }, parent) }
          .toSeq
          .sequence
      }

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val idx = task.data(IndexField)

      index(scope, idx) flatMap {
        case Some(idx) =>
          val iter = IndexBuild(scope, idx, ts)
          paginate(task, DocsField, "ReindexDocs", iter, Selector.All) flatMap {
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
                    s"Reindex (${task.id}): fallback to table scan due to bytes " +
                      s"read limit ${bytesRead} > ${repo.reindexCancelBytesLimit}")
                  Task.Cancelled() // fallback to table scan
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
          } recoverWith {
            // Special case: kill an index build that has to process versions
            // creating a jumbo entry set.
            // TODO: Inform the user somehow.
            case VersionIndexEntriesTooLargeException(scope, id, idx, size, limit) =>
              (Index.getUncached(scope, idx), Database.forScope(scope)) par {
                case (idxOpt, dbOpt) =>
                  val idxName = idxOpt map { _.name } getOrElse "<name unavailable>"
                  val dbName = dbOpt map { _.name } getOrElse "<name unavailable>"
                  log.warn(
                    s"Found index bomb in index $idxName ($idx) in database " +
                      s"$dbName ($scope) for document $id ($size > $limit)")
                  task.abort()
              } map { _ => Task.Cancelled() }
          }
        case None => Query.value(Task.Completed())
      }
    }
  }

  case object Root
      extends PaginateType[(IndexConfig, Vector[IndexTerm])](
        "reindex-root",
        version = 0)
      with SortedRowPaginator {

    val SourceField = Field[IndexID]("source")
    val DestinationField = Field[IndexID]("destination")
    val TermsField = Field[Vector[IRValue]]("terms")
    val DocsField = Field.ZeroOrMore[DocID]("docs")
    val StateField = Field[String]("state")

    override val isHealthy = Query.True

    def create(
      scope: ScopeID,
      src: IndexID,
      terms: Vector[IRValue],
      dst: IndexID,
      parent: Option[TaskID]): Query[Task] = {
      val data = Data(
        ScopeField -> scope,
        SourceField -> src,
        DestinationField -> dst,
        TermsField -> terms,
        StateField -> "start")

      Task.createRandom(scope, name, data, parent)
    }

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val src = task.data(SourceField)
      val dst = task.data(DestinationField)

      (index(scope, src), index(scope, dst)) par {
        case (Some(s), Some(_)) =>
          task.data(StateField) match {
            case "start" =>
              // FIXME: remove this bridge once the TermsField change has been
              // deployed.
              val terms = {
                // val ir = task.data(TermsField)
                val ir = task.data.fields.get(List("terms")) match {
                  case Some(ArrayV(ts)) => ts
                  case Some(t)          => Vector(t)
                  case None             => Vector.empty
                }
                ir.iterator
                  .zip(s.terms)
                  .map { case (t, (_, reverse)) => IndexTerm(t, reverse) }
                  .toVector
              }

              Query.repo flatMap { repo =>
                val iter = new ReindexIter
                paginateOnce(
                  task,
                  (s, terms),
                  "Reindex",
                  iter,
                  Selector.All) flatMap {
                  case Task.Runnable(data, parent) =>
                    val all = iter.result ++ task.data(DocsField)

                    if (all.size > repo.reindexCancelLimit) {
                      log.trace(
                        s"Reindex (${task.id}: fallback to table scan ${all.size} > ${repo.reindexCancelLimit}")
                      Query.value(Task.Cancelled()) // trigger a table scan
                    } else {
                      // continue gathering DocIDs
                      Query.value(
                        Task
                          .Runnable(data.update(DocsField -> all.toVector), parent))
                    }
                  case _: Task.Completed =>
                    val all = iter.result ++ task.data(DocsField)

                    // all DocIDs gathered; fork or fall back to table scan?
                    if (all.size > repo.reindexCancelLimit) {
                      log.trace(
                        s"Reindex (${task.id}): fallback to table scan ${all.size} > ${repo.reindexCancelLimit}")
                      Query.value(Task.Cancelled()) // trigger a table scan
                    } else if (all.isEmpty) { // just in case
                      log.trace(
                        s"Reindex (${task.id}): no documents found in $scope $src")
                      Query.value(Task.Completed())
                    } else {
                      task.fork { // fork and reindex
                        ReindexDocs.createAll(
                          scope,
                          dst,
                          all.toVector,
                          Some(task.id)) map {
                          (_, task.data.update(StateField -> "join"))
                        }
                      }
                    }

                  case s => Query.value(s) // just in case
                }
              }
            case "join" =>
              task.joinThen() {
                case Left(_) => Query.value(Task.Cancelled())
                case _       => Query.value(Task.Completed())
              }
          }
        case _ => Query.value(Task.Completed())
      } recoverWith { case _: VersionTooLargeException =>
        // trigger a table scan - this task has exceeded its
        // capabilities
        log.trace(
          s"Reindex (${task.id}): fallback to table scan, exceeded maximum document size")
        Query.value(Task.Cancelled())
      }
    }

    /** As index rows are passed to this Iteratee, it accumulates a set
      * of DocIDs from the index "add" events seen in the rows. This set
      * of DocIDs will then be used as input to the reindexing phase.
      *
      * NOTE: Assumes that the source index is a collection index, so 1
      * "add" event will exist per retained document of that collection.
      *
      * NOTE: This iteratee is also used to migrate versions by index.
      */
    final class ReindexIter(snapshotTS: Timestamp = Timestamp.MaxMicros)
        extends RangeIteratee[IndexKey, IndexValue] {

      private[this] val docs = MSet.empty[DocID]

      def apply(key: IndexKey) =
        Query.some(colIter())

      def result: Set[DocID] = docs.toSet

      private def colIter(): ColIteratee[IndexValue] =
        ColIteratee {
          case Some(page) =>
            page.iterator
              .collect {
                case v if v.action.isCreate && v.ts.validTS <= snapshotTS => v.docID
              }
              .foreach { docs += _ }
            Query.some(colIter())
          case None => Query.none
        }
    }
  }
}
