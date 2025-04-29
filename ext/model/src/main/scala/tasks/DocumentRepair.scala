package fauna.model.tasks

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model._
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.storage.{ Conflict, Resolved, Selector }
import fauna.storage.doc._

object DocumentRepair {

  val LimitField = Field[Int]("limit")
  val ScopeField = Field[Option[ScopeID]]("scope")
  val CollectionsField = Field[Option[Vector[CollectionID]]]("collections")

  private def checkSelector(
    scope: Option[ScopeID],
    collections: Option[Iterable[CollectionID]]) =
    require(
      collections.isEmpty || scope.nonEmpty,
      "Cannot select collections without a scope. " +
        s"(scope=$scope, collections=$collections)")

  /** This task schedules scan tasks on each data node, and monitors for
    * completion.
    */
  case object Root extends Type("doc-repair-root") {
    val StateField = Field[String]("state")

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val state = task.data(StateField)

      state match {
        case "start" =>
          val scope = task.data(ScopeField)
          val collections = task.data(CollectionsField)
          val limit = task.data(LimitField)
          task.fork {
            Scan.create(scope, collections, task.id, limit) map {
              (_, task.data.update(StateField -> "finish"))
            }
          }

        case "finish" => task.join()
      }
    }

    def create(
      scope: Option[ScopeID] = None,
      collections: Option[Iterable[CollectionID]] = None,
      limit: Int = 50_000): Query[Task] = {
      checkSelector(scope, collections)
      Task.createRandom(
        ScopeID.RootID,
        name,
        Data(
          StateField -> "start",
          ScopeField -> scope,
          CollectionsField -> (collections map { _.toVector }),
          LimitField -> limit),
        None)
    }
  }

  /** One of these tasks runs on each data node, processing all Versions
    * in the local, primary segments through VersionRepair.
    */
  case object Scan
      extends PaginateType[Segment]("doc-repair-scan", version = 0)
      with VersionSegmentMVTPaginator {

    val RangesField = Field[Vector[Segment]]("ranges")

    override def isStealable(task: Task): Boolean = false

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val collections = task.data(CollectionsField)
      checkSelector(scope, collections)

      val limit = task.data(LimitField)

      val selector = (scope, collections) match {
        case (None, None)               => Selector.All
        case (Some(scope), None)        => Selector.from(scope)
        case (Some(scope), Some(colls)) => Selector.from(scope, colls)
        case (None, Some(_))            => sys.error("unreachable")
      }

      val iter = new VersionRepair(limit, ts)
      paginate(task, RangesField, "Document.Repair", iter, selector)
    }

    def create(
      scope: Option[ScopeID],
      collections: Option[Iterable[CollectionID]],
      parent: TaskID,
      limit: Int): Query[Iterable[Task]] = {
      checkSelector(scope, collections)
      Task.createPrimarySegments(ScopeID.RootID, name, Some(parent)) { (segs, _) =>
        create(segs, scope, collections, limit)
      }
    }

    private def create(
      ranges: Iterable[Segment],
      scope: Option[ScopeID],
      collections: Option[Iterable[CollectionID]],
      limit: Int): Data =
      Data(
        RangesField -> ranges.toVector,
        ScopeField -> scope,
        CollectionsField -> (collections map { _.toVector }),
        LimitField -> limit)
  }

  /** An iteratee which compares each Version's diff to the data in the
    * next Version earlier in time. If a Version's diff does not
    * produce the next Version's data when applied, the diff will be
    * rewritten such that it does.
    *
    * This is a transactional process, and can be
    * expensive. Therefore, there is a limit to the number of Versions
    * per document which may be processed. Exceeding this limit causes
    * any repairs accumulated to be committed, and the remainder of
    * the document's history to be skipped.
    */
  final class VersionRepair(limit: Int, snapshotTS: Timestamp)
      extends RangeIteratee[(ScopeID, DocID), Version] {

    def apply(rowID: (ScopeID, DocID)) = {
      val (scope, id) = rowID
      Query.some(colIter(scope, id))
    }

    private def colIter(scope: ScopeID, id: DocID): ColIteratee[Version] =
      ColIteratee { cols =>
        maybeSkip(0, cols map { _.size } getOrElse 0) flatMap { _ =>
          val live = cols map { _ filter { _.ts.validTS <= snapshotTS } }
          repair(scope, id, None, live)
        }
      }

    private def repair(
      scope: ScopeID,
      id: DocID,
      seed: Option[Version],
      cols: Option[Iterable[Version]],
      colsSizeAcc: Int = 0): Query[Option[ColIteratee[Version]]] =
      cols match {
        case Some(cols) =>
          // prepend a previously-seen root, if any. its previous version
          // is the head of this page
          val repairable = seed.toList ++ cols
          val root = repairable.lastOption

          val repairQ = if (repairable.nonEmpty) {
            // repair all but the last column in this page, using the
            // last column as a root
            Index.getIndexer(scope, id.collID) flatMap { indexer =>
              repair2(scope, id, root, repairable.dropRight(1), indexer)
            }
          } else {
            Query.fail(RangeIteratee.SkipRowException)
          }

          repairQ map { _ =>
            // carry the last column into the next page, it will be
            // processed as the head of that page
            Some(ColIteratee { next =>
              val nextAcc = colsSizeAcc + cols.size
              maybeSkip(next.size, nextAcc) flatMap { _ =>
                repair(scope, id, root, next, nextAcc)
              }
            })
          }

        case None =>
          val lastRepairQ = seed match {
            case None => Query.unit
            case Some(root) =>
              Index.getIndexer(scope, id.collID) flatMap { indexer =>
                repair2(scope, id, None, Seq(root), indexer)
              }
          }

          lastRepairQ map { _ => None }
      }

    /** Repairs a sequence of Versions pair-wise, given an optional
      * rooting Version outside the sequence.
      *
      * Note that the rooting Version has a valid time _less_ than the
      * last Version. If no root is provided, a synthetic "origin"
      * Version at Timestamp.Min is used to root the sequence.
      */
    private def repair2(
      scope: ScopeID,
      id: DocID,
      root: Option[Version],
      cols: Seq[Version],
      idx: Indexer): Query[Unit] = {
      val iter = cols.sliding(2)

      def repair0(root: Version): Query[Unit] =
        iter.foldLeft(Query.unit) {
          case (q, Seq(a)) =>
            q flatMap { _ =>
              Store.repair(idx, Conflict(a, Nil), Conflict(root, Nil))
            }
          case (q, Seq(a, b)) if iter.hasNext =>
            q flatMap { _ => Store.repair(idx, Conflict(a, Nil), Conflict(b, Nil)) }
          case (q, Seq(a, b)) =>
            // NB. this is the last pair of the sequence; one additional
            // repair is necessary for (b, root), which is not yielded
            // by Seq.sliding.
            q flatMap { _ =>
              Store.repair(idx, Conflict(a, Nil), Conflict(b, Nil))
            } flatMap { _ =>
              Store.repair(idx, Conflict(b, Nil), Conflict(root, Nil))
            }
          case (q, _) => q // Cannot happen. Satisfy scalac.
        }

      root match {
        case Some(c) => repair0(c)
        case None    =>
          val origin =
            Version.Deleted(
              scope,
              id,
              Resolved(Timestamp.Min, Timestamp.Epoch),
              // NB: This doesn't matter because it's a diff-less delete.
              SchemaVersion.Min)
          repair0(origin)
      }
    }

    private def maybeSkip(cols: Int, acc: Int): Query[Unit] =
      if (cols + acc > limit) {
        Query.fail(RangeIteratee.SkipRowException)
      } else {
        Query.unit
      }
  }
}
