package fauna.model.cache

import fauna.atoms._
import fauna.exec.ImmediateExecutionContext
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.model._
import fauna.model.schema.{ NativeCollectionID, NativeIndex }
import fauna.repo.doc.Indexer
import fauna.repo.query._
import fauna.repo.Store
import fauna.stats.StatsRecorder
import fauna.storage.api.set._
import fauna.storage.doc.ValueRequired
import fauna.storage.index._
import fauna.storage.ir.DocIDV
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent._
import scala.util.control.NonFatal

/** This module provides alternative schema lookup methods that are more suitable
  * for batch jobs. Some differences:
  * - Transactional consistency is less important, since batch jobs can control
  *   their own concurrency.
  * - isDeleted checks can be more aggressive: The normal schema deletion checks
  *   consider a schema item to be live if the meta document cannot be found.
  *   This is to prevent replication errors of meta documents from causing mass
  *   deletion of data. Batch jobs don't necessarily need to be conservative in
  *   this way, and a more exclusive check saves from doing unnecessary work.
  * - Memory efficiency is important, and access is not constrained to a live
  *   working set. Normal live traffic patterns tend to be restricted to a
  *   smaller working subset of the entire cluster corpus, whereas batch jobs
  *   must scan the entire dataset. Batch jobs are also normally functionally
  *   specialized, so being able to pull in a functional segment (i.e. just
  *   indexes) allows for more memory savings.
  */
final class BatchSchemaCache {
  import BatchSchemaCache._

  lazy val database = new DatabaseBatchSchema
  lazy val collection = new CollectionBatchSchema
  lazy val index = new IndexBatchSchema

  def clearState() = {
    database.clear()
    collection.clear()
    index.clear()
  }

  def recordSizeStats(stats: StatsRecorder): Unit = {
    stats.set("BatchSchema.Database.Items", database.size)
    stats.set("BatchSchema.Collection.Items", collection.size)
    stats.set("BatchSchema.Index.InfoItems", index.infoItemSize)
    stats.set("BatchSchema.Index.InfoSize", index.infoTotalSize)
  }
}

object BatchSchemaCache {
  sealed abstract class LiveState(val isLive: Boolean)
  object LiveState {
    case object Live extends LiveState(true) // a schema item is live
    case object Deleted extends LiveState(false) // a schema item is deleted
    case object NotFound extends LiveState(false) // a schema item was not found
  }

  private val log = getLogger

  final class DatabaseBatchSchema {
    private[this] val liveStateMap = new MemoizedQueryMap(lookupLiveState(_))

    def clear() = liveStateMap.clear()
    def size = liveStateMap.size

    def isLive(scope: ScopeID) = liveState(scope).map { _.isLive }
    def isDeleted(scope: ScopeID) = liveState(scope).map { s => !s.isLive }

    def liveState(scope: ScopeID) =
      if (scope == ScopeID.RootID) Query.value(LiveState.Live)
      else liveStateMap(scope)

    protected def lookupLiveState(scope: ScopeID): Query[LiveState] =
      Database.lookupIDForGlobalID(scope) flatMap {
        case None => databaseNotFound(scope, None)
        case Some((parentScope, id)) =>
          RuntimeEnv.Default.Store(parentScope).getVersionNoTTL(id.toDocID) flatMap {
            case None                   => databaseNotFound(scope, None)
            case Some(v) if v.isDeleted => databaseDeleted
            case Some(_)                =>
              // this db has a live version, so schema liveness is based on the
              // parent.
              liveState(parentScope) flatMap {
                case LiveState.Live     => databaseLive
                case LiveState.Deleted  => databaseDeleted
                case LiveState.NotFound => databaseNotFound(scope, Some(parentScope))
              }
          }
      }

    private def databaseLive =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Database.Live")
        LiveState.Live
      }

    private def databaseDeleted =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Database.Deleted")
        LiveState.Deleted
      }

    private def databaseNotFound(scope: ScopeID, parent: Option[ScopeID]) =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Database.Orphaned")
        parent match {
          case None =>
            log.warn(s"Orphaned scope $scope. Database metadata is missing.")
          case Some(parentScope) =>
            log.warn(
              s"Orphaned scope $scope. Database metadata missing for ancestor scope $parentScope.")
        }
        LiveState.NotFound
      }
  }

  final class CollectionBatchSchema {
    private[this] val liveStateMap = new MemoizedQueryMap(lookupLiveState(_))

    def clear() = liveStateMap.clear()
    def size = liveStateMap.size

    def isLive(scope: ScopeID, id: CollectionID) = liveState(scope, id).map {
      _.isLive
    }
    def isDeleted(scope: ScopeID, id: CollectionID) = liveState(scope, id).map { s =>
      !s.isLive
    }

    def liveState(scope: ScopeID, id: CollectionID) =
      id match {
        case NativeCollectionID(_) => Query.value(LiveState.Live)
        case id                    => liveStateMap((scope, id))
      }

    protected def lookupLiveState(t: (ScopeID, CollectionID)) =
      t match {
        case (scope, UserCollectionID(id)) =>
          RuntimeEnv.Default.Store(scope).getVersionNoTTL(id.toDocID) flatMap {
            case None                   => collectionNotFound(scope, id)
            case Some(v) if v.isDeleted => collectionDeleted
            case Some(_)                => collectionLive
          }
        case (scope, id) => collectionInvalid(scope, id)
      }

    private def collectionLive =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Collection.Live")
        LiveState.Live
      }

    private def collectionDeleted =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Collection.Deleted")
        LiveState.Deleted
      }

    private def collectionNotFound(scope: ScopeID, id: CollectionID) =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Collection.Orphaned")
        log.warn(
          s"Orphaned collection data $id in $scope. Collection metadata is missing.")
        LiveState.NotFound
      }

    private def collectionInvalid(scope: ScopeID, id: CollectionID) =
      Query.stats map { stats =>
        stats.incr("BatchSchema.Collection.Invalid")
        log.warn(s"Invalid collection $id found in $scope. Considering orphaned.")
        LiveState.NotFound
      }
  }

  // sources == `null` is the sentinel for all. This doesn't have to be super
  // accurate as we'll filter by the indexer's binders later.
  final case class IndexInfo(id: IndexID, sources: Array[Long] = null) {
    def hasSource(coll: CollectionID) =
      (sources eq null) || sources.contains(coll.toLong)
  }

  final class IndexBatchSchema extends ExceptionLogging {
    private[this] val liveStateMap = new MemoizedQueryMap(lookupLiveState(_))
    private[this] val infoByScopeMap = new MemoizedQueryMap(lookupInfoByScope(_))
    private[this] val _infoTotalSize = new AtomicLong

    def clear() = {
      liveStateMap.clear()
      infoByScopeMap.clear()
    }

    def infoItemSize = infoByScopeMap.size
    def infoTotalSize = _infoTotalSize.get

    // delegate to the regular index cache, which will evict entries. Not sure
    // we can hold all index configs in memory.
    def getUserDefined(scope: ScopeID, id: IndexID) = Index.get(scope, id)

    def isLive(scope: ScopeID, id: IndexID) = liveState(scope, id).map { _.isLive }
    def isDeleted(scope: ScopeID, id: IndexID) = liveState(scope, id).map { s =>
      !s.isLive
    }

    def liveState(scope: ScopeID, id: IndexID) =
      id match {
        case NativeIndexID(_) => Query.value(LiveState.Live)
        // we let invalid native indexes fall through here so that we cache
        // the invalid lookup and as a result log only once per index.
        case id => liveStateMap((scope, id))
      }

    protected def lookupLiveState(t: (ScopeID, IndexID)) =
      t match {
        case (scope, UserIndexID(id)) =>
          RuntimeEnv.Default.Store(scope).getVersionNoTTL(id.toDocID) map {
            case None =>
              log.warn(
                s"Orphaned index data $id in $scope. Index metadata is missing.")
              LiveState.NotFound
            case Some(v) if v.isDeleted => LiveState.Deleted
            case Some(_)                => LiveState.Live
          }
        case (scope, id) =>
          log.warn(s"Invalid native index $id found in $scope.")
          Query.fail(
            NativeIndexID.InvalidNativeIndexID(id, s"Invalid native index $id"))
      }

    // FIXME: This logic mirrors a lot that in model/doc/Index.scala.
    // Should DRY it up in order to not have logical mistakes between the two.
    def getIndexer(scope: ScopeID, coll: CollectionID): Query[Indexer] =
      infoByScopeMap(scope) flatMap { infos =>
        val ids = infos.iterator.collect {
          case info if info.hasSource(coll) => info.id
        }

        ids.map(getUserDefined(scope, _)).to(Iterable).sequence map { idxs =>
          val native = Index.getNativeIndexer(scope, coll)
          val userDefined = idxs.map(_.filter(_.binders(coll).isDefined)).flatten
          userDefined.foldLeft(native) { _ + _.indexer }
        }
      }

    // FIXME: This logic mirrors a lot that in model/doc/Index.scala.
    // Should DRY it up in order to not have logical mistakes between the two.
    protected def lookupInfoByScope(scope: ScopeID): Query[Array[IndexInfo]] =
      Query.stats flatMap { stats =>
        stats.incr("BatchSchema.Index.Lookups")
        val docsIdx = NativeIndex.DocumentsByCollection(scope)

        val idxTerm = Vector(Scalar(DocIDV(IndexID.collID.toDocID)))
        val allIdxsQ = Store.collection(docsIdx, idxTerm, Timestamp.MaxMicros)

        val allInfosQ = allIdxsQ.flatMapValuesT { v =>
          v.docID.as[IndexID] match {
            case UserIndexID(id) =>
              RuntimeEnv.Default.Store(scope).getVersionNoTTL(id.toDocID) map {
                case None                         => Nil
                case Some(vers) if vers.isDeleted => Nil
                // use the lack of an "active" field as a heuristic for finding bad
                // index docs.
                case Some(vers) if !vers.data.fields.contains(List("active")) =>
                  stats.incr("BatchSchema.Index.Invalid")
                  log.warn(
                    s"Invalid index schema document with id $id found in $scope: Document has no `active` field.")
                  Nil

                case Some(vers) =>
                  // see Index.getUncached
                  val cfgs = Index.SourceField.read(vers.data.fields) match {
                    case Left(List(_: ValueRequired)) =>
                      List(SourceConfig(IndexSources.Custom))
                    case Left(errs) =>
                      stats.incr("BatchSchema.Index.Invalid")
                      log.warn(
                        s"Invalid index schema document with id $id found in $scope: ${errs}")
                      errs.foreach(logException)
                      Nil
                    case Right(srcs) => srcs
                  }

                  // see Index.parseBinders
                  var isAll = false
                  val iter = cfgs.iterator
                  val srcb = Array.newBuilder[Long]

                  while (!isAll && iter.hasNext) {
                    iter.next().sources match {
                      case IndexSources.All | IndexSources.Custom => isAll = true
                      case IndexSources.Limit(ids) => srcb ++= ids.map { _.toLong }
                    }
                  }

                  val info = if (isAll) {
                    _infoTotalSize.addAndGet(1)
                    IndexInfo(id)
                  } else {
                    val srcs = srcb.result()
                    _infoTotalSize.addAndGet(srcs.size + 1)
                    IndexInfo(id, srcs)
                  }
                  List(info)
              }

            case id =>
              stats.incr("BatchSchema.Index.Invalid")
              log.warn(
                s"Invalid index schema document with id $id found in $scope: Has id in native index space.")
              Query.value(Nil)
          }
        }

        allInfosQ.flattenT.map { _.toArray }
      }
  }
}

private class MemoizedQueryMap[Key, Value <: AnyRef](
  queryForKey: Key => Query[Value]) {
  private[this] val _size = new AtomicLong
  private[this] val pending = new ConcurrentHashMap[Key, Future[Value]]
  private[this] val values = new ConcurrentHashMap[Key, Value]

  def size = _size.get

  def clear(): Unit = {
    values.clear()
    _size.set(0)
  }

  def apply(key: Key): Query[Value] =
    (values.get(key): @unchecked) match {
      case v if v ne null => Query.value(v)
      case null =>
        val p = Promise[Value]()
        val f = p.future
        implicit val ec = ImmediateExecutionContext
        f onComplete { rv =>
          // on success put the value in the map. This can lead to redundant puts,
          // but is simpler.
          rv.map { values.putIfAbsent(key, _) }
          // always remove the lookup future
          pending.remove(key, f)
        }

        (pending.putIfAbsent(key, f): @unchecked) match {
          case prev if prev ne null =>
            Query.future(prev)
          case null =>
            (values.get(key): @unchecked) match {
              case v if v ne null =>
                p.success(v)
                Query.value(v)
              case null =>
                Query.repo flatMap { repo =>
                  _size.incrementAndGet()
                  val check = repo.runNow(QFail.guard(queryForKey(key))).map(_._2)
                  p.completeWith(check)
                  Query.future(f)
                } recover { case NonFatal(e) =>
                  p.tryFailure(e)
                  throw e
                }
            }
        }
    }
}
