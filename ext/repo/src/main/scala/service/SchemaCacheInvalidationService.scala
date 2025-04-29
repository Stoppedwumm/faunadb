package fauna.repo.service

import fauna.atoms._
import fauna.exec._
import fauna.lang.{ Service, Timestamp }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.repo.query._
import fauna.repo.service.stream.TxnResult
import fauna.repo.store.CacheStore
import fauna.repo.RepoContext
import fauna.stats.StatsRecorder
import fauna.storage.ops.{ DocRemove, VersionAdd, VersionRemove }
import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import scala.util.{ Failure, Success }

object SchemaCacheInvalidationService {
  val InvalidationTimeout = 30.seconds
}

final class SchemaCacheInvalidationService(
  streamService: StreamService,
  stats: StatsRecorder,
  repo: RepoContext,
  versionedCollIDs: Set[CollectionID],
  unversionedCollIDs: Set[CollectionID],
  invalidateCache: (ScopeID, DocID, Timestamp) => Query[Unit]
) extends Service
    with ExceptionLogging {
  import FaunaExecutionContext.Implicits.global
  import SchemaCacheInvalidationService._

  private[this] var _stream: Cancelable = _
  private[this] val collIDs = versionedCollIDs ++ unversionedCollIDs
  private[this] val repoWithTimeout = repo.withQueryTimeout(InvalidationTimeout)

  override def isRunning: Boolean = _stream != null

  override def start(): Unit = {
    val obs = streamService.forCollSet(None, collIDs)

    _stream = obs.subscribe(new Observer.Default[TxnResult] {
      override def onNext(txn: TxnResult) = {
        val invalidates = Seq.newBuilder[Query[Unit]]
        val scopes = Seq.newBuilder[Query[Unit]]

        txn.writes.iterator
          .collect {
            case VersionAdd(scopeID, docID, _, _, _, _, _) => (scopeID, docID)
            case VersionRemove(scopeID, docID, _, _)    => (scopeID, docID)
            case DocRemove(scopeID, docID)              => (scopeID, docID)
          }
          .filter { case (_, docID) =>
            collIDs.contains(docID.collID)
          }
          .foreach { case (scope, docID) =>
            invalidates += invalidateCache(scope, docID, txn.txnTS)
            if (versionedCollIDs.contains(docID.collID)) {
              scopes += CacheStore.invalidateScope(scope)
            }
          }

        val invalidateQ =
          invalidates.result().join ensure {
            scopes.result().join
          }

        val invalidateF =
          repoWithTimeout
            .runNow(Query.timing("Schema.Cache.Invalidation.Time") {
              invalidateQ
            })
            .unit

        invalidateF onComplete {
          case Failure(_: TimeoutException) =>
            stats.incr("Schema.Cache.Invalidation.Timeout")
          case Failure(_) => stats.incr("Schema.Cache.Invalidation.Exception")
          case Success(_) => ()
        }

        Observer.ContinueF // Ignore failures and don't wait for invalidation.
      }

      override def onError(err: Throwable) =
        err match {
          case Cancelable.Canceled => () // closing... ignore.
          case other               => logException(other)
        }
    })
  }

  override def stop(graceful: Boolean): Unit = {
    _stream.cancel()
    _stream = null
  }
}
