package fauna.tools

import com.github.benmanes.caffeine.cache.Caffeine
import fauna.atoms.{
  CollectionID,
  IndexID,
  SchemaVersion,
  ScopeID,
  UserCollectionID,
  UserIndexID
}
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.{ NativeCollectionID, SchemaCollection }
import fauna.model.Database
import fauna.repo.{ RepoContext, Store }
import fauna.repo.query.Query
import fauna.storage.index.NativeIndexID
import org.apache.cassandra.utils.OutputHandler
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

trait SchemaService {
  def collectionExists(scope: ScopeID, collID: CollectionID): Boolean
  def indexExists(scope: ScopeID, indexID: IndexID): Boolean
}

class SchemaServiceCache(
  private val repo: RepoContext,
  private val handler: OutputHandler)
    extends SchemaService {

  // (ScopeID, ID, CollectionID | IndexID) -> existence.
  private val cache =
    Caffeine
      .newBuilder()
      .maximumSize(10_000_000)
      .build[(Long, Long, Long), Boolean]()

  // ScopeID -> active schema version.
  private val svcache =
    Caffeine
      .newBuilder()
      .maximumSize(10_000_000)
      .build[ScopeID, Option[SchemaVersion]]()

  // NB: "scope is deleted" and "scope has no staged schema" both result in
  //     None. This does not cache scope existence, just whether a scope has
  //     an active schema version or not.
  private def getActiveSchemaVersion(scope: ScopeID): Option[SchemaVersion] =
    svcache.get(
      scope,
      _ => {
        runQuery(
          Database.lookupIDForGlobalID(scope).flatMapT { case (parentScope, id) =>
            SchemaCollection.Database(parentScope).getVersionLiveNoTTL(id).map {
              _.flatMap {
                _.data(Database.ActiveSchemaVersField).map(SchemaVersion(_))
              }
            }
          })
      }
    )

  def collectionExists(scope: ScopeID, collID: CollectionID): Boolean = {
    collID match {
      case NativeCollectionID(_) => true
      case UserCollectionID(_) =>
        val cacheKey = (scope.toLong, collID.toLong, CollectionID.collID.toLong)
        cache.get(
          cacheKey,
          _ => {
            // Mimic staged-aware lookups in regular Fauna by looking up the
            // collection document at the active schema time.
            val sv = getActiveSchemaVersion(scope).fold(Timestamp.MaxMicros)(_.ts)
            runQuery(
              Store.getUnmigrated(scope, collID.toDocID, sv).map { _.isDefined })
          }
        )
      case _ => false
    }
  }

  def indexExists(scope: ScopeID, indexID: IndexID): Boolean = {
    indexID match {
      case NativeIndexID(_) => true
      case UserIndexID(_) =>
        val cacheKey = (scope.toLong, indexID.toLong, IndexID.collID.toLong)
        cache.get(
          cacheKey,
          _ => {
            runQuery(
              // NB: This checks the existence of a backing index, so it's OK
              //     even if the front-end user index is staged for deletion.
              Store
                .getUnmigrated(scope, indexID.toDocID)).isDefined
          }
        )
      case _ => false
    }
  }

  private def runQuery[T](query: Query[T], attempts: Int = 3): T = {
    try {
      repo.runSynchronously(query, 30.second).value
    } catch {
      case NonFatal(e) =>
        if (attempts == 0) {
          runQuery(query, attempts - 1)
        } else {
          throw e
        }
    }
  }
}
