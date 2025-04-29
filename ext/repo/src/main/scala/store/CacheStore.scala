package fauna.repo.store

import fauna.atoms._
import fauna.lang.syntax._
import fauna.repo.cache.CacheKey
import fauna.repo.query.Query
import fauna.repo.SchemaContentionException
import fauna.storage.api.SchemaVersionSnapshot
import fauna.storage.ops.RemoveAllWrite
import fauna.storage.Tables
import scala.concurrent.duration._

object CacheStore {

  private val StaleItemDuration = Tables.RowTimestamps.TTL - 15.minutes

  /** Base cache key type for schema items predicated on the current schema version. */
  trait SchemaKey[V] { self =>
    def scope: ScopeID
    def query: Query[Option[V]]
  }

  /** Get a cached item that is not schema version aware. */
  def getItem[V](key: CacheKey[V]): Query[Option[V]] =
    Query.repo flatMap { _.cacheContext.schema.get(key) }

  private def logContention[V](key: SchemaKey[V], v: Any): Unit =
    getLogger(s"[sce] contention looking up key $key; got value $v")

  /** Get a schema item at the query's schema version time. Optionally register a
    * schema OCC check if not present.
    */
  def getItem[V](key: SchemaKey[V]): Query[Option[V]] =
    Query.context flatMap { qc =>
      getLastSeenSchema(key.scope) flatMap { ver =>
        qc.repo.cacheContext.schema2.getItem(key).flatMap {

          // No item
          case None => Query.none

          // Item
          case Some((item, iver)) =>
            def staleItemThreshold =
              qc.snapshotTime - StaleItemDuration

            def retry() = {
              qc.repo.cacheContext.schema2.invalidateItem(key)
              getItem(key)
            }

            (iver, ver) match {
              // feature is disabled
              case _ if !qc.repo.schemaOCCEnabled => Query.some(item)
              // schema versions match
              case (None, None)               => Query.some(item)
              case (iver, ver) if iver == ver => Query.some(item)

              // cached item is stale, invalidate and retry
              case (Some(iver), Some(ver)) if iver < ver =>
                retry()

              // If the cached schema version is empty but the item's version
              // is stale, the schema version has just been TTLed. In this case
              // invalidate the item so that it picks up an empty version.
              case (Some(iver), None) if iver.ts < staleItemThreshold =>
                retry()

              // the cached schema version may be stale, and the item's has ttled
              case (None, Some(ver)) if ver.ts < staleItemThreshold =>
                Query.some(item)

              // There is no cached item, but there is a valid schema version, so
              // retry.
              case (None, Some(_)) => retry()

              // In any other case, contend.
              case (iver, ver) =>
                val ver0 = iver.orElse(ver).get
                logContention(key, item)
                Query.fail(SchemaContentionException(key.scope, ver0))
            }
        }
      }
    }

  /** Update the last-modified timestamp of a scope. */
  def updateSchemaVersion(scope: ScopeID): Query[Unit] = {
    // RemoveAllWrite's purpose is to touch the LMT (no data is stored in the
    // SchemaVersions CF).
    val rk = Tables.SchemaVersions.rowKey(scope)
    Query.write(RemoveAllWrite(Tables.SchemaVersions.CFName, rk))
  }

  /** Get timestamp of the last schema update for a scope for the running query.
    * Optionally register an OCC check if not present.
    */
  def getLastSeenSchema(scope: ScopeID): Query[Option[SchemaVersion]] =
    Query.context flatMap { qec =>
      qec.repo.cacheContext.schema2.getLastSeenSchema(scope) map { sv =>
        // For a given transaction, once a schema valid time has been seen for a
        // scope it must not change. The QEC tracks this state and will throw a
        // SchemaContentionException if the valid time has changed.
        qec.updateSchemaVersionState(scope, sv)
        sv
      }
    }

  /** Refresh the schema version for the current running query. Returns the new schema
    * version, or the current one if unchanged.
    */
  def refreshLastSeenSchema(scope: ScopeID): Query[Unit] =
    Query.context flatMap { qec =>
      getLastSeenSchemaUncached(scope) map { newSV =>
        qec.updateSchemaVersionState(scope, newSV)
        ()
      }
    }

  /** Get the timestamp of last schema update for a scope. Returns the minimum
    * timestamp if there's no record for the scope.
    */
  def getLastSeenSchemaUncached(scope: ScopeID): Query[Option[SchemaVersion]] =
    Query.context flatMap { qec =>
      if (qec.repo.cacheContext.schema2.testing) {
        Query.value(None)
      } else {
        Query.read(SchemaVersionSnapshot(scope, qec.snapshotTime)) map { res =>
          res.schemaVersion
        }
      }
    }

  /** Invalidate schema cache for a given scope. */
  def invalidateScope(scope: ScopeID): Query[Unit] =
    Query.repo flatMap { repo =>
      getLastSeenSchemaUncached(scope) map { sv =>
        repo.cacheContext.schema2.invalidateScopeBefore(scope, sv)
      }
    }
}
