package fauna.model.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.schema.CollectionConfig
import fauna.repo.{ PagedQuery, Store }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.storage.api.version.DocHistory
import fauna.storage.doc.Data
import fauna.storage.VersionID

object ModelStore {
  // Get the schema for the given collection. If none is found,
  // provide an empty schema. This keeps model store's behavior
  // consistent with the prior behavior, which didn't care if the
  // collection existed or not.
  private def getSchema(scope: ScopeID, col: CollectionID) =
    CollectionConfig(scope, col) map {
      _.fold(CollectionSchema.empty(scope, col))(_.Schema)
    }

  def get(
    scope: ScopeID,
    id: DocID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    getSchema(scope, id.collID) flatMap { Store.get(_, id, validTS) }

  def getVersion(
    scope: ScopeID,
    id: DocID,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[Version]] =
    getSchema(scope, id.collID) flatMap { Store.getVersion(_, id, ts) }

  def getVersionNoTTL(
    scope: ScopeID,
    id: DocID,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[Version]] =
    getSchema(scope, id.collID) flatMap { Store.getVersionNoTTL(_, id, ts) }

  def getVersionLiveNoTTL(
    scope: ScopeID,
    id: DocID,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    getSchema(scope, id.collID) flatMap { Store.getVersionLiveNoTTL(_, id, ts) }

  def isDeleted(scope: ScopeID, id: DocID, ts: Timestamp) =
    getSchema(scope, id.collID) flatMap { Store.isDeleted(_, id, ts) }

  def versions(
    scope: ScopeID,
    id: DocID,
    from: VersionID = VersionID.MaxValue,
    to: VersionID = VersionID.MinValue,
    pageSize: Int = DocHistory.DefaultMaxResults,
    reverse: Boolean = false): PagedQuery[Iterable[Version]] =
    getSchema(scope, id.collID) flatMap {
      Store.versions(_, id, from, to, pageSize, reverse)
    }

  def insert(
    scope: ScopeID,
    id: DocID,
    data: Data,
    isCreate: Boolean = false): Query[Version.Live] =
    getSchema(scope, id.collID) flatMap { Store.insert(_, id, data, isCreate) }

  def remove(scope: ScopeID, id: DocID): Query[Version.Deleted] =
    getSchema(scope, id.collID) flatMap { Store.remove(_, id) }

  def insertCreate(
    scope: ScopeID,
    id: DocID,
    validTS: Timestamp,
    data: Data): Query[Version.Live] =
    getSchema(scope, id.collID) flatMap { Store.insertCreate(_, id, validTS, data) }

  def insertDelete(
    scope: ScopeID,
    id: DocID,
    validTS: Timestamp): Query[Version.Deleted] =
    getSchema(scope, id.collID) flatMap { Store.insertDelete(_, id, validTS) }

  def removeVersion(scope: ScopeID, id: DocID, vers: VersionID): Query[Unit] =
    getSchema(scope, id.collID) flatMap { Store.removeVersion(_, id, vers) }
}
