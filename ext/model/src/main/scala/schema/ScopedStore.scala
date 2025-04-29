package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ LookupHelpers, RuntimeEnv }
import fauna.repo.{ IndexRow, PagedQuery, Store }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.schema.DataMode
import fauna.repo.store.{ IndexStore, LookupStore }
import fauna.repo.IndexConfig
import fauna.storage.api.set.Scalar
import fauna.storage.api.version.DocHistory
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.index.IndexTerm
import fauna.storage.VersionID

/** Helpers to query/write to collection configs
  *
  * Note that these will throw an illegal state exception if the given document ID is
  * in a different collection.
  */
object ScopedStore {
  final class Native[I <: ID[I]: CollectionIDTag](
    val cfgQ: Query[CollectionConfig.WithID[I]])
      extends ScopedStore[I] {
    protected def cfg(id: I): Query[CollectionConfig] = cfgQ
    protected def docID(id: I): DocID = id.toDocID

    // writes

    def nextID(): Query[Option[I]] =
      cfgQ.flatMap(c => c.Schema.nextID.mapT(_.as[I]))

    def create(data: Data): Query[Version.Live] =
      cfgQ.flatMap(c => Store.create(c.Schema, data))

    def create(id: I, mode: DataMode, data: Data): Query[Version.Live] =
      cfgQ.flatMap(c => Store.create(c.Schema, docID(id), mode, data))

    // index reads

    def allIDs(ts: Timestamp = Timestamp.MaxMicros): PagedQuery[Iterable[I]] =
      cfgQ.flatMap(getAllIDs(_, ts)).mapValuesT(_.as[I])

    def allDocs(
      ts: Timestamp = Timestamp.MaxMicros): PagedQuery[Iterable[Version.Live]] =
      cfgQ.flatMap(c => getAllIDs(c, ts).collectMT(Store.get(c.Schema, _, ts)))
  }

  final class Schema[I <: ID[I]: CollectionIDTag](
    val cfgQ: Query[CollectionConfig.WithID[I]]) {

    def schemaVersState(id: I): Query[Option[SchemaStatus.VersionView]] =
      cfgQ.flatMap(c => SchemaStatus.getVersState(c.Schema, id.toDocID))

    def uniqueIDForKeyActive(
      idxF: ScopeID => IndexConfig,
      terms: Vector[IndexTerm]): Query[Option[I]] =
      for {
        cfg <- cfgQ
        idx = idxF(cfg.Schema.scope)
        _ = require(idx.sources.contains(cfg.Schema.collID))

        status <- SchemaStatus.forScope(idx.scopeID)
        ts = status.activeSchemaVersion.fold(Timestamp.MaxMicros)(_.ts)

        id <- Store.uniqueIDForKey(idx, terms, ts)
      } yield id.map(_.as[I])
  }

  final class Env(env: RuntimeEnv, scope: ScopeID) extends ScopedStore[DocID] {
    protected def cfg(id: DocID): Query[CollectionConfig] =
      cfg(id.collID)

    protected def cfg(coll: CollectionID): Query[CollectionConfig] =
      env
        .getCollection(scope, coll)
        .getOrElseT(CollectionConfig.NotFound(scope, coll))

    protected def docID(id: DocID): DocID = id

    // writes

    def create(coll: CollectionID, data: Data): Query[Version.Live] =
      cfg(coll).flatMap(c => Store.create(c.Schema, data))

    // index reads

    def allIDs(
      coll: CollectionID,
      ts: Timestamp = Timestamp.MaxMicros): PagedQuery[Iterable[DocID]] =
      cfg(coll).flatMap(getAllIDs(_, ts))

    def allDocs(
      coll: CollectionID,
      ts: Timestamp = Timestamp.MaxMicros): PagedQuery[Iterable[Version.Live]] =
      cfg(coll).flatMap(c => getAllIDs(c, ts).collectMT(Store.get(c.Schema, _, ts)))
  }
}

sealed trait ScopedStore[I] extends Any {
  protected def cfg(id: I): Query[CollectionConfig]
  protected def docID(id: I): DocID

  protected def getAllIDs(c: CollectionConfig, ts: Timestamp) = {
    val terms = Vector(Scalar(c.id.toDocID))
    val idx = NativeIndex.DocumentsByCollection(c.parentScopeID)
    Store.collection(idx, terms, ts).mapValuesT(_.docID)
  }

  // writes

  def replace(id: I, mode: DataMode, data: Data): Query[Version.Live] =
    cfg(id).flatMap(c => Store.replace(c.Schema, docID(id), mode, data))

  // document reads

  def get(
    id: I,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    cfg(id).flatMap(c => Store.get(c.Schema, docID(id), validTS))

  def getVersion(
    id: I,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[Version]] =
    cfg(id).flatMap(c => Store.getVersion(c.Schema, docID(id), ts))

  def getVersionNoTTL(
    id: I,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[Version]] =
    cfg(id).flatMap(c => Store.getVersionNoTTL(c.Schema, docID(id), ts))

  def getVersionLiveNoTTL(
    id: I,
    ts: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    cfg(id).flatMap(c => Store.getVersionLiveNoTTL(c.Schema, docID(id), ts))

  def isDeleted(id: I, ts: Timestamp = Timestamp.MaxMicros) =
    cfg(id).flatMap(c => Store.isDeleted(c.Schema, docID(id), ts))

  def versions(
    id: I,
    from: VersionID = VersionID.MaxValue,
    to: VersionID = VersionID.MinValue,
    pageSize: Int = DocHistory.DefaultMaxResults,
    reverse: Boolean = false): PagedQuery[Iterable[Version]] =
    cfg(id).flatMap(c =>
      Store.versions(c.Schema, docID(id), from, to, pageSize, reverse))

  // live writes

  def internalUpdate(id: I, diff: Diff): Query[Version.Live] =
    cfg(id).flatMap(c => Store.internalUpdate(c.Schema, docID(id), diff))

  def internalReplace(id: I, data: Data): Query[Version.Live] =
    cfg(id).flatMap(c => Store.internalReplace(c.Schema, docID(id), data))

  def internalDelete(id: I): Query[Version.Deleted] =
    cfg(id).flatMap(c => Store.internalDelete(c.Schema, docID(id)))

  // legacy writes

  def insert(id: I, data: Data, isCreate: Boolean = false): Query[Version.Live] =
    cfg(id).flatMap(c => Store.insert(c.Schema, docID(id), data, isCreate))

  def remove(id: I): Query[Version.Deleted] =
    cfg(id).flatMap(c => Store.remove(c.Schema, docID(id)))

  def insertCreate(id: I, validTS: Timestamp, data: Data): Query[Version.Live] =
    cfg(id).flatMap(c => Store.insertCreate(c.Schema, docID(id), validTS, data))

  def insertDelete(id: I, validTS: Timestamp): Query[Version.Deleted] =
    cfg(id).flatMap(c => Store.insertDelete(c.Schema, docID(id), validTS))

  def removeVersion(id: I, vers: VersionID): Query[Unit] =
    cfg(id).flatMap(c => Store.removeVersion(c.Schema, docID(id), vers))

  // remove all versions of the document and all index and lookup entries associated
  // with it
  def clearDocument(id: I): Query[Unit] = {
    cfg(id) flatMap { config =>
      val schema = config.Schema

      Store
        .versions(schema, docID(id))
        .foldLeftValuesMT((Seq.empty[IndexRow], Seq.empty[GlobalID])) {
          case ((rows, globals), v) =>
            schema.indexer.rows(v) map { r =>
              (rows ++ r, globals ++ LookupHelpers.globalIDsFromData(v.id, v.data))
            }
        } flatMap { case (rows, globals) =>
        val entriesQ = globals.distinct.map(Store.lookups(_).flattenT).sequence map {
          entries =>
            entries.flatten.filter { entry =>
              schema.scope == entry.scope && docID(id) == entry.id
            }
        }

        Seq(
          Store.clear(schema.scope, docID(id)),
          entriesQ.flatMap(entries => entries.map(LookupStore.remove).join),
          rows.map(IndexStore.remove).join
        ).join
      }
    }
  }
}
