package fauna.repo

import fauna.atoms._
import fauna.lang.{ ConsoleControl, Page, PageMergeStrategy, Timestamp }
import fauna.lang.syntax._
import fauna.repo.doc._
import fauna.repo.query.{ Query, ReadCache }
import fauna.repo.schema._
import fauna.repo.store._
import fauna.repo.PagedQuery
import fauna.storage._
import fauna.storage.api.debug.{ SetSnapshot => DebugSortedValues }
import fauna.storage.api.scan.{ DocScan, ElementScan, KeyScan }
import fauna.storage.api.set._
import fauna.storage.api.version.{ DocHistory, DocSnapshot, StorageVersion }
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.lookup._
import io.netty.buffer.ByteBuf

object Store {

  private val IndexConsistencyCheckMaxDocIDs = 128

  val log = getLogger()

  // Live writes

  private def failWrite(failure: WriteFailure) =
    Query.fail(WriteFailureException(failure))

  private def liftHookFailures(q: Query[Seq[ConstraintFailure]]): Query[Unit] =
    q.flatMap { fs =>
      if (fs.isEmpty) {
        Query.unit
      } else {
        failWrite(WriteFailure.SchemaConstraintViolation(fs))
      }
    }

  /** Create a new document with an auto-generated ID.
    */
  def create(coll: CollectionSchema, data: Data): Query[Version.Live] = {
    val idQ = coll.nextID flatMap {
      case None     => failWrite(WriteFailure.MaxIDExceeded(coll.scope, coll.collID))
      case Some(id) => Query.value(id)
    }

    idQ flatMap { id =>
      create0(coll, id, SchemaOp.Create(coll, DataMode.Default, data.fields))
    }
  }

  /** Create a new document with a manually specified ID. Fails if a document
    * with the given ID already exists.
    *
    * If the checkIdExists parameter is set to false it will skip the ID exists check.
    * This parameter is provided by the WriteBroker as we will generate the id prior
    * to calling create so that the version can be passed to the ABAC create predicate.
    */
  def create(
    coll: CollectionSchema,
    id: DocID,
    mode: DataMode,
    data: Data,
    checkIdExists: Boolean = true): Query[Version.Live] = {
    require(coll.collID == id.collID)
    val op = SchemaOp.Create(coll, mode, data.fields)
    if (!coll.isValidID(id)) {
      failWrite(WriteFailure.InvalidID(coll.scope, id))
    } else {
      if (checkIdExists) {
        Store.get(coll, id) flatMap {
          case Some(_) => failWrite(WriteFailure.CreateDocIDExists(coll.scope, id))
          case None    => create0(coll, id, op)
        }
      } else {
        create0(coll, id, op)
      }
    }
  }

  private def create0(
    coll: CollectionSchema,
    id: DocID,
    op: SchemaOp.Create): Query[Version.Live] =
    Query.snapshotTime flatMap { snapTS =>
      coll.validateAndTransform(op, snapTS).flatMap {
        case SchemaResult.Ok(transformedData) =>
          val vers =
            Version.Live(coll.scope, id, coll.schemaVersion, transformedData)

          for {
            res <- addVersionAndIndexEntries(
              coll,
              vers,
              applyConstraints = true,
              isCreate = true)
            // hook into other effects
            // todo: do we want to pass our hooks storage or user data?
            _ <- liftHookFailures(
              coll.onWrite(WriteHook.Event(id, None, Some(transformedData))))
          } yield res

        case SchemaResult.Err(validFailures) =>
          failWrite(WriteFailure.SchemaConstraintViolation(validFailures))
      }
    }

  def replace(
    coll: CollectionSchema,
    id: DocID,
    mode: DataMode,
    data: Data): Query[Version.Live] =
    Query.snapshotTime flatMap { snapTS =>
      update(coll, id, Diff(data.fields)) { case (diff, prevData) =>
        coll.validateAndTransform(
          SchemaOp.Replace(coll, mode, diff.fields, prevData),
          snapTS
        )
      }
    }

  /** Update path used for updates originating from a customer query. */
  def externalUpdate(
    coll: CollectionSchema,
    id: DocID,
    mode: DataMode,
    data: Diff): Query[Version.Live] =
    Query.snapshotTime flatMap { snapTS =>
      update(coll, id, data) { case (diff, prevData) =>
        coll.validateAndTransform(
          SchemaOp.Update(coll, mode, diff.fields, prevData),
          snapTS
        )
      }
    }

  /** Update path used for updates originating internally. */
  def internalUpdate(
    coll: CollectionSchema,
    id: DocID,
    data: Diff): Query[Version.Live] = {
    Query.snapshotTime flatMap { snapTS =>
      val schema = coll.internalSchema
      update(schema, id, data) { case (diff, prevData) =>
        schema.validateAndTransform(
          SchemaOp.Update(coll, DataMode.Default, diff.fields, prevData),
          snapTS
        )
      }
    }
  }

  def internalReplace(
    coll: CollectionSchema,
    id: DocID,
    data: Data): Query[Version.Live] =
    Query.snapshotTime flatMap { snapTS =>
      val schema = coll.internalSchema
      update(schema, id, Diff(data.fields)) { case (diff, prevData) =>
        schema.validateAndTransform(
          SchemaOp.Replace(coll, DataMode.Default, diff.fields, prevData),
          snapTS
        )
      }
    }

  private def update(coll: CollectionSchema, id: DocID, data: Diff)(
    updater: (Diff, Data) => (Query[SchemaResult[Data]])): Query[Version.Live] = {
    require(coll.collID == id.collID)
    Store.get(coll, id) flatMap {
      case None => failWrite(WriteFailure.DocNotFound(coll.scope, id))
      case Some(vers) =>
        updater(data, vers.data).flatMap {
          case SchemaResult.Ok(transformedData) =>
            val updatedVers =
              Version.Live(coll.scope, id, coll.schemaVersion, transformedData)

            for {
              res <- addVersionAndIndexEntries(
                coll,
                updatedVers,
                applyConstraints = true,
                isCreate = false)
              _ <- liftHookFailures(
                coll.onWrite(
                  WriteHook.Event(id, Some(vers.data), Some(transformedData))))
            } yield res

          case SchemaResult.Err(validFailures) =>
            failWrite(WriteFailure.SchemaConstraintViolation(validFailures))
        }
    }
  }

  def externalDelete(coll: CollectionSchema, id: DocID): Query[Version.Deleted] =
    deleteImpl(coll, id)

  def internalDelete(coll: CollectionSchema, id: DocID): Query[Version.Deleted] =
    deleteImpl(coll.internalSchema, id)

  // TODO: check inbound refs to determine if the document can be deleted
  // without violating any FK constraints. If not, return a
  // DeleteConstraintViolation.
  private def deleteImpl(
    coll: CollectionSchema,
    id: DocID): Query[Version.Deleted] = {
    require(coll.collID == id.collID)
    Store.get(coll, id) flatMap {
      case None => failWrite(WriteFailure.DocNotFound(coll.scope, id))
      case Some(prev) =>
        val delete = Version.Deleted(coll.scope, id, Unresolved, coll.schemaVersion)
        for {
          res <- addVersionAndIndexEntries(
            coll,
            delete,
            applyConstraints = true,
            isCreate = false)
          // hook into other effects
          _ <- liftHookFailures(
            coll.onWrite(WriteHook.Event(id, Some(prev.data), None)))
        } yield res
    }
  }

  /** Return this document's write prefix compatible with the query's read cache. */
  def writePrefix(scopeID: ScopeID, docID: DocID): Query[ReadCache.Prefix] =
    Query.state map { state =>
      val rowKey = Tables.Versions.rowKeyByteBuf(scopeID, docID)
      state.writesForRowKey(rowKey).to(ReadCache.Prefix)
    }

  /** Feed a partial document to the query's cache. */
  def feedPartial(
    srcHint: ReadCache.CachedDoc.SrcHint,
    prefix: ReadCache.Prefix,
    scopeID: ScopeID,
    docID: DocID,
    validTS: Option[Timestamp],
    partials: ReadCache.Partials): Query[ReadCache.CachedDoc] =
    Query.context map { ctx =>
      val validTime = validTS.getOrElse(Timestamp.MaxMicros)
      ctx.readCache.put(prefix, scopeID, docID, validTime, partials, srcHint)
    }

  /** Peek at docs cache for a (possibly partial) cached entry for the given doc. */
  def peekCache(
    prefix: ReadCache.Prefix,
    scopeID: ScopeID,
    docID: DocID,
    validTS: Option[Timestamp]): Query[Option[ReadCache.CachedDoc]] =
    Query.context map { ctx =>
      val validTime = validTS.getOrElse(Timestamp.MaxMicros)
      ctx.readCache.peek(prefix, scopeID, docID, validTime)
    }

  /** Rewinds query state to the given write prefix for the given document, returning
    * the state of the document without later write effects. The resulting query
    * state is then reset to the state before the rewind so no write effects are
    * lost.
    */
  def rewindAndGet(
    prefix: ReadCache.Prefix,
    schema: CollectionSchema,
    docID: DocID,
    validTS: Option[Timestamp]): Query[Option[Version.Live]] = {

    val rowKey = Tables.Versions.rowKeyByteBuf(schema.scope, docID)
    val validTime = validTS.getOrElse(Timestamp.MaxMicros)

    Query.state flatMap { before =>
      if (before.writesForRowKey(rowKey) forall { prefix.contains(_) }) {
        get(schema, docID, validTime) // No new writes. Read from current state.
      } else {
        for {
          rewind <- Query.updateState { _.rewind(rowKey, prefix) }
          result <- get(schema, docID, validTime)
          _      <- Query.updateState { _.resetWritesOrFail(rewind, before) }
        } yield result
      }
    }
  }

  // Legacy live writes

  def insert(
    schema: CollectionSchema,
    id: DocID,
    data: Data,
    isCreate: Boolean = false): Query[Version.Live] = {
    val validateQ = id.collID match {
      // Run docs in user-collections through v10 schema.
      case UserCollectionID(_) =>
        Query.snapshotTime flatMap { snapTS =>
          val data0 = data.fields.get(List("data")) match {
            case Some(d: MapV) => d
            case _             => MapV.empty
          }

          val latestVersionQ = if (isCreate) {
            Query.value(None)
          } else {
            latestVersion(schema, id)
          }

          val opQ = latestVersionQ.map {
            // NB: `WriteBroker` resolves updates into replaces at a higher
            // level, so at this point everything's a replace.
            case Some(latest) =>
              SchemaOp.Replace(schema, DataMode.PlainData, data0, latest.data)

            case None => SchemaOp.Create(schema, DataMode.PlainData, data0)
          }

          opQ.flatMap { op =>
            schema.validateAndTransform(op, snapTS)
          }
        }

      case _ => Query.value(SchemaResult.Ok(()))
    }

    validateQ flatMap {
      case SchemaResult.Ok(_) =>
        val vers = Version.Live(schema.scope, id, schema.schemaVersion, data)
        addVersionAndIndexEntries(schema, vers, applyConstraints = true, isCreate)
      case SchemaResult.Err(validFailures) =>
        failWrite(WriteFailure.SchemaConstraintViolation(validFailures))
    }
  }

  def insertUnmigrated(
    scope: ScopeID,
    id: DocID,
    data: Data,
    isCreate: Boolean = false): Query[Version.Live] = {
    insert(CollectionSchema.empty(scope, id.collID), id, data, isCreate)
  }

  def remove(schema: CollectionSchema, id: DocID): Query[Version.Deleted] = {
    val vers = Version.Deleted(schema.scope, id, Unresolved, schema.schemaVersion)
    addVersionAndIndexEntries(
      schema,
      vers,
      applyConstraints = true,
      isCreate = false)
  }

  def removeUnmigrated(scope: ScopeID, id: DocID): Query[Version.Deleted] = {
    remove(CollectionSchema.empty(scope, id.collID), id)
  }

  // Historical writes

  private def latestVersion(schema: CollectionSchema, docID: DocID) =
    rawVersions(schema.scope, docID, pageSize = 1).headValueT mapT { raw =>
      Version.fromStorage(raw, schema.migrations)
    }

  private def currVersion(
    schema: CollectionSchema,
    docID: DocID,
    versionID: VersionID) =
    rawVersions(
      schema.scope,
      docID,
      max = versionID,
      min = versionID,
      pageSize = 1
    ).headValueT mapT { raw =>
      Version.fromStorage(raw, schema.migrations)
    }

  private def prevVersion(
    schema: CollectionSchema,
    docID: DocID,
    versionID: VersionID) =
    rawVersions(
      schema.scope,
      docID,
      max = versionID.saturatingDecr,
      min = VersionID.MinValue,
      order = Order.Descending,
      pageSize = 1
    ).headValueT mapT { raw =>
      Version.fromStorage(raw, schema.migrations)
    }

  private def nextVersion(
    schema: CollectionSchema,
    docID: DocID,
    versionID: VersionID) =
    rawVersions(
      schema.scope,
      docID,
      max = VersionID.MaxValue,
      min = versionID.saturatingIncr,
      order = Order.Ascending,
      pageSize = 1
    ).headValueT mapT { raw =>
      Version.fromStorage(raw, schema.migrations)
    }

  /** Computes the revision needed upon inserting the given version to storage. */
  private def insertRevision(
    schema: CollectionSchema,
    version: Version,
    isCreate: Boolean
  ): Query[Revision] = {
    require(
      schema.scope == version.parentScopeID,
      s"version scope ${version.parentScopeID} did not match schema scope ${schema.scope}")

    @inline def origin =
      Version.Deleted(
        version.parentScopeID,
        version.docID,
        AtValid(Timestamp.Epoch),
        version.schemaVersion)

    // If we think this insert is a create, avoid looking for pre-existing versions.
    // Conflicts will resolve themselves later.
    if (isCreate) {
      Query.value(
        Revision(
          origin,
          Seq.empty[Version],
          Seq(version)
        ))
    } else {
      latestVersion(schema, version.docID) flatMap {
        case Some(latest) if latest.ts.validTS >= version.ts.validTS =>
          val (beforeQ, currQ, afterQ) =
            // NB. Skip reading from storage when rewritting the same document
            // multiple times in the same txn (latest & version ts == Unresolved). In
            // this case, the previous and current versions can be determined from
            // the latest unresolved version presuming that it was produced by this
            // method when first added to the query state, thus hitting the else
            // branch first and computing a valid revision.
            if (latest.ts == Unresolved && version.ts == Unresolved) {
              (
                Query.value(latest.prevVersion),
                Query.some(latest),
                Query.none
              )
            } else {
              (
                prevVersion(schema, version.docID, version.versionID),
                currVersion(schema, version.docID, version.versionID),
                nextVersion(schema, version.docID, version.versionID)
              )
            }

          (beforeQ, currQ, afterQ) par { (before, curr, after) =>
            Query.value(
              Revision(
                before.getOrElse(origin),
                curr.toSeq ++ after,
                version +: after.toSeq
              ))
          }

        case latest =>
          Query.value(
            Revision(
              latest.getOrElse(origin),
              Seq.empty[Version],
              Seq(version)
            ))
      }
    }
  }

  /** Computes the revision needed upon removing the given version to storage. */
  private[repo] def removeRevision(
    schema: CollectionSchema,
    docID: DocID,
    versionID: VersionID
  ): Query[Revision] = {
    val mvtQ =
      Query.repo flatMap {
        _.mvtProvider.get(schema.scope, docID.collID)
      }

    mvtQ flatMap { mvt =>
      val origin =
        Version.Deleted(
          schema.scope,
          docID,
          AtValid(Timestamp.Epoch),
          schema.schemaVersion)
      val beforeQ = prevVersion(schema, docID, versionID)
      val currQ = currVersion(schema, docID, versionID)
      val afterQ = nextVersion(schema, docID, versionID)

      (beforeQ, currQ, afterQ) par { (before, curr, after) =>
        val target = curr match {
          case Some(gcRoot: Version.Live) if gcRoot.ts.validTS <= mvt =>
            // NB. A live version below MVT can only be the GC root. Removing the GC
            // root causes already expired versions to reappear. Instead, turn the
            // remove into an insert or a deleted version, which is logically
            // equivalent of removing the root from a data model standpoint.
            val remove =
              Version.Deleted(
                schema.scope,
                docID,
                gcRoot.ts.unresolve,
                schema.schemaVersion)
            gcRoot +: remove +: after.toSeq
          case _ =>
            after.toSeq
        }

        val rev =
          Revision(
            before.getOrElse(origin),
            curr ++ after,
            target
          )
        Query.value(rev)
      }
    }
  }

  private def addVersionAndIndexEntries[V <: Version](
    schema: CollectionSchema,
    version: V,
    applyConstraints: Boolean,
    isCreate: Boolean
  ): Query[V] =
    insertRevision(schema, version, isCreate) flatMap {
      VersionStore.revise(_)
    } flatMap { revision =>
      IndexStore.update(
        schema.indexer,
        revision,
        applyConstraints
      ) map { _ => version }
    }

  def insertCreate(
    schema: CollectionSchema,
    id: DocID,
    validTS: Timestamp,
    data: Data): Query[Version.Live] = {
    val vers =
      Version.Live(
        schema.scope,
        id,
        AtValid(validTS),
        Create,
        schema.schemaVersion,
        data)
    addVersionAndIndexEntries(
      schema,
      vers,
      applyConstraints = false,
      isCreate = false)
  }

  def insertCreateUnmigrated(
    scope: ScopeID,
    id: DocID,
    validTS: Timestamp,
    data: Data): Query[Version.Live] = {
    insertCreate(CollectionSchema.empty(scope, id.collID), id, validTS, data)
  }

  def insertDelete(
    schema: CollectionSchema,
    id: DocID,
    validTS: Timestamp): Query[Version.Deleted] = {
    val vers =
      Version.Deleted(schema.scope, id, AtValid(validTS), schema.schemaVersion)
    addVersionAndIndexEntries(
      schema,
      vers,
      applyConstraints = false,
      isCreate = false)
  }

  def insertDeleteUnmigrated(
    scope: ScopeID,
    id: DocID,
    validTS: Timestamp): Query[Version.Deleted] = {
    insertDelete(CollectionSchema.empty(scope, id.collID), id, validTS)
  }

  def removeVersion(
    schema: CollectionSchema,
    id: DocID,
    vers: VersionID): Query[Unit] =
    removeRevision(schema, id, vers) flatMap {
      VersionStore.revise(_)
    } flatMap { revision =>
      IndexStore.update(schema.indexer, revision, false)
    }

  def removeVersionUnmigrated(
    scope: ScopeID,
    id: DocID,
    vers: VersionID): Query[Unit] =
    removeVersion(CollectionSchema.empty(scope, id.collID), id, vers)

  // Reads.

  /** Flip cursor bounds to its descending order as required by the read ops. */
  private def descendingBounds[A](from: A, to: A, ascending: Boolean) =
    if (ascending) {
      (to, from, Order.Ascending)
    } else {
      (from, to, Order.Descending)
    }

  // Gets the latest version of a document if it is live.
  def getUnmigrated(
    scopeID: ScopeID,
    id: DocID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    get(CollectionSchema.empty(scopeID, id.collID), id, validTS)

  def get(
    schema: CollectionSchema,
    id: DocID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    Query.repo flatMap { _.mvtProvider.get(schema.scope, id.collID) } flatMap {
      mvt =>
        Query.snapshotTime flatMap { snapTS =>
          val op =
            DocSnapshot(schema.scope, id, snapTS, mvt, VersionID(validTS, Delete))
          Query.read(op) map {
            _.version
              .filter { _.isLive }
              .map { raw =>
                Version
                  .fromStorage(raw, schema.migrations)
                  .asInstanceOf[Version.Live]
              }
          }
        }
    }

  /** Gets the latest version of a document <= the given validTS,
    * if one exists.
    */
  def getVersion(
    schema: CollectionSchema,
    id: DocID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version]] =
    checkTTL(getVersionNoTTL(schema, id, validTS))

  /** Finds the latest version of a document <= the given versionID, ignoring TTL. */
  def getVersionNoTTL(
    schema: CollectionSchema,
    id: DocID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version]] =
    versions(schema, id, from = VersionID(validTS, Delete), pageSize = 1).headValueT

  /** Gets the latest _live_ version of a document <= the given validTS, if one exists,
    * ignoring TTL.
    */
  def getVersionLiveNoTTL(
    schema: CollectionSchema,
    id: DocID,
    validTS: Timestamp = Timestamp.MaxMicros): Query[Option[Version.Live]] =
    versions(schema, id, from = VersionID(validTS, Delete), pageSize = 2)
      .findValueT(!_.isDeleted)
      .asInstanceOf[Query[Option[Version.Live]]]

  // Finds the latest version of a document.
  // A version past TTL is still returned.
  // TODO: There's just one very special use of this in IDStore...
  private[repo] def getLatestNoTTLUnmigrated(
    scopeID: ScopeID,
    id: DocID,
    versionID: VersionID = VersionID.MaxValue): Query[Option[Version]] =
    versionsUnmigrated(
      scopeID,
      id,
      from = versionID,
      to = VersionID.MinValue,
      pageSize = 1) headValueT

  def isDeleted(schema: CollectionSchema, id: DocID, ts: Timestamp) =
    getVersion(schema, id, ts) existsT {
      _.isDeleted
    }

  // Build a sparse snapshot query.
  private def qSparseSnapshot(
    cfg: IndexConfig,
    terms: Vector[Term],
    validTS: Timestamp,
    slices: Vector[(IndexValue, IndexValue)],
    ascending: Boolean): PagedQuery[Iterable[Element.Live]] = {
    // Short-circuit: avoid reads on empty slices.
    if (slices.isEmpty) {
      return PagedQuery.empty
    }

    implicit val order =
      if (ascending) {
        Element.Live.ByValueOrdering
      } else {
        Element.Live.ByValueOrdering.reverse
      }

    val readOrder =
      if (ascending) {
        Order.Ascending
      } else {
        Order.Descending
      }

    val sliceBounds =
      slices map { case (from, to) =>
        val (max, min, _) = descendingBounds(from, to, ascending)
        (max, min)
      }

    Query.repo flatMap { _.mvtProvider.get(cfg, terms) } flatMap { mvt =>
      Query.snapshotTime flatMap { snapTS =>
        IndexStore.setReadIsolation(
          cfg,
          partitionedRead(cfg, terms, SetSnapshot.DefaultMaxResults) { (terms, _) =>
            val op =
              SparseSetSnapshot(
                cfg.scopeID,
                cfg.id,
                terms,
                sliceBounds,
                snapTS,
                mvt,
                readTS(validTS, snapTS),
                readOrder
              )
            Query.read(op) map { res =>
              Page[Query](res.values)
            }
          }
        )
      }
    }
  }

  /** Compute a sparse snapshot of a set at the given tuples in `keys`.
    *
    * The doc ID in the tuple doesn't matter-- just the values. So,
    * supplying a tuple like (ScopeID 0, DocID MIN, [Term("x"), Term("y")]
    * will match all entries whose values match the prefix [Term("x"), Term("y")].
    *
    * The results are retrieved in one round-trip to the data node, so it is
    * not appropriate for large result sets.
    * For safety, the maximum number of results that will be returned is
    * SparseSetSnapshot.MaxResults (65536).
    * // TODO: Figure out a sensible limit on the number of keys.
    *
    * This API optimizes set intersections and it's tricky.
    * I suggest avoiding it.
    */
  def sparseCollection(
    cfg: IndexConfig,
    terms: Vector[Term],
    snapshotTS: Timestamp,
    keys: Vector[IndexTuple],
    ascending: Boolean) = {
    val slices = keys map { key => IndexStore.sparseBound(cfg, key, ascending) }
    qSparseSnapshot(cfg, terms, snapshotTS, slices, ascending) mapValuesT {
      _.toIndexValue
    }
  }

  // Build a query producing paginated snapshot read results.
  private def qSetSnapshot(
    cfg: IndexConfig,
    terms: Vector[Term],
    validTS: Timestamp,
    from: IndexTuple = IndexTuple.MinValue,
    to: IndexTuple = IndexTuple.MaxValue,
    pageSize: Int = SetSnapshot.DefaultMaxResults,
    ascending: Boolean = true): PagedQuery[Iterable[Element.Live]] = {

    implicit val baseOrder =
      if (ascending) {
        Element.Live.ByValueOrdering
      } else {
        Element.Live.ByValueOrdering.reverse
      }

    val cursor = {
      val (max, min, order) = descendingBounds(from, to, ascending)
      val (max0, min0) = SetPadding.pad(max, min, cfg.reverseFlags)
      SetSnapshot.Cursor(max0, min0, order)
    }

    Query.repo flatMap { _.mvtProvider.get(cfg, terms) } flatMap { mvt =>
      Query.snapshotTime flatMap { snapTS =>
        IndexStore.setReadIsolation(
          cfg,
          partitionedRead(cfg, terms, pageSize) { (terms, pageSize) =>
            Page.unfold(
              SetSnapshot(
                cfg.scopeID,
                cfg.id,
                terms,
                cursor,
                snapTS,
                mvt,
                readTS(validTS, snapTS),
                pageSize
              )) { op =>
              Query.read(op.forceEncodable()) map { res =>
                (res.values, res.next)
              }
            }
          }
        )
      }
    }
  }

  def collection(
    cfg: IndexConfig,
    terms: Vector[Term],
    snapshotTS: Timestamp,
    from: IndexTuple = IndexTuple.MinValue,
    to: IndexTuple = IndexTuple.MaxValue,
    pageSize: Int = DefaultPageSize,
    ascending: Boolean = true): PagedQuery[Iterable[IndexValue]] =
    qSetSnapshot(cfg, terms, snapshotTS, from, to, pageSize, ascending).mapValuesT {
      _.toIndexValue
    }

  /** Performs a snapshot read of the index defined by `cfg` and
    * `terms` at `snapshotTS`, yielding each entry to
    * `get()`. `snapshotTS` is also passed to `get()` to provide a
    * consistent snapshot across the index and canonical data where
    * necessary.
    */
  def collectDocuments[T](
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    snapshotTS: Timestamp)(
    get: (IndexTuple, Timestamp) => Query[Option[T]]): PagedQuery[Iterable[T]] =
    qSetSnapshot(cfg, terms map { _.value }, snapshotTS) flatMapValuesT { e =>
      get(e.toTuple, snapshotTS) map { _.toSeq }
    }

  /** Returns a document ID matching the terms. */
  def uniqueIDForKey(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    snapshotTS: Timestamp = Timestamp.MaxMicros): Query[Option[DocID]] = {
    val snapshotQ =
      qSetSnapshot(
        cfg,
        terms map { _.value },
        snapshotTS,
        from = IndexTuple.MaxValue,
        to = IndexTuple.MinValue,
        pageSize = 1,
        ascending = false
      ).headValueT mapT { _.docID }

    // NOTE: If a snapshot can't be encoded to read such value, a write to create the
    // value itself can't be encoded either. Presume such combination can not exist.
    snapshotQ recover { case _: ComponentTooLargeException => None }
  }

  /** Returns a document ID matching the given terms and tuple values. */
  def uniqueID(
    cfg: IndexConfig,
    terms: Vector[Term],
    tuple: IndexTuple
  ): Query[Option[DocID]] = {
    val snapshotQ =
      qSetSnapshot(
        cfg,
        terms,
        Timestamp.MaxMicros,
        from = IndexTuple.MaxValue.copy(values = tuple.values),
        to = IndexTuple.MinValue.copy(values = tuple.values),
        pageSize = 1,
        ascending = false
      ).headValueT mapT { _.docID }

    // NOTE: If a snapshot can't be encoded to read such value, a write to create the
    // value itself can't be encoded either. Presume such combination can not exist.
    snapshotQ recover { case _: ComponentTooLargeException => None }
  }

  // Build a query producing paginated set sorted values read results.
  private def qSetSortedValues(
    cfg: IndexConfig,
    terms: Vector[Term],
    from: IndexValue,
    to: IndexValue,
    pageSize: Int,
    ascending: Boolean,
    mvtProvider: MVTProvider): PagedQuery[Iterable[Element]] = {

    implicit val order =
      if (ascending) {
        Element.ByValueOrdering
      } else {
        Element.ByValueOrdering.reverse
      }

    val cursor = {
      val (max, min, order) = descendingBounds(from, to, ascending)
      val (max0, min0) = SetPadding.pad(max, min, cfg.reverseFlags)
      SetSortedValues.Cursor(max0, min0, order)
    }

    mvtProvider.get(cfg, terms) flatMap { mvt =>
      Query.snapshotTime flatMap { snapTS =>
        partitionedRead(cfg, terms, pageSize) { (terms, pageSize) =>
          Page.unfold(
            SetSortedValues(
              cfg.scopeID,
              cfg.id,
              terms,
              cursor,
              snapTS,
              mvt,
              pageSize
            )) { op =>
            Query.read(op.forceEncodable()) map { res =>
              (res.values, res.next)
            }
          }
        }
      }
    }
  }

  def sortedIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm]): PagedQuery[Iterable[IndexValue]] =
    sortedIndex(cfg, terms, pageSize = DefaultPageSize)

  def sortedIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    from: IndexValue = IndexValue.MaxValue,
    to: IndexValue = IndexValue.MinValue,
    pageSize: Int = SetSortedValues.DefaultMaxResults,
    ascending: Boolean = false): PagedQuery[Iterable[IndexValue]] =
    Query.repo flatMap { repo =>
      qSetSortedValues(
        cfg,
        terms map { _.value },
        from,
        to,
        pageSize,
        ascending,
        repo.mvtProvider) mapValuesT {
        _.toIndexValue
      }
    }

  def sortedIndexDebug(
    cfg: IndexConfig,
    terms: Vector[Term],
    from: IndexValue,
    to: IndexValue,
    pageSize: Int)(implicit ctl: ConsoleControl): PagedQuery[Iterable[IndexValue]] =
    Query.snapshotTime flatMap { snapTS =>
      val cursor = {
        val (max, min, _) = descendingBounds(from, to, ascending = false)
        val (max0, min0) = SetPadding.pad(max, min, cfg.reverseFlags)
        DebugSortedValues.Cursor(max0, min0)
      }

      Page.unfold(
        DebugSortedValues(
          cfg.scopeID,
          cfg.id,
          terms,
          cursor,
          snapTS,
          pageSize
        )) { op =>
        Query.read(op) map { res =>
          (res.values, res.next)
        }
      }
    }

  // Historical

  private def versionsUnmigrated(
    scopeID: ScopeID,
    id: DocID,
    from: VersionID,
    to: VersionID,
    pageSize: Int,
    reverse: Boolean = false): PagedQuery[Iterable[Version]] = {
    val (max, min, order) = descendingBounds(from, to, reverse)
    rawVersions(scopeID, id, max, min, order, pageSize) mapValuesT Version.decode
  }

  def versions(
    schema: CollectionSchema,
    id: DocID,
    from: VersionID = VersionID.MaxValue,
    to: VersionID = VersionID.MinValue,
    pageSize: Int = DocHistory.DefaultMaxResults,
    reverse: Boolean = false): PagedQuery[Iterable[Version]] = {
    val (max, min, order) = descendingBounds(from, to, reverse)
    rawVersions(schema.scope, id, max, min, order, pageSize) mapValuesT { raw =>
      Version.fromStorage(raw, schema.migrations)
    }
  }

  @annotation.nowarn("cat=unused")
  def versions(
    scopeID: ScopeID,
    id: DocID,
    from: VersionID,
    to: VersionID,
    pageSize: Int,
    reverse: Boolean,
    mvtProvider: MVTProvider)(
    implicit ctl: ConsoleControl): PagedQuery[Iterable[Version]] = {
    val (max, min, order) = descendingBounds(from, to, reverse)
    rawVersions(
      scopeID,
      id,
      max,
      min,
      order,
      pageSize,
      mvtProvider) mapValuesT Version.decode
  }

  def rawVersions(
    scopeID: ScopeID,
    docID: DocID,
    max: VersionID = VersionID.MaxValue,
    min: VersionID = VersionID.MinValue,
    order: Order = Order.Descending,
    pageSize: Int = DocHistory.DefaultMaxResults
  ): PagedQuery[Iterable[StorageVersion]] =
    Query.repo flatMap { repo =>
      rawVersions(scopeID, docID, max, min, order, pageSize, repo.mvtProvider)
    }

  private def rawVersions(
    scopeID: ScopeID,
    docID: DocID,
    max: VersionID,
    min: VersionID,
    order: Order,
    pageSize: Int,
    mvtProvider: MVTProvider) =
    mvtProvider.get(scopeID, docID.collID) flatMap { mvt =>
      Query.snapshotTime flatMap { snapTS =>
        val cursor = DocHistory.Cursor(max, min, order)
        val readOp = DocHistory(scopeID, docID, cursor, snapTS, mvt, pageSize)
        Page.unfold(readOp) { op =>
          Query.read(op) map { res =>
            (res.versions, res.next)
          }
        }
      }
    }

  // Build a query producing paginated history read results.
  private def qSetHistory(
    cfg: IndexConfig,
    terms: Vector[Term],
    from: IndexValue,
    to: IndexValue,
    pageSize: Int,
    ascending: Boolean,
    mvtProvider: MVTProvider): PagedQuery[Iterable[Element]] = {

    implicit val order =
      if (ascending) {
        Element.ByEventOrdering
      } else {
        Element.ByEventOrdering.reverse
      }

    val cursor = {
      val (max, min, order) = descendingBounds(from, to, ascending)
      SetHistory.Cursor(max, min, order)
    }

    mvtProvider.get(cfg, terms) flatMap { mvt =>
      Query.snapshotTime flatMap { snapTS =>
        partitionedRead(cfg, terms, pageSize) { (terms, pageSize) =>
          Page.unfold(
            SetHistory(
              cfg.scopeID,
              cfg.id,
              terms,
              cursor,
              snapTS,
              mvt,
              pageSize
            )
          ) { op =>
            Query.read(op) map { res =>
              (res.values, res.next)
            }
          }
        }
      }
    }
  }

  /** Returns all events using the default page size and natural sort order.
    */
  def historicalIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm]): PagedQuery[Iterable[IndexValue]] =
    historicalIndex(cfg, terms, pageSize = DefaultPageSize)

  def historicalIndex(
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    from: IndexValue = IndexValue.MaxValue,
    to: IndexValue = IndexValue.MinValue,
    pageSize: Int = SetHistory.DefaultMaxResults,
    ascending: Boolean = false): PagedQuery[Iterable[IndexValue]] =
    Query.repo flatMap { repo =>
      qSetHistory(
        cfg,
        terms map { _.value },
        from,
        to,
        pageSize,
        ascending,
        repo.mvtProvider) mapValuesT {
        _.toIndexValue
      }
    }

  @annotation.nowarn("cat=unused")
  def historicalIndex(
    cfg: IndexConfig,
    terms: Vector[Term],
    from: IndexValue,
    to: IndexValue,
    pageSize: Int,
    ascending: Boolean,
    mvtProvider: MVTProvider)(
    implicit ctl: ConsoleControl): PagedQuery[Iterable[Element]] =
    qSetHistory(cfg, terms, from, to, pageSize, ascending, mvtProvider)

  // Lookups

  /** Returns a parent scope/database ID pair for the provided global ID
    * if the database is live. This pair uniquely identifies the
    * database document within this cluster.
    *
    * The global ID may be either the scope the database defines, or
    * the global ID within the database's document.
    */
  def getDatabaseID(globalID: GlobalID): Query[Option[(ScopeID, DatabaseID)]] = {
    checkGlobalDatabase(globalID)

    val snapshot = Query.repo flatMap { repo =>
      Query.snapshotTime flatMap { ts =>
        CollectionAtTS(
          Store.lookups(globalID) mapValuesT { (globalID, _) },
          ts,
          repo.schemaRetentionDays)
      }
    }

    snapshot
      .selectT { _.isCreate }
      .headValueT
      .mapT { le =>
        (le.scope, le.id.as[DatabaseID])
      }
  }

  /** Returns a parent scope/database ID pair for the provided global
    * ID regardless of the database's liveness.
    *
    * See getDatabaseID().
    */
  def getLatestDatabaseID(
    globalID: GlobalID): Query[Option[(ScopeID, DatabaseID)]] = {
    checkGlobalDatabase(globalID)
    getLatestLookup(globalID) mapT { le =>
      (le.scope, le.id.as[DatabaseID])
    }
  }

  /** Returns an entry from the lookup table for the provided global ID,
    * if one exists.
    */
  def getLatestLookup(globalID: GlobalID): Query[Option[LookupEntry]] = {
    lookups(globalID).foldLeftValuesT(Option.empty[LookupEntry]) {
      case (None, b)    => Some(b)
      case (Some(a), b) => Some(LookupEntry.latestForGlobalID(a, b))
    }
  }

  /** Returns all entries associated with the provided global ID in the
    * lookup table.
    *
    * This is conceptually the same as a history of this global ID,
    * akin to sortedIndex() or versions().
    */
  def lookups(globalID: GlobalID): PagedQuery[Iterable[LookupEntry]] =
    LookupStore.byID(globalID)

  /** Returns all entries associated with the provided global key ID in
    * the lookup table.
    */
  def keys(key: GlobalKeyID): PagedQuery[Iterable[(ScopeID, KeyID)]] =
    lookups(key) mapT { page =>
      page.headOption filter { _.isCreate } map { le =>
        (le.scope, le.id.as[KeyID])
      }
    }

  /** Returns all entries associated with the provided global ID in
    * the lookup table.
    */
  def databases(globalID: GlobalID): PagedQuery[Iterable[(ScopeID, DatabaseID)]] =
    lookups(globalID) mapT { page =>
      page.headOption map { le =>
        (le.scope, le.id.as[DatabaseID])
      } toSeq
    }

  // Utility/Misc

  /** A document scan returns the latest version for each document
    * within the `bounds`.
    */
  def docScanRaw(
    bounds: ScanBounds,
    from: ByteBuf,
    selector: Selector,
    pageSize: Int,
    snapshotOverride: Option[Timestamp] = None)
    : PagedQuery[Iterable[StorageVersion]] = {
    val snapQ = snapshotOverride.fold(Query.snapshotTime)(Query.value)

    snapQ flatMap { snapTS =>
      val scan =
        DocScan(
          snapTS,
          ScanSlice(Tables.Versions.CFName, bounds, from),
          selector,
          pageSize)
      Page.unfold(scan) { op =>
        Query.scan(op) map { res =>
          val next = res.next map { slice =>
            DocScan(snapTS, slice, selector, pageSize)
          }
          (res.values, next)
        }
      }
    }
  }

  def docScan(
    getSchema: (ScopeID, CollectionID) => Query[Option[CollectionSchema]],
    bounds: ScanBounds,
    from: ByteBuf,
    selector: Selector,
    pageSize: Int,
    snapshotOverride: Option[Timestamp] = None
  ): PagedQuery[Iterable[Version]] =
    docScanRaw(bounds, from, selector, pageSize, snapshotOverride).flatMapValuesT {
      raw =>
        // TODO: we get the schema here which means we could do some MVT sanity
        // checking
        getSchema(raw.scopeID, raw.docID.collID).map {
          case Some(sc) => Seq(Version.fromStorage(raw, sc.migrations))
          case None     => Nil
        }
    }

  // Key scan.
  def sortedKeyScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[KeyScan.Entry]] =
    keyScan(Tables.SortedIndex.CFName, bounds, pageSize)

  def historicalKeyScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[KeyScan.Entry]] =
    keyScan(Tables.HistoricalIndex.CFName, bounds, pageSize)

  private def keyScan(
    cf: String,
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[KeyScan.Entry]] =
    Query.snapshotTime flatMap { snapTS =>
      val scan = KeyScan(snapTS, ScanSlice(cf, bounds), pageSize)
      Page.unfold(scan) { op =>
        Query.scan(op) map { res =>
          (res.values, res.next map { slice => KeyScan(snapTS, slice, pageSize) })
        }
      }
    }

  // Element scan.
  def sortedElementScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[ElementScan.Entry]] =
    elementScan(Tables.SortedIndex.CFName, bounds, pageSize)

  def historicalElementScan(
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[ElementScan.Entry]] =
    elementScan(Tables.HistoricalIndex.CFName, bounds, pageSize)

  private def elementScan(
    cf: String,
    bounds: ScanBounds,
    pageSize: Int): PagedQuery[Iterable[ElementScan.Entry]] =
    Query.snapshotTime flatMap { snapTS =>
      val scan = ElementScan(
        snapTS,
        ElementScan.Cursor(ScanSlice(cf, bounds), false),
        pageSize)
      Page.unfold(scan) { op =>
        Query.scan(op) map { res =>
          (
            res.values,
            res.next map { slice => ElementScan(snapTS, slice, pageSize) })
        }
      }
    }

  /** Given a Version and an Indexer, computes the necessary index rows,
    * and inserts them regardless of existing state. Returns the
    * number of rows inserted.
    *
    * See fauna.model.tasks.IndexBuild.
    */
  def build(version: Version, indexer: Indexer): Query[Int] =
    IndexStore.build(version, indexer)

  /** Writes a row tombstone over the provided document, deleting all of
    * its history. This is used to remove data belonging to deleted
    * Databases and Collections.
    *
    * See fauna.model.tasks.SparseDocumentTask.
    */
  def clear(scope: ScopeID, id: DocID): Query[Unit] =
    VersionStore.clear(scope, id)

  /** Writes a row tombstone over the provided index key, deleting all
    * of its history. This is used to remove data belonging to deleted
    * Databases and Indexes.
    *
    * See fauna.model.tasks.SparseIndexTask.
    */
  def clear(key: ByteBuf): Query[Unit] =
    IndexStore.clear(key)

  /** Discovers index `add` events within the currently-executing
    * QueryContext which refer to deleted documents.
    *
    * See fauna.ast.ReadAdaptor.
    */
  def invalidIndexRowsForDocID(scope: ScopeID, docID: DocID)(
    deriveMVT: (ScopeID, CollectionID) => Query[Timestamp])
    : Query[Seq[(Timestamp, IndexRow)]] =
    Query.context flatMap { c =>
      if (
        c.indexConsistencyCheckDocIDs.size >= IndexConsistencyCheckMaxDocIDs ||
        !c.addIndexConsistencyCheckDocID(docID)
      ) {
        Query.value(Seq.empty)
      } else {
        SortedIndex.invalidIndexRowsForDocID(scope, docID)(deriveMVT)
      }
    }

  /** Given two contiguous versions, produces a (possibly empty) set of
    * writes which will:
    *
    * 1. remove a's conflicts from storage
    * 2. rewrite a.diff such that a.patch(a.diff) equals b.data
    * 3. remove a, if both a and b are deletes
    *
    * Secondary indexes will also be updated as necessary.
    *
    * See fauna.model.tasks.DocGarbageCollection.
    */
  def repair(
    indexer: Indexer,
    a: Conflict[Version],
    b: Conflict[Version]): Query[Unit] =
    VersionStore.repair(a, b) foreachT { rev =>
      IndexStore.update(indexer, rev, false)
    }

  private def checkTTL(q: Query[Option[Version]]) = Query.snapshotTime flatMap {
    snapTS =>
      q rejectT { _.ttl.exists { _ <= snapTS } }
  }

  private def checkGlobalDatabase(globalID: GlobalID) =
    globalID match {
      case _: GlobalKeyID =>
        throw new IllegalArgumentException(
          s"globalID must be a ScopeID or GlobalDatabaseID, but received a $globalID")
      case _: ScopeID | _: GlobalDatabaseID => ()
    }

  // See the comment in IndexStore.
  private def overFetchPageSize(pageSize: Int, parts: Long) =
    math.ceil((pageSize.toDouble / parts) * AbstractIndex.OverFetchFactor).toInt

  private def partitionedRead[A](cfg: IndexConfig, terms: Vector[Term], size: Int)(
    fn: (Vector[Term], Int) => PagedQuery[Iterable[A]])(
    implicit order: Ordering[A]): PagedQuery[Iterable[A]] = {

    if (cfg.isPartitioned) {
      val parts = cfg.partitionTerms(terms) { Scalar(_) }
      val partSize = overFetchPageSize(size, cfg.partitions)
      val streams = parts map { terms => fn(terms, partSize) }
      Page.mergeWithStrategy(streams, PageMergeStrategy.Simple[A])
    } else {
      fn(terms, size)
    }
  }

  // The old API specifies valid times after the snapshot time, which is illegal
  // in the new API. This case is equivalent to "read latest" which, in the new
  // API, means reading with no valid time specified.
  private def readTS(validTS: Timestamp, snapshotTS: Timestamp): Option[Timestamp] =
    Option.when(validTS <= snapshotTS)(validTS)

}

// DANGER: NonTransactionalStore interacts with *local* storage *non-transactionally*.
// This object may be temporary: its purpose is only to force the caller to write
// "NonTransactional" before they use the dangerous functions within.
object NonTransactionalStore {
  // See the description in storage-api's Storage.scala.
  def zapEntry(cf: String, e: ElementScan.Entry): Query[Unit] =
    Query.repo map { repo =>
      repo.service.newStorage.zapEntry(cf, e)
    }
}
