package fauna.tools

import fauna.atoms._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Key => FKey, _ }
import fauna.model.schema.NativeCollectionID
import fauna.repo.doc.Version
import fauna.storage.api.MVTMap
import fauna.storage.doc.{ Field, ValueRequired }
import fauna.storage.index.NativeIndexID
import java.io.{ File, FileOutputStream }
import java.nio.file.Files
import java.util.{ Collections, Map => JMap }
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentSkipListSet }
import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.{ HashMap, Map => MMap, Set => MSet }
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{ Try, Using }

/** Saves the latest state of Schema documents.
  *
  * Also used to build Key metadata information. If a key contains the "database"
  * field, it creates an indirection that's not possible to solve without
  * having to read the key document. The "database" field contains the
  * database id of the database this key grant access to, then we also need
  * to read databases documents and read its "scope" field to resolve the
  * final scope id. Essentially we have two cases:
  *
  * 1. Keys without "database" field:
  *   scopeID = key.parentScopeID
  *
  * 2. Keys with "database" field:
  *   scopeID = key.database.scope
  */
final class SchemaMetadataBuilder {
  import SchemaMetadata._

  private[this] val databases =
    new ConcurrentHashMap[(ScopeID, DatabaseID), DB]

  private[this] val collections =
    new ConcurrentHashMap[(ScopeID, CollectionID), Cls]

  private[this] val indexes =
    new ConcurrentHashMap[(ScopeID, IndexID), Idx]

  private[this] val keys =
    new ConcurrentHashMap[(ScopeID, KeyID), Key]

  private[this] val rowTombstones =
    new ConcurrentSkipListSet[(ScopeID, DocID)]({ (a, b) =>
      a._1.compare(b._1) match {
        case 0 => a._2.compare(b._2)
        case n => n
      }
    })

  def getDatabase(scope: ScopeID, keyID: KeyID): Option[DatabaseID] =
    Option(keys.get((scope, keyID))) flatMap { _.database }

  def markDeleted(parentScope: ScopeID, id: DocID): Unit =
    id match {
      case DatabaseID(_) | CollectionID(_) | IndexID(_) | KeyID(_) =>
        rowTombstones.add((parentScope, id))

      case _ => ()
    }

  def handleVersion(version: Version): Unit = {
    version.id match {
      case DatabaseID(dbID) =>
        // Treat disabled databases as though they are
        // deleted. Functionally, they are not live and therefore
        // aren't usable as a restore candidate. This may happen if a
        // previous restore failed, and an operator did not clean up
        // the detritus.
        val db = if (version.isDeleted) {
          Some(
            DB(
              version.ts.validTS,
              version.parentScopeID,
              dbID,
              // NB. scope ID doesn't exist for deleted databases (although it might
              // exist in the diff, so we really shouldn't try to read it here)
              ScopeID(Long.MaxValue),
              GlobalDatabaseID(Long.MaxValue),
              name = None,
              path = None,
              isDeleted = true,
              None,
              globalIDPath = Nil
            ))
        } else if (version.data(Database.DisabledField) == Some(true)) {
          readField(version, Database.ScopeField) map { scope =>
            DB(
              version.ts.validTS,
              version.parentScopeID,
              dbID,
              scope, // NB. scope ID matters for child dbs to also get marked as deleted
              GlobalDatabaseID(Long.MaxValue),
              name = None,
              path = None,
              isDeleted = true,
              None,
              globalIDPath = Nil
            )
          }
        } else {
          readField(version, Database.ScopeField) map { scope =>
            val name = readField(version, SchemaNames.NameField) map {
              _.toString
            }
            val globalID =
              readField(version, Database.GlobalIDField) getOrElse {
                GlobalDatabaseID(scope.toLong)
              }

            DB(
              version.ts.validTS,
              version.parentScopeID,
              dbID,
              scope,
              globalID,
              name,
              // This gets filled in later, in `build()`, once we have read the
              // entire database hierarchy.
              path = None,
              isDeleted = false,
              version.data(Database.ActiveSchemaVersField).map(SchemaVersion(_)),
              // This gets filled in later, in `build()`, once we have read the
              // entire database hierarchy.
              globalIDPath = Nil
            )
          }
        }

        db foreach { db =>
          add((version.parentScopeID, dbID), db, databases)
        }

      case IndexID(idxID) =>
        // Similar to disabled databases, a hidden index should be
        // treated as deleted.
        val idx = Idx(
          version.ts.validTS,
          version.parentScopeID,
          idxID,
          version.isDeleted ||
            version.data(Index.HiddenField).getOrElse(false)
        )

        add((version.parentScopeID, idxID), idx, indexes)

      case CollectionID(clsID) =>
        val cls =
          Cls(
            version.ts.validTS,
            version.parentScopeID,
            clsID,
            version.isDeleted,
            version.data.getOpt(Collection.MinValidTimeFloorField))

        add((version.parentScopeID, clsID), cls, collections)

      case KeyID(keyID) =>
        val databaseID = readField(version, FKey.DatabaseField).flatten
        val key =
          Key(
            version.parentScopeID,
            keyID,
            databaseID,
            version.ts.validTS,
            version.isDeleted)

        add((version.parentScopeID, keyID), key, keys)

      case _ =>
        ()
    }
  }

  private def add[K, V <: SchemaObject](
    key: K,
    value: V,
    collection: ConcurrentHashMap[K, V]) = {
    collection.compute(
      key,
      (_, oldValue) => {
        if (oldValue eq null) {
          value
        } else if (value.ts > oldValue.ts) {
          value
        } else {
          oldValue
        }
      }
    )
  }

  private def readField[T](version: Version, field: Field[T]): Option[T] =
    field.read(version.data.fields) match {
      case Right(v) => Some(v)
      case Left(errs) =>
        val missing = errs exists { err =>
          err match {
            case ValueRequired(_) => true
            case _                => false
          }
        }

        // A required field is missing; try the diff.
        if (missing) {
          version.diff flatMap { d => field.read(d.fields).toOption }
        } else {
          // Accept the invalid field as missing and warn.
          getLogger.warn(s"Field $field in $version failed validation: $errs")
          None
        }
    }

  /** Calls `f` for each parent database, starting from the scope passed in.
    */
  private def forParentDBs(
    databases: Map[ScopeID, SchemaMetadata.DB],
    scope: ScopeID)(f: SchemaMetadata.DB => Unit): Unit =
    scope match {
      case ScopeID.RootID => ()
      // deleted database, just stop the recursion
      case ScopeID.MaxValue => ()
      case scope =>
        databases.get(scope) match {
          case Some(db) =>
            f(db)
            forParentDBs(databases, db.parentScopeID)(f)
          case None =>
            // Bad Things have happened.
            getLogger.error(s"Could not find parent scope for database $scope")
        }
    }

  def build(): SchemaMetadata = {
    add((ScopeID.RootID, DatabaseID.RootID), RootDB, databases)

    // Apply row tombstones.
    rowTombstones.parallelStream
      .forEach {
        case (scope, id @ DatabaseID(_)) =>
          databases.remove((scope, id.as[DatabaseID]))
        case (scope, id @ CollectionID(_)) =>
          collections.remove((scope, id.as[CollectionID]))
        case (scope, id @ IndexID(_)) => indexes.remove((scope, id.as[IndexID]))
        case (scope, id @ KeyID(_))   => keys.remove((scope, id.as[KeyID]))
        case (_, _)                   => ()
      }

    val dbsByScope0 = MMap.empty[ScopeID, DB]

    databases.asScala.foreach { case (_, db) =>
      dbsByScope0.get(db.scopeID) match {
        case Some(prev) =>
          if (!prev.isDeleted || !db.isDeleted) {
            getLogger().error(
              s"found duplicate scope ID ${db.scopeID}: prev=$prev curr=$db")
          }
        case None => dbsByScope0 += db.scopeID -> db
      }
    }
    val dbsByScope = dbsByScope0.toMap

    // Apply disabled databases to children, and fill in database paths.
    databases.replaceAll { case (_, db) =>
      // This used to be deleted=false, because we map
      // deleted dbs to ScopeID.MaxValue, inside forParentDBs()
      // the map was always returning a deleted database, causing
      // the block to be called with some deleted db.
      // With the new change, forParentDBs() will not call the
      // block for ScopeID.MaxValue dbs, so starting with false
      // would "undelete" the database.
      var deleted = db.isDeleted
      var path: Option[Seq[String]] = Some(Nil)
      var globalIDPath: Seq[GlobalDatabaseID] = Nil

      forParentDBs(dbsByScope, db.scopeID) { db =>
        (path, db.name) match {
          case (Some(p), Some(n)) => path = Some(n +: p)
          case _                  => path = None
        }

        globalIDPath = db.globalID +: globalIDPath

        if (db.isDeleted) {
          deleted = true
        }
      }

      db.copy(isDeleted = deleted, path = path, globalIDPath = globalIDPath)
    }

    databases.values.removeIf { _.isDeleted }

    // Leave this as a copy into Scala to preserve the integrity of
    // the CHM. It doesn't outlive this method anyhow.
    val databaseToScope = databases.asScala.view mapValues { _.scopeID }
    val scopes = databaseToScope.values.toSet

    def filter(
      output: JMap[KeyID, ScopeID],
      e: JMap.Entry[(ScopeID, KeyID), Key]): Unit = {
      val (scopeID, keyID) = e.getKey

      e.getValue match {
        case Key(_, _, None, _, false) if scopes.contains(scopeID) =>
          output.put(keyID, scopeID)

        case Key(_, _, Some(dbID), _, false)
            if databaseToScope.contains((scopeID, dbID)) =>
          output.put(keyID, databaseToScope((scopeID, dbID)))

        // It is possible for a Key providing access to a scope to
        // exist, but the database defining that scope no longer
        // does. Ignore it.
        case _ => ()
      }
    }

    // This just helps type inference along a bit.
    def merge(a: JMap[KeyID, ScopeID], b: JMap[KeyID, ScopeID]) = a.putAll(b)

    val ks = keys.entrySet.parallelStream
      .collect(
        { () =>
          // This upcast is necessary because Java is invariant on JMap
          // here, but CHM <: JMap so it's safe.
          (new ConcurrentHashMap[KeyID, ScopeID]).asInstanceOf[JMap[KeyID, ScopeID]]
        },
        filter(_, _),
        merge(_, _)
      )

    SchemaMetadata(databases, collections, indexes, ks)
  }
}

case class SchemaMetadata(
  databases: JMap[(ScopeID, DatabaseID), SchemaMetadata.DB],
  collections: JMap[(ScopeID, CollectionID), SchemaMetadata.Cls],
  indexes: JMap[(ScopeID, IndexID), SchemaMetadata.Idx],
  keys: JMap[KeyID, ScopeID]) {

  private[this] val databasesByScope = {
    val seen = MSet.empty[ScopeID]

    databases.asScala map { case (_, db) =>
      if (seen.contains(db.scopeID)) {
        println(s"More than one live database for scope ${db.scopeID}.")
      } else {
        seen += db.scopeID
      }

      db.scopeID -> db
    }
  }

  /** Returns a live DB object which defines the given scope - i.e. its
    * children have this scope as their parent scope - if any.
    */
  def databaseForScope(scope: ScopeID): Option[SchemaMetadata.DB] =
    databasesByScope.get(scope)

  /** Returns false if a live database defines the provided scope, true
    * otherwise.
    */
  def isDatabaseDeleted(scope: ScopeID): Boolean =
    databaseForScope(scope).isEmpty

  def isCollectionDeleted(key: (ScopeID, CollectionID)): Boolean = {
    val (scopeID, clsID) = key

    if (isDatabaseDeleted(scopeID)) {
      return true
    }

    val activeSchemaVers = databaseForScope(scopeID).flatMap {
      _.activeSchemaVersion
    }

    clsID match {
      case UserCollectionID(_) =>
        // A collection deleted if the following are true:
        //   1. The latest version is deleted.
        //   2. The database's active schema version timestamp is at or
        //      after the latest timestamp for the collection.
        Option(collections.get(key)) forall { coll =>
          coll.isDeleted && activeSchemaVers.forall { _.ts >= coll.ts }
        }

      case NativeCollectionID(_) =>
        false

      case _ =>
        true
    }
  }

  def isIndexDeleted(key: (ScopeID, IndexID)): Boolean = {
    val (scopeID, idxID) = key

    if (isDatabaseDeleted(scopeID)) {
      return true
    }

    idxID match {
      case UserIndexID(_) =>
        // NB: If an FQL2 index is staged for deletion, the backing index is not
        // deleted.
        //     No extra check is required to handle staged deletes.
        Option(indexes.get(key)) forall { _.isDeleted }

      case NativeIndexID(_) =>
        false

      case _ =>
        true
    }
  }

  def toMVTMap: Map[ScopeID, MVTMap] = {
    val entries = collections.entrySet

    val mvts = MMap.empty[ScopeID, HashMap[CollectionID, Timestamp]]

    entries forEach { entry =>
      val (scope, coll) = entry.getKey()
      val cls = entry.getValue()

      cls.minValidTimeFloor foreach { mvt =>
        mvts.get(scope) match {
          case None           => mvts += scope -> HashMap(coll -> mvt)
          case Some(existing) => existing += coll -> mvt
        }
      }
    }

    mvts.view.mapValues { m => MVTMap.Mapping(m.toMap) } toMap
  }
}

object SchemaMetadata {
  val empty = SchemaMetadata(
    Collections.emptyMap,
    Collections.emptyMap,
    Collections.emptyMap,
    Collections.emptyMap)

  sealed trait SchemaObject {
    val ts: Timestamp
    val parentScopeID: ScopeID
    val id: ID[_]
    val isDeleted: Boolean
  }

  case class DB(
    ts: Timestamp,
    parentScopeID: ScopeID,
    id: DatabaseID,
    scopeID: ScopeID,
    globalID: GlobalDatabaseID,
    name: Option[String],
    path: Option[Seq[String]],
    isDeleted: Boolean,
    activeSchemaVersion: Option[SchemaVersion],
    /** This was added so that we can standardize our reporting on the global id path.
      */
    globalIDPath: Seq[GlobalDatabaseID]
  ) extends SchemaObject

  val RootDB = DB(
    Timestamp.MaxMicros,
    ScopeID.MaxValue,
    DatabaseID.RootID,
    ScopeID.RootID,
    GlobalDatabaseID.MinValue,
    Some("root"),
    Some(Nil),
    isDeleted = false,
    None,
    // we put Nil for root global id path here because our other global id paths
    // don't include the root db
    Nil
  )

  case class Cls(
    ts: Timestamp,
    parentScopeID: ScopeID,
    id: CollectionID,
    isDeleted: Boolean,
    minValidTimeFloor: Option[Timestamp])
      extends SchemaObject

  case class Idx(
    ts: Timestamp,
    parentScopeID: ScopeID,
    id: IndexID,
    isDeleted: Boolean)
      extends SchemaObject

  case class Key(
    parentScopeID: ScopeID,
    id: KeyID,
    database: Option[DatabaseID],
    ts: Timestamp,
    isDeleted: Boolean)
      extends SchemaObject

  def writeFile(schemaMetadata: SchemaMetadata, file: File): Try[Unit] = {
    val executor = new Executor("Metadata-Writer", 4)

    val databases = File.createTempFile("databases-", ".meta", file.getParentFile)
    databases.deleteOnExit()

    val collections =
      File.createTempFile("collections-", ".meta", file.getParentFile)
    collections.deleteOnExit()

    val indexes = File.createTempFile("indexes-", ".meta", file.getParentFile)
    indexes.deleteOnExit()

    val keys = File.createTempFile("keys-", ".meta", file.getParentFile)
    keys.deleteOnExit()

    def writeln(stream: FileOutputStream, js: JSValue): Unit = {
      js.writeTo(stream, pretty = false)
      stream.write('\n')
    }

    executor addWorker { () =>
      Using(new FileOutputStream(databases)) { stream =>
        schemaMetadata.databases.values forEach { db =>
          writeln(
            stream,
            JSObject(
              "type" -> "database",
              "ts" -> db.ts.toString,
              "parent_scope_id" -> db.parentScopeID.toLong,
              "scope_id" -> db.scopeID.toLong,
              "database_id" -> db.id.toLong,
              "global_id" -> db.globalID.toLong,
              "name" -> db.name,
              "is_deleted" -> db.isDeleted,
              "schema_version" -> (db.activeSchemaVersion match {
                case Some(v) => v.toMicros
                case None    => JSNull
              }),
              "path" -> (db.path match {
                case Some(path) => path
                case None       => JSNull
              }),
              "global_id_path" -> db.globalIDPath.map(_.toLong)
            )
          )
        }
      }
    }

    executor addWorker { () =>
      Using(new FileOutputStream(collections)) { stream =>
        schemaMetadata.collections.values forEach {
          case Cls(ts, parentScopeID, clsID, isDeleted, mvt) =>
            val minValidTime = mvt map { v => JSString(v.toString) }
            writeln(
              stream,
              JSObject(
                "type" -> "collection",
                "ts" -> ts.toString,
                "parent_scope_id" -> parentScopeID.toLong,
                "collection_id" -> clsID.toLong,
                "is_deleted" -> isDeleted,
                "min_valid_time" -> minValidTime.getOrElse(JSNull)
              )
            )
        }
      }
    }

    executor addWorker { () =>
      Using(new FileOutputStream(indexes)) { stream =>
        schemaMetadata.indexes.values forEach {
          case Idx(ts, parentScopeID, idxID, isDeleted) =>
            writeln(
              stream,
              JSObject(
                "type" -> "index",
                "ts" -> ts.toString,
                "parent_scope_id" -> parentScopeID.toLong,
                "index_id" -> idxID.toLong,
                "is_deleted" -> isDeleted
              ))
        }
      }
    }

    executor addWorker { () =>
      Using(new FileOutputStream(keys)) { stream =>
        schemaMetadata.keys.entrySet forEach { entry =>
          writeln(
            stream,
            JSObject(
              "type" -> "key",
              "key_id" -> entry.getKey.toLong,
              "scope_id" -> entry.getValue.toLong
            ))
        }
      }
    }

    executor.waitWorkers()

    Using(new FileOutputStream(file)) { stream =>
      Seq(databases, collections, indexes, keys) foreach { component =>
        Files.lines(component.toPath) forEach { line =>
          stream.write(line.getBytes)
          stream.write('\n')
        }
      }
    }
  }

  def readFile(file: File): Try[SchemaMetadata] = {
    Using(Source.fromFile(file)) { source =>
      val databases = Map.newBuilder[(ScopeID, DatabaseID), DB]
      val collections = Map.newBuilder[(ScopeID, CollectionID), Cls]
      val indexes = Map.newBuilder[(ScopeID, IndexID), Idx]
      val keys = Map.newBuilder[KeyID, ScopeID]

      source.getLines() foreach { line =>
        val entry = JSON.parse[JSValue](line.getBytes)

        (entry / "type").as[String] match {
          case "database" =>
            val db = DB(
              Timestamp.parseInstant((entry / "ts").as[String]),
              ScopeID((entry / "parent_scope_id").as[Long]),
              DatabaseID((entry / "database_id").as[Long]),
              ScopeID((entry / "scope_id").as[Long]),
              GlobalDatabaseID((entry / "global_id").as[Long]),
              (entry / "name").asOpt[String],
              (entry / "path").asOpt[Seq[String]],
              (entry / "is_deleted").as[Boolean],
              (entry / "schema_version").asOpt[Long].map(SchemaVersion(_)),
              (entry / "global_id_path").as[Seq[Long]].map(GlobalDatabaseID(_))
            )

            databases += ((db.parentScopeID, db.id) -> db)
          case "collection" =>
            val mvt = (entry / "min_valid_time").asOpt[String]

            val cls = Cls(
              Timestamp.parseInstant((entry / "ts").as[String]),
              ScopeID((entry / "parent_scope_id").as[Long]),
              CollectionID((entry / "collection_id").as[Long]),
              (entry / "is_deleted").as[Boolean],
              mvt map { Timestamp.parseInstant(_) }
            )

            collections += ((cls.parentScopeID, cls.id) -> cls)
          case "index" =>
            val idx = Idx(
              Timestamp.parseInstant((entry / "ts").as[String]),
              ScopeID((entry / "parent_scope_id").as[Long]),
              IndexID((entry / "index_id").as[Long]),
              (entry / "is_deleted").as[Boolean]
            )

            indexes += ((idx.parentScopeID, idx.id) -> idx)
          case "key" =>
            val keyID = KeyID((entry / "key_id").as[Long])
            val scopeID = ScopeID((entry / "scope_id").as[Long])

            keys += (keyID -> scopeID)
        }
      }

      SchemaMetadata(
        databases.result().asJava,
        collections.result().asJava,
        indexes.result().asJava,
        keys.result().asJava)
    }
  }
}

class SizeMetadata {
  private[this] val versionsSizes =
    new ConcurrentHashMap[(ScopeID, CollectionID), LongAdder]()

  private[this] val indexesSizes =
    new ConcurrentHashMap[(ScopeID, IndexID), LongAdder]()

  def incrVersionsSize(scopeID: ScopeID, docID: DocID, size: Long): Unit = {
    val collID = docID match {
      case CollectionID(collID) => collID
      case _                    => docID.collID
    }

    versionsSizes.computeIfAbsent((scopeID, collID), _ => new LongAdder).add(size)
  }

  def incrIndexesSize(scopeID: ScopeID, id: IndexID, size: Long): Unit = {
    indexesSizes.computeIfAbsent((scopeID, id), _ => new LongAdder).add(size)
  }

  def calculateSize(schemaMetadata: SchemaMetadata): Map[ScopeID, Long] = {
    val sizes = MMap.empty[ScopeID, Long].withDefaultValue(0L)

    versionsSizes.forEach((key, adder) => {
      val (scopeID, _) = key

      if (!schemaMetadata.isCollectionDeleted(key)) {
        sizes(scopeID) += adder.sum()
      }
    })

    indexesSizes.forEach((key, adder) => {
      val (scopeID, indexID) = key

      // TODO: move changes by collection into a separate field in the output file.
      if (
        !schemaMetadata.isIndexDeleted(key) &&
        indexID != NativeIndexID.ChangesByCollection.id
      ) {
        sizes(scopeID) += adder.sum()
      }
    })

    sizes.toMap
  }
}
