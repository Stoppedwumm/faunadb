package fauna.repo.store

import fauna.atoms.{ DatabaseID, GlobalDatabaseID, GlobalID, GlobalKeyID, ScopeID }
import fauna.codex.json.{ JSObject, JSValue }
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.util.ZBase32
import java.io.InputStream
import scala.collection.mutable.{ ListBuffer, Map => MMap }

/** Value object that interface the Admin API and faunadb-admin.
  */
final case class DumpEntry(
  globalID: GlobalID,
  parentID: ScopeID,
  dbID: DatabaseID,
  deletedTS: Option[Timestamp]) {

  def isLive(snapshotTS: Timestamp = Timestamp.MaxMicros): Boolean =
    deletedTS forall { _ > snapshotTS }

  def toJSON: JSObject = {
    val obj = JSObject(
      "type" -> tpe,
      "id" -> id,
      "parent_id" -> parentID.toLong.toString,
      "db_id" -> dbID.toLong.toString
    )

    deletedTS match {
      case None => obj
      case Some(ts) =>
        obj :+ ("deleted_ts" -> ts.toString)
    }
  }

  private def id = globalID match {
    case GlobalDatabaseID(toLong) => ZBase32.encodeLong(toLong)
    case ScopeID(toLong)          => toLong.toString
    case GlobalKeyID(_) => throw new IllegalStateException("Keys not supported")
  }

  private def tpe = globalID match {
    case GlobalDatabaseID(_) => "global_id"
    case ScopeID(_)          => "scope_id"
    case GlobalKeyID(_)      => throw new IllegalStateException("Keys not supported")
  }
}

object DumpEntry {
  def toJSON(entries: Seq[DumpEntry]): JSObject =
    JSObject("entries" -> entries.map { _.toJSON })

  def fromJSON(js: JSValue): Seq[DumpEntry] = {
    val b = Seq.newBuilder[DumpEntry]

    val entries = (js / "entries").as[Seq[JSValue]]

    entries foreach { entry =>
      b += DumpEntry(
        (entry / "type").as[String],
        (entry / "id").as[String],
        (entry / "parent_id").as[String],
        (entry / "db_id").as[String],
        (entry / "deleted_ts").asOpt[String]
      )
    }

    b.result()
  }

  def fromStream(streams: InputStream*): Seq[DumpEntry] = {
    val b = Seq.newBuilder[DumpEntry]

    streams foreach { stream =>
      b ++= fromJSON(JSON.parse[JSValue](stream.readAllBytes()))
      stream.close()
    }

    b.result()
  }

  private def apply(
    tpe: String,
    globalID: String,
    parentID: String,
    dbID: String,
    deletedTS: Option[String]) = {
    val gID = tpe match {
      case "global_id" => GlobalDatabaseID(ZBase32.decodeLong(globalID))
      case "scope_id"  => ScopeID(globalID.toLong)
      case _           => throw new IllegalStateException(s"Type $tpe not supported")
    }

    new DumpEntry(
      gID,
      ScopeID(parentID.toLong),
      DatabaseID(dbID.toLong),
      deletedTS map { Timestamp.parse(_) })
  }
}

/** Represents a Database Hierarchy.
  */
final case class DatabaseTree(
  parentScopeID: ScopeID,
  dbID: DatabaseID,
  scopeID: ScopeID,
  globalID: GlobalDatabaseID,
  deletedTS: Option[Timestamp]) {

  private[this] val _children = ListBuffer.empty[DatabaseTree]

  def children: List[DatabaseTree] =
    _children.result()

  def addChild(child: DatabaseTree): Unit =
    _children += child

  def isEmpty: Boolean = _children.isEmpty

  def forGlobalID(gID: GlobalDatabaseID): Option[DatabaseTree] = {
    val b = Seq.newBuilder[DatabaseTree]

    def forGlobalID0(node: DatabaseTree): Unit = {
      if (node.globalID == gID) {
        b += node
      }

      node.children foreach { forGlobalID0(_) }
    }

    forGlobalID0(this)

    b.result() maxByOption { _.deletedTS.getOrElse(Timestamp.MaxMicros) }
  }

  def forScopeID(sID: ScopeID): Option[DatabaseTree] = {
    val b = Seq.newBuilder[DatabaseTree]

    def forScopeID0(node: DatabaseTree): Unit = {
      if (node.scopeID == sID) {
        b += node
      }

      node.children foreach { forScopeID0(_) }
    }

    forScopeID0(this)

    b.result() maxByOption { _.deletedTS.getOrElse(Timestamp.MaxMicros) }
  }

  def toEntries: Seq[DumpEntry] = {
    val entries = Seq.newBuilder[DumpEntry]

    def toEntries0(node: DatabaseTree): Unit = {
      entries += DumpEntry(
        node.scopeID,
        node.parentScopeID,
        node.dbID,
        node.deletedTS)
      entries += DumpEntry(
        node.globalID,
        node.parentScopeID,
        node.dbID,
        node.deletedTS)

      node.children foreach toEntries0
    }

    toEntries0(this)

    entries.result()
  }

  def toJSON: JSObject = DumpEntry.toJSON(toEntries)
}

object DatabaseTree {
  def fromJSON(json: JSValue): DatabaseTree = {
    val entries = DumpEntry.fromJSON(json)

    DatabaseTree.build(entries)
  }

  def build(
    entries: Seq[DumpEntry],
    snapshotTS: Timestamp = Timestamp.MaxMicros,
    warn: String => Unit = _ => ()): DatabaseTree = {

    checkMultipleIDsMapsToSameDatabase(entries, snapshotTS)
    checkSingleIDMapsToMultipleDatabase(entries, snapshotTS, warn)

    val nodeByParentAndDatabaseID = MMap.empty[(ScopeID, DatabaseID), DatabaseTree]

    entries foreach { result =>
      val key = (result.parentID, result.dbID)
      val node = nodeByParentAndDatabaseID.get(key)

      val newNode = result.globalID match {
        case g @ GlobalDatabaseID(_) =>
          node match {
            case Some(node) => node.copy(globalID = g)
            case None =>
              DatabaseTree(
                result.parentID,
                result.dbID,
                ScopeID.MaxValue,
                g,
                result.deletedTS)
          }

        case s @ ScopeID(_) =>
          node match {
            case Some(node) => node.copy(scopeID = s)
            case None =>
              DatabaseTree(
                result.parentID,
                result.dbID,
                s,
                GlobalDatabaseID(s.toLong),
                result.deletedTS)
          }

        case tpe =>
          throw new IllegalStateException(s"Unsupported type $tpe")
      }

      nodeByParentAndDatabaseID(key) = newNode
    }

    val nodeByScope = nodeByParentAndDatabaseID.values
      .groupBy { node =>
        node.scopeID
      }
      .view
      .mapValues { nodes =>
        nodes maxBy { _.deletedTS.getOrElse(Timestamp.MaxMicros) }
      }

    nodeByParentAndDatabaseID.values foreach { node =>
      nodeByScope.get(node.parentScopeID) match {
        case Some(parent) =>
          if (parent.scopeID != node.scopeID) {
            parent.addChild(node)
          }

        case None =>
          warn(s"Parent not found for $node")
      }
    }

    val visits = MMap.empty[DatabaseTree, Int].withDefaultValue(0)

    nodeByScope foreach { case (_, current) =>
      var node = current

      visits(node) += 1

      while (
        node.scopeID != node.parentScopeID && nodeByScope.contains(
          node.parentScopeID)
      ) {
        node = nodeByScope(node.parentScopeID)
        visits(node) += 1
      }
    }

    val maxVisits = visits.values.max
    val possibleRoots = visits filter { _._2 == maxVisits }

    if (possibleRoots.sizeIs == 1) {
      possibleRoots.head._1
    } else {
      throw new IllegalStateException("Multiple root nodes")
    }
  }

  private def checkMultipleIDsMapsToSameDatabase(
    entries: Seq[DumpEntry],
    snapshotTS: Timestamp) = {
    val groups = entries groupBy { entry =>
      val group = entry.globalID match {
        case ScopeID(_)          => 1
        case GlobalDatabaseID(_) => 2
        case GlobalKeyID(_)      => 3
      }

      (group, entry.parentID, entry.dbID, entry.isLive(snapshotTS))
    } filter { case ((_, scopeID, dbID, isLive), entries) =>
      scopeID != ScopeID.RootID && dbID != DatabaseID.RootID && isLive && entries.sizeIs > 1
    }

    if (groups.nonEmpty) {
      val msgs = groups flatMap {
        case ((1, scopeID, dbID, _), _) =>
          val scopeIDs = entries collect {
            case DumpEntry(scope @ ScopeID(_), parentID, db, _)
                if parentID == scopeID && db == dbID =>
              scope.toLong.toString
          }

          Some(
            s"There can be only one live scope id entry mapping a database, found ${scopeIDs
                .mkString("(", ", ", ")")}")

        case ((2, scopeID, dbID, _), _) =>
          val globalIDs = entries collect {
            case DumpEntry(globalID @ GlobalDatabaseID(_), parentID, db, _)
                if parentID == scopeID && db == dbID =>
              ZBase32.encodeLong(globalID.toLong)
          }

          Some(
            s"There can be only one live global id entry mapping a database, found ${globalIDs
                .mkString("(", ", ", ")")}")

        case ((3, scopeID, dbID, _), _) =>
          val keyIDs = entries collect {
            case DumpEntry(keyID @ GlobalKeyID(_), parentID, db, _)
                if parentID == scopeID && db == dbID =>
              keyID.toLong.toString
          }

          Some(
            s"GlobalKeyID is not supported, found ${keyIDs.mkString("(", ",", ")")}")

        case _ =>
          None
      }

      throw new IllegalStateException(s"${msgs mkString "\n"}")
    }
  }

  private def checkSingleIDMapsToMultipleDatabase(
    entries: Seq[DumpEntry],
    snapshotTS: Timestamp,
    warn: String => Unit) = {
    val groups = entries groupBy { entry =>
      (entry.globalID, entry.isLive(snapshotTS))
    } filter { case ((_, isLive), entries) =>
      isLive && entries.sizeIs > 1
    }

    if (groups.nonEmpty) {
      val msgs = groups map {
        case ((GlobalDatabaseID(globalID), _), entries) =>
          val databases = entries.map(entry => entry.parentID -> entry.dbID)
          s"A live global ID can only map a single database, found ${ZBase32.encodeLong(globalID)} => ${databases.mkString("[", ", ", "]")}"

        case ((ScopeID(scopeID), _), entries) =>
          val databases = entries.map(entry => entry.parentID -> entry.dbID)
          s"A live scope ID can only map a single database, found $scopeID => ${databases.mkString("[", ", ", "]")}"

        case ((GlobalKeyID(keyID), _), _) =>
          s"GlobalKeyID is not supported, found $keyID"
      }

      msgs foreach warn
    }
  }
}
