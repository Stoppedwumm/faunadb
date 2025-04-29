package fauna.model

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.{ InternalCollection, NativeIndex }
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import scala.concurrent.duration._

/** JournalEntry instances store arbitrary data associated with a node
  * in the cluster. Internally, entries are associated with a
  * Cassandra-assigned UUID. As cluster topology changes, the
  * following will occur:
  *
  * - If a node is added, a new UUID is generated and its journal will
  *   be empty.
  * - If a node is removed, its UUID will cease to exist and its
  *   journal will be orphaned. This implies that decommissioning a
  *   node must drain the associated journal.
  * - If a node is replaced, its UUID will be stolen by the new node,
  *   thereby passing the journal from the old node to the new.
  */
case class JournalEntry(version: Version) extends Document {
  def id = docID.as[JournalEntryID]
  def host: HostID = version.data(JournalEntry.HostField)
  def tag: String = version.data(JournalEntry.TagField)
  def data: Data = version.data
}

object JournalEntry {
  val TTL = 7.days
  val HostField = Field[HostID]("host")
  val TagField = Field[String]("tag")
  val TTLField = Version.TTLField // Because an entry is stored as a version.

  private val JournalEntryColl =
    InternalCollection.JournalEntry(Database.RootScopeID)

  def read(id: JournalEntryID, snapshotTS: Timestamp): Query[Option[JournalEntry]] =
    JournalEntryColl.get(id, snapshotTS) map { _.map { apply(_) } }

  def write(host: HostID, tag: String, msg: Data): Query[Option[JournalEntry]] = {
    val dataQ = Query.snapshotTime map { snapshotTS =>
      msg merge Data(
        HostField -> host,
        TagField -> tag,
        TTLField -> Some(snapshotTS + TTL))
    }

    for {
      id     <- Query.nextID map { JournalEntryID(_) }
      data   <- dataQ
      insert <- JournalEntryColl.insert(id, data, true)
    } yield Some(apply(insert))
  }

  def writeLocal(tag: String, msg: Data): Query[Option[JournalEntry]] =
    CassandraService.instance.localID match {
      case Some(id) => write(id, tag, msg)
      case None     => Query.none
    }

  def rewrite(entry: JournalEntry, msg: Data): Query[JournalEntry] = {
    val data = msg merge Data(HostField -> entry.host, TagField -> entry.tag)
    JournalEntryColl.insert(entry.id, data) map { v => apply(v) }
  }

  def remove(id: JournalEntryID): Query[Unit] =
    JournalEntryColl.get(id).flatMap {
      case Some(_) => JournalEntryColl.internalDelete(id).join
      case None    => Query.unit
    }

  /** Returns the most recent journal entry for the provided host and tag.
    */
  def latestByHostAndTag(host: HostID, tag: String): Query[Option[JournalEntry]] = {
    val terms = Vector(IndexTerm(StringV(host.toString)), IndexTerm(StringV(tag)))
    Store.uniqueIDForKey(
      NativeIndex.JournalEntryByHostAndTag(Database.RootScopeID),
      terms,
      Timestamp.MaxMicros) flatMapT { id =>
      read(id.as[JournalEntryID], Timestamp.MaxMicros)
    }
  }

  def readByHost(host: HostID): PagedQuery[Iterable[JournalEntry]] = {
    val terms = Vector(IndexTerm(StringV(host.toString)))
    Store.collectDocuments(
      NativeIndex.JournalEntryByHost(Database.RootScopeID),
      terms,
      Timestamp.MaxMicros) { (v, ts) =>
      read(v.docID.as[JournalEntryID], ts)
    }
  }

  def readByTag(
    tag: String,
    snapshotTS: Timestamp): PagedQuery[Iterable[JournalEntry]] = {
    val idx = NativeIndex.JournalEntryByTag(Database.RootScopeID)
    val terms = Vector(IndexTerm(StringV(tag)))
    Store.collectDocuments(idx, terms, snapshotTS) { (v, ts) =>
      read(v.docID.as[JournalEntryID], ts)
    }
  }
}
