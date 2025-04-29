package fauna.repo.doc

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.repo.schema.migration.MigrationList
import fauna.storage._
import fauna.storage.api.version.StorageVersion
import fauna.storage.doc._
import fauna.storage.ir._
import io.netty.buffer.ByteBuf
import java.lang.{ Long => JLong }

object Version extends ExceptionLogging {

  implicit val ordering: Ordering[Version] = { (a, b) =>
    def compare0(c: Int, next: => Int) =
      if (c == 0) {
        next
      } else {
        c
      }

    compare0(
      JLong.compare(a.parentScopeID.toLong, b.parentScopeID.toLong),
      compare0(a.id compare b.id, a.versionID compare b.versionID))
  }

  // an Equiv specifically to resolve conflicts between Versions -
  // do not let this escape!
  private[this] val conflictEquiv: Equiv[Version] = { (a, b) =>
    a.parentScopeID.toLong == b.parentScopeID.toLong &&
    a.id == b.id &&
    a.ts.validTS == b.ts.validTS
  }

  def resolveConflicts(versions: Iterable[Version]): Seq[Conflict[Version]] =
    Conflict.resolve(versions)(conflictEquiv)

  // TODO: If we remove TTL from data before storing it, this should
  //            be private (or private[storage]).
  val TTLField = Field[Option[Timestamp]]("ttl")
  val tsPath = List("ts")

  def decodeCell(key: ByteBuf, cell: Cell): Version =
    decodeCell(
      Tables.decodeValue[Tables.Versions.Key](Tables.Versions.Schema, key, cell))

  def decodeCell(v: Value[Tables.Versions.Key]): Version =
    decode(StorageVersion.fromValue(v))

  // TODO: This does the same thing as `fromStorage`, but without migrations.
  // It should be removed once everything migrations as expected.
  def decode(v: StorageVersion): Version =
    v.action match {
      case Create | Update =>
        Live(
          v.scopeID,
          v.docID,
          v.ts,
          v.action,
          v.schemaVersion,
          v.isChange,
          () => v.ttl,
          () => v.data,
          _ => v.diff)
      case Delete =>
        Deleted(v.scopeID, v.docID, v.ts, v.schemaVersion, v.isChange, () => v.diff)
    }

  def fromStorage(v: StorageVersion, migrations: MigrationList) = {
    v.action match {
      case Create | Update =>
        Live(
          v.scopeID,
          v.docID,
          v.ts,
          v.action,
          v.schemaVersion,
          v.isChange,
          () => v.ttl,
          () => migrations.migrate(v.data, v.schemaVersion),
          live =>
            v.diff.map { diff =>
              val prev = v.data patch diff
              val migratedPrev = migrations.migrate(prev, v.schemaVersion)

              // Use the `live.data` here, to get the correct diff. Note that this
              // may go and migrate that as well.
              live.data diffTo migratedPrev
            }
        )

      case Delete =>
        Deleted(
          v.scopeID,
          v.docID,
          v.ts,
          v.schemaVersion,
          v.isChange,
          () => {
            v.diff.map { diff =>
              // This is the same as the live logic, but there's no data in a deleted
              // version.
              val prev = Data.empty patch diff
              val migratedPrev = migrations.migrate(prev, v.schemaVersion)
              Data.empty diffTo migratedPrev
            }
          }
        )
    }
  }

  object Live {
    def apply(scope: ScopeID, id: DocID, schemaVersion: SchemaVersion, data: Data) =
      new Live(
        scope,
        id,
        Unresolved,
        Create,
        schemaVersion,
        false,
        () => data.getOrElse(Version.TTLField, None),
        () => data,
        _ => None)

    def apply(
      scope: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      action: DocAction,
      schemaVersion: SchemaVersion,
      data: Data) =
      new Live(
        scope,
        id,
        ts,
        action,
        schemaVersion,
        false,
        () => data.getOrElse(Version.TTLField, None),
        () => data,
        _ => None)

    def apply(
      scope: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      action: DocAction,
      schemaVersion: SchemaVersion,
      data: Data,
      diff: Option[Diff]) =
      new Live(
        scope,
        id,
        ts,
        action,
        schemaVersion,
        false,
        () => data.getOrElse(Version.TTLField, None),
        () => data,
        _ => diff)

    def apply(
      scope: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      action: DocAction,
      schemaVersion: SchemaVersion,
      isChange: Boolean,
      data: Data,
      diff: Option[Diff]) =
      new Live(
        scope,
        id,
        ts,
        action,
        schemaVersion,
        isChange,
        () => data.getOrElse(Version.TTLField, None),
        () => data,
        _ => diff)

    def apply(
      scope: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      action: DocAction,
      schemaVersion: SchemaVersion,
      isChange: Boolean,
      ttl: () => Option[Timestamp],
      data: () => Data,
      diff: Version.Live => Option[Diff]) =
      new Live(scope, id, ts, action, schemaVersion, isChange, ttl, data, diff)
  }

  /** A document is live when it exists at a particular valid time. A
    * live version represents either a newly-created document (it was
    * deleted at valid time - 1), or an updated document (it exists at
    * valid time - 1).
    *
    * The decoding thunks, `decodeData`, `decodeDiff`, etc stored on
    * this class are for lazy decoding. They close over the storage
    * version used to build this live version, and are called when
    * looking up the `data`, `diff`, or `ttl` fields. These functions
    * also perform migrations on read after decoding the storage version.
    */
  final class Live private (
    val parentScopeID: ScopeID,
    val id: DocID,
    val ts: BiTimestamp,
    val action: DocAction,
    val schemaVersion: SchemaVersion,
    val isChange: Boolean,
    val decodeTTL: () => Option[Timestamp],
    val decodeData: () => Data,
    val decodeDiff: Live => Option[Diff])
      extends Version {

    def event = {
      val d = diff match {
        case Some(d) => data.inverse(d).fields
        case None    => data.fields
      }

      DocEvent(ts, parentScopeID, id, action, d)
    }

    def isDeleted = false

    var _ttl: Option[Timestamp] = _
    def ttl = {
      if (_ttl == null) {
        _ttl = decodeTTL()
      }
      _ttl
    }

    def withData(data: Data) =
      new Live(
        parentScopeID,
        id,
        ts,
        action,
        schemaVersion,
        isChange,
        decodeTTL,
        () => data,
        decodeDiff)

    def withDiff(diff: Option[Diff]) =
      new Live(
        parentScopeID,
        id,
        ts,
        action,
        schemaVersion,
        isChange,
        decodeTTL,
        decodeData,
        _ => diff)

    def withTS(ts: BiTimestamp) =
      new Live(
        parentScopeID,
        id,
        ts,
        action,
        schemaVersion,
        false,
        decodeTTL,
        decodeData,
        decodeDiff)

    def withUnresolvedTS = withTS(ts.unresolve)

    // Allows for data races during concurrent access. The assumption is
    // that avoiding the memory barrier cost is worth the tradeoff of
    // potentially deserializing more than once.
    // As seen in: https://github.com/runarorama/latr
    var _data: MapV = _
    def data = {
      if (_data == null) {
        _data = decodeData().fields
      }
      Data(_data)
    }

    def dataStream: DataStream = IRValueDataStream(data.fields)

    var _diff: Option[Diff] = _
    def diff = {
      if (_diff == null) {
        _diff = decodeDiff(this)
      }
      _diff
    }

    override def equals(o: Any) = o match {
      // Historical note: comparing change IDs with transaction
      // timestamps is non-sensical; in such cases compare only valid
      // timestamps
      case o: Live if isChange || o.isChange =>
        parentScopeID == o.parentScopeID && id == o.id && ts.validTSOpt == o.ts.validTSOpt && data == o.data
      case o: Live =>
        parentScopeID == o.parentScopeID && id == o.id && ts == o.ts && data == o.data
      case _ => false
    }

    override def hashCode =
      5827 * parentScopeID.hashCode * id.hashCode * ts.validTSOpt.hashCode

    override def toString =
      s"Version($subID, $collID, $parentScopeID, $ts, data: $data)"
  }

  object Deleted {
    val EmptyData = MapV("data" -> NullV)

    def apply(
      scopeID: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      schemaVersion: SchemaVersion): Deleted =
      new Deleted(scopeID, id, ts, schemaVersion, false, () => None)

    def apply(
      scopeID: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      schemaVersion: SchemaVersion,
      diff: Option[Diff]): Deleted =
      new Deleted(scopeID, id, ts, schemaVersion, false, () => diff)

    def apply(
      scopeID: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      schemaVersion: SchemaVersion,
      isChange: Boolean,
      diff: Option[Diff]): Deleted =
      new Deleted(scopeID, id, ts, schemaVersion, isChange, () => diff)

    def apply(
      scopeID: ScopeID,
      id: DocID,
      ts: BiTimestamp,
      schemaVersion: SchemaVersion,
      isChange: Boolean,
      diff: () => Option[Diff]): Deleted =
      new Deleted(scopeID, id, ts, schemaVersion, isChange, diff)
  }

  /** A document is deleted when it does not exist at a valid time. All
    * documents are deleted at the epoch (i.e. the beginning of time) in
    * valid time, and become live as they are created. They may be
    * deleted or (re-)created many times throughout their history,
    * creating discrete periods of valid time when they are either live
    * or deleted.
    */
  final class Deleted private (
    val parentScopeID: ScopeID,
    val id: DocID,
    val ts: BiTimestamp,
    val schemaVersion: SchemaVersion,
    val isChange: Boolean,
    val decodeDiff: () => Option[Diff])
      extends Version {

    val ttl = None

    def isDeleted = true
    def action = Delete

    def event = DocEvent(ts, parentScopeID, id, Delete, Deleted.EmptyData)

    def data = Data.empty
    def dataStream = DataStream.empty

    var _diff: Option[Diff] = _
    def diff = {
      if (_diff == null) {
        _diff = decodeDiff()
      }
      _diff
    }

    def withDiff(d: Option[Diff]) =
      new Deleted(parentScopeID, id, ts, schemaVersion, isChange, () => d)

    def withUnresolvedTS =
      new Deleted(parentScopeID, id, ts.unresolve, schemaVersion, false, decodeDiff)

    override def equals(o: Any) = o match {
      // Historical note: comparing change IDs with transaction
      // timestamps is non-sensical; in such cases compare only valid
      // timestamps
      case o: Deleted if isChange || o.isChange =>
        parentScopeID == o.parentScopeID && id == o.id && ts.validTSOpt == o.ts.validTSOpt
      case o: Deleted =>
        parentScopeID == o.parentScopeID && id == o.id && ts == o.ts
      case _ => false
    }

    override def hashCode =
      4441 * parentScopeID.hashCode * id.hashCode * ts.validTSOpt.hashCode

    override def toString = s"Version($subID, $collID, $parentScopeID, $ts DELETED)"
  }
}

/** A Version is a document at a particular valid and transaction
  * time. In most situations, the two times will be equal; however, a
  * "historical" write overrides valid time, creating an amendment to
  * the document's history.
  */
sealed trait Version extends Mutation {
  val parentScopeID: ScopeID
  val id: DocID
  val ts: BiTimestamp
  def schemaVersion: SchemaVersion
  def ttl: Option[Timestamp]
  def event: DocEvent

  import Version._

  // previously, the transaction time component of cell names was a
  // snowflake id - this flag indicates that condition
  def isChange: Boolean

  def isDeleted: Boolean
  def action: DocAction
  def data: Data
  def dataStream: DataStream
  def diff: Option[Diff]

  def withDiff(diff: Option[Diff]): Version
  def withUnresolvedTS: Version

  def docID = id
  def versionID = VersionID(ts.validTS, action)
  def subID = id.subID
  def collID = id.collID

  def prevData() = diff map { data patch _ }
  def prevFields() = diff map { fields patch _ }

  // NOTE: this is different than `version.patch(version.diff)` in which this method
  // returns `None` if no diff is available while patching with a empty diff returns
  // a logical delete.
  def prevVersion = diff map { _ => patch(diff) }

  def validTSV = ts.validTSOpt match {
    case None     => TransactionTimeV.Micros
    case Some(ts) => LongV(ts.micros)
  }

  // NB: Don't forget about TTL, which is quasi-virtual
  //     because it is also stored in data (for now).
  private lazy val virtualFields =
    ("ref" -> DocIDV(docID)) ::
      ("class" -> DocIDV(collID.toDocID)) ::
      ("ts" -> validTSV) :: Nil

  def fields: Data = {
    // Force decoding of data so ttl doesn't double-decode.
    val baseFields = data.fields
    val ttlField = ttl.map { t => "ttl" -> TimeV(t) }
    Data(MapV(virtualFields ++ ttlField) merge baseFields)
  }

  def patch(diff: Option[Diff]): Version =
    diff map { d =>
      val newTS = d.fields.get(tsPath).getOrElse(validTSV) match {
        case LongV(t)            => AtValid(Timestamp.ofMicros(t))
        case TransactionTimeV(_) => Unresolved
        case _                   => ts
      }

      // Don't patch with these special fields. We do patch with TTL,
      // because for now it's a regular field in data, as well as
      // a field on the version instance.
      // TODO: Change this if TTL isn't stored in data.
      val virtualFieldsPaths = List(tsPath, List("ref"), List("class"))
      val dMinusVirtualFields = virtualFieldsPaths.foldLeft(d.fields) {
        case (d, p) => d.remove(p)
      }

      Live(
        parentScopeID,
        id,
        newTS,
        Update,
        schemaVersion,
        data.patch(Diff(dMinusVirtualFields)))
    } getOrElse {
      Deleted(parentScopeID, id, ts, schemaVersion)
    }
}
