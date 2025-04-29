package fauna.storage.api.version

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.storage._
import fauna.storage.doc.{ Data, Diff, Field, InvalidType }
import fauna.storage.ir._
import io.netty.buffer.{ ByteBuf, Unpooled }

/** A StorageVersion is a document at a particular valid time.
  * The data and diff are lazily decoded, which saves CPU cycles, especially
  * on the storage node, which often does nothing with a version other than
  * shove it across the wire to a coordinator.
  *
  * Storage versions represent the data as it was stored, including
  * the schema version at the time it was stored, unlike repo versions,
  * which are always migrated to the current schema version.
  *
  * The owner of a storage version is responsible for managing the lifetimes
  * of its buffers. In practice, the lifetime on the datanode is contained in
  * the lifetime of the backing buffers read from Cassandra, while on the
  * coordinator the buffers are copied.
  *
  * Back in ye olden days, the ttl was stored on `data`. After migrating the
  * cell format, the ttl is now stored directly in the cell tuple. So, the ttl
  * is a field nullable on this class, so that it may be set on construction
  * if it's in the cell, or lazily determined by reading `data`.
  */
final case class StorageVersion private (
  scopeID: ScopeID,
  docID: DocID,
  ts: BiTimestamp,
  action: DocAction,
  // Often at the end of argument lists, but must stay here or it'll cause
  // old/new codec conflicts.
  isChange: Boolean,
  schemaVersion: SchemaVersion,
  // The following three fields are all calculated lazily, possibly by
  // multiple threads. However, they are only written once, and all threads
  // must write the same value, so they are not marked volatile.
  // _ttl is null or Option[Timestamp]; null means not yet calculated.
  var _ttl: AnyRef,
  // _data is a not-yet-decoded ByteBuf or a decoded MapV.
  var _data: AnyRef,
  // _diff is a not-yet-decoded ByteBuf or a decoded MapV.
  var _diff: AnyRef)
    extends Mutation
    with ExceptionLogging {
  import StorageVersion._

  def isLive = action != Delete
  def versionID = VersionID(ts.validTS, action)

  def ttl: Option[Timestamp] = action match {
    case Delete => None
    case _ =>
      if (_ttl eq null) {
        _ttl = decodeTTL(scopeID, docID, _data)
      }
      _ttl.asInstanceOf[Option[Timestamp]]
  }

  // See the TTL class.
  private def versionTTL =
    if (_ttl eq null) {
      TTL.Unknown
    } else {
      TTL.Known(_ttl.asInstanceOf[Option[Timestamp]])
    }

  def diff: Option[Diff] =
    (_diff: @unchecked) match {
      case b: ByteBuf =>
        val decoded = CBOR.parse[IRValue](b) match {
          case v: MapV => Some(Diff(v))
          case NullV   => None
          case v => throw new AssertionError(s"Unexpected version diff value $v")
        }
        _diff = decoded
        decoded

      case o: Option[_] => o.asInstanceOf[Option[Diff]]
    }

  // Deleted versions have empty data by definition.
  def data: Data =
    (_data: @unchecked) match {
      case m: MapV => Data(m)
      case b: ByteBuf =>
        CBOR.parse[IRValue](b) match {
          case NullV =>
            _data = MapV.empty
            Data.empty
          case mapv: MapV =>
            _data = mapv
            Data(mapv)
          case bad =>
            throw new IllegalStateException(s"unexpected version data: $bad")
        }
    }
}

object StorageVersion extends ExceptionLogging {

  // TODO: This is duplicated in repo Version.
  val TTLField = Field[Option[Timestamp]]("ttl")

  // We definitely were no longer using changeIDs after this date.
  private val ChangeEpoch = Timestamp.parse("2019-06-01T00:00:00Z")

  /** Get the TTL from _data, partially decoding if necessary.
    * TODO: Can this be a method on version?
    */
  def decodeTTL(scope: ScopeID, doc: DocID, _data: AnyRef) =
    try {
      (_data: @unchecked) match {
        case m: MapV => Data(m).getOrElse(TTLField, None)
        case dataBuf: ByteBuf =>
          CBORParser(dataBuf.duplicate())
            .read(new PartialIRCodec(TTLField.path)) map { ir =>
            ir.asInstanceOf[TimeV].value
          }
      }
    } catch {
      case ex: InvalidType =>
        logException(ex.copy(context = Some((scope, doc))))
        None
    }

  // This keeps the version undecoded when sending it over the wire.
  // It's not meant to be stored.
  private sealed trait TTL

  private object TTL {
    implicit val codec = CBOR.SumCodec[TTL](
      CBOR.SingletonCodec(Unknown),
      CBOR.AliasCodec[Known, Option[Timestamp]](Known.apply, _.ts)
    )
    // The TTL was not in the cell tuple and must be decoded from data.
    case object Unknown extends TTL
    // The TTL was in the cell tuple.
    case class Known(ts: Option[Timestamp]) extends TTL
  }

  /** Constructs a new StorageVersion given `data` and `diff` buffers directly from the cell.
    *
    * `ttl` is a nullable argument. If it is set to null, the ttl will be parsed from `data`.
    */
  def newByteBuf(
    scopeID: ScopeID,
    docID: DocID,
    ts: BiTimestamp,
    action: DocAction,
    schemaVersion: SchemaVersion,
    ttl: Option[Timestamp],
    dataBuf: ByteBuf,
    diffBuf: ByteBuf,
    isChange: Boolean = false) =
    new StorageVersion(
      scopeID,
      docID,
      ts,
      action,
      isChange,
      schemaVersion,
      ttl,
      dataBuf,
      diffBuf)

  // Constructs a version from previously-decoded info.
  private[version] def fromDecoded(
    scopeID: ScopeID,
    docID: DocID,
    ts: BiTimestamp,
    action: DocAction,
    schemaVersion: SchemaVersion,
    ttl: Option[Timestamp],
    data: Data,
    diff: Option[Diff],
    isChange: Boolean = false) =
    new StorageVersion(
      scopeID,
      docID,
      ts,
      action,
      isChange,
      schemaVersion,
      ttl,
      data.fields,
      diff)

  // This is a bridge codec for raw versions on the wire.
  implicit val Codec = new CBOR.PartialCodec[StorageVersion](TypeLabels.ArrayLabel) {
    override def encode(stream: CBOR.Out, v: StorageVersion): CBOR.Out = {
      (v._data: @unchecked, v._diff: @unchecked) match {
        case (dataBuf: ByteBuf, diffBuf: ByteBuf) =>
          stream.writeArrayStart(9)
          CBOR.encode(stream, v.scopeID)
          CBOR.encode(stream, v.docID)
          CBOR.encode(stream, v.ts)
          CBOR.encode(stream, v.action)
          CBOR.encode(stream, v.schemaVersion)
          CBOR.encode(stream, v.versionTTL)
          CBOR.encode(stream, v.isChange)
          CBOR.encode(stream, dataBuf)
          CBOR.encode(stream, diffBuf)
        case _ =>
          throw new IllegalStateException(
            "Attempted to encode a StorageVersion, expected both _data " +
              s"and _diff to be ByteBufs, received data=${v._data}, diff=${v._diff}." +
              s"  ScopeID: ${v.scopeID} DocID: ${v.docID}"
          )
      }
    }
    override def readArrayStart(length: Long, stream: CBORParser): StorageVersion = {
      require(length == 7 || length == 9, s"expected 7 or 9 fields, got $length")
      if (length == 7) {
        // Old encoding without schema version or ttl.
        val scope = CBOR.decode[ScopeID](stream)
        val doc = CBOR.decode[DocID](stream)
        val ts = CBOR.decode[BiTimestamp](stream)
        val action = CBOR.decode[DocAction](stream)
        val isChange = CBOR.decode[Boolean](stream)
        val data = CBOR.decode[ByteBuf](stream)
        val diff = CBOR.decode[ByteBuf](stream)
        StorageVersion.newByteBuf(
          scope,
          doc,
          ts,
          action,
          SchemaVersion.Min,
          null,
          data,
          diff,
          isChange
        )
      } else {
        // New encoding with schema version and ttl.
        val scope = CBOR.decode[ScopeID](stream)
        val doc = CBOR.decode[DocID](stream)
        val ts = CBOR.decode[BiTimestamp](stream)
        val action = CBOR.decode[DocAction](stream)
        val schemaVersion = CBOR.decode[SchemaVersion](stream)
        val ttl = CBOR.decode[StorageVersion.TTL](stream)
        val isChange = CBOR.decode[Boolean](stream)
        val data = CBOR.decode[ByteBuf](stream)
        val diff = CBOR.decode[ByteBuf](stream)
        StorageVersion.newByteBuf(
          scope,
          doc,
          ts,
          action,
          schemaVersion,
          ttl match {
            case StorageVersion.TTL.Unknown    => null
            case StorageVersion.TTL.Known(ttl) => ttl
          },
          data,
          diff,
          isChange
        )
      }
    }
  }

  val EmptyDiff =
    CBOR.encode(NullV).asReadOnly()

  // Build a storage version from a Versions CF table value.
  def fromValue(v: Value[Tables.Versions.Key]): StorageVersion = {
    val (pkey, validTS, _, txnTS) = v.key
    val (scope, id) = Tables.Versions.decodeRowKey(Unpooled.wrappedBuffer(pkey))

    val ts = BiTimestamp.decode(validTS, txnTS)
    val isChange = txnTS != v.transactionTS && v.transactionTS < ChangeEpoch

    val dataBuf = v.data.duplicate

    // The schema version, TTL, data, and diff are encoded in a CBOR array.
    //   New encoding:
    //     [array length = 4] [schema version] [TTL] [data] [diff]
    //   Old encoding:
    //     [array length = 2] [data] [diff]
    val a = CBORParser(dataBuf).read(CBORParser.ArrayStartSwitch)
    if (a != 2 && a != 4) {
      throw new AssertionError("Version value was not size 2 or 4")
    }

    var schemaVersion = SchemaVersion.Min
    var ttl: StorageVersion.TTL = StorageVersion.TTL.Unknown
    if (a == 4) {
      schemaVersion = CBOR.decode[SchemaVersion](dataBuf)
      ttl = StorageVersion.TTL.Known(CBOR.decode[Option[Timestamp]](dataBuf))
    }

    // At this point the index of `dataBuf` points to the start of data.
    // Duplicate it so we can skip the data and find the diff.
    // The version is a Delete if and only if the data is nil.
    val diffBuf = dataBuf.duplicate
    val parser = CBORParser(diffBuf)
    val action = parser.currentTypeByte match {
      case InitialByte.Nil =>
        parser.skip()
        Delete
      case _ =>
        parser.skip()
        // The non-Delete version is a Create if and only if the diff is nil.
        if (parser.currentTypeByte == InitialByte.Nil) Create else Update
    }

    StorageVersion.newByteBuf(
      scope,
      id,
      ts,
      action,
      schemaVersion,
      ttl match {
        case StorageVersion.TTL.Unknown      => null
        case StorageVersion.TTL.Known(tsOpt) => tsOpt
      },
      dataBuf,
      diffBuf,
      isChange
    )
  }
}
