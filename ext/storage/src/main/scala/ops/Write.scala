package fauna.storage.ops

import fauna.atoms._
import fauna.codex.cbor.{ CBOR, CBORParser }
import fauna.codex.cbor.CBOR.showBuffer
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.stats.QueryMetrics
import fauna.storage.{ Mutation => _, _ }
import fauna.storage.cassandra.comparators._
import fauna.storage.cassandra.ColumnFamilySchema
import fauna.storage.doc.{ Data, Diff, Field }
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.lookup._
import fauna.storage.ops.Write.MergeKey
import io.netty.buffer.{ ByteBuf, Unpooled }
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.duration._

object Write {

  val logger = getLogger()

  // 8 MiB.
  val VersionBytesThreshold = 8388608

  /** Repair/rebuild writes are non-transactional by design, and must
    * themselves ensure they do not violate snapshot isolation.
    *
    * Writing them with a C* timestamp some time in the past will
    * maintain the safety of concurrent reads by ensuring the repair
    * write is visible in transaction past if it overwrites an
    * equivalent cell.
    */
  private[ops] def RepairOffset = _RepairOffset
  // FIXME: Testing repair is impossible w/o overriding this offset or
  // being able to manipulate the underlying Cell timestamps.
  private var _RepairOffset = 1.hour
  protected def mungeOffset(off: FiniteDuration) = _RepairOffset = off

  @annotation.nowarn("cat=deprecation")
  implicit val CBORCodec = {
    implicit val duration = CBOR.AliasCodec[Duration, Long](_.micros, _.toMicros)

    // FIXME: convert these to TupleCodecs when the log handles buffer
    // retention correctly.
    CBOR.SumCodec[Write](
      CBOR.AliasCodec[
        InsertDataValueWrite,
        (String, Array[Byte], Array[Byte], Array[Byte], Option[Duration])](
        { case (cf, rk, n, v, ttl) =>
          InsertDataValueWrite(
            cf,
            Unpooled.wrappedBuffer(rk),
            Unpooled.wrappedBuffer(n),
            Unpooled.wrappedBuffer(v),
            ttl)
        },
        { case InsertDataValueWrite(cf, rk, n, v, ttl) =>
          (cf, rk.toByteArray, n.toByteArray, v.toByteArray, ttl)
        }
      ),
      CBOR.DefunctCodec(RemoveValueWrite),
      CBOR.AliasCodec[RemoveAllWrite, (String, Array[Byte])](
        { case (cf, rk) =>
          RemoveAllWrite(cf, Unpooled.wrappedBuffer(rk))
        },
        { case RemoveAllWrite(cf, rk) =>
          (cf, rk.toByteArray)
        }),
      CBOR.SingletonCodec(NoopWrite),
      VersionAdd.Codec,
      CBOR.TupleCodec[VersionRemove],
      CBOR.TupleCodec[DocRemove],
      CBOR.TupleCodec[SetAdd],
      CBOR.TupleCodec[SetRemove],
      CBOR.TupleCodec[SetRepair],
      CBOR.TupleCodec[LookupAdd],
      CBOR.TupleCodec[LookupRemove],
      CBOR.TupleCodec[LookupRepair],
      CBOR.TupleCodec[SetBackfill]
    )
  }

  final case class Stats(
    added: Int,
    removed: Int,
    cleared: Int,
    totalBytes: Int,
    ops: Int,
    limitedOps: Int)

  @inline private def toOps(bytes: Int) =
    (bytes + QueryMetrics.BytesPerWriteOp - 1) / QueryMetrics.BytesPerWriteOp

  def getStats(writes: Iterable[Write], unlimitedKeys: Set[ByteBuf]): Stats = {
    var added = 0
    var removed = 0
    var cleared = 0
    var bytes = 0
    val bytesPerDoc = MMap.empty[(ScopeID, DocID), (Int, Boolean)]
    var ops = 0
    var limitedOps = 0

    def sumBytes(scope: ScopeID, id: DocID, bytes: Int, limited: Boolean): Unit =
      bytesPerDoc.updateWith((scope, id)) {
        case None => Some((bytes, limited))
        // An unlimited write to a row exempts all write
        // ops on the row from rate limits.
        case Some((b, prevLimited)) => Some((b + bytes, limited && prevLimited))
      }

    // Little harsh getting warned for handling a deprecated case.
    @annotation.nowarn("cat=deprecation")
    def sumWrite(w: Write, limited: Boolean): Unit =
      w match {
        case w: VersionWrite =>
          sumBytes(w.scope, w.id, w.numBytes, limited)
        case w: SetWrite =>
          sumBytes(w.scope, w.doc, w.numBytes, limited)
        case w: LookupWrite =>
          sumBytes(w.scope, w.id, w.numBytes, limited)
        case w: DocRemove =>
          sumBytes(w.scope, w.id, w.numBytes, limited)

        // these ops do not generate billable write ops, and therefore
        // do not need to be summed
        case _: InsertDataValueWrite | RemoveValueWrite | _: RemoveAllWrite |
            NoopWrite =>
          ()
      }

    writes foreach { w =>
      added += w.addOps
      removed += w.removeOps
      cleared += w.clearRowOps
      bytes += w.numBytes
      sumWrite(w, limited = !unlimitedKeys.contains(w.rowKey))
    }

    bytesPerDoc.values foreach { case (bytes, limited) =>
      val docOps = toOps(bytes)
      ops += docOps
      if (limited) limitedOps += docOps
    }

    Stats(added, removed, cleared, bytes, ops, limitedOps)
  }

  object KeyTooLarge {

    def unapply(w: Write): Option[Int] = {
      w match {
        case op: SetWrite => checkBytes(op)
        case _            => None
      }
    }

    private def checkBytes(x: SetWrite): Option[Int] = {
      // Set adds and repairs write to two cfs with _only_ key data.
      val keyBytes = x.keyBytes
      if (keyBytes > IndexKey.KeyBytesThreshold) {
        Some(keyBytes)
      } else {
        None
      }
    }
  }

  object VersionTooLarge {

    def unapply(w: Write): Option[Int] = {
      w match {
        case op: VersionAdd => checkBytes(op)
        case _              => None
      }
    }

    private def checkBytes(x: VersionAdd): Option[Int] = {
      val versionBytes = x.checkBytes
      if (versionBytes > VersionBytesThreshold) {
        Some(versionBytes)
      } else {
        None
      }
    }
  }

  /** See Write.mergeKey for all things merging of Writes related.
    */
  sealed trait MergeKey

  object MergeKey {

    final case class Value(rowKey: ByteBuf, name: ByteBuf) extends MergeKey

    final case class Version(scope: ScopeID, id: DocID, writeTS: BiTimestamp)
        extends MergeKey

    final case class Set(
      scope: ScopeID,
      index: IndexID,
      terms: Vector[IRValue],
      values: Vector[IndexTerm],
      doc: DocID,
      writeTS: BiTimestamp,
      action: SetAction,
      ttl: Option[Timestamp])
        extends MergeKey {

      // Leave `action` and `ttl` out to trigger the equality check below.
      override def hashCode =
        31 * scope.hashCode * index.hashCode * terms.hashCode *
          values.hashCode * doc.hashCode * writeTS.hashCode

      // NB. TTLs are not part of the cell name (see IndexTuple and IndexValue). Set
      // may only merge if the index row has the same TTL or the same action: a
      // REMOVE followed by an ADD at the same ts is preserved if changing TTLs.
      override def equals(other: Any) =
        other match {
          case that: Set =>
            scope == that.scope && index == that.index &&
            terms == that.terms && values == that.values &&
            doc == that.doc && writeTS == that.writeTS &&
            (ttl == that.ttl || action == that.action)
          case _ => false
        }
    }

    final case class Lookup(
      globalID: GlobalID,
      scope: ScopeID,
      doc: DocID,
      writeTS: BiTimestamp)
        extends MergeKey

  }

  def merge(prev: Write, curr: Write): Write = {
    require(prev.mergeKey == curr.mergeKey, s"${prev.mergeKey} != ${curr.mergeKey}")
    require(curr.mergeKey.nonEmpty, s"${curr.mergeKey} is None")

    curr.mergeKey.get match {
      case _: MergeKey.Version =>
        (prev, curr) match {
          case (prev: VersionAdd, curr: VersionAdd) =>
            // NB. Just returning the current state would produce the wrong diff and
            // action wrt. the transition between the previous state and the new one.
            val prevState = prev.diff map { prev.data.patch(_) }
            val correctDiff = prevState map { curr.data.diffTo(_) }
            val correctAction =
              if (curr.action.isCreate) {
                correctDiff.fold(Create: DocAction)(_ => Update)
              } else {
                Delete
              }
            curr.copy(action = correctAction, diff = correctDiff)
          case (_, curr) =>
            // Otherwise, last write wins.
            curr
        }
      case _: MergeKey.Value | _: MergeKey.Set | _: MergeKey.Lookup =>
        curr
    }
  }
}

/** A Write is the concrete type of effect applied to storage after
  * the evaluation of a query expression.
  */
sealed trait Write {

  /** The row ("partition key") affected by this write effect.
    */
  def rowKey: ByteBuf

  def isRemove: Boolean = false

  def isClearRow: Boolean = false

  /** Writes to the same (non-None) mergeKey will be merged by Write.merge.
    * Primarily this involves last-write-wins semantics based on cell key/name collision.
    * In case of MergeKey.Version additionally diff and action will be recomputed (for correctness).
    *
    * In the current implementation some Writes returning a None key may in fact concern the same
    * rows/cells as some other Writes. In that case those with a None mergeKey will be applied last.
    * You are a bad person if you trigger this behaviour. (A concrete example would be combining a
    * VersionAdd with a DocRemove.)
    *
    * A potential future optimization would be to filter out Writes that don't actually result in changes.
    * This could be e.g. either deleting a non-existing item or updating a document to its previous value.
    * A test in BasicFunctions21Spec will start failing when you do this.
    */
  def mergeKey: Option[Write.MergeKey]

  /** Applies the appropriate mutations to the provided `mutation`
    * aggregator, at the given `transactionTS`.
    */
  def mutateAt(mutation: Mutation, transactionTS: Timestamp): Unit

  // Stats

  def numBytes: Int
  def addOps: Int
  def removeOps: Int
  def clearRowOps: Int

  // Query cache interaction

  /** Returns true if this write op covers a slice of the provided row
    * between columns `from` and `to`.
    */
  def isRelevant(cf: String, key: ByteBuf, from: ByteBuf, to: ByteBuf)(
    implicit ord: Ordering[ByteBuf]): Boolean

  /** Returns true if this write op covers any of the column slices
    * within the provided row.
    */
  def isRelevant(cf: String, key: ByteBuf, slices: Vector[(ByteBuf, ByteBuf)])(
    implicit ord: Ordering[ByteBuf]): Boolean

  /** Returns true if this write op falls within the provided ring
    * bounds (specified by `from` and `to`.
    * `from` is either (key, cell, excludeStart) or a location on the ring.
    * `to`   is either (key, cell)               or a location on the ring.
    * `keyOrd` should if so desired already take the token into account.
    */
  def isRelevant(
    schema: ColumnFamilySchema,
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean

  def toPendingCell(cf: String): Option[Cell] = None

  /** This method allows filtering out `Write`s which would throw `ComponentTooLargeException`
    * further down the line.
    */
  def canWrite: Boolean = true
}

/** Read-only queries - by definition - will not result in write
  * effects. A no-op write will be generated if a read-only query
  * should participate in OCC checks; this linearizes the read-only
  * query with writes.
  */
case object NoopWrite extends Write {
  val rowKey = Unpooled.EMPTY_BUFFER

  def mergeKey = None

  def isRelevant(cf: String, key: ByteBuf, from: ByteBuf, to: ByteBuf)(
    implicit ord: Ordering[ByteBuf]): Boolean =
    false

  def isRelevant(cf: String, key: ByteBuf, slices: Vector[(ByteBuf, ByteBuf)])(
    implicit ord: Ordering[ByteBuf]): Boolean =
    false

  def isRelevant(
    schema: ColumnFamilySchema,
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean =
    false

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) = ()

  def numBytes = 0
  def addOps = 0
  def removeOps = 0
  def clearRowOps = 0
}

/** A RemoveAll clears all cells from a row when applied to storage.
  */
case class RemoveAllWrite(cf: String, rowKey: ByteBuf) extends Write {

  def mergeKey = None

  def isRelevant(cf: String, key: ByteBuf, from: ByteBuf, to: ByteBuf)(
    implicit ord: Ordering[ByteBuf]): Boolean =
    this.cf == cf && key == rowKey

  def isRelevant(cf: String, key: ByteBuf, slices: Vector[(ByteBuf, ByteBuf)])(
    implicit ord: Ordering[ByteBuf]): Boolean =
    this.cf == cf && key == rowKey

  def isRelevant(
    schema: ColumnFamilySchema,
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean =
    this.cf == schema.name && contains(from, to, locate, keyOrd)

  override def isClearRow = true

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) =
    mutation.delete(cf, transactionTS)

  def numBytes = 8
  def addOps = 0
  def removeOps = 0
  def clearRowOps = 1

  override def toString =
    s"RemoveAllWrite($cf,${showBuffer(rowKey)})"

  private def contains(
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean = {

    val gtFrom = from match {
      case Right(fromLoc) =>
        fromLoc <= locate(rowKey)
      case Left((fromKey, _, _)) =>
        keyOrd.lteq(fromKey, rowKey)
    }

    def ltTo: Boolean =
      to match {
        case Right(toLoc) =>
          // a location upper bound is exclusive
          locate(rowKey) < toLoc || toLoc.isMin
        case Left((toKey, _)) =>
          // a (key, cell) upper bound is inclusive
          keyOrd.lteq(rowKey, toKey)
      }

    gtFrom && ltTo
  }
}

sealed abstract class ValueWrite extends Write {
  protected def cfs: Set[String]

  override def canWrite: Boolean = {
    try {
      rowKey
      cfs foreach { pendingCellName(_) }
      true
    } catch {
      case _: ComponentTooLargeException => false
    }
  }

  protected def pendingCellName(cf: String): ByteBuf

  def isRelevant(cf: String, key: ByteBuf, from: ByteBuf, to: ByteBuf)(
    implicit ord: Ordering[ByteBuf]): Boolean =
    cfs.contains(cf) && key == rowKey && contains(pendingCellName(cf), from, to)

  def isRelevant(cf: String, key: ByteBuf, slices: Vector[(ByteBuf, ByteBuf)])(
    implicit ord: Ordering[ByteBuf]): Boolean =
    if (cfs.contains(cf) && key == rowKey) {
      val name = pendingCellName(cf)
      slices exists { case (from, to) =>
        contains(name, from, to)
      }
    } else {
      false
    }

  def isRelevant(
    schema: ColumnFamilySchema,
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean =
    cfs.contains(schema.name) && contains(
      pendingCellName(schema.name),
      schema,
      from,
      to,
      locate,
      keyOrd)

  def addOps = if (isRemove) 0 else cfs.size
  def removeOps = if (isRemove) cfs.size else 0
  def clearRowOps = 0

  private def contains(name: ByteBuf, from: ByteBuf, to: ByteBuf)(
    implicit ord: Ordering[ByteBuf]): Boolean =
    (!from.isReadable || ord.gteq(name, from)) &&
      (!to.isReadable || ord.lteq(name, to))

  private def contains(
    name: ByteBuf,
    schema: ColumnFamilySchema,
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean = {
    val rowLoc = locate(rowKey)

    val gtFrom = from match {
      case Right(fromLoc) =>
        fromLoc <= rowLoc
      case Left((fromKey, fromCell, excludeStart)) =>
        def gtName = {
          if (excludeStart) {
            schema.nameOrdering.lt(fromCell, name)
          } else {
            schema.nameOrdering.lteq(fromCell, name)
          }
        }

        keyOrd.lt(fromKey, rowKey) || (keyOrd.equiv(fromKey, rowKey) && gtName)
    }

    def ltTo = {
      to match {
        case Right(toLoc) =>
          // a location upper bound is exclusive
          rowLoc < toLoc || toLoc.isMin
        case Left((toKey, toCell)) =>
          // a (key, cell) upper bound is inclusive
          keyOrd.lt(rowKey, toKey) || (keyOrd.equiv(
            rowKey,
            toKey) && schema.nameOrdering.lteq(name, toCell))
      }
    }

    gtFrom && ltTo
  }
}

/** An InsertDataValue adds a cell to a row when applied to storage.
  *
  * An optional TTL permits storage to remove the cell after some
  * duration beyond the transaction time of the write effect.
  */
case class InsertDataValueWrite(
  cf: String,
  rowKey: ByteBuf,
  name: ByteBuf,
  value: ByteBuf,
  ttl: Option[Duration])
    extends ValueWrite {

  def cfs = Set(cf)

  def mergeKey = Some(MergeKey.Value(rowKey, name))

  def pendingCellName(cf: String) = {
    require(cf == this.cf)
    name
  }

  override def toPendingCell(cf: String): Option[Cell] =
    if (cf == this.cf) {
      Some(Cell(name, value, BiTimestamp.UnresolvedSentinel))
    } else {
      None
    }

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) = {
    val _ttl = ttl.fold(0)(_.toSeconds.toInt)
    mutation.add(cf, name, value, transactionTS, _ttl)
  }

  def numBytes = name.readableBytes + value.readableBytes + 8

  override def toString =
    s"InsertDataValueWrite($cf,${showBuffer(rowKey)},${showBuffer(name)},${showBuffer(value)},${ttl map { _.toCoarsest }})"
}

/** A RemoveValue clears a single cell from a row when applied to storage.
  */
@deprecated("atemporal - do not use!", since = "2020-07")
case object RemoveValueWrite extends ValueWrite {
  def rowKey = Unpooled.EMPTY_BUFFER

  def cfs = Set.empty[String]
  def pendingCellName(cf: String) = Unpooled.EMPTY_BUFFER

  def mergeKey = None

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) = ()

  def numBytes = 0
}

object VersionWrite {
  val cfs = Set(Tables.Versions.CFName)
}

sealed trait VersionWrite extends ValueWrite {
  def scope: ScopeID
  def id: DocID
  def writeTS: BiTimestamp
  def action: DocAction

  protected def cfs = VersionWrite.cfs

  def mergeKey = Some(MergeKey.Version(scope, id, writeTS))

  // the logic encoding create/update/delete can be understood as
  // follows:
  //
  // sealed abstract class DocAction
  // case class Create(data: Data) extends DocAction
  // case class Update(data: Data, diff: Diff) extends DocAction
  // case object Delete extends DocAction

  lazy val rowKey = Predicate(Tables.Versions.rowKey(scope, id)).uncheckedRowKeyBytes
}

/** A VersionAdd adds a new version of a document at a valid time. The
  * new version may indicate that the version has been created,
  * updated, or deleted by its `action`.
  *
  * `transactionTS` is a placeholder value prior to this op being
  * committed to the log. Thereafter, it MUST be the log-determined
  * transaction time.
  *
  * `validTS` is equal to `transactionTS`, if it isn't specifically
  * provided.
  */
object VersionAdd {
  val valueCodec = implicitly[CBOR.Codec[IRValue]]

  val PartialTupleEncoder =
    new CBOR.PartialCodec[(SchemaVersion, Option[Timestamp], ByteBuf, IRValue)](
      "Array") {

      def encode(
        stream: CBOR.Out,
        tuple: (SchemaVersion, Option[Timestamp], ByteBuf, IRValue)): CBOR.Out = {
        val (sv, ttl, encoded, diff) = tuple
        stream.writeArrayStart(4)
        CBOR.encode(stream, sv)
        CBOR.encode(stream, ttl)
        stream.unsafeWriteBytesRaw(encoded, encoded.readableBytes)
        valueCodec.encode(stream, diff)
      }
    }

  // This is a bridge codec for adding schema version to VersionAdd.
  // The original codec was a tuple codec. This overrides decoding to
  // read tuple-encoded VersionAdds that have schema version and ones
  // that don't. This bridge should live as long as there might be
  // schema-version-less VersionAdds in a log.
  val Codec = new CBOR.PartialCodec[VersionAdd]("VersionAdd") {
    override def encode(stream: CBOR.Out, va: VersionAdd) = {
      CBOR.TupleCodec[VersionAdd].encode(stream, va)
    }

    override def readArrayStart(length: Long, stream: CBORParser) = {
      require(
        length == 6 || length == 7,
        s"VersionAdd expected 6 or 7 fields, got $length"
      )
      val scope = CBOR.decode[ScopeID](stream)
      val id = CBOR.decode[DocID](stream)
      val ts = CBOR.decode[BiTimestamp](stream)
      val action = CBOR.decode[DocAction](stream)
      val sv = if (length == 7) {
        CBOR.decode[SchemaVersion](stream)
      } else {
        SchemaVersion.Min
      }
      val data = CBOR.decode[Data](stream)
      val diff = CBOR.decode[Option[Diff]](stream)
      VersionAdd(scope, id, ts, action, sv, data, diff)
    }
  }
}

case class VersionAdd(
  scope: ScopeID,
  id: DocID,
  writeTS: BiTimestamp,
  action: DocAction,
  schemaVersion: SchemaVersion,
  data: Data,
  diff: Option[Diff])
    extends VersionWrite {

  private[this] lazy val pendingDocEncoded =
    encodeDocAt(writeTS.resolvePending, false)

  // Pending writes are encoded as C* values in the query read cache
  // with the transaction time sentinel of MaxMicros. The schema version may
  // be pending because the document may be migrated up to a pending schema, for
  // example during a synchronous migration.
  // FIXME: Cache reads at our instance/index level in order to avoid
  // sentinel-based encoding.
  private[this] lazy val pendingEncoded =
    encodeAt(pendingDocEncoded, writeTS.resolvePending, None)

  private[this] lazy val pendingName =
    pendingEncoded.keyPredicate.uncheckedColumnNameBytes

  protected def pendingCellName(cf: String) = {
    require(cf == Tables.Versions.CFName)
    pendingName
  }

  override def toPendingCell(cf: String) =
    if (cf == Tables.Versions.CFName) {
      Some(Cell(pendingName, pendingEncoded.data, writeTS.transactionTS))
    } else {
      None
    }

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) = {
    val resolved = writeTS.resolve(transactionTS)
    val value = encodeAt(
      encodeDocAt(resolved, true),
      resolved,
      Option.when(schemaVersion.isPending)(SchemaVersion(transactionTS)))
    val name = value.keyPredicate.uncheckedColumnNameBytes

    mutation.add(Tables.Versions.CFName, name, value.data, transactionTS, 0)
  }

  def numBytes =
    rowKey.readableBytes + pendingName.readableBytes + pendingEncoded.data.readableBytes

  override def addOps = 1

  // Resolves transaction times and pending schema versions as part of encoding.
  // The resolved schema version overrides a pending schema version, and is
  // appropriate to use when the write's transaction is being applied (mutateAt),
  // not when it is pending or encoded as part of a cache (toPendingCell).
  private def encodeAt(
    encodedDoc: ByteBuf,
    ts: Resolved,
    resolvedSchemaVersion: Option[SchemaVersion]) = {
    val key = (rowKey, ts.validTS, action, ts.transactionTS)
    val sv = if (schemaVersion.isPending) {
      resolvedSchemaVersion.getOrElse(schemaVersion)
    } else {
      schemaVersion
    }
    val ttl = data.getOpt(Field[Option[Timestamp]]("ttl")).flatten
    new Value[Tables.Versions.Key](
      key,
      CBOR.encode((sv, ttl, encodedDoc, diffV))(VersionAdd.PartialTupleEncoder))
  }

  private def encodeDocAt(ts: Resolved, rewrite: Boolean) = {
    if (action.isCreate) {
      if (rewrite) {
        CBOR.encode(data.fields)(IRValue.ResolvingCodec(ts))
      } else {
        CBOR.encode(data.fields)
      }
    } else {
      CBOR.encode(NullV)
    }
  }

  // N.B. we use NullV to represent a diff from a deleted version
  private def diffV: IRValue =
    diff match {
      case Some(d) => d.fields
      case None    => NullV
    }

  def checkBytes = pendingDocEncoded.readableBytes
}

/** A VersionRemove removes a version of a document at a valid
  * time. It must match the version exactly, or this operation is a
  * no-op.
  */
case class VersionRemove(
  scope: ScopeID,
  id: DocID,
  writeTS: BiTimestamp,
  action: DocAction)
    extends VersionWrite {

  // Pending writes are encoded as C* values in the query read cache
  // with the transaction time sentinel of MaxMicros.
  // FIXME: Cache reads at our instance/index level in order to avoid
  // sentinel-based encoding.
  private[this] lazy val pendingEncoded = encodeAt(writeTS.resolvePending)
  private[this] lazy val pendingName = pendingEncoded.uncheckedColumnNameBytes

  protected def pendingCellName(cf: String) = {
    require(cf == Tables.Versions.CFName)
    pendingName
  }

  override def toPendingCell(cf: String) =
    if (cf == Tables.Versions.CFName) {
      Some(Cell(pendingName, Unpooled.EMPTY_BUFFER, writeTS.transactionTS))
    } else {
      None
    }

  def numBytes = pendingName.readableBytes + 8

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) = {
    val pred = encodeAt(writeTS.resolve(transactionTS))
    val name = pred.uncheckedColumnNameBytes

    mutation.delete(Tables.Versions.CFName, name, transactionTS)
  }

  private def encodeAt(ts: Resolved) =
    Predicate((rowKey, ts.validTS, action, ts.transactionTS))

  override def isRemove = true
}

/** A DocRemove operation removes an document's history entirely, if
  * it exists.
  */
case class DocRemove(scope: ScopeID, id: DocID) extends Write {

  lazy val rowKey = Predicate(Tables.Versions.rowKey(scope, id)).uncheckedRowKeyBytes

  def mergeKey = None

  def isRelevant(cf: String, key: ByteBuf, from: ByteBuf, to: ByteBuf)(
    implicit ord: Ordering[ByteBuf]): Boolean =
    cf == Tables.Versions.CFName && key == rowKey

  def isRelevant(cf: String, key: ByteBuf, slices: Vector[(ByteBuf, ByteBuf)])(
    implicit ord: Ordering[ByteBuf]): Boolean =
    cf == Tables.Versions.CFName && key == rowKey

  def isRelevant(
    schema: ColumnFamilySchema,
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean =
    schema.name == Tables.Versions.CFName && contains(from, to, locate, keyOrd)

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) =
    mutation.delete(Tables.Versions.CFName, transactionTS)

  override def isClearRow = true

  def numBytes = 8
  def addOps = 0
  def removeOps = 0
  def clearRowOps = 1

  private def contains(
    from: Either[(ByteBuf, ByteBuf, Boolean), Location],
    to: Either[(ByteBuf, ByteBuf), Location],
    locate: ByteBuf => Location,
    keyOrd: Ordering[ByteBuf]): Boolean = {
    val rowLoc = locate(rowKey)

    val gtFrom = from match {
      case Right(fromLoc) =>
        fromLoc <= rowLoc
      case Left((fromKey, _, _)) =>
        val fromLoc = locate(fromKey)
        (fromLoc < rowLoc) || (fromLoc == rowLoc && keyOrd.lteq(fromKey, rowKey))
    }

    def ltTo = {
      to match {
        case Right(toLoc) =>
          // a location upper bound is exclusive
          rowLoc < toLoc || toLoc.isMin
        case Left((toKey, _)) =>
          // a (key, cell) upper bound is inclusive
          val toLoc = locate(toKey)
          (rowLoc < toLoc) || (rowLoc == toLoc && keyOrd.lteq(rowKey, toKey))
      }
    }

    gtFrom && ltTo
  }
}

object SetWrite {
  val cfs = Set(Tables.HistoricalIndex.CFName, Tables.SortedIndex.CFName)
  val codecHistoricalIndex = implicitly[CassandraCodec[Tables.HistoricalIndex.Key]]
  val codecSortedIndex = implicitly[CassandraCodec[Tables.SortedIndex.Key]]
}

sealed abstract class SetWrite extends ValueWrite {
  // key
  def scope: ScopeID
  def index: IndexID
  def terms: Vector[IRValue]

  // value
  def values: Vector[IndexTerm]
  def doc: DocID
  def writeTS: BiTimestamp
  def action: SetAction
  def ttl: Option[Timestamp]

  def cfs = SetWrite.cfs

  def mergeKey: Option[MergeKey] =
    Some(MergeKey.Set(scope, index, terms, values, doc, writeTS, action, ttl))

  lazy val rowKey = {
    val resolvedTerms = terms map {
      // key cannot change based on txntime, so as a fallback just
      // null it out.
      case _: TransactionTimeV => NullV
      case v                   => v
    }

    import Tables.Indexes.KeyCBORCodec
    Predicate(CBOR.encode((scope, index, resolvedTerms))).uncheckedRowKeyBytes
  }

  def keyBytes: Int = {
    rowKey.readableBytes + pendingHistoricalName.readableBytes
  }

  private[this] lazy val pendingHistorical =
    historicalAt(writeTS.resolve(writeTS.transactionTS), false)

  private[this] lazy val pendingHistoricalName =
    pendingHistorical.keyPredicate.uncheckedColumnNameBytes

  private[this] lazy val pendingSorted =
    sortedAt(writeTS.resolve(writeTS.transactionTS), false)

  private[this] lazy val pendingSortedName =
    pendingSorted.keyPredicate.uncheckedColumnNameBytes

  def pendingCellName(cf: String) =
    cf match {
      case Tables.HistoricalIndex.CFName => pendingHistoricalName
      case Tables.SortedIndex.CFName     => pendingSortedName
      case _ => throw new IllegalStateException(s"No cell for column family $cf.")
    }

  override def toPendingCell(cf: String) =
    cf match {
      case Tables.HistoricalIndex.CFName =>
        Some(
          Cell(
            pendingHistoricalName,
            pendingHistorical.data,
            BiTimestamp.UnresolvedSentinel))
      case Tables.SortedIndex.CFName =>
        Some(
          Cell(
            pendingSortedName,
            pendingSorted.data,
            BiTimestamp.UnresolvedSentinel))
      case _ => None
    }

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) =
    cfs foreach { cf =>
      val value = encodeAt(cf, writeTS.resolve(transactionTS), true)
      val name = value.keyPredicate.uncheckedColumnNameBytes
      if (isRemove) {
        mutation.delete(cf, name, transactionTS)
      } else {
        mutation.add(cf, name, value.data, transactionTS, 0)
      }
    }

  def numBytes = cfs.size * keyBytes

  protected def resolvedValues(ts: Timestamp, rewrite: Boolean) =
    if (rewrite) {
      values map {
        case IndexTerm(txv: TransactionTimeV, r) => IndexTerm(txv.resolve(ts), r)
        case t                                   => t
      }
    } else {
      values
    }

  protected def historicalAt(ts: Resolved, rewrite: Boolean)
    : Value[(Array[Byte], Timestamp, SetAction, Array[Byte], Timestamp)] = {
    val values = IndexTerm(DocIDV(doc)) +: resolvedValues(ts.transactionTS, rewrite)
    val pred = (rowKey, ts.validTS, action, CBOR.encode(values), ts.transactionTS)
    new Value(pred, encodeTTL())(SetWrite.codecHistoricalIndex)
  }

  protected def sortedAt(ts: Resolved, rewrite: Boolean)
    : Value[(Array[Byte], Array[Byte], Timestamp, SetAction, Timestamp)] = {
    val values = resolvedValues(ts.transactionTS, rewrite) :+ IndexTerm(DocIDV(doc))
    val pred = (rowKey, CBOR.encode(values), ts.validTS, action, ts.transactionTS)
    new Value(pred, encodeTTL())(SetWrite.codecSortedIndex)
  }

  protected def encodeAt(cf: String, ts: Resolved, rewrite: Boolean) =
    cf match {
      case Tables.HistoricalIndex.CFName => historicalAt(ts, rewrite)
      case Tables.SortedIndex.CFName     => sortedAt(ts, rewrite)
      case _ => throw new IllegalStateException(s"No cell for column family $cf.")
    }

  protected def encodeTTL(): ByteBuf =
    ttl match {
      case Some(ttl) => CBOR.encode(TTLModifier(ttl))
      case None      => Unpooled.EMPTY_BUFFER
    }
}

/** Add a new value to a set defined by its key. The value will be
  * written to both sorted and historical versions of the set.
  */
object SetAdd {

  def apply(key: IndexKey, value: IndexValue): SetAdd =
    SetAdd(
      key.scope,
      key.id,
      key.terms map { _.value },
      value.tuple.values,
      value.docID,
      value.ts,
      value.action,
      value.tuple.ttl)
}

case class SetAdd(
  scope: ScopeID,
  index: IndexID,
  terms: Vector[IRValue],
  values: Vector[IndexTerm],
  doc: DocID,
  writeTS: BiTimestamp,
  action: SetAction,
  ttl: Option[Timestamp] = None)
    extends SetWrite

/** Removes a value from a set defined by its key. The value will be
  * removed from both sorted and historical versions of the set.
  *
  *  NB. We must remove exactly the specified value. If the value does
  * not exist in the set, this is a no-op.
  */
object SetRemove {

  def apply(key: IndexKey, value: IndexValue): SetRemove =
    SetRemove(
      key.scope,
      key.id,
      key.terms map { _.value },
      value.tuple.values,
      value.docID,
      value.ts,
      value.action,
      value.tuple.ttl)
}

case class SetRemove(
  scope: ScopeID,
  index: IndexID,
  terms: Vector[IRValue],
  values: Vector[IndexTerm],
  doc: DocID,
  writeTS: BiTimestamp,
  action: SetAction,
  ttl: Option[Timestamp] = None)
    extends SetWrite {
  override def isRemove = true

  override def mutateAt(mutation: Mutation, transactionTS: Timestamp) =
    cfs foreach { cf =>
      val value = encodeAt(cf, writeTS.resolve(transactionTS), true)
      val name = value.keyPredicate.uncheckedColumnNameBytes
      mutation.delete(cf, name, transactionTS)
    }
}

/** Adds new values to either-or-both the sorted and historical
  * versions of the set defined by its key. The result will be exactly
  * equal to an insert having occurred at the transaction and valid
  * times of the source version.
  */
object SetRepair {

  def apply(
    key: IndexKey,
    value: IndexValue,
    missingSorted: Boolean,
    missingHistorical: Boolean): SetRepair =
    SetRepair(
      key.scope,
      key.id,
      key.terms map { _.value },
      value.tuple.values,
      value.docID,
      value.ts,
      value.action,
      missingSorted,
      missingHistorical,
      value.tuple.ttl)

  private val sortedAndHistoricalIndexCfs =
    Set(Tables.SortedIndex.CFName, Tables.HistoricalIndex.CFName)
  private val sortedIndexCfs = Set(Tables.SortedIndex.CFName)
  private val historicalIndexCfs = Set(Tables.HistoricalIndex.CFName)
}

case class SetRepair(
  scope: ScopeID,
  index: IndexID,
  terms: Vector[IRValue],
  values: Vector[IndexTerm],
  doc: DocID,
  writeTS: BiTimestamp,
  action: SetAction,
  missingSorted: Boolean,
  missingHistorical: Boolean,
  ttl: Option[Timestamp] = None)
    extends SetWrite {

  require(writeTS.isResolved, s"Cannot repair unresolved: $this")

  override def mergeKey = None

  override def cfs: Set[String] =
    (missingSorted, missingHistorical) match {
      case (true, true)   => SetRepair.sortedAndHistoricalIndexCfs
      case (true, false)  => SetRepair.sortedIndexCfs
      case (false, true)  => SetRepair.historicalIndexCfs
      case (false, false) => Set.empty
    }

  /** See comment on RepairOffset above.
    */
  override def mutateAt(mut: Mutation, transactionTS: Timestamp) =
    super.mutateAt(mut, transactionTS - Write.RepairOffset)
}

/** Adds new values to either-or-both the sorted and historical
  * versions of the set defined by its key. The result will be exactly
  * equal to an insert having occurred at the transaction and valid
  * times of the source version.
  */
object SetBackfill {

  def apply(
    key: IndexKey,
    value: IndexValue,
    missingSorted: Boolean,
    missingHistorical: Boolean): SetBackfill =
    SetBackfill(
      key.scope,
      key.id,
      key.terms map { _.value },
      value.tuple.values,
      value.docID,
      value.ts,
      value.action,
      missingSorted,
      missingHistorical,
      value.tuple.ttl)

  private val sortedAndHistoricalIndexCfs =
    Set(Tables.SortedIndex.CFName, Tables.HistoricalIndex.CFName)
  private val sortedIndexCfs = Set(Tables.SortedIndex.CFName)
  private val historicalIndexCfs = Set(Tables.HistoricalIndex.CFName)
}

case class SetBackfill(
  scope: ScopeID,
  index: IndexID,
  terms: Vector[IRValue],
  values: Vector[IndexTerm],
  doc: DocID,
  writeTS: BiTimestamp,
  action: SetAction,
  missingSorted: Boolean,
  missingHistorical: Boolean,
  ttl: Option[Timestamp] = None)
    extends SetWrite {

  require(writeTS.isResolved, s"Cannot backfill unresolved: $this")

  override def mergeKey = None

  override def cfs: Set[String] =
    (missingSorted, missingHistorical) match {
      case (true, true)   => SetBackfill.sortedAndHistoricalIndexCfs
      case (true, false)  => SetBackfill.sortedIndexCfs
      case (false, true)  => SetBackfill.historicalIndexCfs
      case (false, false) => Set.empty
    }

  override def mutateAt(mut: Mutation, transactionTS: Timestamp) =
    super.mutateAt(mut, writeTS.transactionTS)
}

object LookupWrite {
  val cfs = Set(Tables.Lookups.CFName)
}

sealed abstract class LookupWrite extends ValueWrite {
  def globalID: GlobalID
  def scope: ScopeID
  def id: DocID
  def writeTS: BiTimestamp
  def action: SetAction

  def cfs = LookupWrite.cfs

  def mergeKey: Option[MergeKey] =
    Some(MergeKey.Lookup(globalID, scope, id, writeTS))

  lazy val rowKey = Predicate(Tables.Lookups.rowKey(globalID)).uncheckedRowKeyBytes

  // Pending writes are encoded as C* values in the query read cache
  // with the transaction time sentinel of MaxMicros.
  // FIXME: Cache reads at our instance/index level in order to avoid
  // sentinel-based encoding.
  private[this] lazy val pendingEncoded = encodeAt(writeTS.resolvePending)

  private[this] lazy val pendingName =
    pendingEncoded.keyPredicate.uncheckedColumnNameBytes

  def pendingCellName(cf: String) = {
    require(cf == Tables.Lookups.CFName)
    pendingName
  }

  override def toPendingCell(cf: String) =
    if (cf == Tables.Lookups.CFName) {
      Some(Cell(pendingName, pendingEncoded.data, BiTimestamp.UnresolvedSentinel))
    } else {
      None
    }

  def mutateAt(mutation: Mutation, transactionTS: Timestamp) = {
    val resolved = writeTS.resolve(transactionTS)
    val value = encodeAt(resolved)
    val name = value.keyPredicate.uncheckedColumnNameBytes

    if (isRemove) {
      mutation.delete(Tables.Lookups.CFName, name, transactionTS)

      val legacyValue = legacyEncodedAt(resolved)
      val legacyName = legacyValue.keyPredicate.uncheckedColumnNameBytes
      mutation.delete(Tables.Lookups.CFName, legacyName, transactionTS)

    } else {
      mutation.add(Tables.Lookups.CFName, name, value.data, transactionTS, 0)
    }
  }

  def numBytes =
    rowKey.readableBytes + pendingName.readableBytes

  def encodeAt(ts: Resolved) = {
    val predicate = Predicate(
      (
        rowKey,
        scope.toLong,
        CBOR.encode(id).toByteArray,
        ts.validTS,
        action,
        ts.transactionTS))
    new Value[Tables.Lookups.Key](predicate, Unpooled.EMPTY_BUFFER)
  }

  def legacyEncodedAt(ts: Resolved) = {
    val predicate = Predicate(
      (rowKey, scope.toLong, CBOR.encode(id).toByteArray, ts.validTS, action))
    new Value[Tables.Lookups.OldKey](predicate, Unpooled.EMPTY_BUFFER)
  }
}

/** Add a new lookup entry.
  */
object LookupAdd {

  def apply(entry: LookupEntry): LookupAdd =
    LookupAdd(entry.globalID, entry.scope, entry.id, entry.ts, entry.action)
}

case class LookupAdd(
  globalID: GlobalID,
  scope: ScopeID,
  id: DocID,
  writeTS: BiTimestamp,
  action: SetAction)
    extends LookupWrite

/** Remove a lookup entry.
  */
object LookupRemove {

  def apply(entry: LookupEntry): LookupRemove =
    LookupRemove(entry.globalID, entry.scope, entry.id, entry.ts, entry.action)
}

case class LookupRemove(
  globalID: GlobalID,
  scope: ScopeID,
  id: DocID,
  writeTS: BiTimestamp,
  action: SetAction)
    extends LookupWrite {
  override def isRemove = true
}

/** Repair a lookup entry. Ensures the transaction time in the cell name
  * is not overridden by the storage engine.
  */
object LookupRepair {

  def apply(entry: LookupEntry): LookupRepair =
    LookupRepair(entry.globalID, entry.scope, entry.id, entry.ts, entry.action)
}

case class LookupRepair(
  globalID: GlobalID,
  scope: ScopeID,
  id: DocID,
  writeTS: BiTimestamp,
  action: SetAction)
    extends LookupWrite {

  require(writeTS.isResolved, s"Cannot repair unresolved: $this")

  override def mergeKey = None

  /** See comment on RepairOffset above.
    */
  override def mutateAt(mut: Mutation, transactionTS: Timestamp) =
    super.mutateAt(mut, transactionTS - Write.RepairOffset)
}
