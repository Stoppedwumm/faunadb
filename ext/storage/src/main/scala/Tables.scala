package fauna.storage

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.storage.cassandra._
import fauna.storage.cassandra.comparators._
import fauna.storage.index._
import fauna.storage.ir.{ DocIDV, IRValue }
import fauna.storage.lookup.LookupEntry
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.concurrent.ConcurrentHashMap
import org.apache.cassandra.config.Schema
import org.apache.cassandra.db.composites.CellNameType
import org.apache.cassandra.db.filter.ColumnSlice
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object Tables {

  lazy val FullSchema = Seq(
    Versions.Schema,
    HistoricalIndex.Schema,
    HistoricalIndex.Schema2,
    SortedIndex.Schema,
    SortedIndex.Schema2,
    Lookups.Schema,
    HealthChecks.Schema,
    RowTimestamps.Schema,
    SchemaVersions.Schema
  )

  private[this] lazy val inMemorySchemas =
    Map(
      HealthChecks.CFName -> HealthChecks.Schema,
      HistoricalIndex.CFName -> HistoricalIndex.Schema,
      Lookups.CFName -> Lookups.Schema,
      RowTimestamps.CFName -> RowTimestamps.Schema,
      SortedIndex.CFName -> SortedIndex.Schema,
      Versions.CFName -> Versions.Schema,
      SchemaVersions.CFName -> SchemaVersions.Schema
    )

  private[this] val cachedComparators = new ConcurrentHashMap[(String, String), CellNameType]()

  private def getCachedComparator(keyspaceName: String, cfName: String) = {
    cachedComparators.computeIfAbsent(
      (keyspaceName, cfName), { _ =>
        inMemorySchemas.getOrElse(
          cfName,
          throw new NoSuchElementException(s"ColumnFamily $cfName not defined.")
        ).metadata(keyspaceName, None).comparator
      })
  }

  def cfMetaData(keyspaceName: String, cf: String) =
    Schema.instance.getKSMetaData(keyspaceName) match {
      case null => Failure(new NoSuchElementException(s"Keyspace ${keyspaceName} not defined."))
      case meta => meta.cfMetaData.get(cf) match {
        case null => Failure(new NoSuchElementException(s"ColumnFamily $cf not defined."))
        case cfmeta => Success(cfmeta)
      }
    }

  def encodeSlice(keyspaceName: String, cf: String, from: ByteBuf, to: ByteBuf): ColumnSlice = {
    def onDisk: Try[ColumnSlice] =
      cfMetaData(keyspaceName, cf) flatMap { cfmeta =>
        val cmp = cfmeta.comparator
        Try(new ColumnSlice(cmp.fromByteBuffer(from.nioBuffer), cmp.fromByteBuffer(to.nioBuffer)))
      }

    def inMemory: ColumnSlice = {
      val cmp = getCachedComparator(keyspaceName, cf)
      new ColumnSlice(cmp.fromByteBuffer(from.nioBuffer), cmp.fromByteBuffer(to.nioBuffer))
    }

    onDisk.getOrElse(inMemory)
  }

  def decodeScope(cf: String, buf: ByteBuf): Option[ScopeID] = cf match {
    case RowTimestamps.CFName
         | Versions.CFName
         | HistoricalIndex.CFName
         | HistoricalIndex.CFName2
         | SortedIndex.CFName
         | SortedIndex.CFName2 =>
      val parser = CBORParser.apply(buf.slice())

      val majorType = parser.currentMajorType
      if (majorType == MajorType.Array) {
        parser.read(CBORParser.ArrayStartSwitch) match {
          case 3 =>
            //Version & SortedIndex & HistoricalIndex encodes scopeID as first element
            Some(CBOR.decode[ScopeID](parser))

          case _ =>
            None
        }
      } else {
        None
      }

    case _ =>
      None
  }

  def decodePredicate(schema: ColumnFamilySchema, rowKey: CValue, name: ByteBuf): Predicate = {
    val prefixComponents = if (schema.prefixComparators.isEmpty) {
      Nil
    } else {
      schema.nameComparator.bytesToCValues(name)
    }

    Predicate(rowKey, prefixComponents)
  }

  def decodeValue[K](schema: ColumnFamilySchema, rowKey: ByteBuf, cell: Cell)(
    implicit keyCodec: CassandraCodec[K]): Value[K] =
    decodeValue(schema, CValue(schema.keyComparator, rowKey), cell)

  def decodeValue[K](schema: ColumnFamilySchema, rowKey: CValue, cell: Cell)(
    implicit keyCodec: CassandraCodec[K]): Value[K] = {
    val pred = decodePredicate(schema, rowKey, cell.name)
    new Value(pred, cell.value, transactionTS = cell.ts)
  }

  object HealthChecks {
    type Key = (String, Array[Byte])

    val CFName = "HealthChecks"
    val Schema = ColumnFamilySchema[Key](CFName, Nil)
  }

  /**
    * Schema:
    *
    *   HistoricalIndex = { // ColumnFamily
    *     <scope><index><terms>: { // Row
    *       <valid_ts><action><values><txn_ts>: <modifier> // Column
    *     }
    *   }
    *
    * <scope>, <index> and <terms> are a CBOR serialized tuple. <scope>
    * is a ScopeID. <index> is an IndexID. <terms> are the fields being
    * indexed, e.g. an index on a field (term) called "name" might have
    * a value "sam". <valid_ts>, <action> and <value> represent the
    * timestamp at which the term was added/removed from the given
    * instance. <action> being a boolean, with true indicating "term
    * added". The ID of the instance is always the first element of
    * <values>. <txn_ts> is the time at which the transaction modifying
    * this index entry occurred.
    *
    * Column values may contain an optional value modifier.
    */
  object HistoricalIndex {
    type Key = (Array[Byte], Timestamp, SetAction, Array[Byte], Timestamp)

    private val comparators =
      Seq(
        BytesType,
        LongType.reverse,
        BooleanType.reverse,
        CBORType.reverse,
        LongType.reverse)

    val CFName = "HistoricalIndex"
    val Schema = ColumnFamilySchema[Key](
      CFName,
      comparators,
      compaction = Compaction.Collection)

    val CFName2 = "HistoricalIndex_2"
    val Schema2 = ColumnFamilySchema[Key](CFName2, comparators, gcGrace = GCGrace.Custom(1000.days))

    def toKey(key: IndexKey, value: IndexValue): Key = {
      val values = IndexTerm(DocIDV(value.docID)) +: value.tuple.values
      val bytes = CBOR.encode(values).toByteArray
      (Indexes.rowKey(key), value.ts.validTS, value.action, bytes, value.ts.transactionTS)
    }
  }

  object Indexes {

    implicit val KeyCBORCodec =
      CBOR.TupleCodec[(ScopeID, IndexID, Vector[IRValue])]

    /** Both HistoricalIndex and SortedIndex use the same rowKey
      * encoding. That is represented here.
      */

    def rowKey(scope: ScopeID, index: IndexID, terms: Vector[IRValue]): ByteBuf =
      CBOR.encode((scope, index, terms))

    def rowKey(key: IndexKey): Array[Byte] =
      rowKey(key.scope, key.id, key.terms map { _.value }).toByteArray

    def decode(rowKey: ByteBuf): (ScopeID, IndexID, Vector[IRValue]) = {
      val (scopeID, indexID, terms) =
        CBOR.decode[(ScopeID, IndexID, Vector[IRValue])](rowKey)
      (scopeID, indexID, terms)
    }
  }

  /**
    * Schema:
    *
    *    LookupStore = { // ColumnFamily
    *        <id>: { // Row
    *            <scopeID><docID><valid_ts><action><txn_ts>: <nil> // Column
    *        }
    *    }
    *
    * <id> is a sum type of (GlobalKeyID | ScopeID | GlobalDatabaseID)
    * <scopeID> is the underlying long of the scopeID
    * <docID> is the underlying DocID of the associated instance ID
    *
    *
    * In summary, this CF allows storing & querying the following mapping:
    * - scopeID     => parentScopeID, databaseID
    * - globalKeyID => parentScopeID, keyID
    * - globalID    => parentScopeID, databaseID
    */
  object Lookups {
    type Key = (Array[Byte], Long, Array[Byte], Timestamp, SetAction, Timestamp)

    // the type of a Key prior to appending the transaction time to each key
    type OldKey = (Array[Byte], Long, Array[Byte], Timestamp, SetAction)

    val CFName = "LookupStore"
    val Schema = ColumnFamilySchema[Key](CFName,
                                    Seq(BytesType,
                                        LongType.reverse,
                                        BytesType.reverse,
                                        LongType.reverse,
                                        BooleanType.reverse,
                                        LongType.reverse))

    val OldSchema = ColumnFamilySchema[Key](CFName,
                                    Seq(BytesType,
                                        LongType.reverse,
                                        BytesType.reverse,
                                        LongType.reverse,
                                        BooleanType.reverse))

    def rowKey(id: GlobalID): Array[Byte] =
      CBOR.encode(id).toByteArray

    def decode(key: ByteBuf): GlobalID =
      CBOR.parse[GlobalID](key)

    def toKey(entry: LookupEntry) =
      (rowKey(entry.globalID),
        entry.scope.toLong,
        CBOR.encode(entry.id).toByteArray,
        entry.ts.validTS,
        entry.action,
        entry.ts.transactionTS)

    def decodeLookup(rowKey: ByteBuf, cell: Cell): LookupEntry =
      LookupEntry(decodeValue(Schema, rowKey, cell))

    def decodeDatabase(rowKey: ByteBuf, cell: Cell): (ScopeID, DocID) =
      LookupEntry.decodeDatabase(decodeValue(Schema, rowKey, cell))
  }

  /**
    * Storage for row modification timestamps. These are used to enforce
    * optimistic locks during transaction resolution.
    *
    * Schema:
    *
    *   _RowTimestamps_ = { // ColumnFamily
    *     <key>: { // Row
    *         <seconds><nanos>: <null> // Column
    *     }
    *   }
    */
  object RowTimestamps {
    type Key = (Array[Byte], Long, Long)

    private val CellNameBytes = (2 + 8 + 1) * 2

    val CFName = "_RowTimestamps_"
    val StorageEngineTTLSeconds = 10.days.toSeconds.toInt
    // this TTL is combined with the current snapshot time to ensure RowTimestamp
    // reads are completely deterministic. This prevents edge cases where in 1 replica
    // the storage engine may have already GC-ed the rowtimestamp while in another this
    // did not yet happen. 
    val TTL = 1.days

    val Schema =
      ColumnFamilySchema[Key](CFName, Seq(BytesType, LongType.reverse, LongType.reverse))

    def encode(snapTime: Timestamp): ByteBuf =
      Unpooled
        .buffer(CellNameBytes)
        .writeShort(8)
        .writeLong(snapTime.seconds)
        .writeByte(Predicate.EQ)
        .writeShort(8)
        .writeLong(snapTime.nanoOffset)
        .writeByte(Predicate.EQ)

    def decode(buf: ByteBuf): Timestamp = {
      require(buf.readableBytes == CellNameBytes)
      val seconds = buf.getLong(2) // skip initial size short
      val nanoOffset = buf.getLong(2 + 8 + 1 + 2) // skip seconds second size short
      Timestamp(seconds, nanoOffset)
    }
  }

  /**
    * Schema:
    *
    *    SortedIndex = { // ColumnFamily
    *        <scope><index><terms>: { // Row
    *          <values><valid_ts><action><txn_ts>: <modifier> // Column
    *        }
    *    }
    *
    * See HistoricalIndex for a description of the Row keys. Column
    * names likewise contain similar values as HistoricalIndex, but are
    * primarily ordered by <values>, rather than <valid_ts>. The ID of
    * the instance is always the last element of <values>.
    *
    * Column values may contain an optional modifier.
    */
  object SortedIndex {
    type Key = (Array[Byte], Array[Byte], Timestamp, SetAction, Timestamp)

    // NOTE: The action is ordered such that a conflict between an
    // "add" and a "remove" event will resolve in favor of the
    // "add". Unlike a typical tombstone, this counter-intuitive
    // behavior enables TTL updates whereby a conflict is
    // intentionally generated (see IndexerSpec.scala).
    private val comparators = Seq(BytesType,
      CBORType.reverse,
      LongType.reverse,
      BooleanType.reverse,
      LongType.reverse)

    val CFName = "SortedIndex"
    val Schema = ColumnFamilySchema[Key](
      CFName,
      comparators,
      compaction = Compaction.Collection)

    val CFName2 = "SortedIndex_2"
    val Schema2 = ColumnFamilySchema[Key](CFName2, comparators, gcGrace = GCGrace.Custom(1000.days))

    def toKey(key: IndexKey, value: IndexValue): Key = {
      val values = value.tuple.values :+ IndexTerm(DocIDV(value.docID))
      val bytes = CBOR.encode(values).toByteArray
      (Indexes.rowKey(key), bytes, value.ts.validTS, value.action, value.ts.transactionTS)
    }
  }

  /**
    * Schema:
    *
    *    Versions = { // ColumnFamily
    *        <scope><instance>: { // Row
    *           // FIXME: Remove <action> in the new storage engine;
    *           // it can be inferred from <data> + <diff>
    *           <valid_ts><action><txn_ts>: <data><diff> // Column
    *        }
    *    }
    */
  object Versions {
    type Key = (Array[Byte], Timestamp, DocAction, Timestamp)

    val CFName = "Versions"
    val Schema = ColumnFamilySchema[Key](
      CFName,
      Seq(BytesType, LongType.reverse, BooleanType, LongType.reverse),
      compaction = Compaction.Collection,
      compression = Compression.CheckedLZ4)


    def rowKeyByteBuf(scope: ScopeID, id: DocID): ByteBuf =
      CBOR.encode((scope, id.collID, id.subID))

    def rowKey(scope: ScopeID, id: DocID): Array[Byte] =
      rowKeyByteBuf(scope, id).toByteArray

    def decodeRowKey(key: ByteBuf) = {
      val (scope, klass, sub) = CBOR.parse[(ScopeID, CollectionID, SubID)](key)
      (scope, DocID(sub, klass))
    }
  }

  /**
    * Schema:
    *
    *    SchemaVersions = { // ColumnFamily
    *        <scope>: { // Row
    *           _: _ // No columns are ever written
    *        }
    *    }
    *
    * This column family exists solely to cause a timestamp to be persisted for each scope.
    * This timestamp is the last time that a schema within the scope was updated.
    */
  object SchemaVersions {
    type Key = (Array[Byte])

    val CFName = "SchemaVersions"
    val Schema = ColumnFamilySchema[Key](CFName, Seq(BytesType))

    def rowKey(scope: ScopeID): ByteBuf =
      CBOR.encode(scope)

    def decodeRowKey(key: ByteBuf): ScopeID =
      CBOR.parse[ScopeID](key)
  }
}
