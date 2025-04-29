package fauna.storage.cassandra

import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.storage._
import fauna.storage.cassandra.comparators._
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.cassandra.cache.CachingOptions
import org.apache.cassandra.config.{ CFMetaData, Schema }
import org.apache.cassandra.db.compaction._
import org.apache.cassandra.db.marshal.{ TypeParser, BytesType => CBytesType }
import org.apache.cassandra.io.compress.CompressionParameters
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Compaction {
  object Strategy {
    implicit val codec = CBOR.SumCodec[Strategy](
      CBOR.SingletonCodec(Leveled),
      CBOR.SingletonCodec(SizeTiered),
      CBOR.SingletonCodec(Collection)
    )
  }
  sealed abstract class Strategy
  case object Leveled extends Strategy
  case object SizeTiered extends Strategy
  case object Collection extends Strategy

  val Default = Leveled
}

object Caching {
  object Strategy {
    implicit val codec = CBOR.SumCodec[Strategy](
      CBOR.SingletonCodec(All),
      CBOR.SingletonCodec(Keys),
      CBOR.SingletonCodec(Rows),
      CBOR.SingletonCodec(None)
    )
  }
  sealed abstract class Strategy
  case object All extends Strategy
  case object Keys extends Strategy
  case object Rows extends Strategy
  case object None extends Strategy

  val Default = Keys
}

object Compression {
  object Strategy {
    implicit val codec = CBOR.SumCodec[Strategy](
      CBOR.SingletonCodec(None),
      CBOR.SingletonCodec(Snappy),
      CBOR.SingletonCodec(LZ4),
      CBOR.SingletonCodec(CheckedLZ4)
    )
  }
  sealed abstract class Strategy
  case object None extends Strategy
  case object Snappy extends Strategy
  case object LZ4 extends Strategy
  case object CheckedLZ4 extends Strategy

  val ChunkLength = 16 // KB

  val Default = LZ4
}

object GCGrace {
  object Strategy {
    private implicit val durationCodec =
      CBOR.AliasCodec[FiniteDuration, Long](FiniteDuration(_, TimeUnit.SECONDS), _.toSeconds)

    implicit val codec = CBOR.SumCodec[Strategy](
      CBOR.SingletonCodec(Default),
      CBOR.TupleCodec[Custom]
    )
  }
  sealed abstract class Strategy {
    def duration: FiniteDuration
    def seconds: Integer = duration.toSeconds.toInt
  }

  object Default extends Strategy {
    def duration = 48.hours
  }

  case class Custom(duration: FiniteDuration) extends Strategy
}

object ColumnFamilySchema {
  implicit val codec = CBOR.TupleCodec[ColumnFamilySchema]

  // random UUID as the "name space UUID" as per RFC 4122 section 4.3; essentially hash salt.
  private[ColumnFamilySchema] val NameSpaceUUID = UUID.fromString("4f03b589-85c1-4e0c-8d47-d5d4c2a48d51")

  def apply[K: CassandraCodec](name: String,
                               comparators: Seq[BasicComparator],
                               gcGrace: GCGrace.Strategy = GCGrace.Default,
                               compaction: Compaction.Strategy = Compaction.Default,
                               compression: Compression.Strategy = Compression.Default): ColumnFamilySchema = {
    val (keycmp, prefixcmp) = {
      val derived = CassandraCodec.components[K]
      val cmps = if (comparators.isEmpty) derived else comparators

      if (cmps zip derived exists { case (a, b) => !(a matches b) }) {
        throw new IllegalArgumentException(
          "Derived key serialization schema is not compatible with configured schema.")
      }

      (cmps.head, cmps.tail)
    }

    ColumnFamilySchema(
      name,
      keycmp,
      prefixcmp.toVector,
      compaction,
      compression,
      Caching.Default,
      gcGrace)  }
}

/**
 * Represents the lowest-level information about Column Family configuration.
 * Used by the schema manager for migrations and by tables for runtime
 * configuration.
 */
case class ColumnFamilySchema(name: String,
  keyComparator: BasicComparator,
  prefixComparators: Vector[BasicComparator],
  compaction: Compaction.Strategy,
  compression: Compression.Strategy,
  caching: Caching.Strategy,
  gcGrace: GCGrace.Strategy) {

  require(name.length < 42, s"Column family name '$name' is over 42 characters.")

  private def compactor = compaction match {
    case Compaction.Leveled => classOf[LeveledCompactionStrategy]
    case Compaction.SizeTiered => classOf[SizeTieredCompactionStrategy]
    case Compaction.Collection => classOf[CollectionStrategy]
  }

  /**
   * Unchecked_tombstone_compaction should be enabled for hash
   * partitioning because Cassandra tries to estimate if key ranges
   * in sstables overlap and they usually do. But that means tombstone
   * compaction can spin if it can't compact enough tombstones to
   * drop under the threshold, since it creates a new sstable each time.
   * Using a large tombstone_compaction_interval limits the speed
   * of the spin.
   *
   * See: https://issues.apache.org/jira/browse/CASSANDRA-6563
   */

  private[this] val compactionOptions = {
    Map(
      // Time in seconds to wait before tombstone compaction is run for an sstable.
      "tombstone_compaction_interval" -> "43200",
      // Maximum ratio of removable tombstones to cells allowed before tombstone compaction is run.
      "tombstone_threshold" -> "0.05",
      // Don't estimate range overlap before running a tombstone compaction.
      "unchecked_tombstone_compaction" -> "true"
    ).asJava
  }

  private[this] val compressionParams = compression match {
    case Compression.None =>
      CompressionParameters.create(Map.empty[String, String].asJava)
    case Compression.LZ4 =>
      CompressionParameters.create(Map(
        "sstable_compression" -> "LZ4Compressor",
        "crc_check_chance" -> "0.0",
        "chunk_length_kb" -> Compression.ChunkLength.toString).asJava)
    case Compression.CheckedLZ4 =>
      CompressionParameters.create(Map(
        "sstable_compression" -> "LZ4Compressor",
        "crc_check_chance" -> "1.0",
        "chunk_length_kb" -> Compression.ChunkLength.toString).asJava)
    case Compression.Snappy =>
      CompressionParameters.create(Map(
        "sstable_compression" -> "SnappyCompressor",
        "chunk_length_kb" -> Compression.ChunkLength.toString).asJava)
  }

  private[this] val cacher = caching match {
    case Caching.All => CachingOptions.ALL
    case Caching.Keys => CachingOptions.KEYS_ONLY
    case Caching.Rows => CachingOptions.ROWS_ONLY
    case Caching.None => CachingOptions.NONE
  }

  // Builders

  def withGCGrace(grace: GCGrace.Strategy): ColumnFamilySchema =
    copy(gcGrace = grace)

  def withCompaction(comp: Compaction.Strategy): ColumnFamilySchema =
    copy(compaction = comp)

  def withCompression(comp: Compression.Strategy): ColumnFamilySchema =
    copy(compression = comp)

  def withCaching(cache: Caching.Strategy): ColumnFamilySchema =
    copy(caching = cache)

  val nameComparator = prefixComparators match {
    case Vector()    => BytesType
    case Vector(cmp) => cmp
    case vector      => CompositeType(vector)
  }

  val nameOrdering = new Ordering[ByteBuf] {
    def compare(a: ByteBuf, b: ByteBuf) = nameComparator.compare(a, b)
  }

  def validatePredicate(pred: Predicate): Unit = {
    if (pred.columnName.length > prefixComparators.length)
      throw SchemaViolationException(s"Invalid predicate: Too many components specified for comparator $prefixComparators.")
  }

  def encodePrefix(pred: Predicate, compareOp: Byte): Option[ByteBuf] = {
    if (pred.columnName.nonEmpty)
      Some(nameComparator.cvaluesToBytes(pred.columnName, compareOp))
    else None
  }

  /**
   * Metadata necessary to create/update this Column Family in Cassandra.
   */
  def metadata(keyspaceName: String, idOverride: Option[UUID]): CFMetaData = {
    // FIXME Comparators should be native
    val nativeKeyValidator = TypeParser.parse(keyComparator.comparator)
    val nativeComparator = TypeParser.parse(nameComparator.comparator)

    val cfID = idOverride
      .orElse(Option(Schema.instance.getId(keyspaceName, name)))
      .getOrElse(makeUUID(keyspaceName))
    val cfMetadata = CFMetaData.denseCFMetaData(keyspaceName, name, nativeComparator).copy(cfID)
    cfMetadata.keyValidator(nativeKeyValidator)
    cfMetadata.compactionStrategyClass(compactor)
    cfMetadata.compressionParameters(compressionParams)
    cfMetadata.compactionStrategyOptions(compactionOptions)
    cfMetadata.caching(cacher)
    cfMetadata.memtableFlushPeriod(0)
    cfMetadata.gcGraceSeconds(gcGrace.seconds)
    cfMetadata.defaultValidator(CBytesType.instance)
    CFMetaData.validateCompactionOptions(cfMetadata.compactionStrategyClass, cfMetadata.compactionStrategyOptions)

    cfMetadata.rebuild() // convert the above "thrift-style" cf def to cql3 style.
    cfMetadata
  }

  private def makeUUID(keyspaceName: String) =
    Unpooled.buffer() releaseAfter { buf =>
      buf.writeLong(ColumnFamilySchema.NameSpaceUUID.getLeastSignificantBits)
      buf.writeLong(ColumnFamilySchema.NameSpaceUUID.getMostSignificantBits)
      CBOR.encode(buf, this)
      if (keyspaceName != Cassandra.KeyspaceName) {
        buf.writeBytes(keyspaceName.getBytes)
      }
      // This is generating type 3 (MD5-based) UUID instead of type 5 (SHA-1)
      // but we'll deem it good enough instead of rolling our own.
      UUID.nameUUIDFromBytes(buf.array())
    }
}
