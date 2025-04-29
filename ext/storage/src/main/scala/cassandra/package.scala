package fauna.storage

import fauna.codex.cbor.CBOR.showBuffer
import fauna.storage.cassandra.comparators.BasicComparator
import io.netty.buffer.ByteBuf
import org.apache.cassandra.db.{ ColumnFamilyStore, DecoratedKey }
import org.apache.cassandra.db.compaction.CompactionController
import scala.collection.immutable.ArraySeq
import scala.collection.mutable

package cassandra {
  package object comparators {
    object CassandraCodec extends CassandraCodecImpl
    type CassandraCodec[A] = CassandraCodec.Codec[A]
    type CassandraDecoder[+A] = CassandraCodec.Decoder[A]
    type CassandraEncoder[-A] = CassandraCodec.Encoder[A]
  }

  case class CValue(comparator: BasicComparator, bytes: ByteBuf) {
    override def toString = if (bytes.readableBytes > 0) {
      comparator show bytes
    } else {
      showBuffer(bytes)
    }
  }

  case class CReader(iter: Iterator[CValue]) extends AnyVal {
    def readNext() = iter.next()
  }

  case class CBuilder(private val builder: mutable.Builder[CValue, Seq[CValue]] = ArraySeq.newBuilder[CValue]) extends AnyVal {
    def add(value: CValue): Unit = builder += value
    def result: Seq[CValue] = builder.result()
  }

  /** A CompactionController which considers no tombstones
    * purgeable. This is useful in offline tools wherein purging a
    * tombstone could never be considered safe.
    *
    * See DataExporter and Rewriter.
    */
  final class NeverPurgeController(cfs: ColumnFamilyStore)
      extends CompactionController(cfs, Int.MaxValue) {

    override def maxPurgeableTimestamp(key: DecoratedKey) =
      Long.MinValue
  }
}
