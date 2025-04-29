package fauna.storage

import fauna.codex.cbor._
import fauna.lang.syntax._
import java.io.{ ByteArrayInputStream, DataInputStream }
import java.nio.ByteBuffer
import org.apache.cassandra.io.util.DataOutputBuffer
import org.apache.cassandra.streaming.compress._

/**
  * This header is prepended to the data stream before transfer, such
  * that the receiving side can decode the ensuing stream into the
  * correct SSTable format.
  *
  * NB. Unlike C*, we cannot rely upon CF UUIDs, because processes
  * have independent schema. We use the CF name as a String, and
  * trust the storage engine to validate schema.
  */
case class TransferHeader(
  cf: String,
  estimatedKeys: Long,
  sections: Vector[(Long, Long)],
  sstableLevel: Long,
  compression: Option[CompressionInfo]
)

object TransferHeader {

  val CompressionVersion = 0 // irrelevant, but C* requires one...

  implicit val CompressionCodec = {
    val serializer = CompressionInfo.serializer

    def from(bytes: ByteBuffer) = {
      // XXX: a "ByteBufferInputStream" would prevent this copying.
      val bis = new ByteArrayInputStream(bytes.toArray)
      val dis = new DataInputStream(bis)
      serializer.deserialize(dis, CompressionVersion)
    }

    def to(info: CompressionInfo) = {
      val size = serializer.serializedSize(info, CompressionVersion)
      val bytes = new DataOutputBuffer(size.toInt)
      serializer.serialize(info, bytes, CompressionVersion)
      bytes.asByteBuffer
    }

    CBOR.AliasCodec(from, to)
  }

  implicit val Codec = CBOR.TupleCodec[TransferHeader]
}

