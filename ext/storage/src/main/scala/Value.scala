package fauna.storage

import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.codex.cbor.CBOR.showBuffer
import fauna.storage.cassandra.comparators._
import io.netty.buffer.{ ByteBuf, Unpooled }

class Value[K](
  val keyPredicate: Predicate,
  val data: ByteBuf,
  val transactionTS: Timestamp = Clock.time)(implicit val keyCodec: CassandraCodec[K]) {

  lazy val key: K = keyPredicate.as[K](keyCodec)

  override def toString =
    s"Value($keyPredicate, ${showBuffer(data)}, ts: $transactionTS)"
}

object Value {

  /**
    * This alternate constructor conveniently enables syntax like
    * `Value(row -> column, data)` in testing, but shouldn't be
    * necessary in production code.
    */
  def apply[K: CassandraCodec](key: K, bytes: Array[Byte], transactionTS: Timestamp = Clock.time): Value[K] =
    new Value[K](key, Unpooled.wrappedBuffer(bytes), transactionTS)
}
