package fauna.storage.cassandra

import fauna.atoms._
import fauna.storage.TxnRead
import fauna.tx.transaction.KeyLocator
import io.netty.buffer.ByteBuf
import org.apache.cassandra.dht.Murmur3Partitioner

object CassandraKeyLocator extends KeyLocator[ByteBuf, TxnRead] {
  private lazy val partitioner = new Murmur3Partitioner

  def region(read: TxnRead): RegionID = read.region

  def locate(key: ByteBuf): Location = {
    val token = partitioner.getToken(key.nioBuffer)
    Location(token.getTokenValue)
  }
}
