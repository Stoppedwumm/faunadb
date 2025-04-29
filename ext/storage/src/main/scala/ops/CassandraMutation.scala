package fauna.storage.ops

import fauna.lang.Timestamp
import fauna.storage.Tables
import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import org.apache.cassandra.config.{ CFMetaData, Schema }
import org.apache.cassandra.db.{ Mutation => CMutation }

case class CassandraMutation(
  keyspaceName: String,
  key: ByteBuffer,
  index1CF: Boolean,
  index2CF: Boolean)
    extends Mutation {
  val cmut = new CMutation(keyspaceName, key)

  private def comparator(keyspaceName: String, cf: String) =
    Schema.instance.getKSMetaData(keyspaceName) match {
      case null =>
        throw new NoSuchElementException(s"Keyspace $keyspaceName not defined.")
      case meta =>
        meta.cfMetaData.get(cf) match {
          case null =>
            throw new NoSuchElementException(s"ColumnFamily $cf not defined.")
          case cfmeta => cfmeta.comparator
        }
    }

  def getCell(keyspaceName: String, cfName: String, cellName: ByteBuf) =
    comparator(keyspaceName, cfName).cellFromByteBuffer(cellName.nioBuffer)

  private def withCF(cfName: String)(f: String => Unit) =
    cfName match {
      case Tables.HistoricalIndex.CFName =>
        if (index1CF) { f(Tables.HistoricalIndex.CFName) }
        if (index2CF) { f(Tables.HistoricalIndex.CFName2) }
      case Tables.SortedIndex.CFName =>
        if (index1CF) { f(Tables.SortedIndex.CFName) }
        if (index2CF) { f(Tables.SortedIndex.CFName2) }
      case _ =>
        f(cfName)
    }

  override def delete(cfName: String, transactionTS: Timestamp): Unit =
    withCF(cfName) { cmut.delete(_, transactionTS.micros) }

  override def delete(
    cfName: String,
    cellName: ByteBuf,
    transactionTS: Timestamp): Unit =
    withCF(cfName) { cfName =>
      cmut.delete(
        cfName,
        getCell(keyspaceName, cfName, cellName),
        transactionTS.micros)
    }

  override def delete(
    cfName: String,
    cfMetaData: CFMetaData,
    cellName: ByteBuf,
    transactionTS: Timestamp): Unit =
    withCF(cfName) { cfName =>
      cmut.delete(
        cfName,
        cfMetaData.comparator.cellFromByteBuffer(cellName.nioBuffer()),
        transactionTS.micros)
    }

  override def add(
    cfName: String,
    cellName: ByteBuf,
    value: ByteBuf,
    transactionTS: Timestamp,
    ttl: Int): Unit =
    withCF(cfName) { cfName =>
      cmut.add(
        cfName,
        getCell(keyspaceName, cfName, cellName),
        value.nioBuffer,
        transactionTS.micros,
        ttl)
    }
}
