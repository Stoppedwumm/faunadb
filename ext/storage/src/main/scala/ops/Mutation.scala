package fauna.storage.ops

import fauna.lang.Timestamp
import io.netty.buffer.ByteBuf
import org.apache.cassandra.config.CFMetaData

/**
  * This trait makes abstraction of modifications that should be applied to storage.
  * It allows adding and removing cells for a specific row.
  * Timestamps should be stored with microsecond precision.
  * */
trait Mutation {
  /**
    * Clear all cell from a row as of the specified timestamp.
    * */
  def delete(cfName: String, transactionTS: Timestamp): Unit
  /**
    * Clear a cell from a row as of the specified timestamp.
    * */
  def delete(cfName: String, cellName: ByteBuf, transactionTS: Timestamp): Unit
  /**
    * Clear a cell from a row as of the specified timestamp.
    * The cellName will be encoded with the comparator from cfMetaData.
    * */
  def delete(cfName: String, cfMetaData: CFMetaData, cellName: ByteBuf, transactionTS: Timestamp): Unit

  /**
    * Add the cell with value to a row at the specified timestamp, with the specified ttl.
    * ttl 0 specifies an unlimited lifetime.
    * */
  def add(cfName: String, cellName: ByteBuf, value: ByteBuf, transactionTS: Timestamp, ttl: Int): Unit
}
