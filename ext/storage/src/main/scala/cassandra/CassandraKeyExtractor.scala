package fauna.storage.cassandra

import fauna.storage.{ Txn, TxnRead }
import fauna.tx.transaction.KeyExtractor
import io.netty.buffer.ByteBuf

object CassandraKeyExtractor extends KeyExtractor[ByteBuf, TxnRead, Txn] {
  def readKey(expr: TxnRead) = expr.rowKey

  def readKeysSize(expr: Txn) = {
    val (rs, _) = expr
    rs.size
  }

  def readKeysIterator(expr: Txn) = {
    val (rs, _) = expr
    rs.keysIterator map { _.rowKey }
  }

  def writeKeys(expr: Txn) = {
    // Choose "write" keys (by proxy the nodes which will actually
    // execute and respond w/ tx result). If the transaction has no
    // writes, then this means the transaction is a linearized read,
    // so just choose the read nodes of the minimal key to run the transaction on.
    val (rs, ws) = expr

    if (ws.isEmpty && rs.nonEmpty) {
      Vector(rs.keysIterator.min.rowKey)
    } else {
      ws map { w => w.rowKey }
    }
  }
}
