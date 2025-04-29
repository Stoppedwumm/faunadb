package fauna.repo.service.stream.test

import fauna.atoms._
import fauna.lang._
import fauna.repo.service.stream._
import fauna.repo.test._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ops._
import fauna.tx.transaction._
import scala.collection.mutable.ArrayBuffer

class WriteStreamSpec extends Spec {

  val scopeID = ScopeID.RootID
  val docID = DocID.MinValue
  val streamKey = StreamKey.DocKey(scopeID, docID)
  val ts = Timestamp.ofSeconds(1)

  val write =
    VersionAdd(
      scope = scopeID,
      id = docID,
      writeTS = Unresolved,
      action = Update,
      schemaVersion = SchemaVersion.Min,
      data = Data.empty,
      diff = None
    )

  val expr = (Map.empty, Vector(write)): Txn
  val txn0 = Transaction(ts, expr, null, None)
  val txn1 = Transaction(ts.nextMicro, expr, null, None)

  var values: ArrayBuffer[(Timestamp, Vector[Write])] = _
  var writeStream: WriteStream = _

  before {
    values = ArrayBuffer.empty
    writeStream = new WriteStream({ case (txnTS, writes) =>
      values += ((txnTS, writes))
    })
    writeStream.start()
  }

  after {
    writeStream.stop()
  }

  "WriteStream" - {

    "process transactions" in {
      writeStream.onSchedule(txn0)
      writeStream.onResult(txn0, Some(StorageEngine.TxnSuccess))
      eventually { values should contain only ((ts, Vector(write))) }
    }

    "ignores read lock contention" in {
      val occFailure = StorageEngine.RowReadLockContention(Array.emptyByteArray, ts)
      writeStream.onSchedule(txn0)
      writeStream.onSchedule(txn1)
      writeStream.onResult(txn0, Some(occFailure))
      writeStream.onResult(txn1, Some(StorageEngine.TxnSuccess))
      eventually { values should contain only ((ts.nextMicro, Vector(write))) }
    }
  }
}
