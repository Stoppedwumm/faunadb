package fauna.tools

import fauna.cluster.MembershipCommand
import fauna.cluster.topology.{ LogTopologyCommand, TopologyCommand }
import fauna.cluster.workerid.WorkerIDCommand
import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.storage.{ Txn, TxnRead }
import fauna.tx.consensus.log._
import fauna.tx.log.{ BinaryLogStore, LogEntry, TX }
import fauna.tx.transaction.{ Batch, Epoch, ScopeSubEntry }
import java.io.PrintStream
import java.nio.file.Paths

abstract class LogReader(path: String) {
  def dir: String
  def logName: String
  def transform(bytes: Array[Byte]): String
  def logDir = Paths.get(path).toAbsolutePath / dir

  def print(): Unit = print(System.out)

  def print(out: PrintStream): Unit = {
    val log = BinaryLogStore.open[Entry](logDir, None, logName, verifyFiles = false)
    log.entries(TX.MinValue) foreach {
      case LogEntry(tx, Some(Entry.Value(term, token, bytes))) =>
        out.println(s"LogEntry($tx, Entry.Value($term, $token, ${transform(bytes)}))")
      case _ => ()
    }
  }
}

case class TxnReader(path: String) extends LogReader(path) {
  val dir = "transactions"
  val logName = {
    val segNum = logDir.toFile.list.collect { case s"segment_$d.binlog.manifest" => d }.head
    s"segment_${segNum}"
  }

  def transform(bytes: Array[Byte]): String = {
    val (_, es) = CBOR.parse[(Epoch, Vector[ScopeSubEntry])](bytes)
    val entries = es map { case ScopeSubEntry(scope, ts) =>
      val txns = ts map { case Batch.Txn(exprBytes, origin, expiry, pos, trace) =>
        val (occRaw, writes) = CBOR.parse[Txn](exprBytes)
        val occ = occRaw map { case (TxnRead(key, region), ts) => s"(ByteBuf(${key.toHexString}),$region)" -> ts }
        s"Batch.Txn(($occ, $writes), $origin, $expiry, $pos, $trace)"
      }
      s"($scope, Vector(), $txns)"
    }
    entries.toString
  }
}

abstract class SysLogReader[T: CBOR.Codec](val logName: String, path: String) extends LogReader(path) {
  val dir = "system"
  def transform(bytes: Array[Byte]): String = CBOR.parse[T](bytes).toString
}
case class LogTopoReader(path: String) extends SysLogReader[LogTopologyCommand]("log_topology", path)
case class DataTopoReader(path: String) extends SysLogReader[TopologyCommand]("topology", path)
case class MembershipReader(path: String) extends SysLogReader[MembershipCommand]("membership", path)
case class WorkerIDReader(path: String) extends SysLogReader[WorkerIDCommand]("worker_id", path)

object BinaryLogReader {
  def read(logName: String, path: String): Boolean = {
    logName match {
      case "txn"           => TxnReader(path).print()
      case "log-topology"  => LogTopoReader(path).print()
      case "data-topology" => DataTopoReader(path).print()
      case "membership"    => MembershipReader(path).print()
      case "worker-id"     => WorkerIDReader(path).print()
      case l =>
        System.err.println(s"Unknown log: $l")
        System.err.println("Known logs are: txn, log-topology, data-topology, membership, worker-id")
        return false
    }
    true
  }
}
