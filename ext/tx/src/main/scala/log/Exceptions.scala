package fauna.tx.log

import fauna.lang.syntax._
import java.nio.file.Path

class LogClosedException extends IllegalStateException("Log closed.")

case class TruncatedLogException[I](prevIdx: I) extends NoSuchElementException(
  s"Log history truncated up to $prevIdx")

sealed abstract class InvalidLogException(msg: String) extends Exception(msg)

case class LogUnknownFormatException(path: Path) extends InvalidLogException(
  s"Unknown log format at $path")

case class LogUnknownVersionException(path: Path) extends InvalidLogException(
  s"Unknown log format version in $path")

case class LogEOFException(path: Path) extends InvalidLogException(
  s"Unexpected end of log file $path")

case class LogEntrySizeException(path: Path, size: Int) extends InvalidLogException(
  s"Invalid entry size in log file $path")

case class LogChecksumException(path: Path, tx: TX) extends InvalidLogException(
  s"$tx in $path does not match checksum.")

case class LogMissingFileException(dir: Path, name: String, ordinal: Long) extends InvalidLogException(
  s"Missing log file ${dir / name + s".binlog.$ordinal"}")

case class LogInvalidFileException(dir: Path, name: String, from: Long, to: Long, ordinal: Long) extends InvalidLogException(
  s"File outside of expected log file range $from-$to ${dir / name + s".binlog.$ordinal"}")

case class LogMissingTransactionsException(path: Path, startTX: TX, endTX: TX) extends InvalidLogException(
  s"Missing transactions from $startTX to $endTX in $path")
