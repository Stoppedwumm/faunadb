package fauna.tools

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.schema.NativeIndex
import fauna.repo.{ Executor => _, _ }
import fauna.storage._
import java.io.{ File, PrintStream }
import java.time.format.DateTimeParseException
import org.apache.commons.cli.Options
import scala.util.{ Failure, Success }

object IndexCompare extends SSTableApp("Index Compare") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(
      null,
      "index-id",
      true,
      "IndexID of the index to compare. Must be a native index. Required.")
    o.addOption(null, "metadata", true, "Metadata file. Required.")
    o.addOption(null, "output", true, "Output file. (default: stdout)")
    o.addOption(null, "scope-id", true, "ScopeID of the index to compare. Required.")
    o.addOption(null, "segments", true, "Number of segments (default: ncpu)")
    o.addOption(null, "snapshot", true, "Snapshot time")
    o.addOption(null, "threads", true, "Number of threads (default: ncpu)")
  }

  start {
    val metadata = Option(cli.getOptionValue("metadata")) match {
      case Some(path) =>
        val file = new File(path)

        SchemaMetadata.readFile(file) match {
          case Success(meta) => meta
          case Failure(ex) =>
            handler.warn("Cannot read metadata", ex)
            sys.exit(1)
        }

      case None =>
        handler.warn("--metadata is required.")
        sys.exit(1)
    }

    val output = Option(cli.getOptionValue("output")) match {
      case Some(path) => new PrintStream(new File(path))
      case None       => System.out
    }

    val scopeID = Option(cli.getOptionValue("scope-id")) flatMap {
      _.toLongOption
    } match {
      case Some(id) => ScopeID(id)
      case None =>
        handler.warn("--scope-id is required.")
        sys.exit(1)
    }

    val indexID = Option(cli.getOptionValue("index-id")) flatMap {
      _.toIntOption
    } match {
      case Some(id) => IndexID(id)
      case None =>
        handler.warn("--index-id is required.")
        sys.exit(1)
    }

    val index = NativeIndex(scopeID, indexID) match {
      case None =>
        handler.warn("--index-id is not a native index.")
        sys.exit(1)
      case Some(idx) => idx
    }

    val snapshotTS = Option(cli.getOptionValue("snapshot")) flatMap { str =>
      try {
        Some(Timestamp.parse(str))
      } catch {
        case ex: DateTimeParseException =>
          handler.warn(s"Bad snapshot time: $str", ex)
          sys.exit(1)
      }
    } getOrElse Timestamp.MaxMicros

    val subsegments = Option(cli.getOptionValue("segments")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors()

    val threads = Option(cli.getOptionValue("threads")) flatMap {
      _.toIntOption
    } getOrElse Runtime.getRuntime.availableProcessors()

    val primary = openColumnFamily(Tables.SortedIndex.CFName, withSSTables = true)
    val secondary = openColumnFamily(Tables.SortedIndex.CFName2, withSSTables = true)
    val executor = new Executor("Index Comparison", threads)
    val segments = Segment.All.subSegments(subsegments)
    val mvts = metadata.toMVTMap

    segments foreach { segment =>
      executor.addWorker { () =>
        val diff = new IndexDiff(
          primary,
          secondary,
          mvts,
          snapshotTS,
          segment,
          index.selector)

        diff.run() foreach {
          case e: IndexDiff.Add    => output.println(s"+ ${e.toJSON}")
          case e: IndexDiff.Remove => output.println(s"- ${e.toJSON}")
        }
      }
    }

    executor.waitWorkers()
    output.flush()

    sys.exit(0)
  }
}
