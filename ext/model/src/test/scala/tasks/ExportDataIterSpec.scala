package fauna.model.test

import fauna.atoms._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.runtime.fql2.serialization.ValueFormat
import fauna.model.tasks._
import fauna.model.tasks.export._
import fauna.repo._
import fauna.stats.StatsRequestBuffer
import fauna.storage.Selector
import java.nio.file.Path
import scala.concurrent.duration._
import scala.concurrent.Await
import ExportDataTask._

class ExportDataIterSpec extends ExportDataSpecHelpers {

  "ExportDataIter" - {
    "only scan versions before snapshotTS" in {
      val auth = newDB
      val exportPath = tempExportPath()

      val (collID, snapshotTS) = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time

        (collID, snapshotTS)
      }

      val stats = new StatsRequestBuffer

      val totalDocsExported =
        scan(
          ctx.withStats(stats),
          Segment.All,
          auth.scopeID,
          Seq(collID),
          snapshotTS,
          exportPath)

      totalDocsExported shouldBe 2L

      val files = getFiles(AccountID.Root, "1", TaskID(1), exportPath)
      files.size shouldBe 1
      readOutfiles(files).size shouldBe 2
    }

    "ignore versions after snapshotTS" in {
      val auth = newDB
      val exportPath = tempExportPath()

      val snapshotTS = Clock.time

      val collID = setupData(auth) { case (collID, _) =>
        collID
      }

      val stats = new StatsRequestBuffer

      val totalDocsExported =
        scan(
          ctx.withStats(stats),
          Segment.All,
          auth.scopeID,
          Seq(collID),
          snapshotTS,
          exportPath)

      totalDocsExported shouldBe 0L

      val files = getFiles(AccountID.Root, "1", TaskID(1), exportPath)
      files.size shouldBe 0
    }

    "export multiple collections" in {
      val auth = newDB

      val (colls, snapshotTS) = setupData(auth) { case (coll0, coll1) =>
        val snapshotTS = Clock.time

        (Seq(coll0, coll1), snapshotTS)
      }

      val stats = new StatsRequestBuffer

      val exportPath = tempExportPath()
      val totalDocsExported =
        scan(
          ctx.withStats(stats),
          Segment.All,
          auth.scopeID,
          colls,
          snapshotTS,
          exportPath)

      totalDocsExported shouldBe 5L

      val files = getFiles(AccountID.Root, "1", TaskID(1), exportPath)
      files.size shouldBe 2
      readOutfiles(files).size shouldBe 5
    }

    "retrying the same file index should not export data multiple times" in {
      val auth = newDB
      val exportPath = tempExportPath()

      val (collID, snapshotTS) = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time

        (collID, snapshotTS)
      }

      val stats = new StatsRequestBuffer

      // just repeatedly scan
      1 to 3 foreach { _ =>
        // scan() will start with the same file index each time
        val totalDocsExported =
          scan(
            ctx.withStats(stats),
            Segment.All,
            auth.scopeID,
            Seq(collID),
            snapshotTS,
            exportPath)

        totalDocsExported shouldBe 2L
      }

      val files = getFiles(AccountID.Root, "1", TaskID(1), exportPath)
      files.size shouldBe 1
      readOutfiles(files).size shouldBe 2
    }
  }

  def scan(
    ctx: RepoContext,
    segment: Segment,
    scope: ScopeID,
    collID: Seq[CollectionID],
    snapshotTS: Timestamp,
    exportPath: Path,
    pageSize: Option[Int] = None) = {

    val colls = collID map { id =>
      id -> (ctx ! Collection.get(scope, id)).value.config
    } toMap

    val info = ExportInfo(
      AccountID.Root,
      scope,
      collID.toVector,
      colls.values.map(_.name).toVector,
      "1",
      TaskID(1L),
      exportPath,
      snapshotTS,
      ValueFormat.Tagged,
      DatafileFormat.JSONL,
      false
    )

    FilesManager.init(TaskID(1L), info, colls, 0, Nil, Fragment.NullIdx)

    var cursor = Option.empty[TableScan.Cursor]
    var fragIdx = Fragment.NullIdx
    var docsExported = 0L

    do {
      fragIdx = fragIdx.incr
      val (iter, nextCursor) =
        step(ctx, segment, cursor, pageSize, fragIdx)

      cursor = moveCursor(cursor, nextCursor)
      docsExported += iter.totalDocsExported
    } while (cursor.nonEmpty)

    val manager = FilesManager
      .forIdx(TaskID(1L), fragIdx)

    manager.processOutfiles(Vector.empty, incremental = false)
    val toEmit = Await.result(manager.processOutfilesResult, 10.seconds)
    manager.processOutfiles(toEmit.map(_.path), incremental = false)
    Await.result(manager.processOutfilesResult, 10.seconds)

    FilesManager.drop(TaskID(1L))

    docsExported
  }

  def moveCursor(cursor: Option[TableScan.Cursor], result: TableScan.Result) =
    result match {
      case TableScan.Continue(cur, _, _) => Some(cur)
      case TableScan.Finished            => None
      case _: TableScan.Retry            => cursor
    }

  def step(
    ctx: RepoContext,
    segment: Segment,
    cursor: Option[TableScan.Cursor],
    pageSize: Option[Int],
    fragIdx: Fragment.Idx) = {

    val manager = FilesManager.forIdx(TaskID(1L), fragIdx)

    val scope = manager.info.scope
    val snapshotTS = manager.info.snapshotTS
    val collIDs = manager.collections.keys.toVector
    val iter = new ExportDataIter(manager)

    val nextQ = TableScan.paginate(
      "Export",
      segment,
      Some(snapshotTS),
      cursor,
      iter,
      selector = Selector.from(scope, collIDs),
      pageSize = pageSize)

    val nextCursor = ctx ! nextQ

    manager.close()

    (iter, nextCursor)
  }

  def getFiles(
    accountID: AccountID,
    externalID: String,
    id: TaskID,
    exportPath: Path): List[Path] = {
    val info = ExportInfo(
      accountID,
      ScopeID(Long.MaxValue), // dummy
      Vector.empty, // dummy
      Vector.empty, // dummy
      externalID,
      id,
      exportPath,
      Timestamp.MaxMicros,
      ValueFormat.Tagged,
      DatafileFormat.JSONL,
      false
    )
    info.getBasePath(isTemp = false).findAllRecursively
  }
}
