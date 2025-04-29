package fauna.model.test

import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.runtime.fql2.serialization.ValueFormat
import fauna.model.schema.{ CollectionConfig, NativeIndex }
import fauna.model.tasks._
import fauna.model.tasks.export._
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.values.Value
import fauna.stats.StatsRequestBuffer
import fauna.storage.doc.Field
import fauna.storage.index.IndexTerm
import java.io.{ BufferedReader, InputStreamReader }
import java.nio.file.{ Files, Path }
import scala.jdk.CollectionConverters._
import scala.util.Random
import ExportDataTask._

class ExportDataTaskSpec extends ExportDataSpecHelpers {

  "ExportDataTaskSpec - MVTPin" - {
    // Stale DB entries mess up checking the pins,
    // but not adding or removing them.
    def invalidate() = ctx.cacheContext.invalidateAll()

    "export task pins and unpins MVT" in {
      val auth = newDB

      // Start an export for each collection.
      val (snapshotTS, task2) = setupData(auth) { case (collID1, collID2) =>
        val snapshotTS = Clock.time

        ctx ! tableScanTask("1", auth, Seq(collID1), snapshotTS)

        val task2 = ctx ! tableScanTask("2", auth, Seq(collID2), snapshotTS)

        (snapshotTS, task2)
      }

      // Pin is in place.
      (ctx ! Database.pinnedMVTForScope(auth.scopeID)).get shouldBe snapshotTS

      // Pause one task so we can check that pins remain until all exports on the
      // scope are done.
      val pausedTask = ctx ! task2.pause("test")

      // Run the other task and check the pin is still there.
      val stats = new StatsRequestBuffer
      val exec = taskExecutor(stats, tempExportPath())
      processAllTasks(exec)
      invalidate()
      (ctx ! Database.pinnedMVTForScope(auth.scopeID)).get shouldBe snapshotTS

      // Unpause and run the paused task and check the pin is gone.
      ctx ! pausedTask.unpause()
      processAllTasks(exec)
      invalidate()
      (ctx ! Database.pinnedMVTForScope(auth.scopeID)).isEmpty shouldBe true
    }

    "export task unpins MVT when canceled" in {
      val auth = newDB

      val (task, snapshotTS) = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time
        val task = ctx ! tableScanTask("1", auth, Seq(collID), snapshotTS)
        (task, snapshotTS)
      }

      // Pin is in place.
      invalidate()
      (ctx ! Database.pinnedMVTForScope(auth.scopeID)).get shouldBe snapshotTS

      // Cancel the task and check the pin is removed.
      ctx ! TaskRouter.cancel(task, None)
      invalidate()
      (ctx ! Database.pinnedMVTForScope(auth.scopeID)).isEmpty shouldBe true
    }
  }

  "ExportDataTaskSpec - TableScan" - {
    "only scan versions before snapshotTS" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time
        ctx0 ! tableScanTask("1", auth, Seq(collID), snapshotTS)
        snapshotTS
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 2

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 2

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    "ignore versions after snapshotTS" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = Clock.time

      setupData(auth) { case (collID, _) =>
        ctx0 ! tableScanTask("1", auth, Seq(collID), snapshotTS)
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 0L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)

      // only the metadata file
      files.size shouldBe 1

      val data = dataFiles(files)
      val lines = readOutfiles(data)
      lines.size shouldBe 0

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 0,
        "files_bytes" -> 0
      )
    }

    "export only the latest version if docs span multiple pages" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time
        ctx0 ! tableScanTask("1", auth, Seq(collID), snapshotTS)
        snapshotTS
      }

      val stats = new StatsRequestBuffer

      val exec = taskExecutor(stats, exportPath)
      exec.step()

      val task = getTableScanTask(auth.scopeID)
      ctx0 ! Task.runnable(
        task,
        task.data.update(TableScan.PageSizeField -> Some(1)),
        task.parent,
        0)

      processAllTasks(exec)

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 2

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 2

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    "retry should not export data multiple times" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val stats = new StatsRequestBuffer
      val ctx0 = ctx.withStats(stats).copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time
        ctx0 ! tableScanTask("1", auth, Seq(collID), snapshotTS)
        snapshotTS
      }

      val exec = taskExecutor(stats, exportPath)

      exec.step()

      var task = getTableScanTask(auth.scopeID)

      // reach deep into the bowels of the executor...
      task = task.copy(state = ctx0 ! exec.router.preStep(task))
      task = task.copy(state = ctx0 ! exec.router(task, Clock.time))
      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

      // oops, no checkpoint! Flush writes so we get a consistent on-disk presence.
      FilesManager.resetAll()

      val rootTask = getRootTask(auth.scopeID)

      val files0 = getTempFiles(rootTask, exportPath)
      files0.size shouldBe 1
      readFragments(files0).size shouldBe 2

      // run task to completion
      processAllTasks(exec)

      // we still end up re-exporting to temp files.
      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 4L

      val files1 = getFinalFiles(rootTask, exportPath)

      // data and metadata
      files1.size shouldBe 2

      val data = dataFiles(files1)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 2

      readMetadata(metadataFile(files1)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    Seq(true, false) foreach { retry =>
      s"update documents during export: retry=$retry" in {
        val auth = newDB
        val exportPath = tempExportPath()
        val stats = new StatsRequestBuffer
        val ctx0 = ctx.withStats(stats).copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (collID, _) =>
          val snapshotTS = Clock.time
          ctx0 ! tableScanTask("1", auth, Seq(collID), snapshotTS)
          snapshotTS
        }

        val exec = taskExecutor(stats, exportPath)

        // fork the table scan
        exec.step()

        // setup retry scenario
        if (retry) {
          var task = getTableScanTask(auth.scopeID)

          // reach deep into the bowels of the executor...
          task = task.copy(state = ctx0 ! exec.router.preStep(task))
          task = task.copy(state = ctx0 ! exec.router(task, Clock.time))
          stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

          // oops, no checkpoint! Flush writes so we get a consistent on-disk
          // presence.
          FilesManager.resetAll()
        }

        val rootTask = getRootTask(auth.scopeID)

        getFinalFiles(rootTask, exportPath).size shouldBe 0

        // update the documents
        evalOk(auth, "Coll0.all().map(.id.toString()).toArray()")
          .as[Vector[String]] foreach { id =>
          (0 to 10) foreach { i =>
            evalOk(auth, s"Coll0.byId($id)!.update({desc: 'update: $i'})")
          }
        }

        // finish the tasks
        processAllTasks(exec)

        if (retry) {
          stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 4L
        } else {
          stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L
        }

        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 2

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data)
        lines.size shouldBe 2
        lines.exists(_ contains "update") shouldBe false

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> "tagged",
          "datafile_format" -> "jsonl",
          "datafile_compression" -> false,
          "datafile_ext" -> "jsonl",
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 1,
          "files_bytes" -> bytes
        )
      }
    }

    "export multiple collections" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (coll0, coll1) =>
        val snapshotTS = Clock.time
        ctx0 ! tableScanTask("1", auth, Seq(coll0, coll1), snapshotTS)
        snapshotTS
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 5L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 3

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 5

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0", "Coll1"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 2,
        "files_bytes" -> bytes
      )
    }

    "export Credential documents" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (_, _) => }

      val id = evalOk(auth, s"Coll0.all().first()!.id").as[Long]
      evalOk(auth, s"Credential.create({ document: Coll0('$id'), password: 'foo' })")

      val snapshotTS = Clock.time
      ctx0 ! tableScanTask("1", auth, Seq(CredentialsID.collID), snapshotTS)

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 1L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 2

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 1

      lines.head should include("hashed_password")

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Credential"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    "no Credential files if no docs" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (_, _) => }

      val snapshotTS = Clock.time
      ctx0 ! tableScanTask("1", auth, Seq(CredentialsID.collID), snapshotTS)

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 0L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 1

      val data = dataFiles(files)
      val lines = readOutfiles(data)
      lines.size shouldBe 0

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Credential"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 0,
        "files_bytes" -> 0
      )
    }

    "cancel if collection is deleted" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! tableScanTask("1", auth, Seq(collID), Clock.time)
      }

      evalOk(auth, "Coll1.definition.delete()")
      evalOk(auth, "Coll0.definition.delete()")

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // root task is cancelled
      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("collection deleted")

      // table scan is cancelled
      val scan = getTableScanTask(auth.scopeID)
      scan.isCancelled shouldBe true
      scan.data(ExportDataTask.ErrorField) shouldBe Some("collection deleted")

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 0L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "cancel if collection schema is updated" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! tableScanTask("1", auth, Seq(collID), Clock.time)
      }

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Coll0 {
             |  desc: String
             |  test: String?
             |
             |  compute computedField = (v) => ">>>>>>" + v.desc + "<<<<<<"
             |
             |  migrations {
             |    add .test
             |  }
             |}
             |""".stripMargin
      )

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // root task is cancelled
      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("schema changed")

      // table scan is cancelled
      val scan = getTableScanTask(auth.scopeID)
      scan.isCancelled shouldBe true
      scan.data(ExportDataTask.ErrorField) shouldBe Some("schema changed")

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 0L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "export with schema changes" - {
      def validate(staged: Boolean): Unit = {
        val auth = newDB
        val exportPath = tempExportPath()
        val ctx0 = ctx.copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (collID, _) =>
          updateSchema0(
            auth,
            overrideMode = true,
            pin = staged,
            "main.fsl" ->
              """|collection Coll0 {
                 |  desc: String
                 |  staged: String = "staged field"
                 |
                 |  migrations {
                 |    add .staged
                 |  }
                 |}
                 |
                 |collection Coll1 {
                 |  desc: String
                 |  doc: Ref<Coll0>
                 |}
                 |""".stripMargin
          )

          val ts = Clock.time
          ctx0 ! tableScanTask("1", auth, Seq(collID), ts)
          ts
        }

        val stats = new StatsRequestBuffer

        processAllTasks(taskExecutor(stats, exportPath))

        stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

        val rootTask = getRootTask(auth.scopeID)
        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 2

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data)
        lines.size shouldBe 2

        lines map (l => JSON.parse[JSValue](l.getBytes)) foreach { doc =>
          (doc / "id").asOpt[JSValue] should not be None
          (doc / "coll").asOpt[JSValue] should not be None
          (doc / "ts").asOpt[JSValue] should not be None

          (doc / "desc").as[String] shouldBe "bar32"

          if (staged) {
            (doc / "staged").asOpt[JSValue] shouldBe None
          } else {
            (doc / "staged").as[String] shouldBe "staged field"
          }
        }

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> "tagged",
          "datafile_format" -> "jsonl",
          "datafile_compression" -> false,
          "datafile_ext" -> "jsonl",
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 1,
          "files_bytes" -> bytes
        )
      }

      "staged" in {
        validate(staged = true)
      }

      "committed" in {
        validate(staged = false)
      }
    }

    "export with migration" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (collID, _) =>
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|collection Coll0 {
                 |  desc: String
                 |  migrated: String = "migrated field"
                 |
                 |  migrations {
                 |    add .migrated
                 |  }
                 |}
                 |
                 |collection Coll1 {
                 |  desc: String
                 |  doc: Ref<Coll0>
                 |}
                 |""".stripMargin
        )

        val ts = Clock.time
        ctx0 ! MigrationTask.Root.create(auth.scopeID, collID, isOperational = true)
        ctx0 ! tableScanTask("1", auth, Seq(collID), ts)
        ts
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 2

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 2

      lines map (l => JSON.parse[JSValue](l.getBytes)) foreach { doc =>
        (doc / "id").asOpt[JSValue] should not be None
        (doc / "coll").asOpt[JSValue] should not be None
        (doc / "ts").asOpt[JSValue] should not be None

        (doc / "desc").as[String] shouldBe "bar32"
        (doc / "migrated").as[String] shouldBe "migrated field"
      }

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    "cancel if cannot write data" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! tableScanTask("1", auth, Seq(collID), Clock.time)
      }

      val stats = new StatsRequestBuffer

      {
        // mark temp folder as readonly to cause a failure on ExportDataTask.Scan
        val root = getRootTask(auth.scopeID)
        val path = ExportInfo(exportPath, root).getBasePath(isTemp = true)
        path.toFile.mkdirs()
        path.toFile.setReadOnly()
      }

      processAllTasks(taskExecutor(stats, exportPath))

      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("internal error")

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 0L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "cancel if cannot move files" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! tableScanTask("1", auth, Seq(collID), Clock.time)
      }

      val stats = new StatsRequestBuffer

      {
        // mark data folder as readonly to cause a failure on ExportDataTask.Scan
        val root = getRootTask(auth.scopeID)
        val path = ExportInfo(exportPath, root).getBasePath(isTemp = false)
        path.toFile.mkdirs()
        path.toFile.setReadOnly()
      }

      processAllTasks(taskExecutor(stats, exportPath))

      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("internal error")

      // they still exported
      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "cancel running export" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! tableScanTask("1", auth, Seq(collID), Clock.time)
      }

      val stats = new StatsRequestBuffer

      val exec = taskExecutor(stats, exportPath)
      exec.step() // fork table scan

      val root = getRootTask(auth.scopeID)
      ctx0 ! TaskRouter.cancel(root, None)

      processAllTasks(exec)

      getRootTask(auth.scopeID).state shouldBe Task.Cancelled()
      getTableScanTask(auth.scopeID).state shouldBe Task.Cancelled()

      stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 0L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "cancel running export should clean temp folder" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (coll1, coll2) =>
        ctx0 ! tableScanTask("1", auth, Seq(coll1, coll2), Clock.time)
      }

      val stats = new StatsRequestBuffer

      val exec = taskExecutor(stats, exportPath)
      exec.step() // fork table scan

      var task = getTableScanTask(auth.scopeID)
      task = task.copy(state = ctx0 ! exec.router.preStep(task))
      task = task.copy(state = ctx0 ! exec.router(task, Clock.time))

      val root = getRootTask(auth.scopeID)

      getTempFiles(root, exportPath).size shouldBe 2

      ctx0 ! TaskRouter.cancel(root, None)

      // no account dir
      Files.exists(
        ExportInfo(exportPath, root)
          .getBasePath(isTemp = true)
          .getParent
          .getParent) shouldBe false
      getTempFiles(root, exportPath).size shouldBe 0
    }

    "cancel export if MVT is above snapshot_ts" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (coll1, coll2) =>
        val ts = Clock.time
        ctx0 ! tableScanTask("1", auth, Seq(coll1, coll2), ts)
        ts
      }

      {
        val root = getRootTask(auth.scopeID)
        val data =
          root.data.update(
            ExportDataTask.SnapshotTSField -> (snapshotTS.prevMicro - Collection.MVTOffset))
        ctx ! Task.write(root.copy(state = root.state.withData(data)))
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // root task is cancelled
      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("mvt is above snapshot_ts")

      // table scan is cancelled
      val scan = getTableScanTask(auth.scopeID)
      scan.isCancelled shouldBe true
      scan.data(ExportDataTask.ErrorField) shouldBe Some("mvt is above snapshot_ts")
    }

    Seq(ValueFormat.Simple, ValueFormat.Tagged) foreach { format =>
      s"validate doc output: $format" in {
        val auth = newDB
        val exportPath = tempExportPath()
        val ctx0 = ctx.copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (coll0, coll1) =>
          val snapshotTS = Clock.time
          ctx0 ! tableScanTask("1", auth, Seq(coll0, coll1), snapshotTS, format)
          snapshotTS
        }

        val stats = new StatsRequestBuffer

        processAllTasks(taskExecutor(stats, exportPath))

        stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 5L

        val rootTask = getRootTask(auth.scopeID)
        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 3

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data)
        lines.size shouldBe 5

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0", "Coll1"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> format.toString,
          "datafile_format" -> "jsonl",
          "datafile_compression" -> false,
          "datafile_ext" -> "jsonl",
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 2,
          "files_bytes" -> bytes
        )

        val coll0Docs = getVersions(auth, "Coll0", snapshotTS) map { v =>
          renderDocWithComputeField(format, v.id, v.ts.validTS, v.data(DescField))
        }
        val coll1Docs = getVersions(auth, "Coll1", snapshotTS) map { v =>
          renderDocWithRef(
            format,
            v.id,
            v.ts.validTS,
            v.data(DescField),
            v.data(DocField))
        }

        (coll0Docs ++ coll1Docs).toSet shouldBe lines.toSet
      }
    }

    "validate datafile format" - {
      def testOutput(fileFormat: DatafileFormat) = {
        val auth = newDB
        val exportPath = tempExportPath()
        val ctx0 = ctx.copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (coll0, _) =>
          val snapshotTS = Clock.time
          ctx0 ! tableScanTask(
            "1",
            auth,
            Seq(coll0),
            snapshotTS,
            datafileFormat = fileFormat)
          snapshotTS
        }

        val stats = new StatsRequestBuffer

        processAllTasks(taskExecutor(stats, exportPath))

        stats.countOrZero("Task.ExportDataTableScan.DocsExported") shouldBe 2L

        val rootTask = getRootTask(auth.scopeID)
        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 2

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data, fileFormat)
        lines.size shouldBe 2

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> "tagged",
          "datafile_format" -> fileFormat.toString,
          "datafile_compression" -> false,
          "datafile_ext" -> fileFormat.toString,
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 1,
          "files_bytes" -> bytes
        )
      }

      "JSONL" in {
        testOutput(DatafileFormat.JSONL)
      }

      "JSON" in {
        testOutput(DatafileFormat.JSON)
      }
    }

    "assign correct priorities" in {
      val auth = newDB(100)
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (coll1, coll2) =>
        ctx0 ! tableScanTask("1", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! tableScanTask("2", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! tableScanTask("3", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! tableScanTask("4", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! tableScanTask("5", auth, Seq(coll1, coll2), Clock.time)
      }

      val stats = new StatsRequestBuffer

      val exec = taskExecutor(stats, exportPath)
      exec.step() // fork table scan

      val roots = ctx ! getTask(auth.scopeID, Root.name)
      roots.map(_.priority) shouldBe Seq(0, -10, -20, -30, -40)

      (ctx ! getTask(auth.scopeID, ByIndex.Root.name)) should be(empty)
    }
  }

  "ExportDataTaskSpec - IndexScan" - {
    "only scan versions before snapshotTS" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (collID, _) =>
        val snapshotTS = Clock.time
        ctx0 ! indexScanTask("1", auth, Seq(collID), snapshotTS)
        snapshotTS
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 2L

      // no table scan task should be created
      (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(empty)

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 2

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 2

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    "ignore versions after snapshotTS" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = Clock.time

      setupData(auth) { case (collID, _) =>
        ctx0 ! indexScanTask("1", auth, Seq(collID), snapshotTS)
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 0L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)

      // only the metadata file
      files.size shouldBe 1

      val data = dataFiles(files)
      val lines = readOutfiles(data)
      lines.size shouldBe 0

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 0,
        "files_bytes" -> 0
      )
    }

    Seq(true, false) foreach { retry =>
      s"update documents during export: retry=$retry" in {
        val auth = newDB
        val exportPath = tempExportPath()
        val stats = new StatsRequestBuffer
        val ctx0 = ctx.withStats(stats).copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (collID, _) =>
          val snapshotTS = Clock.time
          ctx0 ! indexScanTask("1", auth, Seq(collID), snapshotTS)
          snapshotTS
        }

        val exec = taskExecutor(stats, exportPath)

        // fork the index root
        exec.step()
        // fork the index scan
        exec.step()

        // setup retry scenario
        if (retry) {
          var task = getIndexScanTask(auth.scopeID)

          // reach deep into the bowels of the executor...
          task = task.copy(state = ctx0 ! exec.router.preStep(task))
          task = task.copy(state = ctx0 ! exec.router(task, Clock.time))
          stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 2L

          // oops, no checkpoint! Flush writes so we get a consistent on-disk
          // presence.
          FilesManager.resetAll()
        }

        val rootTask = getRootTask(auth.scopeID)

        getFinalFiles(rootTask, exportPath).size shouldBe 0

        // update the documents
        evalOk(auth, "Coll0.all().map(.id.toString()).toArray()")
          .as[Vector[String]] foreach { id =>
          (0 to 10) foreach { i =>
            evalOk(auth, s"Coll0.byId($id)!.update({desc: 'update: $i'})")
          }
        }

        // finish the tasks
        processAllTasks(exec)

        // no table scan task should be created
        (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(
          empty)

        if (retry) {
          stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 4L
        } else {
          stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 2L
        }

        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 2

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data)
        lines.size shouldBe 2
        lines.exists(_ contains "update") shouldBe false

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> "tagged",
          "datafile_format" -> "jsonl",
          "datafile_compression" -> false,
          "datafile_ext" -> "jsonl",
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 1,
          "files_bytes" -> bytes
        )
      }
    }

    "export multiple collections" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (coll0, coll1) =>
        val snapshotTS = Clock.time
        ctx0 ! indexScanTask("1", auth, Seq(coll0, coll1), snapshotTS)
        snapshotTS
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // no table scan task should be created
      (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(empty)

      getIndexScanTask(auth.scopeID).state shouldBe Task.Completed()

      stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 5L

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 3

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 5

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0", "Coll1"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 2,
        "files_bytes" -> bytes
      )
    }

    "cancel if collection is deleted" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! indexScanTask("1", auth, Seq(collID), Clock.time)
      }

      evalOk(auth, "Coll1.definition.delete()")
      evalOk(auth, "Coll0.definition.delete()")

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // root task is cancelled
      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("collection deleted")

      // index scan is cancelled
      val idx = getIndexScanTask(auth.scopeID)
      idx.isCancelled shouldBe true
      idx.data(ExportDataTask.ErrorField) shouldBe Some("collection deleted")

      // no table scan task should be created
      (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(empty)

      stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 0L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "cancel if collection schema is updated" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (collID, _) =>
        ctx0 ! indexScanTask("1", auth, Seq(collID), Clock.time)
      }

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Coll0 {
             |  desc: String
             |  test: String?
             |
             |  compute computedField = (v) => ">>>>>>" + v.desc + "<<<<<<"
             |
             |  migrations {
             |    add .test
             |  }
             |}
             |""".stripMargin
      )

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // root task is cancelled
      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("schema changed")

      // index scan is cancelled
      val idx = getIndexScanTask(auth.scopeID)
      idx.isCancelled shouldBe true
      idx.data(ExportDataTask.ErrorField) shouldBe Some("schema changed")

      // no table scan task should be created
      (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(empty)

      stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 0L

      getFinalFiles(root, exportPath).size shouldBe 0
    }

    "export with schema changes" - {
      def validate(staged: Boolean): Unit = {
        val auth = newDB
        val exportPath = tempExportPath()
        val ctx0 = ctx.copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (collID, _) =>
          updateSchema0(
            auth,
            overrideMode = true,
            pin = staged,
            "main.fsl" ->
              """|collection Coll0 {
                 |  desc: String
                 |  staged: String = "staged field"
                 |
                 |  migrations {
                 |    add .staged
                 |  }
                 |}
                 |
                 |collection Coll1 {
                 |  desc: String
                 |  doc: Ref<Coll0>
                 |}
                 |""".stripMargin
          )

          val ts = Clock.time
          ctx0 ! indexScanTask("1", auth, Seq(collID), ts)
          ts
        }

        val stats = new StatsRequestBuffer

        processAllTasks(taskExecutor(stats, exportPath))

        stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 2L

        // no table scan task should be created
        (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(
          empty)

        val rootTask = getRootTask(auth.scopeID)
        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 2

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data)
        lines.size shouldBe 2

        lines map (l => JSON.parse[JSValue](l.getBytes)) foreach { doc =>
          (doc / "id").asOpt[JSValue] should not be None
          (doc / "coll").asOpt[JSValue] should not be None
          (doc / "ts").asOpt[JSValue] should not be None

          (doc / "desc").as[String] shouldBe "bar32"

          if (staged) {
            (doc / "staged").asOpt[JSValue] shouldBe None
          } else {
            (doc / "staged").as[String] shouldBe "staged field"
          }
        }

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> "tagged",
          "datafile_format" -> "jsonl",
          "datafile_compression" -> false,
          "datafile_ext" -> "jsonl",
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 1,
          "files_bytes" -> bytes
        )
      }

      "staged" in {
        validate(staged = true)
      }

      "committed" in {
        validate(staged = false)
      }
    }

    "export with migration" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (collID, _) =>
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|collection Coll0 {
               |  desc: String
               |  migrated: String = "migrated field"
               |
               |  migrations {
               |    add .migrated
               |  }
               |}
               |
               |collection Coll1 {
               |  desc: String
               |  doc: Ref<Coll0>
               |}
               |""".stripMargin
        )

        val ts = Clock.time
        ctx0 ! MigrationTask.Root.create(auth.scopeID, collID, isOperational = true)
        ctx0 ! indexScanTask("1", auth, Seq(collID), ts)
        ts
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 2L

      // no table scan task should be created
      (ctx0 ! getTask(auth.scopeID, ExportDataTask.TableScan.name)) should be(empty)

      val rootTask = getRootTask(auth.scopeID)
      val files = getFinalFiles(rootTask, exportPath)
      files.size shouldBe 2

      val data = dataFiles(files)
      val bytes = filesBytes(data)
      val lines = readOutfiles(data)
      lines.size shouldBe 2

      lines map (l => JSON.parse[JSValue](l.getBytes)) foreach { doc =>
        (doc / "id").asOpt[JSValue] should not be None
        (doc / "coll").asOpt[JSValue] should not be None
        (doc / "ts").asOpt[JSValue] should not be None

        (doc / "desc").as[String] shouldBe "bar32"
        (doc / "migrated").as[String] shouldBe "migrated field"
      }

      readMetadata(metadataFile(files)) shouldBe JSObject(
        "host_id" -> rootTask.host.toString,
        "account_id" -> auth.accountID.toLong.toString,
        "export_id" -> "1",
        "task_id" -> rootTask.id.toLong.toString,
        "collections" -> Seq("Coll0"),
        "snapshot_ts" -> snapshotTS.toString,
        "doc_format" -> "tagged",
        "datafile_format" -> "jsonl",
        "datafile_compression" -> false,
        "datafile_ext" -> "jsonl",
        "part" -> 0,
        "total_parts" -> 1,
        "files_count" -> 1,
        "files_bytes" -> bytes
      )
    }

    "cancel running export should clean temp folder" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (coll1, coll2) =>
        ctx0 ! indexScanTask("1", auth, Seq(coll1, coll2), Clock.time)
      }

      val stats = new StatsRequestBuffer

      val exec = taskExecutor(stats, exportPath)

      // fork the index root
      exec.step()
      // fork the index scan
      exec.step()

      var task = getIndexScanTask(auth.scopeID)
      task = task.copy(state = ctx0 ! exec.router.preStep(task))
      task = task.copy(state = ctx0 ! exec.router(task, Clock.time))

      val root = getRootTask(auth.scopeID)

      getTempFiles(root, exportPath).size shouldBe 2

      ctx0 ! TaskRouter.cancel(root, None)

      // no account dir
      Files.exists(
        ExportInfo(exportPath, root)
          .getBasePath(isTemp = true)
          .getParent
          .getParent) shouldBe false
      getTempFiles(root, exportPath).size shouldBe 0
    }

    "cancel export if MVT is above snapshot_ts" in {
      val auth = newDB
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      val snapshotTS = setupData(auth) { case (coll1, coll2) =>
        val ts = Clock.time
        ctx0 ! indexScanTask("1", auth, Seq(coll1, coll2), ts)
        ts
      }

      {
        val root = getRootTask(auth.scopeID)
        val data =
          root.data.update(
            ExportDataTask.SnapshotTSField -> (snapshotTS.prevMicro - Collection.MVTOffset))
        ctx ! Task.write(root.copy(state = root.state.withData(data)))
      }

      val stats = new StatsRequestBuffer

      processAllTasks(taskExecutor(stats, exportPath))

      // root task is cancelled
      val root = getRootTask(auth.scopeID)
      root.isCancelled shouldBe true
      root.data(ExportDataTask.ErrorField) shouldBe Some("mvt is above snapshot_ts")

      // index scan is cancelled
      val scan = getIndexScanTask(auth.scopeID)
      scan.isCancelled shouldBe true
      scan.data(ExportDataTask.ErrorField) shouldBe Some("mvt is above snapshot_ts")
    }

    Seq(ValueFormat.Simple, ValueFormat.Tagged) foreach { format =>
      s"validate doc output: $format" in {
        val auth = newDB
        val exportPath = tempExportPath()
        val ctx0 = ctx.copy(exportPath = Some(exportPath))

        val snapshotTS = setupData(auth) { case (coll0, coll1) =>
          val snapshotTS = Clock.time
          ctx0 ! indexScanTask("1", auth, Seq(coll0, coll1), snapshotTS, format)
          snapshotTS
        }

        val stats = new StatsRequestBuffer

        processAllTasks(taskExecutor(stats, exportPath))

        stats.countOrZero("Task.ExportDataIndexScan.DocsExported") shouldBe 5L

        val rootTask = getRootTask(auth.scopeID)
        val files = getFinalFiles(rootTask, exportPath)
        files.size shouldBe 3

        val data = dataFiles(files)
        val bytes = filesBytes(data)
        val lines = readOutfiles(data)
        lines.size shouldBe 5

        readMetadata(metadataFile(files)) shouldBe JSObject(
          "host_id" -> rootTask.host.toString,
          "account_id" -> auth.accountID.toLong.toString,
          "export_id" -> "1",
          "task_id" -> rootTask.id.toLong.toString,
          "collections" -> Seq("Coll0", "Coll1"),
          "snapshot_ts" -> snapshotTS.toString,
          "doc_format" -> format.toString,
          "datafile_format" -> "jsonl",
          "datafile_compression" -> false,
          "datafile_ext" -> "jsonl",
          "part" -> 0,
          "total_parts" -> 1,
          "files_count" -> 2,
          "files_bytes" -> bytes
        )

        val coll0Docs = getVersions(auth, "Coll0", snapshotTS) map { v =>
          renderDocWithComputeField(format, v.id, v.ts.validTS, v.data(DescField))
        }
        val coll1Docs = getVersions(auth, "Coll1", snapshotTS) map { v =>
          renderDocWithRef(
            format,
            v.id,
            v.ts.validTS,
            v.data(DescField),
            v.data(DocField))
        }

        (coll0Docs ++ coll1Docs).toSet shouldBe lines.toSet
      }
    }

    "assign correct priorities" in {
      val auth = newDB(200)
      val exportPath = tempExportPath()
      val ctx0 = ctx.copy(exportPath = Some(exportPath))

      setupData(auth) { case (coll1, coll2) =>
        ctx0 ! indexScanTask("1", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! indexScanTask("2", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! indexScanTask("3", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! indexScanTask("4", auth, Seq(coll1, coll2), Clock.time)
        ctx0 ! indexScanTask("5", auth, Seq(coll1, coll2), Clock.time)
      }

      val stats = new StatsRequestBuffer

      val exec = taskExecutor(stats, exportPath)

      // fork the index root
      exec.step()
      // fork the index scan
      exec.step()

      val roots = ctx ! getTask(auth.scopeID, Root.name)
      roots.map(_.priority) shouldBe Seq(0, -10, -20, -30, -40)

      val indexTasks = ctx ! getTask(auth.scopeID, ByIndex.Root.name)
      indexTasks.map(_.priority) shouldBe Seq(1, -9, -19, -29, -39)

      val scanTasks = ctx ! getTask(auth.scopeID, ByIndex.Leaf.name)
      scanTasks.map(_.priority) shouldBe Seq(2, -8, -18, -28, -38)
    }
  }

  val DescField = Field[String]("data", "desc")
  val DocField = Field[DocID]("data", "doc")

  private def getCollNames(auth: Auth, collIDs: Seq[CollectionID]) =
    collIDs.map(CollectionConfig(auth.scopeID, _)).sequence.map(_.map(_.get.name))

  def tableScanTask(
    id: String,
    auth: Auth,
    collIDs: Seq[CollectionID],
    snapTS: Timestamp,
    docFormat: ValueFormat = ValueFormat.Tagged,
    datafileFormat: DatafileFormat = DatafileFormat.JSONL) =
    getCollNames(auth, collIDs).flatMap { collNames =>
      ExportDataTask.tableScan(
        id,
        auth.scopeID,
        collIDs.toVector,
        collNames.toVector,
        snapTS,
        docFormat,
        datafileFormat,
        false)
    }

  def indexScanTask(
    id: String,
    auth: Auth,
    collIDs: Seq[CollectionID],
    snapTS: Timestamp,
    docFormat: ValueFormat = ValueFormat.Tagged,
    datafileFormat: DatafileFormat = DatafileFormat.JSONL) =
    getCollNames(auth, collIDs).flatMap { collNames =>
      ExportDataTask.indexScan(
        id,
        auth.scopeID,
        collIDs.toVector,
        collNames.toVector,
        snapTS,
        docFormat,
        datafileFormat,
        false)
    }

  def getRootTask(scopeID: ScopeID) =
    (ctx ! getTask(scopeID, Root.name)).head

  def getTableScanTask(scopeID: ScopeID) =
    (ctx ! getTask(scopeID, TableScan.name)).head

  def getIndexScanTask(scopeID: ScopeID) =
    (ctx ! getTask(scopeID, ByIndex.Leaf.name)).head

  def getFinalFiles(task: Task, exportPath: Path): List[Path] = {
    val info = ExportInfo(exportPath, task)
    info.getBasePath(isTemp = false).findAllRecursively
  }

  def getTempFiles(task: Task, exportPath: Path) = {
    val info = ExportInfo(exportPath, task)
    info.getBasePath(isTemp = true).findAllRecursively
  }

  private def isMetadataFile(p: Path) =
    p.getFileName.toString.matches("metadata_\\d\\d.json")

  def dataFiles(files: List[Path]) =
    files.filterNot(isMetadataFile)

  def metadataFile(files: List[Path]) = files.find(isMetadataFile).value

  def filesBytes(files: List[Path]) = files.map(Files.size).sum

  def getVersions(auth: Auth, coll: String, snapshotTS: Timestamp): Seq[Version] = {

    val store = RuntimeEnv.Default.Store(auth.scopeID)

    val docs = evalOk(auth, s"at(Time('$snapshotTS')) { $coll.all().toArray() }")
      .as[Vector[Value]]

    val versionsQ = docs map { doc =>
      val id = doc.asInstanceOf[Value.Doc]

      store.getVersion(id.id, snapshotTS)
    } sequence

    (ctx ! versionsQ).flatten
  }

  // I'm deliberately interpolating strings here to reinforce the format of the
  // output data.
  def renderDocWithComputeField(
    format: ValueFormat,
    id: DocID,
    ts: Timestamp,
    desc: String): String = format match {
    case ValueFormat.Simple =>
      s"""{"id":"${id.subID.toLong}","coll":"Coll0","ts":"$ts","desc":"$desc"}"""
    case ValueFormat.Tagged =>
      s"""{"id":"${id.subID.toLong}","coll":{"@mod":"Coll0"},"ts":{"@time":"$ts"},"desc":"$desc"}"""
    case ValueFormat.Decorated =>
      s"""\"{\\n  id: \\"${id.subID.toLong}\\",\\n  coll: Coll0,\\n  ts: Time(\\"$ts\\"),\\n  desc: \\"$desc\\"\\n}\""""
  }

  // I'm deliberately interpolating strings here to reinforce the format of the
  // output data.
  def renderDocWithRef(
    format: ValueFormat,
    id: DocID,
    ts: Timestamp,
    desc: String,
    doc: DocID): String = {
    format match {
      case ValueFormat.Simple =>
        s"""{"id":"${id.subID.toLong}","coll":"Coll1","ts":"$ts","desc":"$desc","doc":{"id":"${doc.subID.toLong}","coll":"Coll0"}}"""
      case ValueFormat.Tagged =>
        s"""{"id":"${id.subID.toLong}","coll":{"@mod":"Coll1"},"ts":{"@time":"$ts"},"desc":"$desc","doc":{"@ref":{"id":"${doc.subID.toLong}","coll":{"@mod":"Coll0"}}}}"""
      case ValueFormat.Decorated =>
        s"""\"{\\n  id: \\"${id.subID.toLong}\\",\\n  coll: Coll1,\\n  ts: Time(\\"$ts\\"),\\n  desc: \\"$desc\\",\\n  doc: Coll0(\\"${doc.subID.toLong}\\")\\n}\""""
    }
  }

  def readMetadata(metadata: Path) =
    JSON.parse[JSValue](Files.readAllBytes(metadata))

  def taskExecutor(stats: StatsRequestBuffer, exportPath: Path) =
    TaskExecutor(ctx.withStats(stats).copy(exportPath = Some(exportPath)))

  def processAllTasks(exec: Executor): Unit =
    while ((exec.repo ! getTasks()).nonEmpty) {
      exec.step()
    }

  def allTasks(scope: ScopeID) = {
    val idx = NativeIndex.DocumentsByCollection(Database.RootScopeID)
    val terms = Vector(IndexTerm(TaskID.collID))

    Store.collectDocuments(idx, terms, Clock.time) { (v, ts) =>
      Task.get(v.docID.as[TaskID], ts)
    } selectT { _.scopeID == scope } flattenT
  }

  def getTask(scope: ScopeID, name: String) =
    allTasks(scope) selectT { _.name == name }
}

trait ExportDataSpecHelpers extends FQL2Spec {

  def tempExportPath() =
    Files.createTempDirectory(Random.nextLong().toHexString).toAbsolutePath

  def readFragments(files: List[Path]): List[(Long, String)] =
    files.flatMap { f =>
      FragmentReader(Fragment(f)).map { case (id, buf) =>
        id.toLong -> buf.toUTF8String
      }
    }

  def readOutfiles(
    files: List[Path],
    format: DatafileFormat = DatafileFormat.JSONL): List[String] = {
    files flatMap { f =>
      if (!f.getFileName.toString.contains(format.toString)) {
        println(s"Skipping file $f")
        Nil
      } else {
        format match {
          case DatafileFormat.JSON =>
            JSON
              .parse[Seq[JSValue]](Files.readString(f).toUTF8Buf)
              .map(_.toString)
              .iterator
          case DatafileFormat.JSONL =>
            val stream = Files.newInputStream(f)
            val reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
            reader
              .lines()
              .iterator()
              .asScala
        }
      }
    }
  }

  // Steps:
  //   Coll0: 4 creates + 1 TTL update + 32 updates + 1 delete
  //   Coll1: 3 creates
  //   Create task
  //   Coll0: 32 updates
  //   Coll0: 2 create + 1 delete
  //   Coll1: 1 create
  def setupData[T](auth: Auth)(createTask: (CollectionID, CollectionID) => T): T = {

    def createDocWithComputeField(desc: String): Long =
      evalOk(auth, s"Coll0.create({desc: '$desc'}).id").as[Long]

    def updateDoc(id: Long, desc: String): Unit =
      evalOk(auth, s"Coll0.byId('$id')!.update({desc: '$desc'})")

    def createDocWithRef(desc: String, id: Long): Long =
      evalOk(auth, s"Coll1.create({desc: '$desc', doc: Coll0.byId('$id')}).id")
        .as[Long]

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Coll0 {
           |  desc: String
           |
           |  compute computedField = (v) => ">>>>>>" + v.desc + "<<<<<<"
           |
           |  document_ttls true
           |}
           |
           |collection Coll1 {
           |  desc: String
           |  doc: Ref<Coll0>
           |}
           |""".stripMargin
    )

    val coll0 = (ctx ! Collection.idByNameActive(auth.scopeID, "Coll0")).value
    val coll1 = (ctx ! Collection.idByNameActive(auth.scopeID, "Coll1")).value

    // Coll0: 4 creates
    val doc0 = createDocWithComputeField("bar0")
    val doc1 = createDocWithComputeField("bar0")
    val doc2 = createDocWithComputeField("bar0")
    val doc3 = createDocWithComputeField("bar0 (not exported)")

    // TTL update
    evalOk(auth, s"Coll0.byId('$doc3')!.update({ ttl: Time.now() })")

    // Coll0: 32 updates
    (1 to 32) foreach { i =>
      updateDoc(doc0, s"bar$i")
      updateDoc(doc1, s"bar$i")
    }

    // Coll0: 1 delete
    evalOk(auth, s"Coll0.byId($doc2)!.delete()")

    // Coll1: 3 creates
    (0 to 2) foreach { i =>
      createDocWithRef(s"foo$i", doc0)
    }

    // Create task
    val task = createTask(coll0, coll1)

    // NB: None of the creates/updates as of this point should be exported

    // Coll0: 32 updates
    (33 to 64) foreach { i =>
      updateDoc(doc0, s"bar$i (not exported)")
      updateDoc(doc1, s"bar$i (not exported)")
    }

    // Coll0: 1 create
    val doc4 = createDocWithComputeField("not exported")

    // Coll0: 1 delete
    evalOk(auth, s"Coll0.byId($doc4)!.delete()")

    // Coll1: 1 create
    createDocWithRef("not exported", doc1)

    task
  }
}
