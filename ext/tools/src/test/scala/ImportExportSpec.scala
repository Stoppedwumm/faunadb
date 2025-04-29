package fauna.tools.test

import fauna.api.test.FQL2Helpers
import fauna.atoms._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.config.CoreConfig
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.net.http.{ Body, ContentType }
import fauna.prop.api.{ CoreLauncher, FaunaDB, FeatureFlags }
import fauna.storage.Tables
import fauna.tools.AdminCommands._
import fauna.tools.MappingFile
import fauna.util.ZBase32
import java.io.{
  ByteArrayOutputStream,
  FileInputStream,
  FileOutputStream,
  PrintStream
}
import java.lang.{ Integer => JInteger }
import java.nio.file.{ Files, Path, Paths }
import java.util.{ Arrays => JArrays }
import org.scalatest.OptionValues
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Using }
import scala.util.control.NonFatal

abstract class ImportExportSpec extends Spec with FQL2Helpers with OptionValues {

  val TenantProduction = "faunadb-cloud-tenants-production"

  val inst = CoreLauncher.nextInstanceID()
  val snapshotSizeEstimatorInst = CoreLauncher.nextInstanceID()

  val apiVers = APIVersion.V4.toString

  def launchInstance(flags: Option[FeatureFlags] = None) = {
    CoreLauncher.launch(inst, apiVers, syncOnShutdown = true, flags = flags)
    val js = JSObject("replica_name" -> CoreLauncher.defaultReplica)
    Await.ready(
      admin1.post(
        "/admin/init",
        Body(js.toByteBuf, ContentType.JSON),
        FaunaDB.rootKey),
      30.seconds)

    eventually(timeout(scaled(60.seconds)), interval(1.seconds)) {
      api1.get("/ping?scope=write", rootKey) should respond(OK)
    }
  }

  def reLaunchInstance() = {
    CoreLauncher.relaunch(inst, apiVers)
    val js = JSObject("replica_name" -> CoreLauncher.defaultReplica)
    Await.ready(
      admin1.post(
        "/admin/init",
        Body(js.toByteBuf, ContentType.JSON),
        FaunaDB.rootKey),
      30.seconds)

    eventually(timeout(scaled(60.seconds)), interval(1.seconds)) {
      api1.get("/ping?scope=write", rootKey) should respond(OK)
    }
  }

  def terminate(): Unit =
    try {
      CoreLauncher.terminate(inst)
    } catch {
      case NonFatal(_) => ()
    }

  val client = CoreLauncher.client(inst, apiVers)

  // TODO: Convert uses to the FQL2 helpers.
  val api1 = client.api
  val admin1 = client.admin

  def createDB(name: String, tok: String): String = {
    val res = api1.query(CreateDatabase(MkObject("name" -> name)), tok)
    res should respond(Created)

    (res.resource / "global_id").as[String]
  }

  def deleteDB(name: String, tok: String): Unit = {
    val res = api1.query(DeleteF(DatabaseRef(name)), tok)
    res should respond(OK)
  }

  def keyForDB(dbs: String*): String = {
    if (dbs.isEmpty) {
      FaunaDB.rootKey
    } else {
      s"${FaunaDB.rootKey}:${dbs mkString "/"}:admin"
    }
  }

  def snapshotDir = {
    val config = CoreConfig.load(Some(CoreLauncher.configFile(inst))).config

    val path = Option(config.storage_snapshot_path)
      .getOrElse(s"${config.storagePath}/snapshots")

    Paths.get(path)
  }

  def takeSnapshot(name: String): Path = {
    CreateSnapshot(admin1, FaunaDB.rootKey, name, false).apply() shouldBe true

    val dir = snapshotDir.resolve(name)
    val outputDir = Files.createTempDirectory(name)

    dir.copyRecursively(outputDir)
    dir.deleteRecursively()

    outputDir
  }

  def loadSnapshot(dir: Path): Unit = {
    LoadSnapshot(admin1, FaunaDB.rootKey, dir.toString, None, asJson = false)
      .apply() shouldBe true
  }

  def enableDatabase(db: String, enable: Boolean): String = {
    val out = new ByteArrayOutputStream()
    val err = new ByteArrayOutputStream()

    val success = EnableDatabase(
      admin1,
      FaunaDB.rootKey,
      db,
      enable,
      asJson = false,
      out = new PrintStream(out),
      err = new PrintStream(err)
    ).apply()

    if (!success) {
      fail(err.toString)
    }

    out.toString
  }

  def moveDatabase(target: String, destination: String, restore: Boolean): Unit = {
    val output = new ByteArrayOutputStream()

    val success = MoveDatabase2(
      admin1,
      FaunaDB.rootKey,
      target,
      destination,
      restore,
      asJson = true,
      err = new PrintStream(output)
    ).apply()

    if (!success) {
      fail(output.toString)
    }
  }

  def globalID2ScopeID(globalID: String): ScopeID = {
    val output = new ByteArrayOutputStream()
    val error = new ByteArrayOutputStream()

    val success = GlobalID2ScopeID(
      admin1,
      FaunaDB.rootKey,
      globalID,
      asJson = true,
      out = new PrintStream(output),
      err = new PrintStream(error)
    ).apply()

    if (!success) {
      fail(error.toString)
    }

    val js = JSON.parse[JSValue](output.toByteArray)

    val scopeID = (js / "scope_id").as[Long]

    ScopeID(scopeID)
  }

  def repair(): Unit = {
    Repair(
      admin1,
      FaunaDB.rootKey,
      dryRun = false,
      dataOnly = false,
      scopeID = None,
      asJson = true
    ).apply() shouldBe true

    var pending = true

    while (pending) {
      val output = new ByteArrayOutputStream()

      RepairStatus(
        admin1,
        FaunaDB.rootKey,
        asJson = true,
        out = new PrintStream(output)
      ).apply() shouldBe true

      val js = JSON.parse[JSValue](output.toByteArray)

      pending = (js / "resource" / "pending").as[Boolean]

      Thread.sleep(500)
    }
  }

  def remapScopes(dumpTree: Path, globalID: String, preserveGlobalID: Boolean) = {
    val mappings = Files.createTempFile("mappings", ".txt")
    mappings.toFile.deleteOnExit()
    val output = new PrintStream(new FileOutputStream(mappings.toFile))
    AllocateScopes(
      admin1,
      FaunaDB.rootKey,
      globalID,
      Some(dumpTree.toString),
      preserveGlobalID,
      output,
      asJson = false).apply() shouldBe true
    output.close()

    mappings
  }

  // WARNING: Do not change this! See Storage.CFIDs for more
  // information.
  val cfMapping = Map(
    "Versions" -> "Versions-2de7f3e26de53e8c8c0d4958a71de9c4",
    "SortedIndex" -> "SortedIndex-39d428ec3ba2383287c7ceea9975a95d",
    "HistoricalIndex" -> "HistoricalIndex-5aff13b786333b11b373536f7e844681",
    "LookupStore" -> "LookupStore-812b1bfcdcad3186acbe4367d4965b19"
  )

  def copyData(src: Path, dst: Path) = {
    src
      .resolve(cfMapping("Versions"))
      .copyRecursively(dst.resolve(cfMapping("Versions")))
    src
      .resolve(cfMapping("SortedIndex"))
      .copyRecursively(dst.resolve(cfMapping("SortedIndex")))
    src
      .resolve(cfMapping("HistoricalIndex"))
      .copyRecursively(dst.resolve(cfMapping("HistoricalIndex")))
    src
      .resolve(cfMapping("LookupStore"))
      .copyRecursively(dst.resolve(cfMapping("LookupStore")))
  }

  def copyBackupToFauna(
    backupPath: Path,
    storagePathOpt: Option[Path] = None): Path = {
    val storagePath = storagePathOpt match {
      case Some(dir) =>
        dir

      case None =>
        val config = CoreConfig.load(Some(CoreLauncher.configFile(inst))).config
        Paths.get(config.storagePath)
    }

    val faunaDir = storagePath.resolve("data").resolve("FAUNA")

    faunaDir.deleteRecursively()
    faunaDir.toFile.exists() shouldBe false
    copyData(backupPath, faunaDir)
    faunaDir.findAll.isEmpty shouldBe false

    faunaDir
  }

  def rewriteData(
    backupPath: Path,
    mappings: Path,
    includeKeys: Boolean,
    ignoreStats: Boolean = false): Unit = {
    def stats(path: Path): JSObject = {
      val content = Using(new FileInputStream(path.toFile)) { stream =>
        JSON.parse[JSValue](stream.readAllBytes())
      } get

      (content / "stats").as[JSObject]
    }

    def checkStats(cf: String): Unit = {
      // On rare occasions, like testing workarounds for weird
      // situations, we may want to skip this.
      if (ignoreStats) return

      val exportStats = loadStats(backupPath, cf)
      val rewriteStats = stats(
        backupPath.resolve("tmp").resolve(s"rewrite-$cf.json"))

      val rows = (rewriteStats / "Rewriter.Rows.Output").as[Long] +
        (rewriteStats / "Rewriter.Rows.Discarded").orElse(0L)

      (exportStats / "Exporter.Rows.Output").as[Long] should equal(rows)

      // If includeKeys is false, the rewrite never sees discarded
      // cells, so it is not possible to count them.
      if (includeKeys) {
        val cells = rewriteStats / "Rewriter.Cells.Rewritten"
        (exportStats / "Exporter.Cells.Output") should equal(cells)
      }
    }

    val config = copyConfig(CoreLauncher.configFile(inst), backupPath)

    val faunaDir = backupPath.resolve("data").resolve("FAUNA")

    launchRewrite("Versions", mappings, includeKeys, config) shouldBe 0
    checkStats("Versions")

    launchRewrite("SortedIndex", mappings, includeKeys, config) shouldBe 0
    checkStats("SortedIndex")

    launchRewrite("HistoricalIndex", mappings, includeKeys, config) shouldBe 0
    checkStats("HistoricalIndex")

    launchRewrite("LookupStore", mappings, includeKeys, config) shouldBe 0
    checkStats("LookupStore")

    faunaDir.resolve("dump-tree.json").toFile.exists() shouldBe true

    cfMapping foreach { case (_, folderName) =>
      val cfFolder = faunaDir.resolve(folderName)
      cfFolder.toFile.exists() shouldBe true
    }
  }

  def launchRewrite(
    cf: String,
    mappings: Path,
    includeKeys: Boolean,
    config: Path) = {
    val args = if (includeKeys) {
      List("-k", mappings.toString)
    } else {
      List(mappings.toString)
    }

    val (exitValue, out) = Await.result(
      launchTool(
        main = "fauna.tools.Rewriter",
        flags = Map("name" -> cf, "threads" -> "1"),
        args = args,
        config = Some(config.toString)
      ),
      10.minutes
    )

    if (exitValue != 0) {
      out.forEach(println)
    }

    exitValue
  }

  def dumpTree(): Path = {
    val dumpTree = Files.createTempFile("dump-tree", ".txt")
    dumpTree.toFile.deleteOnExit()
    DumpTree(admin1, FaunaDB.rootKey, None, Some(dumpTree.toString))
      .apply() shouldBe true
    dumpTree
  }

  def loadStats(outputDir: Path, cf: String): JSObject = {
    val path = outputDir.resolve("data").resolve("FAUNA").resolve(s"stats-$cf.json")

    val content = Using(new FileInputStream(path.toFile)) { stream =>
      JSON.parse[JSValue](stream.readAllBytes())
    } get

    (content / "stats").as[JSObject]
  }

  case class ExportedData(
    globalID: String,
    userBackup: Path,
    metadata: Path,
    checksum: Map[String, Checksum])

  def exportTenantData(
    dumpTree: Path,
    tenants: Seq[TenantData],
    snapshotTS: Timestamp = Timestamp.MaxMicros): Seq[ExportedData] = {
    exportData(
      dumpTree,
      tenants map { t =>
        (t.userDbGlobalIDEnc, t.tenantDbName, t.failed)
      },
      snapshotTS)
  }

  def exportData(
    dumpTree: Path,
    data: Seq[(String, String, Boolean)],
    snapshotTS: Timestamp = Timestamp.MaxMicros): Seq[ExportedData] = {
    val output = data map { case (globalID, name, failed) =>
      val outputDir = Files.createTempDirectory(name)
      outputDir.toFile.deleteOnExit()

      (globalID, outputDir, failed)
    }

    val requests = output map { case (globalID, outputDir, failed) =>
      val faunaDir = outputDir.resolve("data").resolve("FAUNA")

      faunaDir.toFile.mkdirs() shouldBe true

      if (failed) {
        faunaDir.toFile.setReadOnly()
      }

      JSObject(
        "global_id" -> (if (failed) "not-valid-global-id" else globalID),
        "output_dir" -> faunaDir.toString
      )
    }

    val exportFile = Files.createTempFile("exporter", ".json")
    exportFile.toFile.deleteOnExit()
    val stream = new FileOutputStream(exportFile.toFile)
    JSObject("export" -> requests).writeTo(stream, pretty = false)
    stream.close()

    val metadata = Files.createTempFile("metadata", ".json")
    metadata.toFile.deleteOnExit()

    launchDataExporter(
      Tables.Versions.CFName,
      dumpTree,
      exportFile,
      metadata,
      snapshotTS) shouldBe 0

    launchDataExporter(
      Tables.SortedIndex.CFName,
      dumpTree,
      exportFile,
      metadata,
      snapshotTS) shouldBe 0

    launchDataExporter(
      Tables.HistoricalIndex.CFName,
      dumpTree,
      exportFile,
      metadata,
      snapshotTS) shouldBe 0

    launchDataExporter(
      Tables.Lookups.CFName,
      dumpTree,
      exportFile,
      metadata,
      snapshotTS) shouldBe 0

    compact(16 * 1024 * 1024, snapshotTS)

    output map {
      case (globalID, outputDir, failed) if !failed =>
        val faunaDir = outputDir.resolve("data").resolve("FAUNA")
        val dumpTree = faunaDir.resolve("dump-tree.json")
        val versionsStats =
          faunaDir.resolve(s"stats-${Tables.Versions.CFName}.json")
        val historicalIndexStats =
          faunaDir.resolve(s"stats-${Tables.HistoricalIndex.CFName}.json")
        val sortedIndexStats =
          faunaDir.resolve(s"stats-${Tables.SortedIndex.CFName}.json")
        val lookupsStats = faunaDir.resolve(s"stats-${Tables.Lookups.CFName}.json")

        val checksums = if (dumpTree.toFile.exists()) {
          def assertStats(path: Path, cf: String): Unit = {
            if (!path.toFile.exists()) {
              fail(s"$path doesn't exist")
            }

            val stats = Using(new FileInputStream(path.toFile)) { stream =>
              JSON.parse[JSValue](stream.readAllBytes())
            } get

            (stats / "column_family").as[String] shouldBe cf
            (stats / "compression_ratio").as[Double] should be <= 1.0
            (stats / "stats").asOpt[JSValue].isDefined shouldBe true
          }

          assertStats(versionsStats, Tables.Versions.CFName)
          assertStats(sortedIndexStats, Tables.SortedIndex.CFName)
          assertStats(historicalIndexStats, Tables.HistoricalIndex.CFName)
          assertStats(lookupsStats, Tables.Lookups.CFName)

          val checksums = checksum(outputDir, globalID, Some(metadata), snapshotTS)

          checksums(Tables.Versions.CFName).length shouldBe 32
          checksums(Tables.SortedIndex.CFName).length shouldBe 32
          checksums(Tables.HistoricalIndex.CFName).length shouldBe 32
          checksums(Tables.Lookups.CFName).length shouldBe 32

          checksums
        } else {
          Map.empty[String, Checksum]
        }

        ExportedData(globalID, outputDir, metadata, checksums)

      case (globalID, outputDir, _) =>
        ExportedData(globalID, outputDir, metadata, Map.empty)
    }
  }

  def launchDataExporter(
    cf: String,
    dumpTree: Path,
    exportFile: Path,
    metadata: Path,
    snapshotTS: Timestamp) = {
    val (exitValue, out) = Await.result(
      launchTool(
        main = "fauna.tools.DataExporter",
        Map(
          "cf" -> cf,
          "dump-tree" -> dumpTree.toString,
          "snapshot" -> snapshotTS.toString,
          "export" -> exportFile.toString,
          "metadata" -> metadata.toString,
          "threads" -> "1"
        ),
        config = Some(CoreLauncher.configFile(inst).toString)
      ),
      10.minutes
    )

    if (exitValue != 0) {
      out.forEach(println)
    }

    exitValue
  }

  def launchChecksum(
    cf: String,
    dumpTree: Path,
    globalID: String,
    metadata: Option[Path],
    snapshotTS: Timestamp,
    config: Path): Map[String, String] = {
    val (exitCode, out) = Await.result(
      launchTool(
        main = "fauna.tools.Checksum",
        flags = Map(
          "name" -> cf,
          "dump-tree" -> dumpTree.toString,
          "global-id" -> globalID,
          "snapshot" -> snapshotTS.toString) ++ (metadata map {
          "metadata" -> _.toString
        }),
        config = Some(config.toString)
      ),
      10.minutes
    )

    if (exitCode != 0) {
      out.forEach(println)
      fail()
    }

    val regex = """^(.*) Checksum: \[(.*)\]$""".r

    out.iterator().asScala flatMap {
      regex.findFirstMatchIn(_)
    } filter {
      _.groupCount == 2
    } map { m =>
      m.group(1) -> m.group(2)
    } toMap
  }

  case class Checksum(sum: Array[Byte]) {
    def length = sum.length

    override def equals(obj: Any): Boolean = obj match {
      case other: Checksum => JArrays.equals(sum, other.sum)
      case _               => false
    }

    override def toString: String = sum.toHexString
  }

  object Checksum {
    val Zeroes = Checksum(Array.fill(32)(0.toByte))

    def apply(sum: String) =
      new Checksum(sum.split("\\s") map { JInteger.decode(_).toByte })
  }

  def checksum(
    backupPath: Path,
    globalID: String,
    metadata: Option[Path],
    snapshotTS: Timestamp): Map[String, Checksum] = {
    val cf = Seq(
      Tables.Versions.CFName,
      Tables.HistoricalIndex.CFName,
      Tables.SortedIndex.CFName,
      Tables.Lookups.CFName) mkString ","

    val dumpTree =
      backupPath.resolve("data").resolve("FAUNA").resolve("dump-tree.json")
    val config = copyConfig(CoreLauncher.configFile(inst), backupPath)

    launchChecksum(
      cf,
      dumpTree,
      globalID,
      metadata,
      snapshotTS,
      config).view mapValues {
      Checksum(_)
    } toMap
  }

  def launchSizeEstimator(
    cf: Seq[String],
    output: Seq[Path],
    multipliers: Seq[Int],
    metadata: Path,
    coreInst: Int = inst
  ) = {
    val (exitValue, out) = Await.result(
      launchTool(
        main = "fauna.tools.SizeEstimator",
        Map(
          "cf" -> cf.mkString(","),
          "output" -> output.map(_.toString).mkString(","),
          "multiplier" -> multipliers.mkString(","),
          "metadata" -> metadata.toString,
          "threads" -> "1",
          "segments" -> Seq.fill(cf.size)("1").mkString(",")
        ),
        config = Some(CoreLauncher.configFile(coreInst).toString)
      ),
      10.minutes
    )

    if (exitValue != 0) {
      out.forEach(println)
    }

    exitValue
  }

  def launchSnapshotSizeEstimator(
    cf: Seq[String],
    output: Seq[Path]
  ): Int = {

    val (exitValue, out) = Await.result(
      launchTool(
        main = "fauna.tools.SnapshotSizeEstimator",
        Map(
          "cf" -> cf.mkString(","),
          "output" -> output.map(_.toString).mkString(","),
          "threads" -> "1",
          "segments" -> Seq.fill(cf.size)("1").mkString(",")
        ),
        config = Some(CoreLauncher.configFile(snapshotSizeEstimatorInst).toString)
      ),
      1.minutes
    )

    if (exitValue != 0) {
      out.forEach(println)
    }

    exitValue
  }

  case class SizeEstimatorEntry(
    dbPath: Seq[String],
    globalIDPath: Seq[String],
    size: Long)

  /** Providing a backup path will run the snapshot size estimator on the data at the provided path. Otherwise
    * it will take a snapshot of the data currently in the `inst` directory and run on that.
    */
  def snapshotSizeEstimator(
    backupPath: Option[Path] = None): (Map[ScopeID, Long], Map[ScopeID, Long]) = {
    val sePath =
      CoreLauncher.createInstanceDirAndConfigFile(snapshotSizeEstimatorInst)
    val seDataPath = sePath.resolve("data").resolve("FAUNA")
    Files.createDirectories(seDataPath)

    val versions = Files.createTempFile(Tables.Versions.CFName, ".txt")
    val indexes = Files.createTempFile(Tables.HistoricalIndex.CFName, ".txt")

    versions.toFile.deleteOnExit()
    indexes.toFile.deleteOnExit()

    backupPath
      .map { bp =>
        bp.copyRecursively(seDataPath)

        launchSnapshotSizeEstimator(
          Seq(Tables.Versions.CFName, Tables.HistoricalIndex.CFName),
          Seq(versions, indexes)
        ) shouldBe 0
      }
      .getOrElse {
        offlineWithBackup { bp =>
          bp.copyRecursively(seDataPath)

          launchSnapshotSizeEstimator(
            Seq(Tables.Versions.CFName, Tables.HistoricalIndex.CFName),
            Seq(versions, indexes)
          ) shouldBe 0
        }
      }

    def parseOutput(output: Path) = {
      Using(Source.fromFile(output.toFile)) { source =>
        val b = Map.newBuilder[ScopeID, Long]

        source.getLines() foreach { line =>
          val entry = JSON.parse[JSValue](line.getBytes)
          val scopeID = (entry / "scope_id").as[Long]
          val stats = entry / "stats"
          val uncompressed = (stats / "storage_uncompressed_bytes").as[Long]

          b += ScopeID(scopeID) -> uncompressed
        }

        b.result()
      } match {
        case Failure(ex) =>
          println(ex)
          fail(ex)
        case Success(v) => v
      }
    }

    (parseOutput(versions), parseOutput(indexes))
  }

  /** If backupPath is provided, the Size Estimator will run on the data present in the backup.
    */
  def sizeEstimator(metadata: Path, backupPath: Option[Path] = None)
    : (Map[GlobalDatabaseID, Long], Map[GlobalDatabaseID, Long]) = {

    val (versions, indexes) = sizeEstimator0(metadata, backupPath)

    (
      versions.view mapValues { _.size } toMap,
      indexes.view mapValues { _.size } toMap)
  }

  /** If backupPath is provided, the Size Estimator will run on the data present in the backup. This is done by
    * copying the data from the backup into a fresh core instance directory, and running the size estimator pointing
    * at that directory.
    */
  def sizeEstimator0(metadata: Path, backupPath: Option[Path] = None): (
    Map[GlobalDatabaseID, SizeEstimatorEntry],
    Map[GlobalDatabaseID, SizeEstimatorEntry]) = {

    val seInst = backupPath
      .map { bp =>
        val sePath =
          CoreLauncher.createInstanceDirAndConfigFile(snapshotSizeEstimatorInst)
        val seDataPath = sePath.resolve("data").resolve("FAUNA")
        Files.createDirectories(seDataPath)
        bp.copyRecursively(seDataPath)
        snapshotSizeEstimatorInst
      }
      .getOrElse(inst)

    val versions = Files.createTempFile(Tables.Versions.CFName, ".txt")
    val indexes = Files.createTempFile(Tables.HistoricalIndex.CFName, ".txt")

    versions.toFile.deleteOnExit()
    indexes.toFile.deleteOnExit()

    val cfs = Seq(Tables.Versions.CFName, Tables.HistoricalIndex.CFName)
    val outputs = Seq(versions, indexes)
    val multipliers = Seq(1, 2)
    launchSizeEstimator(
      cfs,
      outputs,
      multipliers,
      metadata,
      coreInst = seInst) shouldBe 0

    def parseOutput(output: Path) = {
      Using(Source.fromFile(output.toFile)) { source =>
        val b = Map.newBuilder[GlobalDatabaseID, SizeEstimatorEntry]

        source.getLines() foreach { line =>
          val entry = JSON.parse[JSValue](line.getBytes)
          val globalID = ZBase32.decodeLong((entry / "global_id").as[String])
          val globalIDPath = (entry / "global_id_path").as[Seq[String]]
          val stats = entry / "stats"
          val uncompressed = (stats / "storage_uncompressed_bytes").as[Long]

          def getPath(ref: JSValue): Seq[String] = ref match {
            case JSObject(_) =>
              val id = (ref / "@ref" / "id").as[String]

              (ref / "@ref" / "database").asOpt[JSValue] match {
                case Some(parent) => getPath(parent) :+ id
                case None         => Seq(id)
              }
            case JSString(id) => Seq(id)
            case _            => Seq.empty
          }

          b += GlobalDatabaseID(globalID) -> SizeEstimatorEntry(
            getPath(entry / "database"),
            globalIDPath,
            uncompressed)
        }

        b.result()
      } match {
        case Failure(ex) => fail(ex)
        case Success(v)  => v
      }
    }

    (parseOutput(versions), parseOutput(indexes))
  }

  def launchSizeCompactor(cf: String, size: Long, snapshotTS: Timestamp) = {
    val (exitValue, out) = Await.result(
      launchTool(
        main = "fauna.tools.SizeCompactor",
        Map(
          "cf" -> cf,
          "size" -> size.toString,
          "snapshot" -> snapshotTS.toString,
          "threads" -> "1"),
        config = Some(CoreLauncher.configFile(inst).toString)
      ),
      10.minutes
    )

    if (exitValue != 0) {
      out.forEach(println)
    }

    exitValue
  }

  def compact(size: Long, snapshotTS: Timestamp): Unit = {
    val cf = Seq(
      Tables.Versions.CFName,
      Tables.HistoricalIndex.CFName,
      Tables.SortedIndex.CFName,
      Tables.Lookups.CFName) mkString ","

    launchSizeCompactor(cf, size, snapshotTS) shouldBe 0
  }

  def offlineWithBackup[T](fn: Path => T): T = {
    val systemBackup = takeSnapshot(s"system_backup_${System.nanoTime()}")
    try {
      CoreLauncher.terminate(inst, mode = CoreLauncher.Gracefully)
      fn(systemBackup)
    } finally {
      systemBackup.deleteRecursively()
      reLaunchInstance()
    }
  }

  def runOffline[T](fn: Path => T): T = {
    val systemBackup = takeSnapshot(s"system_backup_${System.nanoTime()}")
    try {
      runOfflineWithDumpTree(systemBackup)(fn)
    } finally {
      systemBackup.deleteRecursively()
    }
  }

  def runOfflineWithDumpTree[T](systemBackup: Path)(fn: Path => T): T = {
    try {
      val dumpTreeFile = dumpTree()

      CoreLauncher.terminate(inst, mode = CoreLauncher.Gracefully)

      fn(dumpTreeFile)
    } finally {
      // start clean, with no data
      launchInstance()

      // load system snapshot
      loadSnapshot(systemBackup)
    }
  }

  def copyConfig(config: Path, storagePath: Path) = {
    val newConfig = Files.createTempFile("faunadb", ".yml")
    newConfig.toFile.deleteOnExit()

    val output = new PrintStream(newConfig.toFile)
    val input = Source.fromFile(config.toFile)

    try {
      input.getLines() foreach { line =>
        val l = if (line.trim.startsWith("storage_data_path")) {
          s"storage_data_path: \"${storagePath.toString}\""
        } else {
          line.trim
        }

        output.println(l)
      }
    } finally {
      input.close()
      output.close()
    }

    newConfig
  }

  class TenantData(
    val tenantDbName: String,
    val numDocs: Int,
    val failed: Boolean = false) {

    val tenantDbKey = keyForDB(TenantProduction, tenantDbName)

    val userDbName = "user_db"

    val userDbKey =
      keyForDB(TenantProduction, tenantDbName, userDbName)

    // database
    val userDbGlobalIDEnc = createDB(userDbName, tenantDbKey)
    val userDbGlobalID = GlobalDatabaseID(ZBase32.decodeLong(userDbGlobalIDEnc))

    // key
    val keyF = api1.query(CreateKey(MkObject("role" -> "admin")), userDbKey)
    keyF should respond(Created)
    val keySecret = (keyF.resource / "secret").as[String]

    val key1F = api1.query(
      CreateKey(MkObject("role" -> "admin", "database" -> DatabaseRef(userDbName))),
      tenantDbKey)
    key1F should respond(Created)
    val key1Secret = (key1F.resource / "secret").as[String]

    // documents
    val (docsRefs, docsTs) = {
      seedData(userDbKey)
    }

    private def seedData(dbKey: String) = {
      // function
      api1.query(
        CreateFunction(
          MkObject(
            "name" -> "multiply_by_10",
            "body" -> QueryF(Lambda("x" -> Multiply(Var("x"), 10)))
          )),
        dbKey) should respond(Created)

      // collection
      api1.query(
        CreateCollection(MkObject("name" -> "cls", "history_days" -> 30)),
        dbKey) should respond(Created)

      // index
      api1.query(
        CreateIndex(
          MkObject(
            "name" -> "idx",
            "source" -> ClassRef("cls"),
            "terms" -> MkObject("field" -> Seq("data", "foo"))
          )),
        dbKey) should respond(Created)

      // documents
      val resources = (1 to numDocs) map { i =>
        val docF = api1.query(
          CreateF(
            MkRef(ClassRef("cls"), i.toString),
            MkObject("data" -> MkObject("foo" -> "bar"))
          ),
          dbKey)
        docF should respond(Created)

        docF.resource
      }

      val refs = resources map { r => r / "ref" }
      val ts = resources map { r => Timestamp.ofMicros((r / "ts").as[Long]) }

      (refs, ts)
    }

    def createDBWithDataUnderUserDb(db: String) = {
      val userDbGlobalIDEnc = createDB(db, userDbKey)
      val dbKey =
        keyForDB(TenantProduction, tenantDbName, userDbName, db)

      seedData(dbKey)

      (userDbGlobalIDEnc, dbKey)
    }

    def checkDoesntExist(): Unit = {
      // tenant db exist
      api1
        .query(
          Exists(DatabaseRef(tenantDbName)),
          keyForDB(TenantProduction)
        )
        .json shouldBe JSObject("resource" -> true)

      // but user db don't
      api1
        .query(
          Exists(DatabaseRef(userDbName)),
          tenantDbKey
        )
        .json shouldBe JSObject("resource" -> false)
    }

    def assertDatabaseData(
      dbKey: String,
      snapshotTS: Timestamp = Timestamp.MaxMicros): Unit = {
      // function
      api1
        .query(
          Exists(FunctionRef("multiply_by_10")),
          dbKey
        )
        .json shouldBe JSObject("resource" -> true)

      api1
        .query(
          Call("multiply_by_10", 10),
          dbKey
        )
        .json shouldBe JSObject("resource" -> 100)

      // collection
      api1
        .query(
          Exists(ClassRef("cls")),
          dbKey
        )
        .json shouldBe JSObject("resource" -> true)

      // index
      api1
        .query(
          Exists(IndexRef("idx")),
          dbKey
        )
        .json shouldBe JSObject("resource" -> true)

      // match on index
      api1
        .query(
          Exists(Match("idx", "bar")),
          dbKey
        )
        .json shouldBe JSObject("resource" -> true)

      // document
      docsRefs zip docsTs foreach { case (ref, ts) =>
        val exists = ts <= snapshotTS

        api1
          .query(
            Exists(ref),
            dbKey
          )
          .json shouldBe JSObject("resource" -> exists)
      }

    }

    def assertData(snapshotTS: Timestamp = Timestamp.MaxMicros): Unit = {
      // database
      api1
        .query(
          Exists(DatabaseRef(userDbName)),
          tenantDbKey
        )
        .json shouldBe JSObject("resource" -> true)

      // key
      api1
        .query(
          Exists(keyF.resource / "ref"),
          userDbKey
        )
        .json shouldBe JSObject("resource" -> true)

      api1
        .query(
          Exists(key1F.resource / "ref"),
          tenantDbKey
        )
        .json shouldBe JSObject("resource" -> true)

      assertDatabaseData(userDbKey, snapshotTS)

      // use the key
      api1
        .query(
          Exists(docsRefs.head),
          keySecret
        )
        .json shouldBe JSObject("resource" -> true)

      api1
        .query(
          Exists(docsRefs.head),
          key1Secret
        )
        .json shouldBe JSObject("resource" -> true)
    }
  }

  case class Restore(
    tenant: TenantData,
    exportedData: ExportedData,
    destination: Option[String] = None,
    newName: Option[String] = None,
    repair: Boolean = false) {

    val inPlace = destination.isEmpty && newName.isEmpty
  }

  def restore(
    tenant: TenantData,
    exportedData: ExportedData,
    destination: Option[String] = None,
    newName: Option[String] = None,
    repair: Boolean = false,
    ignoreStats: Boolean = false): Unit =
    restoreMultiple(
      Seq(Restore(tenant, exportedData, destination, newName, repair)),
      ignoreStats = ignoreStats)

  def restoreMultiple(restores: Seq[Restore], ignoreStats: Boolean = false): Unit = {
    val restoreMapped = restores map { restore =>
      if (restore.exportedData.userBackup.findAll.isEmpty) {
        fail(s"User backup on folder ${restore.exportedData.userBackup} is empty")
      }

      // Get the current scope id of the database before load-snapshot to avoid
      // conflicts with the loaded lookup entries that didn't have global ids
      // rewritten.
      //
      // Lets say we have the database with the following properties:
      //
      // {
      //   parentScopeID: scope0
      //   dbID         : db0
      //   scopeID      : scope1
      //   globalID     : global1
      // }
      //
      // This document will generate the following lookup entries:
      //
      // global1 -> (scope0, db0, TS0, Add)
      // scope1  -> (scope0, db0, TS0, Add)
      //
      // If we try to resolve the database by globalID (ie: using
      // Database.forGlobalID())
      // it will uniquely return the database pointed by (scope0, db0).
      //
      // However when we restore a backup, we need to rewrite all scopes in the
      // backup
      // and load it back to the cluster using load-snapshot admin command.
      //
      // The effect of this is that now we gonna have 2 database documents:
      //
      // {
      //   parentScopeID: scope0
      //   dbID         : db0
      //   scopeID      : scope1
      //   globalID     : global1
      // }
      //
      // {
      //   parentScopeID: scope2
      //   dbID         : db0
      //   scopeID      : scope3
      //   globalID     : global1
      // }
      //
      // And the following lookup entries:
      //
      // global1 -> (scope2, db0, TS0, Add)
      // global1 -> (scope0, db0, TS0, Add)
      // scope3  -> (scope2, db0, TS0, Add)
      // scope1  -> (scope0, db0, TS0, Add)
      //
      // Now we have 2 lookup entries for the same global1.
      //
      // Caveats:
      //
      // Any change in the mapping global_id => scope_id between global-to-scope
      // and move-database can lead to unpredicted consequences.
      //
      // Cases when this scenario can happen:
      //
      // 1. user schedule a restore of A
      // 2. user schedule a copy of B under A
      //
      // If these 2 operations happens in parallel, the copy of B under A would
      // result in an invalid state.
      //
      // To avoid such scenarios the workers udf's has some concurrency controls
      // in place.
      //
      val actualScope = globalID2ScopeID(restore.tenant.userDbGlobalIDEnc)

      if (restore.inPlace) {
        try {
          // try to disable the database, if it's deleted, just ignore the error
          enableDatabase(restore.tenant.userDbGlobalIDEnc, enable = false)
        } catch {
          case NonFatal(_) => ()
        }
      }

      val backupCloned = Files.createTempDirectory("cloned_backup")
      restore.exportedData.userBackup.copyRecursively(backupCloned)

      val tempName =
        s"temp_db_${restore.tenant.userDbGlobalIDEnc}_${System.nanoTime()}"
      val tempDB = createDB(tempName, FaunaDB.rootKey)

      val snapshotDumpTree =
        backupCloned.resolve("data").resolve("FAUNA").resolve("dump-tree.json")
      if (!snapshotDumpTree.toFile.exists()) {
        fail(s"$snapshotDumpTree doesn't exist")
      }

      val mappings =
        remapScopes(snapshotDumpTree, tempDB, preserveGlobalID = restore.inPlace)

      (restore, tempName, backupCloned, mappings, actualScope)
    }

    restoreMapped foreach { case (restore, _, backupCloned, mappings, _) =>
      val actualChecksum = checksum(
        backupCloned,
        restore.exportedData.globalID,
        None,
        Timestamp.MaxMicros
      )

      restore.exportedData.checksum shouldBe actualChecksum

      rewriteData(
        backupCloned,
        mappings,
        includeKeys = restore.inPlace,
        ignoreStats = ignoreStats)
    }

    restoreMapped foreach {
      case (restore, tempName, backupCloned, mappings, actualScope) =>
        try {
          // load rewritten data
          loadSnapshot(backupCloned.resolve("data").resolve("FAUNA"))

          // move database
          val mapping = MappingFile.read(mappings.toString)

          restore.newName foreach { name =>
            val key = keyForDB(tempName)

            val res = api1.query(
              Update(
                DatabaseRef(restore.tenant.userDbName),
                MkObject("name" -> name)),
              key)

            res should respond(OK)
          }

          noException shouldBe thrownBy {
            mapping.globalIDForGlobalID(restore.tenant.userDbGlobalID)
          }

          val map = mapping.forGlobalID(restore.tenant.userDbGlobalID).get

          if (restore.repair) {
            // run repair after load-snapshot and before move-database
            repair()
          }

          moveDatabase(
            map.newScope.toLong.toString,
            restore.destination getOrElse actualScope.toLong.toString,
            restore = restore.inPlace)

          // the new database is already enabled, but lets use this as a flag
          // that we can access and manipulate it
          enableDatabase(map.newScope.toLong.toString, enable = true) shouldBe
            s"Database '${map.newScope.toLong}' updated successfully\n"

          if (restore.inPlace) {
            the[Exception] thrownBy {
              // the old database is deleted
              enableDatabase(actualScope.toLong.toString, enable = true)
            } should have message s"Failed: Database '${actualScope.toLong}' is deleted\n"
          }

          deleteDB(tempName, FaunaDB.rootKey)
        } finally {
          backupCloned.deleteRecursively()
        }
    }
  }

}
