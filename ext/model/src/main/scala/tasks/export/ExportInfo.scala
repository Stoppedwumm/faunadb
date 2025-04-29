package fauna.model.tasks.export

import fauna.atoms._
import fauna.codex.json.JSObject
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.{ Collection, Task }
import fauna.model.runtime.fql2.serialization.ValueFormat
import fauna.model.schema.{ CollectionConfig, NativeCollection }
import fauna.model.tasks.ExportDataTask._
import fauna.repo.query.Query
import fauna.storage.doc.Data
import java.io.FileOutputStream
import java.nio.file.{ DirectoryNotEmptyException, Files, Path, Paths }
import scala.collection.mutable.Stack

object ExportInfo {
  // used for getting information out of the task. passing null will cause
  // path generation helpers to fail.
  def apply(task: Task): ExportInfo = ExportInfo(null, task)

  def apply(path: Path, task: Task): ExportInfo = ExportInfo(
    task.accountID,
    task.data(ScopeField),
    task.data(CollIDsField),
    task.data(CollNamesField),
    task.data(ExportIDField),
    task.data(RootTaskIDField).getOrElse(task.id),
    path,
    task.data(SnapshotTSField),
    task.data(DocFormatField),
    task.data(DatafileFormatField),
    task.data(DatafileCompressionField)
  )

  val NativeAllowed = Map("Credential" -> Some(CredentialsID.collID))

  def collectionIDForExport(
    scope: ScopeID,
    name: String): Query[Option[CollectionID]] =
    name match {
      case name if NativeAllowed contains name =>
        Query.value(NativeAllowed(name))
      case _ =>
        Collection.idByNameActive(scope, name)
    }
}

case class ExportInfo(
  accountID: AccountID,
  scope: ScopeID,
  collIDs: Vector[CollectionID],
  collNames: Vector[String],
  exportID: String,
  rootTaskID: TaskID,
  exportPath: Path,
  snapshotTS: Timestamp,
  docFormat: ValueFormat,
  datafileFormat: DatafileFormat,
  datafileCompression: Boolean) {

  def datafileExt = datafileFormat.fileExt(datafileCompression)

  // <EXPORT_PATH>/<tenant_id>/export_<export_id>/<task_id>/

  def toTaskData = Data(
    ScopeField -> scope,
    CollIDsField -> collIDs,
    CollNamesField -> collNames,
    ExportIDField -> exportID,
    RootTaskIDField -> Some(rootTaskID),
    SnapshotTSField -> snapshotTS,
    DocFormatField -> docFormat,
    DatafileFormatField -> datafileFormat,
    DatafileCompressionField -> datafileCompression
  )

  def validatedCollections
    : Query[Either[String, Map[CollectionID, CollectionConfig]]] = {

    val (native, user) = collIDs.partition(NativeCollection.unapply(_).isDefined)

    val nativeQ =
      native.map(NativeCollection(_)(scope, lookupIndexes = false)).sequence

    val userQ = user
      .map(id => Collection.get(scope, id).map(id -> _))
      .sequence

    (nativeQ, userQ) par { (native, userLookups) =>
      var deleted = List.empty[CollectionID]
      var modified = List.empty[CollectionID]

      userLookups.foreach {
        case (id, None) => deleted ::= id
        case (id, Some(c)) =>
          if (c.schemaVersion.ts > snapshotTS) modified ::= id
      }

      if (deleted.nonEmpty) {
        Query.value(Left("collection deleted"))
      } else if (modified.nonEmpty) {
        Query.value(Left("schema changed"))
      } else {
        val b = Map.newBuilder[CollectionID, CollectionConfig]

        b ++= native.view.map(cfg => cfg.id -> cfg)
        b ++= userLookups.view.map { case (id, c) => id -> c.get.config }

        Query.value(Right(b.result()))
      }
    }
  }

  def subPath: Path =
    Paths.get(s"${accountID.toLong}/export_$exportID/${rootTaskID.toLong}")

  def cleanupTempPaths() = {
    val taskDir = getBasePath(isTemp = true)
    val exportDir = taskDir.getParent
    val accountDir = exportDir.getParent

    // kill this task's tmp files
    taskDir.deleteRecursively()

    // delete export and account dirs if they are otherwise empty
    try {
      exportDir.delete()
      accountDir.delete()
    } catch {
      // some other task is doing stuff, so leave things alone
      case _: DirectoryNotEmptyException =>
    }
  }

  def getBasePath(isTemp: Boolean): Path = {
    val tmpOrData = if (isTemp) "core_tmp" else "core_out"
    exportPath / tmpOrData / subPath
  }

  def existsTempFiles(): Boolean = {
    val base = getBasePath(isTemp = true)

    if (!Files.exists(base)) return false

    val dirs = Stack(base)

    while (dirs.nonEmpty) {
      val stream = Files.newDirectoryStream(dirs.pop())

      try {
        val iter = stream.iterator
        while (iter.hasNext) {
          val e = iter.next()

          if (!Files.isDirectory(e)) {
            return true
          }

          dirs += e
        }
      } finally {
        stream.close()
      }
    }

    false
  }

  def getMetadataPath(isTemp: Boolean, part: Int): Path =
    getBasePath(isTemp) / f"metadata_$part%02d.json"

  def getCollectionsPath(isTemp: Boolean) =
    getBasePath(isTemp) / "data" / "collections"

  def finalizeExport(
    hostID: HostID,
    part: Int,
    totalParts: Int,
    filesCount: Int,
    filesBytes: Long): Unit = {
    val tempMeta = getMetadataPath(isTemp = true, part)
    tempMeta.delete()
    tempMeta.getParent.mkdirs()

    val stream = new FileOutputStream(tempMeta.toFile)
    val obj = JSObject(
      "host_id" -> hostID.toString,
      "account_id" -> accountID.toLong.toString,
      "export_id" -> exportID,
      "task_id" -> rootTaskID.toLong.toString,
      "collections" -> collNames,
      "snapshot_ts" -> snapshotTS.toString,
      "doc_format" -> docFormat.toString,
      "datafile_format" -> datafileFormat.toString,
      "datafile_compression" -> datafileCompression,
      "datafile_ext" -> datafileExt,
      "part" -> part,
      "total_parts" -> totalParts,
      "files_count" -> filesCount,
      "files_bytes" -> filesBytes
    )
    obj.writeTo(stream, pretty = true)
    stream.close()

    val finalMeta = getMetadataPath(isTemp = false, part)
    tempMeta.moveAtomically(finalMeta, mkdirs = true)

    // clean up temp state
    cleanupTempPaths()
  }
}
