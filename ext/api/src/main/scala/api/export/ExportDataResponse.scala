package fauna.api.export

import fauna.api.APIEndpoint.Response
import fauna.api.ProtocolErrorCode
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.model.tasks.export.ExportInfo
import fauna.model.tasks.ExportDataTask
import fauna.model.Task
import fauna.net.http.{ Body, ContentType, HTTPHeaders, HttpResponse, NoBody }
import io.netty.buffer.ByteBufAllocator
import io.netty.util.AsciiString

abstract class AbstractExportDataResponse {
  def toResponse(): Response
}

object ExportDataResponse {
  type Headers = Seq[(AsciiString, AnyRef)]

  object Fields {
    val Path = JSON.Escaped("path")
    val AccountID = JSON.Escaped("account_id")
    val ExportID = JSON.Escaped("export_id")
    val TaskID = JSON.Escaped("task_id")
    val Collections = JSON.Escaped("collections")
    val SnapshotTS = JSON.Escaped("snapshot_ts")
    val DocFormat = JSON.Escaped("doc_format")
    val DatafileFormat = JSON.Escaped("datafile_format")
    val DatafileCompression = JSON.Escaped("datafile_compression")
    val DatafileExt = JSON.Escaped("datafile_ext")
    val Parts = JSON.Escaped("parts")
    val State = JSON.Escaped("state")
    val Error = JSON.Escaped("error")
  }

  def Ok(task: Task) = Success(task, 200)
  def Created(task: Task) = Success(task, 201)

  case class Success(task: Task, status: Int) extends AbstractExportDataResponse {
    val info = ExportInfo(task)
    def accountID: String = info.accountID.toLong.toString
    def exportID: String = info.exportID
    def taskID: String = info.rootTaskID.toLong.toString
    def collections: Seq[String] = info.collNames.sorted
    def snapshotTS: String = info.snapshotTS.toString
    def docFormat: String = info.docFormat.toString
    def datafileFormat: String = info.datafileFormat.toString
    def datafileCompression: Boolean = info.datafileCompression
    def datafileExt: String = info.datafileExt
    def parts: Int = task.data(ExportDataTask.TotalPartitionsField).getOrElse(1)
    def path: String = info.subPath.toString
    def error: Option[String] = task.data(ExportDataTask.ErrorField)

    def state =
      if (task.isCompleted) {
        if (error.isDefined) "failed" else "complete"
      } else {
        "pending"
      }

    def toResponse(): Response = {
      val buf = ByteBufAllocator.DEFAULT.buffer()
      val out = JSONWriter(buf)

      out.writeObjectStart()
      out.writeObjectField(Fields.Path, out.writeString(path))
      out.writeObjectField(Fields.AccountID, out.writeString(accountID))
      out.writeObjectField(Fields.ExportID, out.writeString(exportID))
      out.writeObjectField(Fields.TaskID, out.writeString(taskID))

      def writeCollections() = {
        out.writeArrayStart()
        collections.foreach(out.writeString)
        out.writeArrayEnd()
      }

      out.writeObjectField(Fields.Collections, writeCollections())
      out.writeObjectField(Fields.SnapshotTS, out.writeString(snapshotTS))
      out.writeObjectField(Fields.DocFormat, out.writeString(docFormat))
      out.writeObjectField(Fields.DatafileFormat, out.writeString(datafileFormat))
      out.writeObjectField(
        Fields.DatafileCompression,
        out.writeBoolean(datafileCompression))
      out.writeObjectField(Fields.DatafileExt, out.writeString(datafileExt))
      out.writeObjectField(Fields.Parts, out.writeNumber(parts))
      out.writeObjectField(Fields.State, out.writeString(state))
      error.foreach { err =>
        out.writeObjectField(Fields.Error, out.writeString(err))
      }
      out.writeObjectEnd()

      Response(HttpResponse(status, Body(buf, ContentType.JSON)))
    }
  }

  object Deleted extends AbstractExportDataResponse {
    def toResponse(): Response = {
      Response(HttpResponse(204, NoBody))
    }
  }

  def BadRequest(message: String) =
    ErrorResponse(ProtocolErrorCode.BadRequest, message)

  case class ErrorResponse(
    code: ProtocolErrorCode,
    message: String,
    headers: Headers = Nil)
      extends AbstractExportDataResponse {
    def toResponse(): Response = {
      val buf = ByteBufAllocator.DEFAULT.buffer()
      val out = JSONWriter(buf)

      out.writeObjectStart()
      out.writeObjectField(Fields.Error, out.writeString(message))
      out.writeObjectEnd()

      Response(
        HttpResponse(
          code.httpStatus,
          Body(buf, ContentType.JSON),
          headers
        ))
    }
  }

  val NotFound = ErrorResponse(ProtocolErrorCode.NotFound, "Not found")
  val MethodNotAllowed =
    ErrorResponse(ProtocolErrorCode.MethodNotAllowed, "Method not allowed")
  val Forbidden = ErrorResponse(ProtocolErrorCode.Forbidden, "Access denied")

  val Unauthorized = ErrorResponse(
    ProtocolErrorCode.Unauthorized,
    "Access token required",
    Seq(HTTPHeaders.WWWAuthenticate -> "Bearer"))
}
