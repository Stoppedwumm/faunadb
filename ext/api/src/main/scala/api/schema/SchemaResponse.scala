package fauna.api.schema

import fauna.api.{ APIEndpoint, ProtocolErrorCode => Code }
import fauna.api.schema.SchemaRequest.DiffFormat
import fauna.atoms.SchemaVersion
import fauna.codex.json2.JSONWriter
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.serialization.{
  FQL2ValueEncoder,
  MaterializedValue,
  ValueFormat
}
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.schema.{ Result, SchemaError, SchemaSource }
import fauna.model.schema.manager.ValidateResult
import fauna.model.schema.SchemaIndexStatus
import fauna.net.http.{ Body, ContentType, HTTPHeaders, HttpResponse }
import fql.ast._
import fql.color.ColorKind
import fql.schema.{ Diff => SchemaDiff }
import io.netty.buffer.ByteBufAllocator
import io.netty.util.AsciiString
import scala.collection.mutable.ListBuffer

sealed abstract class AnySchemaResponse {
  import APIEndpoint.Response
  import Result.Ok

  def httpStatus: Int

  def toResponse(txnTS: Timestamp): Response

  final def toResult = Ok(this)
  final def toQuery = toResult.toQuery

  protected def render(schemaVersion: SchemaVersion)(
    renderFields: JSONWriter => Any): APIEndpoint.Response
}

sealed abstract class SchemaResponse(val httpStatus: Int = 200)
    extends AnySchemaResponse {
  import APIEndpoint.Response
  import SchemaResponse._

  override def render(schemaVersion: SchemaVersion)(
    renderFields: JSONWriter => Any): Response = {
    val buf = ByteBufAllocator.DEFAULT.buffer()
    val out = JSONWriter(buf)

    // Default fields
    out.writeObjectStart()
    out.writeObjectField(Field.Version, out.writeNumber(schemaVersion.toMicros))
    renderFields(out)
    out.writeObjectEnd()

    Response(HttpResponse(httpStatus, Body(buf, ContentType.JSON)))
  }
}

object SchemaResponse {

  type Headers = Seq[(AsciiString, AnyRef)]

  object Field {
    val Version = AsciiString.cached("version")
    val Files = AsciiString.cached("files")
    val Filename = AsciiString.cached("filename")
    val Error = AsciiString.cached("error")
    val Message = AsciiString.cached("message")
    val Content = AsciiString.cached("content")
    val PendingSummary = AsciiString.cached("pending_summary")
    val Diff = AsciiString.cached("diff")
    val Span = AsciiString.cached("span")
    val Start = AsciiString.cached("start")
    val End = AsciiString.cached("end")
    val Definition = AsciiString.cached("definition")
    val Status = AsciiString.cached("status")
  }

  final case class Unchanged(version: SchemaVersion) extends SchemaResponse(204) {
    def toResponse(txnTS: Timestamp) = render(version) { _ => () }
  }

  case object SchemaUpdated extends SchemaResponse {
    def toResponse(txnTS: Timestamp) = render(SchemaVersion(txnTS)) { _ => () }
  }

  final case class SchemaSources(version: SchemaVersion, sources: Seq[SchemaSource])
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(version) { out =>
        out.writeObjectField(
          Field.Files,
          sources
            .foldLeft(out.writeArrayStart()) { (out, source) =>
              out
                .writeObjectStart()
                .writeObjectField(Field.Filename, out.writeString(source.filename))
                .writeObjectEnd()
            }
            .writeArrayEnd()
        )
      }
  }

  final case class SourceContent(version: SchemaVersion, source: SchemaSource)
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(version) { out =>
        out.writeObjectField(Field.Content, out.writeString(source.file.content))
      }
  }

  final case class Diff(
    version: SchemaVersion,
    before: Map[Src.Id, String],
    after: Map[Src.Id, String],
    diffs: Seq[SchemaDiff],
    color: ColorKind,
    diffKind: DiffFormat)
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(version) { out =>
        out.writeObjectField(
          Field.Diff,
          out.writeString(diffKind.render(before, after, diffs, color)))
      }
  }

  object Diff {
    def fromValidate(
      version: SchemaVersion,
      res: ValidateResult,
      req: SchemaRequest.Authenticated): Diff =
      Diff(
        version,
        before = res.beforeMap,
        after = res.afterMap,
        diffs = res.diffs,
        color = req.colorKind,
        diffKind = req.diffFormat.getOrElse(DiffFormat.Semantic)
      )
  }

  final case class StagedStatus(
    version: SchemaVersion,
    status: Option[SchemaIndexStatus])
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(version) { out =>
        out.writeObjectField(
          Field.Status,
          out.writeString(status.fold("none") { _.asStr }))
      }
  }

  final case class StagedSummary(
    version: SchemaVersion,
    diff: fauna.model.schema.manager.StagedDiff,
    before: Map[Src.Id, String],
    after: Map[Src.Id, String],
    color: ColorKind,
    diffKind: DiffFormat)
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(version) { out =>
        out.writeObjectField(Field.Status, out.writeString(diff.statusStr))
        out.writeObjectField(
          Field.PendingSummary,
          out.writeString(diff.pendingSummary))
        out.writeObjectField(
          Field.Diff,
          out.writeString(diffKind.render(before, after, diff.diffs, color))
        )
      }
  }

  object SourceItem {

    private[SchemaResponse] def render(
      out: JSONWriter,
      txnTS: Timestamp,
      filename: String,
      snippet: String,
      span: Span,
      definition: MaterializedValue) =
      out
        .writeObjectField(Field.Filename, out.writeString(filename))
        .writeObjectField(
          Field.Span,
          out
            .writeObjectStart()
            .writeObjectField(Field.Start, out.writeNumber(span.start))
            .writeObjectField(Field.End, out.writeNumber(span.end))
            .writeObjectEnd()
        )
        .writeObjectField(Field.Content, out.writeString(snippet))
        .writeObjectField(
          Field.Definition,
          FQL2ValueEncoder.encode(
            ValueFormat.Simple,
            out,
            definition,
            txnTS
          ))
  }

  final case class SourceItem(
    version: SchemaVersion,
    filename: String,
    snippet: String,
    span: Span,
    definition: MaterializedValue)
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(version) {
        SourceItem.render(_, txnTS, filename, snippet, span, definition)
      }
  }

  final case class ItemUpdated(
    filename: String,
    snippet: String,
    span: Span,
    definition: MaterializedValue)
      extends SchemaResponse {

    def toResponse(txnTS: Timestamp) =
      render(SchemaVersion(txnTS)) {
        SourceItem.render(_, txnTS, filename, snippet, span, definition)
      }
  }

  final case class ErrorResponse(code: Code, message: String, headers: Headers = Nil)
      extends SchemaResponse {
    import APIEndpoint.Response

    override def toResponse(ts: Timestamp): Response = {
      val buf = ByteBufAllocator.DEFAULT.buffer()
      val out = JSONWriter(buf)
      render(out)

      Response(
        HttpResponse(
          code.httpStatus,
          Body(buf, ContentType.JSON),
          headers
        ))
    }

    private def render(out: JSONWriter) =
      out
        .writeObjectStart()
        .writeObjectField(
          Field.Error,
          out
            .writeObjectStart()
            .writeObjectField(Field.Message, out.writeString(message))
            .writeObjectEnd()
        )
        .writeObjectEnd()
  }

  object ErrorResponse {
    val NotFound = ErrorResponse(Code.NotFound, "Not found")
    val MethodNotAllowed = ErrorResponse(Code.MethodNotAllowed, "Method not allowed")
    val Forbidden = ErrorResponse(Code.Forbidden, "Access denied")

    val Unauthorized = ErrorResponse(
      Code.Unauthorized,
      "Access token required",
      Seq(HTTPHeaders.WWWAuthenticate -> "Bearer"))

    def Conflict(msg: String) = ErrorResponse(Code.Conflict, msg)
    def BadRequest(msg: String) = ErrorResponse(Code.BadRequest, msg)

    def apply(errs: List[SchemaError]): ErrorResponse = {
      def render(
        errs: Iterable[SchemaError],
        strs: ListBuffer[String] = ListBuffer.empty,
        ctx: Map[Src.Id, String] = Map.empty)
        : Either[ErrorResponse, ListBuffer[String]] = {
        val iter = errs.iterator
        while (iter.hasNext) {
          iter.next() match {
            case SchemaError.Forbidden         => return Left(Forbidden)
            case SchemaError.NotFound          => return Left(NotFound)
            case SchemaError.Conflict(msg)     => return Left(Conflict(msg))
            case SchemaError.Unexpected(cause) => throw cause // let it 500
            case SchemaError.Validation(msg)   => strs += msg
            case SchemaError.FQLError(err)     => strs += err.renderWithSource(ctx)

            case SchemaError.QueryFailure(qf) =>
              qf match {
                case rf: QueryRuntimeFailure => strs += rf.message // nicer msg
                case other                   => strs += other.failureMessage
              }
            case SchemaError.SchemaSourceErrors(errs, files) =>
              val ctx0 = ctx ++ files.view.map { f => f.src -> f.content }.toMap
              render(errs, strs, ctx0)

            case SchemaError.InvalidStoredSchema =>
              strs.clear() // nothing else matters.
              strs +=
                """|An unexpected error occurred during schema generation resulting in invalid schema
                   |files. Please check your FSL files using the Fauna CLI tool.
                   |
                   |Contact support via https://support.fauna.com if you need assistance.""".stripMargin
              return Right(strs)
          }
        }
        Right(strs)
      }
      render(errs) match {
        case Right(strs) => BadRequest(strs.mkString("\n"))
        case Left(err)   => err
      }
    }
  }
}
