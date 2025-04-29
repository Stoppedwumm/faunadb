package fauna.api.fql2

import fauna.api.util.RequestLogging
import fauna.api.APIEndpoint.{ Response, ResponseInfo }
import fauna.api.ProtocolErrorCode
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.model.runtime.fql2.{ InfoWarns, QueryFailure, QueryRuntimeFailure }
import fauna.model.runtime.fql2.serialization.{
  FQL2ValueEncoder,
  MaterializedValue,
  ValueFormat
}
import fauna.model.runtime.fql2.RedactedQuery
import fauna.net.http._
import fauna.repo.schema.ConstraintFailure
import fql.ast.{ Src, TypeExpr }
import fql.ast.display._
import fql.error.Diagnostic
import io.netty.buffer.ByteBufAllocator
import io.netty.util.AsciiString
import scala.util.control.NoStackTrace

case class FQL2ResponseError(response: FQL2Response) extends NoStackTrace

sealed trait FQL2Response {
  def toResponse(info: ResponseInfo): Response
}

object FQL2Response {
  type Headers = Seq[(AsciiString, AnyRef)]

  object Field {
    val Data = AsciiString.cached("data")
    val StaticType = AsciiString.cached("static_type")

    val Error = AsciiString.cached("error")
    val Code = AsciiString.cached("code")
    val Message = AsciiString.cached("message")
    val Abort = AsciiString.cached("abort")
    val ConstraintFailures = AsciiString.cached("constraint_failures")
    val Paths = AsciiString.cached("paths")

    val Summary = AsciiString.cached("summary")
    val TxnTs = AsciiString.cached("txn_ts")
    val SchemaVersion = AsciiString.cached("schema_version")
    val QueryTags = AsciiString.cached("query_tags")

    val Stats = AsciiString.cached("stats")
    val ComputeOps = AsciiString.cached("compute_ops")
    val ReadOps = AsciiString.cached("read_ops")
    val WriteOps = AsciiString.cached("write_ops")
    val QueryTime = AsciiString.cached("query_time_ms")
    val BytesIn = AsciiString.cached("bytes_in")
    val BytesOut = AsciiString.cached("bytes_out")
    val StorageBytesRead = AsciiString.cached("storage_bytes_read")
    val StorageBytesWrite = AsciiString.cached("storage_bytes_write")
    val ContentionRetries = AsciiString.cached("contention_retries")
  }

  sealed protected trait FQL2JSONResponse extends FQL2Response {
    def httpStatus: Int
    def headers: FQL2Response.Headers
    def redacted: Option[RedactedQuery]
    def schemaVersion: Option[Long]

    protected def renderResult(info: ResponseInfo, out: JSONWriter): Unit

    protected def renderInfo(info: ResponseInfo, out: JSONWriter): Unit

    def render(info: ResponseInfo, out: JSONWriter): Unit = {
      out.writeObjectStart()
      renderResult(info, out)
      renderInfo(info, out)

      // all responses echo query tags
      if (info.userTags.tags.nonEmpty) {
        out.writeObjectField(
          Field.QueryTags,
          out.writeString(info.userTags.toHTTPHeader))
      }

      // add schema version when present
      schemaVersion.foreach { sv =>
        out.writeObjectField(
          Field.SchemaVersion,
          out.writeNumber(sv)
        )
      }

      out.writeObjectEnd()
    }

    def toResponse(info: ResponseInfo): Response = {
      val alloc = ByteBufAllocator.DEFAULT
      val buf = alloc.buffer()
      val out = JSONWriter(buf)
      render(info, out)
      Response(
        HttpResponse(httpStatus, Body(buf, ContentType.JSON), headers),
        redacted = redacted
      )
    }
  }

  sealed protected trait FQL2InfoResponse extends FQL2JSONResponse {

    protected def checkDiagnostics: Iterable[Diagnostic]
    protected def runtimeDiagnostics: Iterable[Diagnostic]
    protected def sourceCtx: Map[Src.Id, String]

    protected def renderInfo(info: ResponseInfo, out: JSONWriter): Unit = {

      // summary
      out.writeObjectField(
        Field.Summary, {
          // TODO: We might want a distinct separator between check and runtime
          // diagnostics.
          val str =
            checkDiagnostics.iterator
              .concat(runtimeDiagnostics)
              .map(_.renderWithSource(sourceCtx))
              .mkString("\n\n")
          out.writeString(str)
        }
      )

      // txntime
      out.writeObjectField(Field.TxnTs, out.writeNumber(info.txnTime.micros))

      // stats
      out.writeObjectField(Field.Stats, renderStats(info, out))
    }

    protected def renderStats(info: ResponseInfo, out: JSONWriter): Unit = {
      out.writeObjectStart()

      out.writeObjectField(
        Field.ComputeOps,
        out.writeNumber(info.metrics.computeOps))
      out.writeObjectField(Field.ReadOps, out.writeNumber(info.metrics.byteReadOps))
      out.writeObjectField(
        Field.WriteOps,
        out.writeNumber(info.metrics.byteWriteOps))
      out.writeObjectField(Field.QueryTime, out.writeNumber(info.metrics.queryTime))
      out.writeObjectField(
        Field.ContentionRetries,
        out.writeNumber(info.metrics.txnRetries))
      // TODO: bytes in/out, why
      // out.writeObjectField(Field.BytesIn,
      // out.writeNumber(info.metrics.queryBytesIn))
      // out.writeObjectField(Field.BytesOut,
      // out.writeNumber(info.metrics.queryBytesOut))
      out.writeObjectField(
        Field.StorageBytesRead,
        out.writeNumber(info.metrics.storageBytesRead))
      out.writeObjectField(
        Field.StorageBytesWrite,
        out.writeNumber(info.metrics.storageBytesWrite))

      out.writeObjectField(
        RequestLogging.RateLimitsField, {
          out.writeArrayStart()
          if (info.metrics.rateLimitComputeHit) {
            out.writeString(RequestLogging.RateLimitHitCompute)
          }
          if (info.metrics.rateLimitReadHit) {
            out.writeString(RequestLogging.RateLimitHitRead)
          }
          if (info.metrics.rateLimitWriteHit) {
            out.writeString(RequestLogging.RateLimitHitWrite)
          }
          out.writeArrayEnd()
        }
      )

      // FIXME: enabling this has stalled, so I didn't bother plumbing through
      // config here.
      // if (config.query_enable_expected_query_time) {
      // out.writeObjectField("expected_query_time_ms",
      // out.writeNumber(metrics.expectedQueryTime))
      // }
      out.writeObjectEnd()
    }
  }

  // A Success response will always contain a "data" field with the result of
  // the query. It will not contain an "error" field. It may contain a
  // "static_type" field. The HTTP status code is 200.
  final case class Success(
    result: MaterializedValue,
    staticType: Option[TypeExpr],
    sourceCtx: Map[Src.Id, String],
    infoWarns: InfoWarns,
    redacted: Option[RedactedQuery],
    format: ValueFormat,
    schemaVersion: Option[Long],
    headers: Headers = Seq.empty)
      extends FQL2InfoResponse {

    val httpStatus = 200

    def checkDiagnostics = infoWarns.check
    def runtimeDiagnostics = infoWarns.runtime

    def renderResult(info: ResponseInfo, out: JSONWriter) = {
      out.writeObjectField(
        Field.Data,
        FQL2ValueEncoder.encode(format, out, result, info.txnTime))
      staticType.foreach { ty =>
        out.writeObjectField(Field.StaticType, out.writeString(ty.display))
      }
    }
  }

  // An Error response will always contain an "error" field with information
  // about the error. It will not contain a "data" field. The HTTP response code
  // will depend on the underlying error.
  sealed trait Error extends FQL2JSONResponse {
    def renderResult(info: ResponseInfo, out: JSONWriter): Unit =
      out.writeObjectField(Field.Error, renderError(info, out))

    def renderError(info: ResponseInfo, out: JSONWriter): JSON.Out
  }

  case class ProtocolError(
    code: ProtocolErrorCode,
    message: String,
    headers: Headers = Nil,
    withStats: Boolean = false)
      extends Error
      with FQL2InfoResponse {

    def httpStatus = code.httpStatus

    def redacted = None
    def sourceCtx = Map.empty
    def checkDiagnostics = Iterable.empty
    def runtimeDiagnostics = Iterable.empty
    def schemaVersion = None

    override def renderResult(info: ResponseInfo, out: JSONWriter): Unit = {
      super.renderResult(info, out)
      if (withStats) {
        out.writeObjectField(Field.Stats, renderStats(info, out))
      }
    }

    def renderError(info: ResponseInfo, out: JSONWriter): JSON.Out = {
      out.writeObjectStart()
      out.writeObjectField(
        Field.Code,
        out.writeString(ProtocolError.codeToString(code)))
      out.writeObjectField(Field.Message, out.writeString(message))
      out.writeObjectEnd()
    }

    override def renderInfo(info: ResponseInfo, out: JSONWriter): Unit = ()

  }

  object ProtocolError {
    val Unauthorized = ProtocolError(
      ProtocolErrorCode.Unauthorized,
      "Access token required",
      Seq(HTTPHeaders.WWWAuthenticate -> "Bearer"))

    val Forbidden = ProtocolError(ProtocolErrorCode.Forbidden, "Access denied")

    def codeToString(code: ProtocolErrorCode) =
      code match {
        case ProtocolErrorCode.Unauthorized                => "unauthorized"
        case ProtocolErrorCode.Forbidden                   => "forbidden"
        case ProtocolErrorCode.NotFound                    => "not_found"
        case ProtocolErrorCode.MethodNotAllowed            => "method_not_allowed"
        case ProtocolErrorCode.Conflict                    => "conflict"
        case ProtocolErrorCode.Disabled                    => "forbidden"
        case ProtocolErrorCode.BadRequest                  => "invalid_request"
        case ProtocolErrorCode.RequestTooLarge             => "request_size_exceeded"
        case ProtocolErrorCode.ProcessingTimeLimitExceeded => "time_out"
        case ProtocolErrorCode.ServiceTimeout              => "time_out"
        case ProtocolErrorCode.TooManyRequests             => "limit_exceeded"
        case ProtocolErrorCode.InternalError               => "internal_error"
        case ProtocolErrorCode.OperatorError               => "internal_error"
        case ProtocolErrorCode.ContendedTransaction        => "contended_transaction"
      }
  }

  final case class QueryError(
    fail: QueryFailure,
    sourceCtx: Map[Src.Id, String],
    infoWarns: InfoWarns,
    redacted: Option[RedactedQuery],
    format: ValueFormat,
    schemaVersion: Option[Long],
    headers: Headers = Seq.empty)
      extends Error
      with FQL2InfoResponse {

    // FIXME: change status code depending on the type of error.
    def httpStatus: Int = 400

    def checkDiagnostics = infoWarns.check ++ fail.errors
    def runtimeDiagnostics = infoWarns.runtime

    def renderError(info: ResponseInfo, out: JSONWriter): JSON.Out = {
      out.writeObjectStart()
      out.writeObjectField(Field.Code, out.writeString(fail.code))
      out.writeObjectField(Field.Message, out.writeString(fail.failureMessage))
      fail match {
        case e: QueryRuntimeFailure =>
          if (e.constraintFailures.nonEmpty) {
            out.writeObjectField(
              Field.ConstraintFailures,
              renderConstraintFailures(e.constraintFailures, out))
          }
          if (e.abortReturn.isDefined) {
            out.writeObjectField(
              Field.Abort,
              FQL2ValueEncoder.encode(format, out, e.abortReturn.get, info.txnTime))
          }
        case _ => ()
      }

      out.writeObjectEnd()
    }

    private def renderConstraintFailures(
      failures: Seq[ConstraintFailure],
      out: JSONWriter): Unit = {

      out.writeArrayStart()

      failures.foreach { failure =>
        out.writeObjectStart()
        out.writeObjectField(
          Field.Paths, {
            out.writeArrayStart()
            failure.fields foreach { field =>
              out.writeArrayStart()
              field.elements foreach {
                case Left(i)  => out.writeNumber(i)
                case Right(s) => out.writeString(s)
              }
              out.writeArrayEnd()
            }
            out.writeArrayEnd()
          }
        )
        out.writeObjectField(Field.Message, out.writeString(failure.message))
        out.writeObjectEnd()
      }

      out.writeArrayEnd()
    }
  }
}
