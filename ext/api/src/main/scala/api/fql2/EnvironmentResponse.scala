package fauna.api.fql2

import fauna.api.fql2.FQL2Response.ProtocolError
import fauna.api.APIEndpoint.Response
import fauna.api.ProtocolErrorCode
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.net.http.{ Body, ContentType, HttpResponse }
import io.netty.buffer.ByteBufAllocator
import io.netty.util.AsciiString

sealed trait EnvironmentResponse {
  protected def renderResult(out: JSONWriter): Unit
  def statusCode: Int
  def toResponse: Response = {
    val alloc = ByteBufAllocator.DEFAULT
    val buf = alloc.buffer()
    val out = JSONWriter(buf)
    out.writeObjectStart()
    renderResult(out)
    out.writeObjectEnd()
    Response(
      HttpResponse(
        statusCode,
        Body(buf, ContentType.JSON)
      )
    )
  }
}

object EnvironmentResponse {
  object Field {
    val SchemaVersion = AsciiString.cached("schema_version")
    val Environment = AsciiString.cached("environment")
    val Globals = AsciiString.cached("globals")
    val Types = AsciiString.cached("types")
    val Fields = AsciiString.cached("fields")
    val Self = AsciiString.cached("self")
    val Ops = AsciiString.cached("ops")
    val Apply = AsciiString.cached("apply")
    val Access = AsciiString.cached("access")
    val Alias = AsciiString.cached("alias")
    val TypeHint = AsciiString.cached("type_hint")
    val Error = AsciiString.cached("error")
    val Code = AsciiString.cached("code")
    val Message = AsciiString.cached("message")
    val TruncatedResources = AsciiString.cached("truncated_resources")
  }

  final case class Success(environment: Environment, schemaVersion: Long)
      extends EnvironmentResponse {
    override def statusCode: Int = 200

    override protected def renderResult(out: JSONWriter): Unit = {
      out.writeObjectField(
        Field.SchemaVersion,
        out.writeNumber(schemaVersion)
      )
      out.writeObjectField(
        Field.Environment,
        JSON.encode(out, environment)
      )
    }
  }

  final case class Error(statusCode: Int, code: String, message: String)
      extends EnvironmentResponse {
    override def renderResult(out: JSONWriter): Unit = {
      out.writeObjectField(Field.Code, out.writeString(code))
      out.writeObjectField(Field.Message, out.writeString(message))
    }
  }

  object Error {
    def apply(protocolErrorCode: ProtocolErrorCode, message: String): Error = {
      val status = protocolErrorCode.httpStatus
      val code = ProtocolError.codeToString(protocolErrorCode)
      Error(status, code, message)
    }

    val Unauthorized = Error(
      ProtocolErrorCode.Unauthorized,
      "Invalid token, unable to authenticate request")
    val Forbidden = Error(
      ProtocolErrorCode.Forbidden,
      "A valid admin/server key must be provided to use this endpoint")
  }
}
