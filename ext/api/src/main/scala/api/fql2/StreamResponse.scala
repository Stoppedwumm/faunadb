package fauna.api.fql2

import fauna.api.fql2.FQL2Response.ProtocolError
import fauna.api.APIEndpoint.Response
import fauna.api.ProtocolErrorCode
import fauna.codex.json2.JSONWriter
import fauna.exec.Observable
import fauna.lang.syntax.CharSequenceOps
import fauna.model.runtime.stream.Event
import fauna.model.stream.StreamID
import fauna.net.http.{ Body, Chunked, ContentType, HTTPHeaders, HttpResponse }
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.util.AsciiString

trait StreamResponse {
  def toResponse: Response
}

object StreamResponse {
  private[this] val EventSeparator = "\r\n".toUTF8Buf.asReadOnly()

  final case class Success(
    streamID: StreamID,
    events: Observable[(Event, Event.Metrics)])
      extends StreamResponse {

    def toResponse: Response =
      Response(
        HttpResponse(
          HttpResponseStatus.OK,
          (HTTPHeaders.FaunaStreamID -> streamID.toLong.toString) :: Nil,
          Chunked(renderEvents(events), ContentType.JSON)
        )
      )

    private def renderEvents(
      events: Observable[(Event, Event.Metrics)]): Observable[ByteBuf] =
      events map { case (event, metrics) =>
        val buf = ByteBufAllocator.DEFAULT.buffer()
        event.encode(JSONWriter(buf), metrics)
        buf.writeBytes(EventSeparator.duplicate())
      }
  }

  object Error {
    object Field {
      val Error = AsciiString.cached("error")
      val Code = AsciiString.cached("code")
      val Message = AsciiString.cached("message")
    }
    val Unauthorized = Error(
      ProtocolErrorCode.Unauthorized,
      "Invalid token, unable to authenticate request")

    def badRequest(message: String): Error =
      Error(errorCode = ProtocolErrorCode.BadRequest, message = message)
  }

  final case class Error(
    errorCode: ProtocolErrorCode,
    message: String
  ) extends StreamResponse {
    import Error._

    def toResponse: Response = {
      val alloc = ByteBufAllocator.DEFAULT
      val buf = alloc.buffer()
      val out = JSONWriter(buf)
      out.writeObjectStart()
      out.writeObjectField(Field.Error, renderError(out))
      out.writeObjectEnd()
      Response(
        HttpResponse(errorCode.httpStatus, Body(buf, ContentType.JSON))
      )
    }

    private def renderError(out: JSONWriter) = {
      out.writeObjectStart()
      out.writeObjectField(
        Field.Code,
        out.writeString(ProtocolError.codeToString(errorCode)))
      out.writeObjectField(Field.Message, out.writeString(message))
      out.writeObjectEnd()
    }
  }
}
