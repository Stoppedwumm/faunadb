package fauna.api.fql2

import fauna.api.fql2.FQL2Response.ProtocolError
import fauna.api.APIEndpoint.Response
import fauna.api.ProtocolErrorCode
import fauna.codex.json2.JSONWriter
import fauna.model.runtime.stream.Feed
import fauna.net.http.{ Body, ContentType, HttpResponse }
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.util.AsciiString

trait FeedResponse {
  def toResponse: Response
}

object FeedResponse {

  object Success {
    object Field {
      val Events = AsciiString.cached("events")
      val Next = AsciiString.cached("cursor")
      val HasNext = AsciiString.cached("has_next")
      val Stats = AsciiString.cached("stats")
    }
  }

  final case class Success(page: Feed.Page) extends FeedResponse {

    def toResponse: Response =
      Response(
        HttpResponse(
          HttpResponseStatus.OK,
          Body(renderEvents(page), ContentType.JSON)
        )
      )

    private def renderEvents(page: Feed.Page): ByteBuf = {
      val buf = ByteBufAllocator.DEFAULT.buffer()
      val out = JSONWriter(buf)
      out.writeObjectStart()
      out.writeObjectField(
        Success.Field.Events, {
          out.writeArrayStart()
          page.events map { case (event, metrics) =>
            event.encode(out, metrics)
          }
          out.writeArrayEnd()
        })
      out.writeObjectField(Success.Field.Next, out.writeString(page.cursor.toBase64))
      out.writeObjectField(Success.Field.HasNext, out.writeBoolean(page.hasNext))
      out.writeObjectField(Success.Field.Stats, page.stats.encode(out))
      out.writeObjectEnd()
      buf
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
  ) extends FeedResponse {
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
