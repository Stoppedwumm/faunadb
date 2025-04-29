package fauna.api.fql2

import fauna.codex.json.{ JsonCodec, JsonDecoder }
import fauna.lang.Timestamp
import fauna.repo.values.Value
import io.netty.buffer.ByteBuf
import scala.util.control.NoStackTrace
import scala.util.Try

final case class StreamRequest(
  streamIR: Value.EventSource.IR,
  cursorOverride: Option[Value.EventSource.Cursor]) {

  @inline def cursor = cursorOverride.getOrElse(streamIR.cursor)
}

object StreamRequest {
  final case class Body(
    stream: String,
    startTime: Option[Long],
    cursor: Option[String]
  )

  object Body {
    implicit val Codec =
      JsonDecoder.Record("token", "start_ts", "cursor")(Body.apply)
  }

  final class ParseError(val message: String)
      extends Exception(message)
      with NoStackTrace

  def fromBody(buf: ByteBuf): Try[StreamRequest] = {
    def parseError(msg: String) =
      throw new ParseError(msg)

    JsonCodec.decode[Body](buf) map { body =>
      if (body.startTime.nonEmpty && body.cursor.nonEmpty) {
        parseError("can not provide both `start_ts` and `cursor`")
      } else {
        val stream =
          Value.EventSource
            .fromBase64(body.stream)
            .getOrElse {
              parseError("invalid event source provided")
            }
        val cursor =
          body.cursor map { str =>
            Value.EventSource.Cursor
              .fromBase64(str)
              .getOrElse {
                parseError("invalid event cursor provided")
              }
          } orElse {
            body.startTime map { ts =>
              Value.EventSource.Cursor(
                Timestamp.ofMicros(ts),
                Value.EventSource.Cursor.MaxOrdinal)
            }
          }
        StreamRequest(stream, cursor)
      }
    } recover { case err =>
      parseError(s"Invalid request body: ${err.getMessage}")
    }
  }
}
