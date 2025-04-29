package fauna.api.fql2

import fauna.codex.json.{ JsonCodec, JsonDecoder }
import fauna.lang.Timestamp
import fauna.model.runtime.stream.Feed
import fauna.repo.values.Value
import io.netty.buffer.ByteBuf
import scala.util.control.NoStackTrace
import scala.util.Try

final case class FeedRequest(
  sourceIR: Value.EventSource.IR,
  cursorOverride: Option[Value.EventSource.Cursor],
  pageSize: Int
)

object FeedRequest {

  final case class Body(
    source: String,
    startTime: Option[Long],
    cursor: Option[String],
    pageSize: Option[Int]
  )

  object Body {
    implicit val Codec =
      JsonDecoder.Record("token", "start_ts", "cursor", "page_size")(Body.apply)
  }

  final class ParseError(val message: String)
      extends Exception(message)
      with NoStackTrace

  def fromBody(buf: ByteBuf): Try[FeedRequest] = {
    def parseError(msg: String) =
      throw new ParseError(msg)

    JsonCodec.decode[Body](buf) map { body =>
      if (body.startTime.nonEmpty && body.cursor.nonEmpty) {
        parseError("can not provide both `start_ts` and `cursor`.")
      } else {
        val source =
          Value.EventSource
            .fromBase64(body.source)
            .getOrElse {
              parseError("invalid event source provided.")
            }

        val cursor =
          body.cursor map { str =>
            Value.EventSource.Cursor
              .fromBase64(str)
              .getOrElse {
                parseError("invalid event cursor provided.")
              }
          } orElse {
            body.startTime map { ts =>
              Value.EventSource.Cursor(Timestamp.ofMicros(ts))
            }
          }

        val pageSize = body.pageSize.getOrElse(Feed.DefaultPageSize)
        if (pageSize <= 0) {
          parseError("page size must be greater than zero.")
        }
        if (pageSize > Feed.MaxPageSize) {
          parseError(s"page size can not be greater than ${Feed.MaxPageSize}.")
        }

        FeedRequest(source, cursor, pageSize)
      }
    } recover { case err =>
      parseError(s"Invalid request body: ${err.getMessage}")
    }
  }
}
