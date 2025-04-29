package fauna.net.http

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.{
  DefaultHttpContent,
  HttpRequest => NettyRequest
}
import io.netty.handler.codec.http.multipart.{
  DefaultHttpDataFactory,
  HttpData,
  HttpPostMultipartRequestDecoder,
  HttpPostRequestDecoder
}
import io.netty.handler.codec.http.HttpConstants
import io.netty.util.CharsetUtil
import scala.util.control.NoStackTrace

final case class HttpBodyPart(name: String, content: String)

object HttpBodyPart {
  object NonMultiPartRequestException extends NoStackTrace

  def parse(
    req: NettyRequest,
    body: ByteBuf,
    // Default to the netty default for number of files.
    maxFiles: Int = 128): Seq[HttpBodyPart] = {
    if (!HttpPostRequestDecoder.isMultipart(req)) {
      throw NonMultiPartRequestException
    }

    val decoder = new HttpPostMultipartRequestDecoder(
      new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE),
      req,
      HttpConstants.DEFAULT_CHARSET,
      maxFiles,
      1024 // HttpPostRequestDecoder.DEFAULT_MAX_BUFFERED_BYTES (private)
    )
    try {
      decoder.offer(new DefaultHttpContent(body.duplicate()))
      val res = Seq.newBuilder[HttpBodyPart]
      while (decoder.hasNext) {
        decoder.next() match {
          case data: HttpData =>
            val content = data.getString(CharsetUtil.UTF_8)
            val part = HttpBodyPart(data.getName, content)
            res += part
          case _ => ()
        }
      }
      res.result()
    } finally {
      decoder.destroy()
    }
  }
}
