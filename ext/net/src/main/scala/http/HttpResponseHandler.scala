package fauna.net.http

import fauna.exec._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.net.netty.ChannelClosedException
import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.channel._
import io.netty.handler.codec.DecoderException
import io.netty.handler.codec.http.{ HttpResponse => NettyResponse, _ }
import javax.net.ssl.SSLException
import scala.concurrent.Promise

class HttpResponseHandler(responseP: Promise[HttpResponse], pooledBuffers: Boolean)
    extends SimpleChannelInboundHandler[HttpObject](false) {

  private val done = Promise[Unit]()
  def onComplete(f: => Any): Unit = done.future onComplete { _ => f }

  private def closeHandler(publisher: Publisher[ByteBuf]): Unit = {
    publisher.close()
    done.success(())
  }

  private var responseData: Option[Publisher[ByteBuf]] = None

  override def channelRead0(ctx: ChannelHandlerContext, resp: HttpObject): Unit =
    (resp: @unchecked) match {
      case response: NettyResponse =>
        val contentType = Option(response.headers.get(HTTPHeaders.ContentType)).getOrElse(ContentType.Text)
        val contentLength = HttpUtil.getContentLength(response, 0)
        val isChunked = HttpUtil.isTransferEncodingChunked(response)

        val body = if (isChunked || contentLength > 0) {
          val (publisher, buffers) =
            Observable.gathering[ByteBuf](
              OverflowStrategy.callbackOnOverflow {
                _.release()
              })

          val body = Chunked(buffers, contentType, Some(contentLength))

          // We got a FullHttpResponse back.
          response match {
            case chunk: HttpContent =>
              publisher.publish(outboundChunk(chunk))
              if (chunk.content.readableBytes == contentLength) {
                closeHandler(publisher)
              } else {
                responseData = Some(publisher)
              }
            case _ =>
              responseData = Some(publisher)
          }

          body
        } else {
          NoBody
        }

        responseP.success(new HttpResponse(response, body))

      case chunk: LastHttpContent =>
        val publisher = responseData.getOrElse(throw MessageOutOfOrderException)
        publisher.publish(outboundChunk(chunk))
        responseData = None
        closeHandler(publisher)

      case chunk: HttpContent =>
        val publisher = responseData.getOrElse(throw MessageOutOfOrderException)
        publisher.publish(outboundChunk(chunk))
    }

  private def outboundChunk(chunk: HttpContent) =
    if (pooledBuffers) chunk.content else {
      val rv = Unpooled.copiedBuffer(chunk.content)
      chunk.release()
      rv
    }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    // On some platforms (Windows), an SSL exception can get emitted during
    // the decoding phase. In this case, its better to emit the underlying exception.
    val unwrapped = e match {
      case ex: DecoderException if ex.getCause.isInstanceOf[SSLException] => ex.getCause
      case _ => e
    }

    responseData foreach { _.fail(unwrapped) }
    responseP.tryFailure(unwrapped)
    ctx.channel.closeQuietly()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    responseData foreach { _.fail(new ChannelClosedException) }
    responseP.tryFailure(new ChannelClosedException)
    super.channelInactive(ctx)
  }
}
