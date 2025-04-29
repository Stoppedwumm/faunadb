package fauna.net.test

import fauna.exec._
import fauna.lang.syntax._
import fauna.net.http._
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.embedded._
import io.netty.handler.codec.http.{ HttpResponse => _, _ }
import io.netty.util.CharsetUtil
import java.nio.channels.ClosedChannelException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class HttpResponseSpec extends Spec {
  object MockHandler extends ChannelHandler {
    def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit = ()
    def handlerAdded(ctx: ChannelHandlerContext): Unit = ()
    def handlerRemoved(ctx: ChannelHandlerContext): Unit = ()
  }

  class MockChannel extends EmbeddedChannel(MockHandler) {
    var messages: List[Any] = Nil

    override def write(msg: Object) = { messages :+= msg; newSucceededFuture }
    override def writeAndFlush(msg: Object) = { messages :+= msg; newSucceededFuture }
  }

  "HttpResponse" - {
    "will handle channel exceptions" in {
      val chan = new MockChannel {
        override def write(msg: Object): ChannelFuture = {
          newFailedFuture(new ClosedChannelException)
        }
      }

      val msg = HttpResponse(200, NoBody)
      Await.result(msg.writeTo(chan), Duration.Inf) should equal(())
    }

    "will write a NoBody message" in {
      val chan = new MockChannel
      val msg = HttpResponse(200, NoBody)
      Await.result(msg.writeTo(chan), Duration.Inf)

      chan.messages match {
        case List(resp: DefaultHttpResponse, LastHttpContent.EMPTY_LAST_CONTENT) =>
          HttpUtil.isTransferEncodingChunked(resp) should equal (false)
          resp.status should equal(HttpResponseStatus.OK)
          resp.headers.get(HTTPHeaders.ContentLength) should equal("0")
        case resp =>
          fail(s"wrong response $resp")
      }
    }

    "will write a NoBody message w/o Content-Length" in {
      val chan = new MockChannel
      val msg = HttpResponse(200, NoBody)
      Await.result(msg.writeTo(chan, setContentLength = false), Duration.Inf)

      chan.messages match {
        case List(resp: DefaultHttpResponse, LastHttpContent.EMPTY_LAST_CONTENT) =>
          HttpUtil.isTransferEncodingChunked(resp) should equal(false)
          resp.status should equal(HttpResponseStatus.OK)
          resp.headers.contains(HTTPHeaders.ContentLength) should equal(false)
        case resp =>
          fail(s"wrong response $resp")
      }
    }

    "will write a standard message" in {
      val chan = new MockChannel
      val msg = HttpResponse(200, Body("response", ContentType.Text))
      Await.result(msg.writeTo(chan), Duration.Inf)

      chan.messages match {
        case List(resp: DefaultHttpResponse, cont: DefaultHttpContent, LastHttpContent.EMPTY_LAST_CONTENT) =>
          HttpUtil.isTransferEncodingChunked(resp) should equal (false)
          resp.status should equal(HttpResponseStatus.OK)
          resp.headers.get(HTTPHeaders.ContentLength) should equal("8")
          resp.headers.get(HTTPHeaders.ContentType) should equal(ContentType.Text)
          cont.content.toString(CharsetUtil.UTF_8) should equal("response")
        case resp =>
          fail(s"wrong response $resp")
      }
    }

    "will write a chunked message" in {
      def buf(str: String) = str.toUTF8Buf

      val chan = new MockChannel {
        @volatile var count: Int = 0
        override def writeAndFlush(msg: Object) = {
          val p = newPromise
          Future {
            val sleep = count
            synchronized { count += 1 }
            if (sleep % 2 == 0) Thread.sleep(20)
            messages :+= msg
            p.setSuccess()
          }
          p
        }
      }

      val bufs = Seq(buf("a"), buf("b"), buf("c"), buf("d"))
      val (pub, obs) = Observable.gathering[ByteBuf](OverflowStrategy.unbounded)
      val body = Chunked(obs, ContentType.Text)
      val msg = HttpResponse(200, body)
      bufs foreach { pub.publish(_) }
      pub.close()
      Await.result(msg.writeTo(chan), Duration.Inf)

      chan.messages.size should equal(6)

      chan.messages.head match {
        case resp: DefaultHttpResponse =>
          HttpUtil.isTransferEncodingChunked(resp) should equal (true)
          resp.status should equal(HttpResponseStatus.OK)
          resp.headers.get(HTTPHeaders.ContentType) should equal(ContentType.Text)
        case resp =>
          fail(s"wrong response: $resp")
      }

      val values = chan.messages.collect {
        case msg: HttpContent => msg.content.toString(CharsetUtil.UTF_8)
      }
      values should equal(List("a", "b", "c", "d", ""))
    }
  }
}
