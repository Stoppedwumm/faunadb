package fauna.net.test

import fauna.lang.syntax._
import fauna.net._
import fauna.net.netty._
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.socket._
import io.netty.util.CharsetUtil
import java.net.{
  BindException,
  ConnectException,
  InetAddress,
  InetSocketAddress
}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future, Promise }

class SimpleNettyServerSpec extends Spec with NetworkHelpers {
  "SimpleNettyServer" - {
    "starts and stops" in {
      val port = findFreePort()
      val server = new EchoServer(port)
      val client = new StringClient

      server.isRunning should equal (false)

      a[ConnectException] should be thrownBy { Await.result(client.send(port, "foo"), Duration.Inf) }

      server.start()

      server.isRunning should equal (true)

      Await.result(client.send(port, "foo"), Duration.Inf) should equal ("foo")

      server.stop()

      server.isRunning should equal (false)

      a[ConnectException] should be thrownBy { Await.result(client.send(port, "foo"), Duration.Inf) }
    }

    "honors timeouts" in {
      pending
      val port = findFreePort()
      val server = new HoleServer(port)
      val client = new StringClient

      server.start()
      server.isRunning should equal (true)
      Await.result(client.send(port, "foo"), Duration.Inf) should equal ("timeout")
      server.stop()
      server.isRunning should equal (false)
    }

    "fails to bind to invalid address" in {
      val addr = new InetSocketAddress(InetAddress.getByName("8.8.8.8"), 8888)

      val server = new SimpleNettyServer(addr) {
        protected def initChannel(ch: SocketChannel) = ()
      }

      a[BindException] should be thrownBy {
        server.start()
      }
    }
  }

  class StringClient extends SimpleNettyClient {
    class PromiseHandler(promise: Promise[String]) extends SimpleChannelInboundHandler[ByteBuf] {
      def channelRead0(ctx: ChannelHandlerContext, e: ByteBuf): Unit =
        promise.trySuccess(e.toString(CharsetUtil.UTF_8))

      override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable): Unit =
        promise.tryFailure(e)
    }

    def conn[T](port: Int)(f: (String => Future[String]) => Future[T]): Future[T] = {
      val promise = Promise[String]()
      val handler = new PromiseHandler(promise)
      connect("127.0.0.1", port, _.pipeline.addLast(handler)) flatMap { chan =>
        val writer = { msg: String =>
          chan.writeAndFlush(msg.toUTF8Buf)
          promise.future
        }

        f(writer) ensure { chan.close() }
      }
    }

    def send(port: Int, msg: String) = conn(port) { _(msg) }
  }

  class EchoServer(port: Int) extends SimpleNettyServer(Network.address(port)) {
    protected def initChannel(ch: SocketChannel): Unit =
      ch.pipeline.addLast("handler", new SimpleChannelInboundHandler[ByteBuf](false) {
        def channelRead0(ctx: ChannelHandlerContext, e: ByteBuf) =
          ctx.channel.writeAndFlush(e)
      })
  }

  class HoleServer(port: Int) extends SimpleNettyServer(Network.address(port)) {
    protected def initChannel(ch: SocketChannel): Unit =
      ch.pipeline.addLast("handler", new SimpleChannelInboundHandler[ByteBuf] {
        def channelRead0(ctx: ChannelHandlerContext, e: ByteBuf) = ()

        override def exceptionCaught(ctx: ChannelHandlerContext, e: Throwable) =
          e.getCause match {
            case _: Throwable =>
              val chan = ctx.channel
              if (chan.isOpen) {
                chan.writeAndFlush("timeout".toUTF8Buf)
              }
          }
      })
  }
}
