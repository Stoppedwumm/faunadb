package fauna.net.netty

import fauna.lang.syntax._
import fauna.net.netty.DefaultEventLoopGroup
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import java.net.{ InetSocketAddress, SocketAddress }
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

// FIXME: set WRITE_BUFFER_HIGH_WATERMARK
// FIXME: set WRITE_BUFFER_LOW_WATERMARK
// FIXME: set logging handler

abstract class SimpleNettyClient(connectTimeout: Duration = Duration.Zero) { self =>

  private[this] var state: Option[Bootstrap] = None

  init()

  protected def initChannel(ch: SocketChannel): Unit = ()

  protected def eventLoopGroup: EventLoopGroup = DefaultEventLoopGroup
  protected def socketChannelClass: Class[_ <: SocketChannel] = DefaultSocketChannelClass

  protected def initBoot(b: Bootstrap): Unit = {
    b.group(eventLoopGroup)
    b.channel(socketChannelClass)
    b.option[Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout.toMillis.toInt)
  }

  protected def boot = state getOrElse {
    throw new IllegalStateException("Client is stopped. start again with Client.start()")
  }

  protected def connect(addr: SocketAddress, initF: SocketChannel => Unit): Future[Channel] =
    Try(boot.clone()) match {
      case Success(cloned) =>
        cloned.handler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel) = {
            self.initChannel(ch)
            initF(ch)
          }
        })
        cloned.connect(addr).toFuture
      case Failure(t) =>
        Future.failed(t)
    }

  protected def connect(host: String, port: Int, initF: SocketChannel => Unit): Future[Channel] =
    connect(new InetSocketAddress(host, port), initF)

  def isInitialized = state.isDefined

  def init(): Unit =
    state = state orElse {
      val b = new Bootstrap
      initBoot(b)
      Some(b)
    }

  def clear(): Unit = state = None
}
