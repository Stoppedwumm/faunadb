package fauna.net.netty

import fauna.lang.Service
import fauna.net.netty.DefaultEventLoopGroup
import java.net.SocketAddress
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.socket._
import io.netty.handler.logging._

// FIXME: set WRITE_BUFFER_HIGH_WATERMARK
// FIXME: set WRITE_BUFFER_LOW_WATERMARK

abstract class SimpleNettyServer(val bindAddr: SocketAddress) extends Service { self =>

  private[this] var state: Option[Channel] = None

  protected def initChannel(ch: SocketChannel): Unit

  protected def eventLoopGroup: EventLoopGroup = DefaultEventLoopGroup
  protected def socketChannelClass: Class[_ <: ServerSocketChannel] = DefaultServerSocketChannelClass

  protected def initBoot(b: ServerBootstrap): Unit = {
    b.group(eventLoopGroup)
    b.channel(socketChannelClass)
    b.option[java.lang.Boolean](ChannelOption.SO_REUSEADDR, true)

    // 4K is the default on 5.1+ kernels; go with that until further
    // evidence suggest otherwise.
    b.option[java.lang.Integer](ChannelOption.SO_BACKLOG, 4096)

    b.childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel) = {
        ch.pipeline.addLast(new LoggingHandler(LogLevel.DEBUG))
        self.initChannel(ch)
      }

    })
  }

  // Satisfy Service

  def isRunning = state.isDefined

  def start(): Unit =
    try {
      state = state orElse {
        val b = new ServerBootstrap()
        initBoot(b)
        Some(b.bind(bindAddr).sync.channel)
      }
    } catch {
      // When using native epoll, this the bind exception.
      case e: io.netty.channel.unix.Errors$NativeIoException =>
        throw new java.net.BindException(e.getMessage)
    }

  def stop(graceful: Boolean): Unit =
    state = state flatMap { listener =>
      listener.close().await
      None
    }
}
