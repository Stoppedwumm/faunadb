package fauna.net

import fauna.lang.NamedPoolThreadFactory
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll._
import io.netty.channel.nio._
import io.netty.channel.socket.nio._
import io.netty.incubator.channel.uring._
import java.io.IOException
import java.lang.{ Boolean => JBoolean }
import scala.language.existentials

package netty {
  class ChannelClosedException extends IOException("Connection unexpectedly closed.")
  class ChannelMaxRequestsExceeded extends IOException(
    "Exceeded limit of waiting outgoing requests. Request dropped.")
}

package object netty {
  val UseEpoll = Epoll.isAvailable
  val UseIOUring = IOUring.isAvailable && JBoolean.getBoolean("fauna.net.io_uring")
  val EventLoopThreads = 2

  /**
    * The default netty event loop singleton for all network IO via subclasses
    * of SimpleNettyClient and SimpleNettyServer. As configured, it is meant to
    * drive async event polling only. Care should be taken in client and server
    * implementations to delegate to a compute threadpool as soon as possible,
    * e.g. the FaunaExecutionContext.Implicits.global.
    */
  val DefaultEventLoopGroup: EventLoopGroup = {
    val tf = new NamedPoolThreadFactory("Network-IO", true)
    if (UseIOUring) {
      new IOUringEventLoopGroup(EventLoopThreads, tf)
    } else if (UseEpoll) {
      new EpollEventLoopGroup(EventLoopThreads, tf)
    } else {
      new NioEventLoopGroup(EventLoopThreads, tf)
    }
  }

  val DefaultSocketChannelClass =
    if (UseIOUring) {
      classOf[IOUringSocketChannel]
    } else if (UseEpoll) {
      classOf[EpollSocketChannel]
    } else {
      classOf[NioSocketChannel]
    }

  val DefaultServerSocketChannelClass =
    if (UseIOUring) {
      classOf[IOUringServerSocketChannel]
    } else if (UseEpoll) {
      classOf[EpollServerSocketChannel]
    } else {
      classOf[NioServerSocketChannel]
    }
}
