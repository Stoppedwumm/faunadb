package fauna.qa.net

import fauna.codex.json._
import fauna.lang.syntax._
import fauna.lang.NamedPoolThreadFactory
import fauna.net.http.HTTPHeaders
import fauna.net.netty.UseEpoll
import fauna.qa._
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.epoll._
import io.netty.channel.nio._
import io.netty.channel.socket.nio._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.codec.MessageToMessageDecoder
import java.net.InetSocketAddress
import java.util.{ List => JList }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

class FaunaClientFactory(parallelism: Int, useHttp2Client: Boolean = false) {
  require(parallelism > 0, "parallelism must be greater than 0")

  private[this] val ELGroup = {
    val tf = new NamedPoolThreadFactory("FaunaClient", true)
    if (UseEpoll) {
      new EpollEventLoopGroup(parallelism, tf)
    } else {
      new NioEventLoopGroup(parallelism, tf)
    }
  }

  private[this] val ChannelClass =
    if (UseEpoll) {
      classOf[EpollSocketChannel]
    } else {
      classOf[NioSocketChannel]
    }

  val boot = new Bootstrap
  boot.group(ELGroup)
  boot.channel(ChannelClass)

  def connect(node: CoreNode): Future[FaunaClient] =
    if (!useHttp2Client)
      Try(boot.clone()) match {
        case Success(cloned) =>
          cloned.handler(new ChannelInitializer[SocketChannel] {
            override def initChannel(ch: SocketChannel) =
              ch.pipeline
                .addLast("http-codec", new HttpClientCodec)
                .addLast("aggregator", new HttpObjectAggregator(5242880)) // 5MB
                .addLast("fauna-decoder", new FaunaDecoder)
          })
          val addr = new InetSocketAddress(node.addr, node.port)
          implicit val ec = ExecutionContext.parasitic
          cloned.connect(addr).toFuture map { ch =>
            new FaunaClientHttp1_1(node, ch)
          }
        case Failure(t) =>
          Future.failed(t)
      }
    else
      Future.successful(new FaunaClientHttp2(node, ELGroup))
}

private class FaunaDecoder extends MessageToMessageDecoder[FullHttpResponse] {

  override def decode(
    ctx: ChannelHandlerContext,
    http: FullHttpResponse,
    out: JList[Object]
  ): Unit = {
    val txnTime: Long = http.headers.get(HTTPHeaders.TxnTime) match {
      case value: String => value.toLongOption.getOrElse(0)
      case _ => 0L
    }
    val queryTime: Int = http.headers.get(HTTPHeaders.QueryTime) match {
      case value: String => value.toIntOption.getOrElse(0)
      case _ => 0
    }

    val rep = FaunaResponse(
      http.status.code,
      txnTime,
      queryTime,
      JS.parse(http.content).as[JSObject]
    )
    out.add(rep.asInstanceOf[Object])
  }
}
