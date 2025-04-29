package fauna.qa.net

import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.net.netty.SimpleNettyServer
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import java.net.{ InetAddress, InetSocketAddress }
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

/**
  * Server listens on the given `port` on all interfaces. It accepts a `Req` and
  * passes it to the `handler`. The handler is expected to provides a `Rep` which
  * will be encoded and sent back to the client.
  */
class Server[Req: CBOR.Codec: ClassTag, Rep: CBOR.Codec: ClassTag](
  port: Int,
  handler: Req => Future[Rep]
) extends SimpleNettyServer(
      new InetSocketAddress(InetAddress.getByName("0.0.0.0"), port)
    ) {

  private[this] val log = getLogger()

  override protected def initChannel(ch: SocketChannel): Unit =
    Codec
      .server[Req, Rep](ch.pipeline)
      .addLast(
        "handler",
        new ChannelInboundHandlerAdapter {
          implicit val ec = ExecutionContext.parasitic

          override def channelRead(ctx: ChannelHandlerContext, obj: Object): Unit =
            obj match {

              case req: Req =>
                val addr = ctx.channel.remoteAddress match {
                  case ia: InetSocketAddress => ia.getHostName
                  case s                     => s.toString
                }

                log.info(s"RECV[$addr]: $req")
                handler(req) onComplete { rep =>
                  if (ctx.channel.isActive) {
                    log.info(s"SEND[$addr]: $rep")
                    ctx.writeAndFlush(rep.get)
                  } else {
                    log.info(s"DROP[$addr]: $rep (channel inactive)")
                  }
                }

              case _ => ctx.fireChannelRead(obj)
            }
        }
      )
}
