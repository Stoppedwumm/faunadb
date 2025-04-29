package fauna.qa.net

import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.net.netty.SimpleNettyClient
import fauna.qa.Host
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
  * A Client will connect to the provided `host` and `port`. It accepts `Req` as
  * the request type and expects `Rep` as the response type. Simultaneous
  * requests can be made via a connection pool. This is not intended for high
  * volume, parallel requests. It's meant for sending control messages to QA
  * agents.
  */
class Client[Req: CBOR.Codec: ClassTag, Rep: CBOR.Codec: ClassTag](
  val host: Host,
  val port: Int
) extends SimpleNettyClient {

  implicit val ec = ExecutionContext.parasitic

  private[this] val log = getLogger()

  private[this] val addr =
    new InetSocketAddress(InetAddress.getByName(host.addr), port)

  override protected def initBoot(b: Bootstrap): Unit = {
    super.initBoot(b)
    b.option[java.lang.Integer](ChannelOption.SO_LINGER, 120)
    b.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
  }

  override protected def initChannel(ch: SocketChannel): Unit =
    Codec.client[Req, Rep](ch.pipeline)

  private[this] val pool = new ConcurrentLinkedQueue[Channel]()

  def close(): Future[Unit] =
    pool.iterator.asScala.map(_.close().toFuture).toSeq.join

  private def getChannel: Future[(Channel, Future[Rep])] = {
    @annotation.tailrec
    def chanF: Future[(Channel, Future[Rep])] =
      Option(pool.poll()) match {
        case Some(ch) if ch.isActive =>
          val handler = new ClientHandler[Rep]
          handler.future ensure {
            if (ch.isActive) {
              pool.offer(ch)
            }
          }

          ch.pipeline.replace(classOf[ClientHandler[Rep]], "handler", handler)
          Future.successful((ch, handler.future))

        case Some(_) =>
          chanF

        case None =>
          val handler = new ClientHandler[Rep]
          connect(addr, _.pipeline.addLast(handler)) map { ch =>
            handler.future ensure {
              if (ch.isActive) {
                pool.offer(ch)
              }
            }

            (ch, handler.future)
          }
      }

    chanF
  }

  def send(msg: Req): Future[Rep] = {
    log.info(s"SEND[${addr.getHostName}]: $msg")
    getChannel flatMap {
      case (ch, response) =>
        ch.writeAndFlush(msg)
        response onComplete { msg =>
          log.info(s"RECV[${addr.getHostName}]: $msg")

          if (msg.isFailure) {
            ch.close
          }
        }
        response
    }
  }
}

abstract class ClientGroup[Req, Rep] {
  val clients: Vector[Client[Req, Rep]]
  def limited(clients: Vector[Client[Req, Rep]]): ClientGroup[Req, Rep]

  implicit val ec = ExecutionContext.parasitic

  def close(): Future[Unit] =
    clients.map(_.close()).join

  def send(req: Req): Future[Unit] =
    clients.map { _.send(req) }.join

  def all(req: Req): Future[Seq[(Host, Rep)]] = {
    val reps = clients map { c =>
      c.send(req) map { rep =>
        (c.host, rep)
      }
    }
    Future.sequence(reps)
  }

  def all(reqs: Seq[(Host, Req)]): Future[Seq[(Host, Rep)]] = {
    val reqsMap = reqs.toMap
    val reps = clients flatMap { c =>
      reqsMap.get(c.host) map { req =>
        c.send(req) map { (c.host, _) }
      }
    }
    Future.sequence(reps)
  }

  def limit(hosts: Vector[Host]) =
    limited(clients.filter { c =>
      hosts.contains(c.host)
    })
}
