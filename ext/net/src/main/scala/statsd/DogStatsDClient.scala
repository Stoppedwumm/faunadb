package fauna.net.statsd

import fauna.lang.{ NamedPoolThreadFactory, Service }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.netty.UseEpoll
import fauna.stats._
import io.netty.bootstrap._
import io.netty.buffer.ByteBufAllocator
import io.netty.channel._
import io.netty.channel.epoll._
import io.netty.channel.nio._
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.InetSocketAddress
import java.util.concurrent.{ ConcurrentHashMap, TimeUnit }

sealed abstract class DDMetric(override val toString: String)
object DDMetric {
  case object Counter extends DDMetric("c")
  case object Distribution extends DDMetric("d")
  case object Gauge extends DDMetric("g")
  case object Histogram extends DDMetric("h")
  case object Set extends DDMetric("s")
  case object Timer extends DDMetric("ms")
}

/**
  * Sends metrics over a UDP/IP socket in the
  * [[https://github.com/etsy/statsd statsd]] format.
  *
  * @param host Hostname of a statsd-compatible service
  * @param port Listening port on `host`
  */
case class DogStatsDClient(
  host: String,
  port: Int
) extends StatsRecorder
    with Service
    with ExceptionLogging {
  private[this] val alloc = ByteBufAllocator.DEFAULT
  // don't pollute the buffer pools with long-lived objects
  private[this] val server = new InetSocketAddress(host, port)
  private[this] var channel: Option[Channel] = None
  private[this] val group = {
    val tf = new NamedPoolThreadFactory("DogStatsD", true)
    if (UseEpoll) {
      new EpollEventLoopGroup(0, tf)
    } else {
      new NioEventLoopGroup(0, tf)
    }
  }

  private[this] val bootstrap: Bootstrap = {
    val chanClass = if (Epoll.isAvailable) {
      classOf[EpollDatagramChannel]
    } else {
      classOf[NioDatagramChannel]
    }

    new Bootstrap()
      .group(group)
      .channel(chanClass)
      .option(ChannelOption.SO_SNDBUF, Integer.valueOf(1_000_000))
      .handler(new ChannelOutboundHandlerAdapter)
  }

  def start(): Unit =
    if (channel.isEmpty) {
      channel = Some(bootstrap.bind(0).sync().channel)
    }

  def stop(graceful: Boolean): Unit =
    try {
      channel foreach { c =>
        c.flush()
        c.close().sync()
      }

      if (graceful) {
        group.shutdownGracefully()
      } else {
        group.shutdownGracefully(0, 0, TimeUnit.SECONDS)
      }
      channel = None
    } catch {
      case e: InterruptedException => logException(e)
    }

  def isRunning = channel.isDefined

  def count(key: String, value: Long, tags: StatTags = StatTags.Empty) =
    sendMetric(key, value.toDouble, DDMetric.Counter, tags)

  def incr(key: String) = count(key, 1)
  def decr(key: String) = count(key, -1)

  def set(key: String, value: Double) = sendMetric(key, value, DDMetric.Gauge, StatTags.Empty)
  def set(key: String, value: String) = sendMetric(key, value, DDMetric.Gauge, StatTags.Empty)

  def timing(key: String, value: Long) =
    sendMetric(key, value.toDouble, DDMetric.Timer, StatTags.Empty)

  def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty) =
    sendMetric(key, value.toDouble, DDMetric.Distribution, tags)

  def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
    sendString {
      s"_e{${title.length},${text.length}}:$title|$text|t:${level.str}|#${tags.str}"
    }

  private val LogFrequency = 500
  private val nameCheckCache = new ConcurrentHashMap[String, Int]
  private val NameRegex = """^[a-zA-Z][a-zA-Z0-9_\.]{0,199}$""".r
  private def checkName(key: String): Unit =
    if (!NameRegex.matches(key)) {
      nameCheckCache.compute(key, {
        case (_, 0) =>
          getLogger.error(s"Invalid DataDog Stat Name: $key")
          1
        case (_, `LogFrequency`) =>
          0
        case (_, n) =>
          n + 1
      })
    }

  private def sendMetric[T <: Any](key: String, value: T, mType: DDMetric, tags: StatTags) =
    sendString {
      checkName(key)
      s"$key:$value|$mType|#${tags.str}"
    }

  private def sendString(s: => String) =
    channel foreach { c =>
      if (c.isWritable) { // avoid netty queueing.
        val buf = s.toUTF8Buf(alloc)
        c.writeAndFlush(new DatagramPacket(buf, server))
      }
    }
}
