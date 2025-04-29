package fauna.qa.net

import fauna.codex.json._
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.net.http.{ AuthType, ContentType, HTTPHeaders, Http2Client }
import fauna.net.netty.DefaultEventLoopGroup
import fauna.prop.api.HttpResponseHelpers
import fauna.qa._
import io.netty.channel._
import io.netty.handler.codec.http._
import io.netty.util.AsciiString
import java.util.concurrent.atomic.AtomicLong
import fauna.atoms.APIVersion
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.util.{ Success, Try }

case class FaunaResponse(code: Int, txnTime: Long, queryTime: Int, body: JSObject) {
  def isSuccess = code >= 200 && code < 300
}

trait FaunaClient {
  def coreNode: CoreNode

  protected val host = new AsciiString(coreNode.host.addr)
  protected val lastSeenTxn = new AtomicLong(0)

  def lastTxnTime = Timestamp.ofMicros(lastSeenTxn.get)

  def syncLastTxnTime(ts: Timestamp): Unit =
    lastSeenTxn.updateAndGet { _ max ts.micros }

  def requestPath: String = "/"

  def query(
    key: AuthType,
    query: JSValue,
    timeout: Duration = Duration.Inf,
    apiVersion: Option[APIVersion] = None,
    maxRetriesOnContention: Option[Int] = None,
    trace: Boolean = false,
    requestPathOverride: Option[String] = None,
    source: Option[String] = None
  ): Future[FaunaResponse]

  def close(): Future[Unit]

  def onClose(cb: => Any): Unit
}

class FaunaClientHttp1_1(
  val node: CoreNode,
  channel: Channel
) extends FaunaClient {
  override def coreNode: CoreNode = node

  def query(
    key: AuthType,
    query: JSValue,
    timeout: Duration,
    apiVersion: Option[APIVersion],
    maxRetriesOnContention: Option[Int],
    trace: Boolean,
    requestPathOverride: Option[String],
    source: Option[String]
  ): Future[FaunaResponse] = {
    Try(channel.pipeline.remove("response-handler"))
    val handler = new ClientHandler[FaunaResponse]
    channel.pipeline.addLast("response-handler", handler)

    val buf = channel.alloc.buffer
    query.writeTo(buf, false)

    val req = new DefaultFullHttpRequest(
      HttpVersion.HTTP_1_1,
      HttpMethod.POST,
      requestPathOverride.getOrElse(requestPath),
      buf,
      DefaultHttpHeadersFactory.headersFactory().withValidation(true),
      DefaultHttpHeadersFactory.trailersFactory().withValidation(true)
    )

    maxRetriesOnContention foreach { retries =>
      req.headers.set(HTTPHeaders.MaxRetriesOnContention, retries.toString())
    }

    if (trace) {
      req.headers.set(HTTPHeaders.Trace, "1")
    }

    req.headers.set(HTTPHeaders.Connection, HttpHeaderValues.KEEP_ALIVE)
    req.headers.set(HTTPHeaders.ContentType, ContentType.JSON)
    req.headers.set(HTTPHeaders.Host, host)
    key.header foreach { authHeader =>
      req.headers.set(HTTPHeaders.Authorization, authHeader)
    }
    req.headers.set(HTTPHeaders.ContentLength, buf.readableBytes.toString)
    req.headers.set(HTTPHeaders.LastSeenTxn, lastSeenTxn.get.toString)
    apiVersion foreach { apiVer =>
      req.headers.set(HTTPHeaders.FaunaDBAPIVersion, apiVer.toString)
    }
    source.foreach { source =>
      req.headers.set(HTTPHeaders.FaunaSource, source)
    }

    val writeF = channel.writeAndFlush(req)

    implicit val ec = ExecutionContext.parasitic
    if (timeout.isFinite) {
      writeF.toFuture foreach { _ =>
        handler.timeoutAfter(timeout)(Timer.Global)
      }
    }

    handler.future andThen {
      case Success(rep) =>
        lastSeenTxn.accumulateAndGet(rep.txnTime, math.max)
    }
  }

  def close(): Future[Unit] =
    channel.close().toFuture.unit

  def onClose(cb: => Any): Unit = {
    implicit val ec = ExecutionContext.parasitic
    channel.closeFuture.toFuture ensure { cb }
  }
}

class FaunaClientHttp2(
  node: CoreNode,
  elGroup: EventLoopGroup = DefaultEventLoopGroup
) extends FaunaClient
    with HttpResponseHelpers {

  override def coreNode: CoreNode = node
  override def requestPath: String = "/query/1"

  lazy val api = Http2Client(node.addr, node.port, eventLoopGroupOverride = elGroup)

  def query(
    key: AuthType,
    query: JSValue,
    timeout: Duration = Duration.Inf,
    apiVersion: Option[APIVersion] = None,
    maxRetriesOnContention: Option[Int] = None,
    trace: Boolean = false,
    requestPathOverride: Option[String] = None,
    source: Option[String] = None
  ): Future[FaunaResponse] = {

    val headers: Seq[(String, String)] = Seq.empty
    headers :+ (HTTPHeaders.ContentType, ContentType.JSON)

    maxRetriesOnContention foreach { retries =>
      headers :+ (HTTPHeaders.MaxRetriesOnContention, retries.toString())
    }

    if (trace) {
      headers :+ (HTTPHeaders.Trace, "1")
    }

    apiVersion foreach { apiVer =>
      headers :+ (HTTPHeaders.FaunaDBAPIVersion, apiVer.toString)
    }
    source.foreach { source =>
      headers :+ (HTTPHeaders.FaunaSource, source)
    }

    val response = api.post(
      requestPathOverride.getOrElse(requestPath),
      query.as[JSObject],
      key,
      headers = Seq(HTTPHeaders.Format -> "simple") ++ headers.map { case (k, v) =>
        new AsciiString(k) -> v
      })

    val faunaResponse = FaunaResponse(
      response.statusCode,
      response.header(HTTPHeaders.TxnTime.toString()).toLongOption.getOrElse(0),
      response.header(HTTPHeaders.QueryTime.toString()).toIntOption.getOrElse(0),
      response.json
    )

    Future.successful(faunaResponse)
  }

  def close(): Future[Unit] = {
    api.die()
    Future.unit
  }

  def onClose(cb: => Any): Unit = ()
}
