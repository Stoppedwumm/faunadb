package fauna.net.http

import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.Service
import fauna.lang.syntax._
import fauna.net.http.HTTPHeaders
import fauna.net.netty.SimpleNettyClient
import fauna.net.security._
import fauna.util.Base64
import io.netty.channel.Channel
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.{
  HttpClientCodec,
  HttpContentDecompressor,
  HttpHeaderValues,
  HttpHeaders,
  HttpMethod
}
import io.netty.handler.logging._
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.util.AsciiString
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ ConcurrentLinkedQueue, TimeoutException }
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }
import scala.language.implicitConversions

object HttpClient {
  val DefaultResponseTimeout = 60.seconds

  private val HostRegex = """(?:(http|https)://)?([^:]+)(?::(\d+))?""".r

  // Setting this key will enable Netty's logging handler at the INFO level.
  // This will only enable it for new HttpClients.
  // Useful when debugging responses from a remote endpoint in REPL.
  val LoggingSyspropKey = "fauna.httpclient.info.logging"

  def apply(
    host: String,
    port: Int = -1,
    ssl: Option[SSLConfig] = None,
    headers: Seq[(AsciiString, AnyRef)] = Nil,
    responseTimeout: Duration = DefaultResponseTimeout,
    connectTimeout: Duration = Duration.Zero,
    pooledBuffers: Boolean = false,
    pooledConnections: Boolean = false
  ): HttpClient = {

    val (hostArg, portArg, sslArg) = deriveDefaults(host, port, ssl)

    val useLoggingHandler = System.getProperty(LoggingSyspropKey) != null

    RealHttpClient(
      new AsciiString(hostArg),
      portArg,
      sslArg,
      headers,
      responseTimeout,
      connectTimeout,
      pooledBuffers,
      pooledConnections,
      useLoggingHandler)
  }

  private[http] def deriveDefaults(
    host: String,
    port: Int,
    ssl: Option[SSLConfig]): (String, Int, SSLConfig) =
    host match {
      case HostRegex(scheme, host, hPort) =>
        val sslArg = ssl getOrElse {
          scheme match {
            case "https" => DefaultSSL
            case _       => NoSSL
          }
        }

        val portArg = if (port >= 0) port else {
          Option(hPort) map { _.toInt } getOrElse {
            sslArg match {
              case NoSSL => 80
              case _     => 443
            }
          }
        }

        (host, portArg, sslArg)

      case host =>
        throw new IllegalArgumentException(s"Invalid host string: $host")
    }

  // discard eats the body of a response, ensuring that the underlying nio body is not leaked.
  def discard(resp: Future[HttpResponse]): Future[Unit] = {
    resp flatMap { resp =>
      resp.body.data.unit ensure {
        resp.body.maybeRelease()
      }
    }
  }
}

object AuthType {
  //make string Basic auth by default
  implicit def strToAuth(auth: String): AuthType = BasicAuth(auth)

  private def removeColon(str: String) = if (str.endsWith(":")) {
    str.substring(0, str.length - 1)
  } else {
    str
  }

  def fromHeaders(headers: HttpHeaders) = {
    Option(headers.get(HTTPHeaders.Authorization)) map { header =>
      HttpRequest.SplitHeaderPattern.split(header, 2) match {
        case Array("Basic", encoded) =>
          try {
            val decoded = new String(Base64.decodeStandard(removeColon(encoded)), StandardCharsets.UTF_8).trim

            BasicAuth(removeColon(decoded), Some(header))
          } catch {
            case _: IllegalArgumentException => NoAuth
          }

        case Array("Bearer", encoded) =>
          BearerAuth(encoded, Some(header))

        case _ =>
          NoAuth
      }
    } getOrElse {
      NoAuth
    }
  }
}

sealed trait AuthType {
  val header: Option[String]
}

class RawAuth(authorization: String) extends AuthType {
  val header: Option[String] = Some(authorization)
}

case class BasicAuth(token: String, auth: Option[String] = None)
    extends RawAuth(
      auth getOrElse s"Basic ${Base64.encodeStandard((token + ":").getBytes)}")

case class BearerAuth(token: String, auth: Option[String] = None)
    extends RawAuth(auth getOrElse s"Bearer $token")

object NoAuth extends AuthType {
  val header: Option[String] = None
}

trait HttpClient extends Service {
  val host: AsciiString

  val port: Int

  val ssl: SSLConfig

  val base: String

  def lastSeenTxnTime: Long

  def base(base: String) = new RelativeHttpClient(this, base)

  def sendRequest(req: HttpRequest, setContentLength: Boolean): Future[HttpResponse]

  def makeAndSendRequest(
    method: HttpMethod,
    path: String,
    query: String,
    body: HttpBody,
    token: AuthType,
    headers: Seq[(AsciiString, AnyRef)]): Future[HttpResponse]

  def get(
    path: String,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def options(
    path: String,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def head(
    path: String,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def delete(
    path: String,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def post(
    path: String,
    body: HttpBody,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def put(
    path: String,
    body: HttpBody = NoBody,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def patch(
    path: String,
    body: HttpBody = NoBody,
    token: AuthType = NoAuth,
    query: String = null,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse]

  def query(
    body: HttpBody,
    token: AuthType,
    headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    post("/", body, token, headers = headers)

  def trace(body: HttpBody, token: AuthType) =
    post("/", body, token, null, Seq((HTTPHeaders.Trace, "1")))
}

trait AbstractHttpClient extends HttpClient {

  // Only used for QA tests.
  private[this] val lastSeenTxn = new AtomicLong

  def lastSeenTxnTime: Long = lastSeenTxn.get

  val base = ""
  def keepAlive: Boolean = false

  private lazy val connHeaders = {
    val b = Seq.newBuilder[(AsciiString, AsciiString)]

    b += HTTPHeaders.Host -> host

    if (keepAlive) {
      b += HTTPHeaders.Connection -> HttpHeaderValues.KEEP_ALIVE
    } else {
      b += HTTPHeaders.Connection -> HttpHeaderValues.CLOSE
    }

    b.result()
  }

  def makeAndSendRequest(
    method: HttpMethod,
    path: String,
    query: String,
    body: HttpBody,
    token: AuthType,
    headers: Seq[(AsciiString, AnyRef)]): Future[HttpResponse] = {

    val authHeader = Option(token) match {
      case Some(token) => token.header map { HTTPHeaders.Authorization -> _ } toSeq
      case _           => Seq.empty
    }

    val uri = uriForPath(path, query)

    val txnTime = lastSeenTxn.get
    val timing =
      if (txnTime == 0) Seq.empty
      else Seq(HTTPHeaders.LastSeenTxn -> txnTime.toString)

    val req = HttpRequest(method, uri, authHeader ++ headers ++ connHeaders ++ timing, body)

    // If Body contains a buffer, retain it across the lifetime of the
    // recover handler, to ensure HttpRequest.toString has access to
    // it.
    body match {
      case Body(buf, _, _) => buf.retain()
      case _               => ()
    }

    // RFC 9110 Sec. 8.6 "A user agent SHOULD NOT send a
    // Content-Length header field when the request message does not
    // contain content and the method semantics do not anticipate such
    // data."
    val setContentLength = method match {
      case HttpMethod.GET | HttpMethod.HEAD | HttpMethod.OPTIONS => false
      case _                                                     => true
    }

    sendRequest(req, setContentLength = setContentLength) map { r =>
      r.getHeader(HTTPHeaders.TxnTime) foreach { t =>
        lastSeenTxn.accumulateAndGet(t.toLong, math.max)
      }
      r
    } recoverWith {
      case _: ReadTimeoutException =>
        Future.failed(new TimeoutException(s"Request timed out: $req"))
    } ensure {
      body match {
        case Body(buf, _, _) => buf.release()
        case _               => ()
      }
    }
  }

  def get(path: String, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.GET, path, query, NoBody, token, headers)

  def head(path: String, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.HEAD, path, query, NoBody, token, headers)

  def options(path: String, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.OPTIONS, path, query, NoBody, token, headers)

  def delete(path: String, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.DELETE, path, query, NoBody, token, headers)

  def post(path: String, body: HttpBody, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.POST, path, query, body, token, headers)

  def put(path: String, body: HttpBody = NoBody, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.PUT, path, query, body, token, headers)

  def patch(path: String, body: HttpBody = NoBody, token: AuthType, query: String = null, headers: Seq[(AsciiString, AnyRef)] = Seq.empty) =
    makeAndSendRequest(HttpMethod.PATCH, path, query, body, token, headers)

  // helpers

  protected def uriForPath(path: String, query: String) = {
    val querySuffix = Option(query).map { "?"+_ } getOrElse ("")
    new URI(s"http://$host:$port$base$path$querySuffix")
  }
}

case class RealHttpClient(
  host: AsciiString,
  port: Int,
  ssl: SSLConfig,
  baseHeaders: Seq[(AsciiString, AnyRef)],
  responseTimeout: Duration,
  connectTimeout: Duration,
  pooledBuffers: Boolean,
  pooledConnections: Boolean,
  useLoggingHandler: Boolean
) extends SimpleNettyClient(connectTimeout) with AbstractHttpClient {
  override def keepAlive = pooledConnections

  override protected def initChannel(ch: SocketChannel) = {
    ssl.getClientHandler(ch.alloc, Some((host.toString, port))) foreach {
      ch.pipeline.addLast("ssl", _)
    }

    ch.pipeline.addLast("codec", new HttpClientCodec)
    ch.pipeline.addLast("inflator", new HttpContentDecompressor)

    if (useLoggingHandler) {
      ch.pipeline.addLast("logging", new LoggingHandler(LogLevel.INFO))
    }

    if (responseTimeout.isFinite && responseTimeout != Duration.Zero) {
      ch.pipeline.addLast("timeout handler", new HttpClientTimeoutHandler(responseTimeout))
    }
  }

  private[this] val pool = new ConcurrentLinkedQueue[Channel]()

  protected def getChannel(): Future[(Channel, Future[HttpResponse])] = {
    @annotation.tailrec
    def chanF: Future[(Channel, Future[HttpResponse])] =
      Option(pool.poll()) match {
        case Some(ch) if ch.isActive =>
          val responseP = Promise[HttpResponse]()
          val handler = new HttpResponseHandler(responseP, pooledBuffers)
          if (pooledConnections) handler.onComplete { pool.offer(ch) }

          ch.pipeline.replace(classOf[HttpResponseHandler], "response-handler", handler)
          Future.successful((ch, responseP.future))
        case Some(_) =>
          chanF
        case None =>
          val responseP = Promise[HttpResponse]()
          val handler = new HttpResponseHandler(responseP, pooledBuffers)

          connect(host.toString, port, _.pipeline.addLast(handler)) map { ch =>
            if (pooledConnections) handler.onComplete { pool.offer(ch) }
            (ch, responseP.future)
          }
      }

    chanF
  }

  def sendRequest(
    req: HttpRequest,
    setContentLength: Boolean): Future[HttpResponse] = {
    baseHeaders foreach { case (k, v) =>
      if (!req.containsHeader(k)) req.raw.headers.set(k, v)
    }

    getChannel() flatMap { case (ch, response) =>
      req.writeTo(ch, setContentLength = setContentLength) before response
    }
  }

  // satisfy Service

  def isRunning = isInitialized

  def start() = init()

  def stop(graceful: Boolean) = clear()
}

class HttpClientProxy(inner: HttpClient) extends AbstractHttpClient {
  val host = inner.host

  val port = inner.port

  val ssl = inner.ssl

  override val base = inner.base

  def sendRequest(
    req: HttpRequest,
    setContentLength: Boolean): Future[HttpResponse] =
    inner.sendRequest(req, setContentLength)

  def isRunning = inner.isRunning

  def start() = inner.start()

  def stop(graceful: Boolean) = inner.stop(graceful)
}

class RelativeHttpClient(inner: HttpClient, override val base: String) extends HttpClientProxy(inner) {

  override def base(base: String) = new RelativeHttpClient(inner, base)
}
