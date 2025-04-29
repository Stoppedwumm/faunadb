package fauna.net.http

import fauna.codex.json.JSObject
import fauna.exec._
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.syntax._
import fauna.trace.TraceContext
import fauna.util.Base64
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }
import io.netty.channel.Channel
import io.netty.handler.codec.http.{
  HttpMessage => RawHttpMessage,
  HttpRequest => RawHttpRequest,
  HttpResponse => RawHttpResponse,
  _
}
import io.netty.util.AsciiString
import java.net.URI
import java.nio.channels.ClosedChannelException
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

object MessageOutOfOrderException extends Exception

abstract class HttpMessage(msg: RawHttpMessage, val body: HttpBody) {
  /**
    * Returns all header names in this message prefixed by the
    * string "x-" as a comma-delimited String for use by CORS access
    * control.
    */
  def exposeHeaders: String = {
    val b = List.newBuilder[CharSequence]
    val iter = msg.headers.iteratorCharSequence

    while (iter.hasNext) {
      val entry = iter.next
      val key = entry.getKey
      if (key.length >= 2 &&
        (key.charAt(0) == 'x') &&
        (key.charAt(1) == '-')) {
        b += key
      }
    }

    b.result().mkString(", ")
  }

  def getHeader(name: AsciiString) = Option(msg.headers.get(name))

  def header(name: AsciiString) = getHeader(name).get

  def containsHeader(name: AsciiString) = msg.headers contains name

  def hasBody = !body.isEmpty

  def isKeepAlive = HttpUtil.isKeepAlive(msg)

  def writeTo(
    chan: Channel,
    isHeadResponse: Boolean = false,
    setContentLength: Boolean = true): Future[Unit] = {
    val writeF: Future[Unit] = body match {
      case _: NoBody =>
        if (setContentLength) {
          HttpUtil.setContentLength(msg, 0)
        }

        chan.write(msg)
        chan.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).toFuture.unit

      case b: Body =>
        HttpUtil.setContentLength(msg, b.contentLength.get)
        msg.headers.set(HTTPHeaders.ContentType, b.contentType)

        chan.write(msg)

        if (!isHeadResponse) {
          chan.write(new DefaultHttpContent(b.content))
        } else {
          b.maybeRelease()
        }

        chan.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).toFuture.unit

      case c: Chunked =>
        HttpUtil.setTransferEncodingChunked(msg, true)
        msg.headers.set(HTTPHeaders.ContentType, c.contentType)

        if (isHeadResponse) {
          chan.write(msg)
          c.maybeRelease()
          chan.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).toFuture.unit
        } else {
          // NB. Chunked is only being used for streaming. Message order must be
          // preserved. It flushes at every message as stream events might be sparse,
          // thus it can't rely on future events to decide when to flush. Pursue an
          // implementation with less flush calls if non-streaming use-cases arise.
          chan.writeAndFlush(msg).toFuture.unit before {
            val sendP =
              new Observer.ObsPromise[ByteBuf, Unit] {
                def onNext(buf: ByteBuf) = {
                  val content = new DefaultHttpContent(buf)
                  chan.writeAndFlush(content).toFuture map { _ =>
                    Observer.Continue
                  }
                }
                override def onComplete() = {
                  val content = LastHttpContent.EMPTY_LAST_CONTENT
                  val writeF = chan.writeAndFlush(content).toFuture
                  promise.completeWith(writeF.unit)
                }
              }

            val sub = c.events.subscribe(sendP)
            chan.closeFuture().toFuture ensure { sub.cancel() }
            sendP.future
          }
        }
    }

    writeF recoverWith {
      // client closed the connection before we could reply. ignore and move on
      case _: ClosedChannelException => Future.unit
      case _: Cancelable.Canceled    => Future.unit
    }
  }

  protected def headersToString: String =
    msg.headers.iteratorCharSequence.asScala map { entry =>
      s"${entry.getKey}:${entry.getValue}"
    } mkString (", ")
}

object HttpRequest {
  val SplitHeaderPattern = Pattern.compile("\\s+")

  def apply(method: HttpMethod, uri: URI, headers: Iterable[(AsciiString, AnyRef)], body: HttpBody): HttpRequest = {
    val raw = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1,
      method,
      s"${uri.getRawPath}${
        Option(uri.getRawQuery) map { "?"+_ } getOrElse ("")
      }")

    val userInfo = Option(uri.getUserInfo)

    body match {
      case b: Body    => raw.headers.set(HTTPHeaders.ContentType, b.contentType)
      case c: Chunked => raw.headers.set(HTTPHeaders.ContentType, c.contentType)
      case _          =>
    }

    userInfo foreach { info =>
      val encoded = Base64.encodeStandard(info.toUTF8Bytes)
      raw.headers.set(HTTPHeaders.Authorization, "Basic " + encoded)
    }

    headers foreach { case (k, v) => raw.headers.set(k, v) }

    new HttpRequest(
      raw,
      body,
      AuthType.fromHeaders(raw.headers()),
      TraceContext.random())
  }
}

class HttpRequest(val raw: RawHttpRequest,
                  override val body: HttpBody,
                  auth: => AuthType,
                  val traceContext: TraceContext) extends HttpMessage(raw, body) {

  val uri = new URI(raw.uri)

  val authorization: AuthType = auth

  def method = raw.method

  def path = Option(uri.getRawPath)

  def query = Option(uri.getRawQuery)

  override def toString = s"HttpRequest($method,\n$uri,\n$headersToString,\n$body)"

  def toShortString = s"HttpRequest($method, $uri, $headersToString, ${body.toString.replaceAll("\n\\s*", " ")})"

  def tracingEnabled = containsHeader(HTTPHeaders.Trace)
}

object HttpResponse {

  protected val headerCache = new ConcurrentHashMap[String, String]

  def apply(
    status: HttpResponseStatus,
    headers: Iterable[(AsciiString, AnyRef)],
    body: HttpBody) = {
    val raw = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)

    body match {
      case b: Body    => raw.headers.set(HTTPHeaders.ContentType, b.contentType)
      case c: Chunked => raw.headers.set(HTTPHeaders.ContentType, c.contentType)
      case _          =>
    }

    headers foreach { case (name, value) => raw.headers.set(name, value) }

    new HttpResponse(raw, body)
  }

  def apply(status: HttpResponseStatus, body: HttpBody): HttpResponse =
    apply(status, Nil, body)

  def apply(
    status: Int,
    body: HttpBody,
    headers: Iterable[(AsciiString, AnyRef)]): HttpResponse =
    apply(HttpResponseStatus.valueOf(status), headers, body)

  def apply(status: Int, body: HttpBody): HttpResponse =
    apply(HttpResponseStatus.valueOf(status), Nil, body)

  // Convenience method for generic JSON responses
  def apply(
    status: Int,
    body: JSObject,
    headers: Iterable[(AsciiString, AnyRef)] = Iterable.empty): HttpResponse =
    apply(
      HttpResponseStatus.valueOf(status),
      headers,
      Body(body.writeTo(ByteBufAllocator.DEFAULT.buffer, true), ContentType.JSON))
}

class HttpResponse(val raw: RawHttpResponse, override val body: HttpBody) extends HttpMessage(raw, body) {
  def status = raw.status

  def code = status.code

  def contentLength: Option[Int] = body.contentLength

  def setHeader(header: AsciiString, value: Any): Unit =
    raw.headers.set(header, value)

  def setHeader(header: String, value: Any): Unit =
    raw.headers.set(header, value)

  override def toString = s"HttpResponse($status,\n$headersToString,\n$body)"

  def toShortString = s"HttpRequest($status, $headersToString, ${body.toString.replaceAll("\n\\s*", " ")})"

  def withBody(body: HttpBody): HttpResponse =
    new HttpResponse(raw, body)
}
