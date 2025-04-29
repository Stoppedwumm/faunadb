package fauna.net.http

import fauna.exec._
import fauna.lang.syntax._
import io.netty.buffer._
import io.netty.util.CharsetUtil
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.language.implicitConversions

object RequestTooLarge extends Exception

object ContentType {
  val OctetStream = "application/octet-stream"
  val JSON = "application/json;charset=utf-8"
  val XML = "application/xml"
  val Text = "text/plain;charset=utf-8"
  val HTML = "text/html"
  val FormData = "multipart/form-data"
  val URLEncoded = "application/x-www-form-urlencoded"
}

object HttpBody {
  private[http] val MultipartRegex = "multipart/.*".r
  implicit def fromTextString(s: String): HttpBody = {
    Body(s.toUTF8Buf, ContentType.Text)
  }
}

sealed trait HttpBody {
  def data(implicit ec: ExecutionContext): Future[ByteBuf]
  def events: Observable[ByteBuf]
  def lines: Observable[ByteBuf]
  def contentLength: Option[Int]
  def isEmpty: Boolean
  def isMultipart: Boolean
  def maybeRelease(): Future[Unit]
}

sealed trait NoBody extends HttpBody {
  def data(implicit ec: ExecutionContext) = Future.successful(Unpooled.EMPTY_BUFFER)
  def events = Observable.single(Unpooled.EMPTY_BUFFER)
  def lines = Observable.single(Unpooled.EMPTY_BUFFER)
  def isEmpty = true
  def contentLength = Some(0)
  def isMultipart = false
  def maybeRelease() = Future.unit
}

case object NoBody extends NoBody

case class Body(content: ByteBuf, contentType: String, maxLength: Option[Int] = None) extends HttpBody {

  def data(implicit ec: ExecutionContext) = {
    maxLength foreach { l =>
      if (content.readableBytes > l) throw RequestTooLarge
    }

    Future.successful(content)
  }

  def events = Observable.single(content)

  def lines = {
    val lines = content.toUTF8String.split("\r\n")
    Observable.from(lines.map(l => Unpooled.wrappedBuffer(l.getBytes)))
  }

  def contentLength = Some(content.readableBytes())

  def isEmpty = false

  def isMultipart = HttpBody.MultipartRegex.matches(contentType)

  def maybeRelease() = {
    if (content.refCnt != 0) content.release
    Future.unit
  }

  override def toString = s"""Body(${contentType}, ${content.toString(CharsetUtil.UTF_8).trim})"""
}

object Body {

  def apply(bytes: Array[Byte], contentType: String): Body =
    Body(Unpooled.wrappedBuffer(bytes), contentType)

  def apply(string: String, contentType: String): Body =
    Body(string.toUTF8Buf, contentType)
}

/**
  * Chunked retains the head of the ByteBuf stream until receiver begins reading by
  * calling either `events` or `data`. The first call to events resumes the stream,
  * therefore, subsequent calls will NOT read all events received. Calling `data`
  * multiple times is safe, however, interleaving calls to `events` and `data` may
  * return in unpredictable results.
  */
final case class Chunked(
  events: Observable[ByteBuf],
  contentType: String,
  contentLength: Option[Int] = None,
  maxLength: Option[Int] = None)
    extends HttpBody {

  // Setting the maximum number of components prevents reallocs in
  // composite buffers due to "consolidation". See
  // CompositeByteBuf.consolidateIfNeeded.
  private[this] val maxComponents =
    (maxLength.getOrElse(HttpServer.MaxRequestSize) / HttpServer.MaxChunkSize) + 1

  private[this] val _dataReady = new AtomicBoolean(false)
  private[this] val _data = Promise[ByteBuf]()

  /** Gather received chunks into ByteBuf while waiting for stream termination. */
  def data(implicit ec: ExecutionContext): Future[ByteBuf] = {
    if (_dataReady.compareAndSet(false, true)) {
      maxLength match {
        case Some(maxLength) =>
          if (contentLength exists { _ > maxLength }) {
            _data.failure(RequestTooLarge)
          } else {
            val cmpF =
              events.foldLeftF((Unpooled.compositeBuffer(maxComponents), 0)) {
                case ((cmp, len), buf) =>
                  val curLen = len + buf.readableBytes
                  if (curLen <= maxLength) {
                    (cmp.addComponent(true, buf), curLen)
                  } else {
                    // discard non-referenced buffers
                    buf.release()
                    cmp.release()

                    // Fail the promise with the error to ensure the
                    // HTTP request is handled correctly, but also abort
                    // iteration by throwing.
                    _data.failure(RequestTooLarge)
                    throw RequestTooLarge
                  }
              }
            _data.completeWith(cmpF map {
              case (buf, _) => buf
            })
          }

        case None =>
          val f = events.foldLeftF(Unpooled.compositeBuffer(maxComponents)) {
            case (cmp, buf) => cmp.addComponent(true, buf)
          }
          _data.completeWith(f)
      }
    }
    _data.future
  }

  def lines: Observable[ByteBuf] = {
    @volatile var buf = Unpooled.compositeBuffer()

    events.transform { (observer, value) =>
      val index = value.bytesBefore('\r'.toByte)

      if (index < 0 || value.getByte(index + 1) != '\n') {
        buf.addComponent(true, value)
        Observer.ContinueF
      } else {
        val line = buf.addComponent(true, value)
        buf = Unpooled.compositeBuffer()
        observer.onNext(line)
      }
    }
  }

  def isEmpty = false

  def isMultipart = HttpBody.MultipartRegex.matches(contentType)

  def maybeRelease(): Future[Unit] = {
    implicit val ec = ImmediateExecutionContext
    if (_dataReady.compareAndSet(false, true)) { // gathering has not started yet
      _data.trySuccess(Unpooled.EMPTY_BUFFER)
      events foreachF { _.release() }
    } else {
      data map { buf =>
        if (buf.refCnt != 0) {
          buf.release()
        }
      }
    }
  }

  override def toString = s"Chunked($contentType, ${_data})"
}
