package fauna.net.test

import fauna.exec._
import fauna.lang.syntax._
import fauna.net.http._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

class HttpBodySpec extends Spec {

  "HttpBodySpec" - {

    "Chunked rejects large requests" in {
      val buffers = Observable.single(("f" * 9 * 1024 * 1024).toUTF8Buf)
      val body = Chunked(
        buffers,
        "application/json",
        Some(512),
        Some(HttpServer.MaxRequestSize))

      val ex = intercept[Exception] {
        Await.result(body.data, Duration.Inf)
      }

      assert(ex === RequestTooLarge)
    }

    "Chunked rejects content length > max limit" in {
      val buffers = Observable.single("foo".toUTF8Buf)
      val body = Chunked(
        buffers,
        "application/json",
        Some(9 * 1024 * 1024),
        Some(HttpServer.MaxRequestSize))

      val ex = intercept[Exception] {
        Await.result(body.data, Duration.Inf)
      }

      assert(ex === RequestTooLarge)
    }

    "detect multipart requests" in {
      val buffers = Observable.single("foo".toUTF8Buf)
      val multipartChunked = Chunked(buffers, ContentType.FormData)
      val multipartBody = Body("foo", ContentType.FormData)
      assert(Seq(multipartChunked, multipartBody).forall { _.isMultipart })

      val textChunked = Chunked(buffers, ContentType.Text)
      val textBody = Body("foo", ContentType.Text)
      assert(Seq(textChunked, textBody).forall { !_.isMultipart })
    }
  }
}
