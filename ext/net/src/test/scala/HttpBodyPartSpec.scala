package fauna.net.test

import fauna.lang.syntax._
import fauna.net.http._
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{
  DefaultFullHttpRequest,
  HttpHeaderNames,
  HttpMethod,
  HttpVersion
}

class HttpBodyPartSpec extends Spec {

  val req =
    new DefaultFullHttpRequest(
      HttpVersion.HTTP_1_1,
      HttpMethod.POST,
      "/uri"
    )

  req.headers
    .add(
      HttpHeaderNames.CONTENT_TYPE,
      s"${ContentType.FormData}; boundary=abc1234"
    )

  "HttpBodyPart" - {

    "parse an empty body" in {
      val parts = HttpBodyPart.parse(req, Unpooled.EMPTY_BUFFER)
      parts should have size 0
    }

    "parse a single part" in {
      val parts =
        HttpBodyPart.parse(
          req,
          """|--abc1234
             |Content-Disposition: form-data; name="name"
             |Content-Type: application/octet-stream
             |
             |content
             |--abc1234--""".stripMargin.toUTF8Buf
        )
      parts should have size 1
      parts.head shouldEqual HttpBodyPart("name", "content")
    }

    "parse multiple parts" in {
      val parts =
        HttpBodyPart.parse(
          req,
          """|--abc1234
             |Content-Disposition: form-data; name="name 1"
             |Content-Type: application/octet-stream
             |
             |content 1
             |--abc1234
             |Content-Disposition: form-data; name="name 2"
             |Content-Type: application/octet-stream
             |
             |content 2
             |--abc1234--""".stripMargin.toUTF8Buf
        )
      parts should have size 2
      parts should contain.inOrder(
        HttpBodyPart("name 1", "content 1"),
        HttpBodyPart("name 2", "content 2")
      )
    }

    "do not consume body" in {
      val body =
        """|--abc1234
           |Content-Disposition: form-data; name="name"
           |Content-Type: application/octet-stream
           |
           |content
           |--abc1234--""".stripMargin.toUTF8Buf

      HttpBodyPart.parse(req, body) should have size 1
      body.toUTF8String should not be empty
    }
  }
}
