package fauna.api.test

import fauna.net.http._
import io.netty.util.AsciiString
import java.util.UUID
import scala.concurrent.Future

trait HttpClientHelpers {

  implicit final class HttpClientOps(cli: HttpClient) {

    def upload(
      path: String,
      files: Seq[(String, String)],
      token: AuthType = NoAuth,
      query: String = null,
      headers: Seq[(AsciiString, AnyRef)] = Seq.empty): Future[HttpResponse] = {

      val boundary = UUID.randomUUID.toString
      val content = new StringBuilder

      files foreach { case (name, fsl) =>
        content.append(
          s"""|
              |--$boundary
              |Content-Disposition: form-data; name="$name"
              |Content-Type: application/octet-stream
              |
              |$fsl""".stripMargin
        )
      }

      content.append(
        s"""|
        |--${boundary}--""".stripMargin
      )

      val contentType = s"${ContentType.FormData}; boundary=$boundary"
      val body = Body(content.result(), contentType)
      cli.post(path, body, token, query, headers)
    }
  }
}
