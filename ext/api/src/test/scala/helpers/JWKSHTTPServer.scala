package fauna.api.test

import java.net.URL
import java.util
import fauna.exec.ImmediateExecutionContext
import fauna.net.Network
import fauna.net.http._
import fauna.net.security.{ JWK, JWKS, NoSSL }
import io.netty.handler.codec.http.HttpResponseStatus
import scala.annotation.unused
import scala.concurrent.Future
import scala.util.Random

class JWKSHTTPServer(val port: Int = Network.findFreePort()) {
  val address = s"https://localhost:$port"

  private val keys = new util.HashMap[String, (JWKS, String)]()
  private val server = HttpServer(Network.address(port), ssl = NoSSL)(handler)

  private def handler(
    @unused info: HttpRequestChannelInfo,
    httpRequest: HttpRequest): Future[HttpResponse] = {
    val etag = httpRequest.getHeader(HTTPHeaders.IfNoneMatch)

    val httpResponse = Option(keys.get(httpRequest.path.getOrElse("/"))) match {
      case Some((_, tag)) if etag.contains(tag) =>
        val headers = Seq(HTTPHeaders.ETag -> tag)
        HttpResponse(HttpResponseStatus.NOT_MODIFIED, headers, NoBody)

      case Some((jwks, tag)) =>
        val headers = Seq(HTTPHeaders.ETag -> tag)
        HttpResponse(
          HttpResponseStatus.OK,
          headers,
          Body(jwks.toJSON.toByteBuf, "application/json"))

      case _ =>
        HttpResponse(HttpResponseStatus.BAD_REQUEST, NoBody)
    }

    implicit val ec = ImmediateExecutionContext
    HttpServer.discard(httpRequest) map { _ => httpResponse }
  }

  def add(jwksUri: String, jwk: JWK) = {
    val url = new URL(jwksUri)
    val etag = Random.alphanumeric take 16 mkString

    keys.put(url.getPath, (JWKS(Seq(jwk)), etag))
  }

  def start() = server.start()
  def stop() = server.stop()
}
