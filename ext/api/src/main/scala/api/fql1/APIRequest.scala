package fauna.api.fql1

import fauna.api.RequestParams
import fauna.atoms.APIVersion
import fauna.auth._
import fauna.net.http.HttpRequest
import fauna.net.util.URIQueryString
import fauna.trace.traceMsg
import io.netty.buffer.ByteBuf
import io.netty.util.AsciiString

object APIRequest {
  def apply(
    req: HttpRequest,
    auth: Option[Auth],
    body: Option[ByteBuf]): APIRequest = {
    val apiVersion = RequestParams.APIVersionParam(req)
    APIRequest(req, body, auth, apiVersion)
  }
}

case class APIRequest(
  req: HttpRequest,
  body: Option[ByteBuf],
  getAuth: Option[Auth],
  version: APIVersion) {

  lazy val params: Map[String, String] =
    query match {
      case Some(qs) =>
        traceMsg(s"Querystring: $qs")
        URIQueryString.parse(qs)
      case None =>
        traceMsg("Querystring: <none>")
        Map.empty
    }

  def method = req.method

  def path = req.path.getOrElse("")

  def query = req.query

  def auth = getAuth.get

  def header(name: AsciiString) = req.header(name)

  def getHeader(name: AsciiString) = req.getHeader(name)

  def containsHeader(name: AsciiString) = req.containsHeader(name)
}
