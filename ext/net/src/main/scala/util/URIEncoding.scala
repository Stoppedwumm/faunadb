package fauna.net.util

import java.net.{ URLEncoder, URLDecoder }
import fauna.trace._

object URIEncoding {
  def encode(string: String): String = Option(string) match {
    case Some(string) =>
      val r = URLEncoder.encode(string, "UTF-8").replace("+", "%20")
      traceMsg(s"  URIEncoding.encode: $string -> $r")
      r
    case None => ""
  }

  def decode(string: String): Option[String] = try {
    val r = Some(URLDecoder.decode(string.replace("+", "%20"), "UTF-8"))
    traceMsg(s"  URIEncoding.decode: $string -> $r")
    r
  } catch {
    case _: IllegalArgumentException => None
  }
}
