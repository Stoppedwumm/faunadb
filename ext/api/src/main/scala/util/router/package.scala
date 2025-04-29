package fauna.util

import fauna.net.util.URIEncoding

// TODO: Nice, type-safe dsl, something like: GET("users" / digits / "stream") { id => ... }
//       Might consider something like webbing: https://github.com/finagle/webbing

package object router {
  val PathSeparator = """/"""
  val QuerySeparator = """\?"""
  val Wildcard = """^\{([^}]*)\}$""".r

  def TokenizePath(path: String): List[String] = {
    val decodedTokens = path.split(PathSeparator).map { t => URIEncoding.decode(t).getOrElse(t) }
    decodedTokens.toList filterNot { _.isEmpty }
  }
}
