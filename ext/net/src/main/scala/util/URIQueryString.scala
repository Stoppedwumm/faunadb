package fauna.net.util

object URIQueryString {

  val QueryKeySeparator = '&'
  val QueryValueSeparator = '='

  def parse(qs: String): Map[String, String] = {
    val b = Map.newBuilder[String, String]
    var pos = 0

    while (pos >= 0 && pos < qs.length) {
      val pairSep = qs.indexOf(QueryKeySeparator, pos)
      val keySep = qs.indexOf(QueryValueSeparator, pos)

      val pairEnd = if (pairSep < 0) qs.length else pairSep
      val keyEnd = if (keySep < 0 || keySep > pairEnd) pairEnd else keySep
      val valueStart = if (keyEnd < pairEnd) keyEnd + 1 else keyEnd

      val key = qs.substring(pos, keyEnd)
      val value = qs.substring(valueStart, pairEnd)

      (URIEncoding.decode(key), URIEncoding.decode(value)) match {
        case (Some(k), Some(v)) => b += (k -> v)
        case _                  => return Map.empty[String, String]
      }

      pos = pairEnd + 1
    }

    b.result()
  }
}
