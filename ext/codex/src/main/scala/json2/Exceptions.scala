package fauna.codex.json2

case class JSONParseException(msg: String) extends Exception(msg)

object JSONUnexpectedTypeException {
  def apply(provided: String, expected: String = null) = {
    val expmsg = if (expected ne null) s" Expected $expected." else ""
    JSONParseException(s"Unexpected JSON $provided.$expmsg")
  }
}
