package fauna.codex.cbor

case class CBORUnexpectedTypeException(provided: String, expected: String = null) extends Exception(
  s"Unexpected CBOR $provided." + (if (expected eq null) "" else s" Expected $expected."))

case class CBORInvalidLengthException(provided: Long, expected: Long) extends Exception(
  s"Unexpected CBOR Array length $provided. Expected $expected")

case object CBORValidationException extends Exception("Invalid CBOR")
