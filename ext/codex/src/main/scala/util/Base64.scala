package fauna.util

import io.netty.util.AsciiString
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.util.{ Base64 => JBase64 }

object Base64 {
  private[this] val urlSafeNoPadding = JBase64.getUrlEncoder.withoutPadding

  // URL safe

  def encodeUrlSafeBuffer(byteBuf: ByteBuffer): ByteBuffer =
    urlSafeNoPadding.encode(byteBuf)

  def encodeUrlSafeAscii(buf: ByteBuffer): AsciiString = {
    val enc = encodeUrlSafeBuffer(buf)
    new AsciiString(enc, false)
  }

  def encodeUrlSafeAscii(bytes: Array[Byte]): AsciiString =
    encodeUrlSafeAscii(ByteBuffer.wrap(bytes))

  def encodeUrlSafe(bytes: Array[Byte]): String =
    encodeUrlSafeAscii(bytes).toString

  def decodeUrlSafe(string: String): Array[Byte] =
    JBase64.getUrlDecoder.decode(string)

  // standard

  def encodeStandardBuffer(buf: ByteBuffer): ByteBuffer =
    JBase64.getEncoder.encode(buf)

  def encodeStandardAscii(buf: ByteBuffer): AsciiString = {
    val enc = encodeStandardBuffer(buf)
    new AsciiString(enc, false)
  }

  def encodeStandardAscii(bytes: Array[Byte]): AsciiString =
    encodeStandardAscii(ByteBuffer.wrap(bytes))

  def encodeStandard(bytes: Array[Byte]): String =
    encodeStandardAscii(bytes).toString

  def decodeStandard(string: String): Array[Byte] =
    JBase64.getDecoder.decode(string)

  def decode(string: String): Array[Byte] = {
    val bytes = string.getBytes(StandardCharsets.ISO_8859_1)

    (0 until bytes.length) foreach { i =>
      (bytes(i): @annotation.switch) match {
        case '+' => bytes(i) = '-'
        case '/' => bytes(i) = '_'
        case _   => ()
      }
    }

    JBase64.getUrlDecoder.decode(bytes)
  }
}
