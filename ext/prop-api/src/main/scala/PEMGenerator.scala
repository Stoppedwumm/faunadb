package fauna.prop.api

import java.security._
import java.util.Base64
import java.io._
import java.nio.file.{ Files, Path }

object RSAPEMGenerator {
  def generate(out: OutputStream): OutputStream = {
    val kpg = KeyPairGenerator.getInstance("RSA")
    kpg.initialize(2048)
    val kp = kpg.generateKeyPair

    val printStream = new PrintStream(out)
    printStream.println("-----BEGIN PRIVATE KEY-----")
    printStream.println(Base64.getMimeEncoder.encodeToString(kp.getPrivate.getEncoded))
    printStream.println("-----END PRIVATE KEY-----")
    printStream.println("-----BEGIN PUBLIC KEY-----")
    printStream.println(Base64.getMimeEncoder.encodeToString(kp.getPublic.getEncoded))
    printStream.println("-----END PUBLIC KEY-----")
    printStream.close()

    out
  }

  def genPEM(path: Path): File = {
    Files.createDirectories(path)
    val file = File.createTempFile("rsa", ".pem", path.toFile)

    generate(Files.newOutputStream(file.toPath)).close()

    file
  }
}