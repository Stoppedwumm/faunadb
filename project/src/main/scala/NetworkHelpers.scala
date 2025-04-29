package fauna.sbt

import java.net.{ InetSocketAddress, Socket }

object NetworkHelpers {
  def waitForPort(
    port: Int,
    ip: String = "127.0.0.1",
    timeout: Int = 1000): Boolean = {
    val start = System.currentTimeMillis()
    def timeLeft = timeout - (System.currentTimeMillis - start).toInt

    while (timeLeft > 0) {
      if (portListening(port, ip, timeLeft)) return true
      Thread.sleep(50)
    }
    false
  }

  def portListening(port: Int, ip: String, timeout: Int) = {
    val sock = new Socket()
    try {
      sock.connect(new InetSocketAddress(ip, port), timeout)
      sock.close()
      true
    } catch {
      case _: Throwable => false
    }
  }
}
