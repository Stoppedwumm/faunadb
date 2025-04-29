package fauna.net

import java.io.IOException
import java.net.{ InetAddress, InetSocketAddress, ServerSocket, Socket }
import scala.collection.mutable.{ Set => MSet }
import scala.util.Random

object Network extends NetworkHelpers

trait NetworkHelpers {

  def address(host: String, port: Int) = new InetSocketAddress(host, port)

  def address(host: String) = InetAddress.getByName(host)

  def address(port: Int) = new InetSocketAddress(port)

  def hostListening(port: Int, host: String = "127.0.0.1", connectTimeout: Int = 1000) = {
    val sock = new Socket()
    try {
      sock.connect(address(host, port), connectTimeout)
      sock.close()
      true
    } catch {
      case _: Throwable => false
    }
  }

  def waitForHost(port: Int, host: String = "127.0.0.1", timeout: Int = 1000): Boolean = {
    val start = System.currentTimeMillis

    def timeLeft = timeout - (System.currentTimeMillis - start).toInt

    while (timeLeft > 0) {
      if (hostListening(port, host, timeLeft)) return true
    }

    false
  }

  // quick and dirty port reservation system

  private val usedPorts = MSet[Int]()

  def findFreePort(): Int = synchronized { genFreePort(10000, 20000) }

  @annotation.tailrec
  private[this] def genFreePort(low: Int, high: Int): Int = {
    val rand = new Random
    val port = rand.nextInt(high - low) + low
    var found = false

    usedPorts.synchronized {
      if (!(usedPorts contains port)) {
        try {
          (new ServerSocket(port)).close()
          usedPorts += port
          found = true
        } catch {
          case _: IOException => ()
        }
      }
    }

    if (found) port else genFreePort(low, high)
  }
}
