package fauna.test

import fauna.api.test.APISpecMatchers
import fauna.lang.syntax._
import fauna.prop.api.APIResponseHelpers
import java.io._
import java.lang.{ ProcessBuilder => JProcessBuilder }
import java.net.{ InetSocketAddress, Socket }
import java.nio.file.{ Files, Path }
import org.scalatest.concurrent.Eventually
import scala.util.{ Failure, Success, Try }

class ServiceAppSpec
    extends Spec
    with APIResponseHelpers
    with APISpecMatchers
    with Eventually {

  val tmpdir =
    Files.createTempDirectory("fauna-test-service-startup-spec")
  val configFile = Files.createTempFile(tmpdir, "cfg", ".yml")

  val sampleConfig =
    s"""---
       |auth_root_key: secret
       |storage_data_path: ${tmpdir.toString + "/data"}
       |log_path: ${tmpdir.toString + "/log"}
       |flags_path: ${tmpdir.toString + "/flags.json"}
       |
       |network_broadcast_address: 127.0.0.1
       |network_coordinator_http_address: 0.0.0.0
      """.stripMargin

  override def beforeAll() = {}

  override def afterAll() = {
    tmpdir.toAbsolutePath.deleteRecursively()
  }

  def withServiceApp(configArgs: Map[String, Any], cliArgs: Map[String, Any])(
    f: (Try[Process]) => Unit): Unit = {

    val config = sampleConfig + "\n" + (configArgs map { case (k, v) => s"$k: $v" } mkString ("\n"))
    Files.write(configFile, config.getBytes)

    val e = ServiceAppSpec.launch(cliArgs, configFile)
    try {
      f(e)
    } finally {
      e foreach { _.destroyForcibly().waitFor() }
      configFile.toFile.delete()
    }
  }

  "Service" in {
    // Lifecycle tests

    // start ok without replica name
    withServiceApp(Map.empty[String, Any], Map.empty[String, Any]) { e =>
      e should matchPattern { case Success(_) => }
    }
  }
}

case class CoreStartupTimeoutException(ip: String, port: Int)
    extends Exception(s"Timed out waiting for $ip:$port to come up")

case object CoreTerminationException
    extends Exception("The Core process terminated within the startup window")

//TODO: It would be amazing if we could unify this code with CoreLauncher.
//TODO: commonalities between this and AdminSpec should be moved into some sort of SpecHelper class.
object ServiceAppSpec {
  private val coreIP = "127.0.0.1"
  private val coreAdminPort = 8444
  private val coreStartupTimeout = 60000
  private val classpath = getSysProp("fauna.RunCore.classpath")

  def launch(args: Map[String, Any], configFile: Path): Try[Process] = {
    val pargs = List.newBuilder[String]

    pargs ++= Seq("java", "-server", "-cp", classpath)

    // NOTE: as opposed to RunCorePlugin, we don't bother with configuring the console and the JMX agent ports.
    // If they are needed, the functionality can be ported over.

    pargs ++= Seq(
      "-Djava.net.preferIPv4Stack=true",
      "-Dhttp.connection.timeout=2",
      "-Dhttp.connection-manager.timeout=2",
      "-Dhttp.socket.timeout=6",
      "-Dcassandra.ring_delay_ms=1", // defaults to 30000, making 2nd and subsequent node sleep for 30s during startup
      "-Dcassandra.skip_wait_for_gossip_to_settle=0", // disables 8s waiting for the gossip to settle
      "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens", "java.base/java.util=ALL-UNNAMED",
      "--add-opens", "java.base/java.io=ALL-UNNAMED",
      "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
      "-XX:+UseG1GC",
      "-XX:MaxGCPauseMillis=200",
      "-XX:G1HeapRegionSize=1m",
      "-Xss256k",
      "-Xms256M",
      "-Xmx1536M",
      "fauna.core.Service",
      "-c",
      configFile.toString
    )

    args foreach {
      case (k, v) =>
        pargs ++= Seq(k, v.toString)
    }

    val pb = new JProcessBuilder(pargs.result(): _*)
    pb.redirectErrorStream(true)
    val p = pb.start()

    new Thread(() => p.waitFor).start()

    printProcessStream(p.getInputStream, s"service:out")
    printProcessStream(p.getErrorStream, s"service:err")

    waitForCoreAPI(p)
  }

  private def printProcessStream(is: InputStream, name: String) = {
    new Thread(() =>
      try {
        val reader = new BufferedReader(new InputStreamReader(is))
        var line = reader.readLine()
        while (line != null) {
          println(s"[core:$name] %s" format line)
          line = reader.readLine()
        }
      } catch {
        case _: IOException => ()
        // Ignore it; it just got asynchronously closed
        // FIXME: make ignoring dependent on known process termination
    }).start()
  }

  private def getSysProp(key: String): String =
    Option(System.getProperty(key)) match {
      case Some(value) => value
      case None =>
        throw new IllegalStateException(
          s"$key system property not set! Your project should declare .enablePlugins(fauna.sbt.RunCoreConfigExportPlugin)")
    }

  def waitForCoreAPI(p: Process): Try[Process] = {
    val deadline = System.currentTimeMillis() + coreStartupTimeout
    def timeLeft = (deadline - System.currentTimeMillis).toInt

    while (timeLeft > 0) {
      if (coreAPIListening(timeLeft)) return Success(p)
      if (!p.isAlive) return Failure(CoreTerminationException)
      Thread.sleep(50)
    }
    Failure(CoreStartupTimeoutException(coreIP, coreAdminPort))
  }

  def coreAPIListening(timeout: Int) = {
    val sock = new Socket()
    try {
      sock.connect(new InetSocketAddress(coreIP, coreAdminPort), timeout)
      sock.close()
      true
    } catch {
      case _: Throwable => false
    }
  }
}
