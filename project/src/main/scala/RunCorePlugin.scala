package fauna.sbt

import java.io.{ BufferedReader, File, InputStreamReader }
import java.lang.{ ProcessBuilder => JProcessBuilder }
import java.net.HttpURLConnection
import java.nio.file.{ Files, Paths }
import java.util.Base64
import sbt._
import sbt.plugins.JvmPlugin
import sbt.Keys._
import scala.annotation.tailrec
import scala.sys.process._

object RunCorePlugin extends AutoPlugin {

  val DefaultCoreInstances = Option(System.getenv("FAUNA_NODES")).fold(1) { _.toInt }
  val Secret = Option(System.getenv("FAUNA_ROOT_KEY")) getOrElse "secret"

  import Keys._
  import RamdiskPlugin.Keys._

  object Keys {

    // Global keys
    val initCore = TaskKey[Unit]("init-core", "Cleans up initial core env.")
    val cleanupCore =
      TaskKey[Unit]("cleanup-core", "Clean up any running core process.")
    val runCore = TaskKey[Unit]("run-core", "Run an external core process.")
    val coreClasspath =
      TaskKey[Classpath]("core-classpath", "The classpath used to run core.")
    val corePort = SettingKey[Int]("core-port", "Port to wait on for core to start.")
    val coreConfig = SettingKey[String]("core-config", "Config file for core.")
    val coreAgentPort = SettingKey[Int]("core-agent-port", "Port for debug agent.")
    val coreJMXPort = SettingKey[Int]("core-jmx-port", "Port for JMX.")
    val coreInstances =
      SettingKey[Int]("core-instances", "Number of core nodes to run.")

    // Project task modifiers

    def withLocalCore[T](task: TaskKey[T]) =
      if (isLocalCore) withCore(task) else task

    def withCore[T](task: TaskKey[T]) = task dependsOn runCoreTask

    def withCore[T](task: InputKey[T]) = task dependsOn runCoreTask

    def withCoreEnv[T](task: TaskKey[T]) = task dependsOn configureEnvTask

    def withCoreEnv[T](task: InputKey[T]) = task dependsOn configureEnvTask
  }

  override def requires = JvmPlugin && RamdiskPlugin
  val autoImport = Keys

  override val globalSettings = Seq(
    coreConfig := RunCore.UnspecifiedConfigPath,
    coreInstances := DefaultCoreInstances,
    corePort := 8443,
    coreAgentPort := 5005,
    coreJMXPort := 7199,
    cleanupCore := RunCore.cleanup(streams.value.log),
    initCore := (resetRamdisk dependsOn cleanupCore).value,
    runCore := runCoreTask.value
  )

  // helpers

  // These key-values should always match ones in FaunaDB.scala
  private lazy val isLocalCore =
    Seq(
      "FAUNADB_HOST" -> "localhost",
      "FAUNADB_PORT" -> "8443",
      "FAUNADB_ADMIN_HOST" -> "localhost",
      "FAUNADB_ADMIN_PORT" -> "8444"
    ) forall { kv => Option(System.getenv(kv._1)) forall { v => v == kv._2 } }

  private lazy val runCoreTask = Def.sequential(
    initCore,
    Def.task {
      // FIXME: not sure why this has to be scoped to LocalRootproject.
      val s = (LocalRootProject / streams).value
      RunCore.cleanup(s.log)
      RunCore.run(
        coreClasspath.value,
        coreConfig.value,
        coreInstances.value,
        corePort.value,
        coreAgentPort.value,
        coreJMXPort.value,
        s.log)
    }
  )

  private lazy val configureEnvTask = Def.sequential(
    initCore,
    Def.task {
      RunCore.cleanup(streams.value.log)
      sys.props("fauna.RunCore.faunaRoot") = SystemSettings.os.faunaRoot.toString
      sys.props("fauna.RunCore.classpath") = coreClasspath.value map {
        _.data.getPath
      } mkString File.pathSeparator
    }
  )
}

object RunCoreConfigExportPlugin extends AutoPlugin {
  import RunCorePlugin.Keys._

  override def requires = RunCorePlugin

  override val projectSettings = inConfig(Test)(
    Seq(
      test := (withCoreEnv(test) dependsOn (Test / compile)).value,
      testQuick := (withCoreEnv(testQuick) dependsOn (Test / compile)).evaluated,
      testOnly := (withCoreEnv(testOnly) dependsOn (Test / compile)).evaluated
    ))
}

object RunCore {
  val UnspecifiedConfigPath = "__UNSPECIFIED_CONFIG_PATH__"
  val HttpAuth = {
    val bytes = s"${RunCorePlugin.Secret}:".getBytes("UTF-8")
    val encoded = Base64.getEncoder.encodeToString(bytes)
    s"Basic $encoded"
  }

  def rootPath = SystemSettings.os.faunaRoot

  def defaultConfigPath(instance: Int) = new RunCore(
    instance).InstanceDefaultConfigPath

  def address(instance: Int) = new RunCore(instance).address

  private def instances(count: Int) = (1 to count) map { i => new RunCore(i) }

  def cleanup(log: Logger): Unit = {
    sys.props.remove("source.config")
    sys.props.remove("source.hosts")

    var pids = SystemSettings.os.getCorePids()
    pids.foreach { pid =>
      log.info(s"[runCore] Killing pid: $pid")
      SystemSettings.os.kill(pid)
    }

    while (pids.nonEmpty) {
      Thread.sleep(500)
      val stillAlive = SystemSettings.os.getCorePids()
      (pids diff stillAlive) foreach { pid =>
        log.info(s"[runCore] Killed pid: $pid")
      }
      pids = stillAlive
    }

    apiDirs foreach { dir => s"rm -rf $dir" ! }
    apiDirs foreach { dir => log.warn(s"[runCore] FAILED TO DELETE: $dir") }
  }

  private def apiDirs =
    Option(rootPath.toFile.listFiles).toList flatMap {
      _ filter { _.getName.startsWith("api-") }
    }

  def run(
    cp: Keys.Classpath,
    configPath: String,
    instanceCount: Int,
    port: Int,
    agentPort: Int,
    jmxPort: Int,
    log: Logger): Unit = synchronized {
    sys.props("source.config") = (1 to instanceCount) map {
      defaultConfigPath(_)
    } mkString File.pathSeparator
    sys.props("source.hosts") = (1 to instanceCount) map { address(_) } mkString ","

    val nodes = instances(instanceCount)
    // Start first node, then...
    nodes.head.run(cp, configPath, port, agentPort, jmxPort, log)
    // ... start the rest of them in parallel.
    nodes.drop(1).toParArray foreach {
      _.run(cp, configPath, port, agentPort, jmxPort, log)
    }
  }
}

class RunCore(instance: Int) {
  val address = s"127.0.0.$instance"

  lazy val CorePath = RunCore.rootPath resolve s"api-$instance"
  lazy val InstanceDefaultConfigPath = CorePath.resolve("faunadb.yml").toString
  lazy val LogPath = CorePath.resolve("log").toString
  lazy val FlagsPath = CorePath.resolve("flags.json").toString
  lazy val ExportPath = CorePath.resolve("export").toString

  def run(
    cp: Keys.Classpath,
    aConfigPath: String,
    port: Int,
    agentPort: Int,
    jmxPort: Int,
    log: Logger): Unit = {
    val logPrefix = s"[core:$instance:run]"
    val cpstr = cp map { _.data.getPath } mkString File.pathSeparator

    val adminPort = port + 1

    if (SystemSettings.os == SystemSettings.MacOSX) {
      if (("ifconfig lo0" !!).indexOf(address) == -1) {
        val q = "\""
        log.error(
          s"You need to enable $address loopback address first with: ${q}sudo ifconfig lo0 alias $address up$q")
        sys.error("")
      }
    }

    if (NetworkHelpers.waitForPort(ip = address, port = adminPort, timeout = 3000)) {
      log.info(s"$logPrefix Core is already running.")
      return
    }

    val configPath = if (aConfigPath == RunCore.UnspecifiedConfigPath) {
      log.info(s"$logPrefix Using default config.")
      Files.createDirectories(CorePath)
      Files.write(
        Paths.get(InstanceDefaultConfigPath),
        DefaultConfig.getBytes("UTF-8"))
      InstanceDefaultConfigPath
    } else {
      aConfigPath
    }

    log.info(s"$logPrefix Running core with configPath $configPath")

    val pargs = List.newBuilder[String]

    pargs ++= Seq("java", "-server", "-cp", cpstr)

    if (agentPort > 0) {
      pargs += s"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$address:$agentPort"
    }

    if (jmxPort > 0) {
      pargs ++= Seq(
        s"-Dcom.sun.management.jmxremote.port=$jmxPort",
        s"-Dcom.sun.management.jmxremote.host=$address",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        s"-Djava.rmi.server.hostname=$address"
      )
    }

    // TODO: remove when we can (see faunadb script for details)
    pargs ++= Seq(
      "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "java.base/java.util.concurrent=ALL-UNNAMED",
      "java.base/java.util=ALL-UNNAMED",
      "java.base/java.io=ALL-UNNAMED",
      "java.base/java.lang=ALL-UNNAMED",
      "java.base/java.math=ALL-UNNAMED",
      "java.base/java.net=ALL-UNNAMED",
      "java.base/sun.net.www.protocol.http=ALL-UNNAMED",
      "java.base/sun.net.www.protocol.https=ALL-UNNAMED",
      "java.base/sun.nio.ch=ALL-UNNAMED"
    ).flatMap(Seq("--add-opens", _))

    pargs ++= Seq(
      "-javaagent:./service/lib/jamm.jar",
      "-Djava.net.preferIPv4Stack=true",
      "-Dhttp.connection.timeout=2",
      "-Dhttp.connection-manager.timeout=2",
      "-Dhttp.socket.timeout=6",
      "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
      "-Dcassandra.ring_delay_ms=1", // defaults to 30000, making 2nd and subsequent node sleep for 30s during startup
      "-Dcassandra.skip_wait_for_gossip_to_settle=0", // disables 8s waiting for the gossip to settle
      "-XX:+UseG1GC",
      "-XX:MaxGCPauseMillis=200",
      "-XX:G1HeapRegionSize=1m",
      "-XX:+StartAttachListener", // enable perf-map-agent to attach to jvm for symbol generation
      "-XX:+PreserveFramePointer", // enables symbol extraction
      "-Xss256k",
      "-Xms256M",
      "-Xmx2G",
      "-Dio.netty.allocator.type=unpooled",
      "-Dio.netty.noPreferDirect=true",
      "-Dfauna.safe.access.providers=localhost",
      "fauna.core.Service",
      "-c",
      configPath
    )

    val pb = new JProcessBuilder(pargs.result: _*)

    pb.redirectErrorStream(true)
    val t1 = System.currentTimeMillis()
    val process = pb.start()

    new Thread(new Runnable {
      def run(): Unit = {
        val is = process.getInputStream
        val reader = new BufferedReader(new InputStreamReader(is))
        var line = reader.readLine()
        while (line != null) {
          log.info(s"[core:$instance:out] %s" format line)
          line = reader.readLine()
        }
      }
    }).start()

    new Thread(new Runnable {
      def run(): Unit = {
        val is = process.getErrorStream
        val reader = new BufferedReader(new InputStreamReader(is))
        var line = reader.readLine()
        while (line != null) {
          log.info(s"[core:$instance:err] %s" format line)
          line = reader.readLine()
        }
      }
    }).start()

    if (
      NetworkHelpers.waitForPort(ip = address, port = adminPort, timeout = 60000)
    ) {
      log.info(
        s"$logPrefix Core successfully started in ${System.currentTimeMillis() - t1}ms.")
    } else {
      log.info(s"$logPrefix Core did not start up successfully.")
    }

    val t2 = System.nanoTime()
    if (instance == 1) {
      postInitOrJoin(adminPort, "/admin/init", "{\"replica_name\": \"NoDC\"}")
      log.info(s"$logPrefix Core initialized as first in the cluster in ${(System
          .nanoTime() - t2) / 1000000}ms.")
    } else {
      postInitOrJoin(
        adminPort,
        "/admin/join",
        "{\"seed\": \"127.0.0.1:7500\", \"replica_name\": \"NoDC\"}")
      log.info(
        s"$logPrefix Core successfully joined the cluster in ${(System.nanoTime() - t2) / 1000000}ms.")
    }
    val t3 = System.nanoTime()
    waitForPing(port)
    log.info(
      s"$logPrefix Core became ready to take writes in ${(System.nanoTime() - t3) / 1000000}ms.")
  }

  private def makeHttpRequest(
    method: String,
    address: String,
    port: Int,
    path: String,
    body: String) = {
    val url = new URI("http", null, address, port, path, null, null).toURL()
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    conn.setRequestProperty("Connection", "close")
    conn.setRequestProperty("Host", address)
    conn.setRequestProperty("Authorization", RunCore.HttpAuth)
    conn.setDoInput(true)

    if (body != "") {
      val bodyBytes = body.getBytes("utf-8")
      conn.setRequestProperty("Content-type", "application/json;charset=utf-8")
      conn.setRequestProperty("Content-length", bodyBytes.length.toString)
      conn.setDoOutput(true)
      val output = conn.getOutputStream
      output.write(bodyBytes)
      output.close()
    }

    val input =
      try {
        conn.getInputStream
      } catch {
        case e: Exception => sys.error(s"Error requesting $url: ${e.getMessage}")
      }

    try {
      if (conn.getResponseCode != HttpURLConnection.HTTP_OK) {
        sys.error(
          s"Got unexpected response code ${conn.getResponseCode} ${conn.getResponseMessage} from $url")
      }
      val length = conn.getContentLength
      val inputBuf = new Array[Byte](length)
      var ofs = 0
      while (ofs < length) {
        val readLen = input.read(inputBuf, ofs, length - ofs)
        if (readLen == -1) {
          sys.error(
            "Could only read $ofs bytes from response; expected $length from $url")
        }
        ofs = ofs + readLen
      }
      new String(inputBuf, "UTF-8")
    } finally {
      input.close()
    }
  }

  private def postInitOrJoin(port: Int, path: String, body: String) = {
    val resp = makeHttpRequest("POST", address, port, path, body)
    if (resp != "{\"changed\":true}") {
      sys.error(s"$path returned unexpected response $resp")
    }
  }

  @tailrec
  private def waitForPing(port: Int): Unit = {
    try {
      val resp = makeHttpRequest("GET", address, port, "/ping", "")
      if (resp == "{\"resource\":\"Scope write is OK\"}") {
        return
      }
    } catch {
      case _: RuntimeException => ()
    }

    Thread.sleep(100)
    waitForPing(port)
  }
  // hashed 'secret'
  val dataExportSharedKeyHash =
    "$2a$05$kilpNsNs23NdY7I/Y5a.A.cZCJSYc7z/Jt.bibx3Zgt3HVokQC3Q2"

  // Backslashes in YAML need to be escaped. Don't forget!
  lazy val DefaultConfig = (s"""
    |auth_root_key: ${RunCorePlugin.Secret}
    |export_shared_key_hashes: ["$dataExportSharedKeyHash"]
    |
    |cache_schema_size_mb: 3
    |cache_schema_ttl_seconds: 2
    |
    |log_path: "${LogPath.replace("\\", "\\\\")}"
    |
    |network_broadcast_address: $address
    |
    |network_admin_http_address: $address
    |network_console_address: $address
    |network_console_port: 7777
    |network_coordinator_http_address: $address
    |network_read_timeout_ms: 60000
    |network_write_timeout_ms: 60000
    |network_round_trip_time_ms: 150
    |
    |storage_data_path: "${CorePath.toString.replace("\\", "\\\\")}"
    |storage_enable_qos: true
    |
    |export_path: "${ExportPath.replace("\\", "\\\\")}"
    |
    |shutdown_grace_period_seconds: 0
    |
    |flags_path: "${FlagsPath.replace("\\", "\\\\")}"
    |
    |transaction_log_backup: false
    |
    |background_task_exec_backoff_time_seconds: 60
    |
    |jwks_cache_refresh_rate_seconds: 300
    |jwks_cache_expire_rate_seconds: 3600
    |
    |jwk_encryption_key_store: ${RSAPEMGenerator
                                .genPEM(CorePath.resolve("keys"))
                                .toString}
    |jwk_encryption_password: ""
    |jwk_encryption_kid: "1234"
    |  """).stripMargin
}
