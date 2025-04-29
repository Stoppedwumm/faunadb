package fauna.prop.api

import fauna.atoms.APIVersion
import fauna.codex.json.JSObject
import fauna.flags.test.{ FlagProps, FlagsHelpers }
import fauna.lang.syntax._
import fauna.net.http.{ Body, ContentType, HttpClient }
import fauna.net.security.SSLConfig
import java.io.{ BufferedReader, IOException, InputStream, InputStreamReader }
import java.lang.{ ProcessBuilder => JProcessBuilder }
import java.net.{ InetSocketAddress, Socket }
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.TimeoutException
import scala.collection.mutable.{ Map => MMap, Set => MSet }
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class OpsLimits(read: Double, write: Double, compute: Double)
case class FeatureFlags(version: Int, props: Vector[FlagProps])

object CoreLauncher {
  sealed trait TerminationMode {
    def terminate(p: Process): Unit
  }

  case object Gracefully extends TerminationMode {
    def terminate(p: Process) = p.destroy()
  }

  case object Forcibly extends TerminationMode {
    def terminate(p: Process) = p.destroyForcibly()
  }

  private val SigkillExitValue = 137
  private val SigtermExitValue = 143
  private val coreStartupTimeout = 60000
  private val corePort = 8443
  private val coreAdminPort = 8444
  val defaultReplica = "NoDC"
  val LoadSnapshotTimeoutSeconds = 60

  // System properties set by fauna.sbt.RunCoreConfigExportPlugin
  val rootDir = Paths.get(getSysProp("fauna.RunCore.faunaRoot"))
  private val classpath = getSysProp("fauna.RunCore.classpath")

  private var processes = Map.empty[Int, Process]

  private val ids = MSet.empty[Int]

  // Efficiency :shrug:.
  def nextInstanceID(): Int = synchronized {
    val next = (1 to 255) find { !ids.contains(_) }
    if (next.isEmpty) throw new NoSuchElementException("too many instances!")
    ids.add(next.get)
    next.get
  }

  def instanceDir(i: Int) = rootDir.resolve(s"api-$i")

  def configFile(i: Int) = instanceDir(i).resolve("faunadb.yml")

  def address(i: Int) = s"127.0.0.$i"

  def client(
    i: Int,
    apiVers: String,
    ssl: Option[SSLConfig] = None,
    responseTimeout: FiniteDuration = HttpClient.DefaultResponseTimeout) =
    FaunaDB.client(apiVers, address(i), ssl, Seq.empty, responseTimeout)

  // These are shims to keep supporting multicore tests.
  def apiClient(i: Int, apiVers: String) = client(i, apiVers).api
  def adminClient(
    i: Int,
    apiVers: String,
    responseTimeout: FiniteDuration = HttpClient.DefaultResponseTimeout) =
    client(i, apiVers, responseTimeout = responseTimeout).admin

  def terminate(i: Int, mode: TerminationMode = Forcibly) = {
    processes.get(i) match {
      case None    => throw new IllegalStateException(s"Instance $i not running")
      case Some(p) => terminateProcess(p, i, mode)
    }
  }

  // Annihilates an instance by terminating the process and clearing out all of the
  // data.
  def delete(id: Int) = {
    terminate(id, Gracefully)
    instanceDir(id).deleteRecursively()
    synchronized {
      ids.remove(id)
    }
  }

  private def terminateProcess(p: Process, i: Int, mode: TerminationMode) = {
    mode.terminate(p)
    waitForTerminationAndRemove(p, i)
  }

  def terminateAll(mode: TerminationMode = Forcibly) = {
    val ps = processes map { case (i, p) =>
      Future { terminateProcess(p, i, mode) }
    }

    Await.ready(Future.sequence(ps), Duration.Inf)
  }

  /** Waits for a node to terminate. If the node is not running, returns immediately.
    * @param i the index of the node to terminate
    */
  def waitForTermination(i: Int) = processes.get(i) foreach { p =>
    waitForTerminationAndRemove(p, i)
  }

  private def waitForTerminationAndRemove(p: Process, i: Int) = {
    val exitValue = p.waitFor()
    synchronized {
      if (processes.get(i) contains p) {
        if (
          exitValue != 0 && exitValue != SigtermExitValue && exitValue != SigkillExitValue
        ) {
          println(s"Process for instance $i exited with value $exitValue")
        }
        processes = processes - i
      }
    }
  }

  def launchMultiple(
    is: Seq[Int],
    networkWriteTimeout: Int = 60,
    syncOnShutdown: Boolean = false,
    withSSL: Boolean = false,
    withAdminSSL: Boolean = false,
    withApiSSL: Boolean = false,
    withDualWrite: Boolean = false,
    opsLimits: Option[OpsLimits] = None,
    flags: Option[FeatureFlags] = None
  ): Unit = {
    // Clients returned by launch() are discarded, so this is arbitrary.
    val apiVers = APIVersion.Default.toString
    val f = (is: @unchecked) match {
      case hd +: rest =>
        Future(
          launch(
            hd,
            apiVers,
            networkWriteTimeout,
            syncOnShutdown,
            withSSL,
            withAdminSSL,
            withApiSSL,
            withDualWrite,
            opsLimits = opsLimits,
            flags = flags
          )
        ) flatMap { c =>
          val fs = rest map { i =>
            Future(
              launch(
                i,
                apiVers,
                networkWriteTimeout,
                syncOnShutdown,
                withSSL,
                withAdminSSL,
                withApiSSL,
                withDualWrite,
                opsLimits = opsLimits,
                flags = flags
              )
            )
          }

          Future.sequence(fs) map {
            c +: _
          }
        }
    }
    Await.result(f, Duration.Inf)
  }

  def launch(
    i: Int,
    apiVers: String,
    networkWriteTimeout: Int = 60,
    syncOnShutdown: Boolean = false,
    withSSL: Boolean = false,
    withAdminSSL: Boolean = false,
    withApiSSL: Boolean = false,
    withDualWrite: Boolean = false,
    index2CFValidationRatio: Double = 0.0,
    withStdOutErr: Boolean = false,
    opsLimits: Option[OpsLimits] = None,
    burstSeconds: Option[Double] = None,
    flags: Option[FeatureFlags] = None
  ): FaunaDB.Client = {

    createInstanceDirAndConfigFile(
      i,
      networkWriteTimeout,
      syncOnShutdown,
      withSSL,
      withAdminSSL,
      withApiSSL,
      withDualWrite,
      index2CFValidationRatio,
      opsLimits,
      burstSeconds,
      flags
    )

    launchConfigured(i, apiVers, withStdOutErr)
  }

  def createInstanceDirAndConfigFile(
    i: Int,
    networkWriteTimeout: Int = 60,
    syncOnShutdown: Boolean = false,
    withSSL: Boolean = false,
    withAdminSSL: Boolean = false,
    withApiSSL: Boolean = false,
    withDualWrite: Boolean = false,
    index2CFValidationRatio: Double = 0.0,
    opsLimits: Option[OpsLimits] = None,
    burstSeconds: Option[Double] = None,
    flags: Option[FeatureFlags] = None
  ): Path = {
    checkNotLaunched(i)

    val instDir = instanceDir(i)
    // Delete previous directory if it exists
    deleteDir(instDir)
    Files.createDirectories(instDir)
    if (!instDir.toFile.isDirectory) {
      throw new IOException(s"Failed to create directory $instDir")
    }
    writeConfigFile(
      i,
      networkWriteTimeout,
      syncOnShutdown,
      withSSL,
      withAdminSSL,
      withApiSSL,
      withDualWrite,
      index2CFValidationRatio,
      opsLimits,
      burstSeconds,
      flags
    )

    instDir
  }

  case class IDAndClient(id: Int, client: FaunaDB.Client)

  // Launches a single node cluster and waits up to 30 seconds for it to be
  // initialized.
  def launchOneNodeCluster(
    apiVers: String,
    withAdminSSL: Boolean = false,
    withStdOutErr: Boolean = false,
    opsLimits: Option[OpsLimits] = None,
    flags: Option[FeatureFlags] = None
  ): IDAndClient = {
    val id = nextInstanceID()
    val client = CoreLauncher.launch(
      id,
      apiVers,
      withAdminSSL = withAdminSSL,
      withStdOutErr = withStdOutErr,
      opsLimits = opsLimits,
      flags = flags)
    val js = JSObject("replica_name" -> defaultReplica)
    Await.ready(
      client.admin
        .post("/admin/init", Body(js.toByteBuf, ContentType.JSON), FaunaDB.rootKey),
      30.seconds)
    IDAndClient(id, client)
  }

  private def copySSLFiles(dir: Path): (Path, Path) = {
    val devName = "device.pem"
    val keyFile = dir.resolve(devName)
    val keyResource = Paths.get(getClass.getClassLoader.getResource(devName).toURI)
    Files.copy(keyResource, keyFile, StandardCopyOption.REPLACE_EXISTING)

    val trustName = "rootCA.pem"
    val trustFile = dir.resolve(trustName)
    val trustResource =
      Paths.get(getClass.getClassLoader.getResource(trustName).toURI)
    Files.copy(trustResource, trustFile, StandardCopyOption.REPLACE_EXISTING)

    (keyFile, trustFile)
  }

  def writeConfigFile(
    i: Int,
    networkWriteTimeout: Int = 60,
    syncOnShutdown: Boolean = false,
    withSSL: Boolean = false,
    withAdminSSL: Boolean = false,
    withApiSSL: Boolean = false,
    withDualWrite: Boolean = false,
    index2CFValidationRatio: Double = 0.0,
    opsLimits: Option[OpsLimits] = None,
    burstSeconds: Option[Double] = None,
    flags: Option[FeatureFlags] = None
  ) = {

    def pathToStr(p: Path) = p.toString.replace("\\", "\\\\")

    val configPath = configFile(i)
    val instAddress = address(i)
    val instDir = instanceDir(i)
    val flagsPath = instDir.resolve("flags.json")
    val exportPath = instDir.resolve("export")
    exportPath.toFile.mkdirs()

    flags match {
      case Some(f) =>
        FlagsHelpers.writeFlags(flagsPath, f.version, f.props)
      case None =>
        FlagsHelpers.writeFlags(flagsPath, 0, Vector.empty)
    }

    // hashed 'secret'
    val dataExportSharedKeyHash =
      "$2a$05$kilpNsNs23NdY7I/Y5a.A.cZCJSYc7z/Jt.bibx3Zgt3HVokQC3Q2"

    // FIXME: Re-enable schema
    var config = s"""
      |auth_root_key: ${FaunaDB.rootKey}
      |export_shared_key_hashes: ["$dataExportSharedKeyHash"]
      |
      |cache_key_size_mb: 1
      |cache_key_ttl_seconds: 2
      |cache_schema_size_mb: 1
      |cache_schema_ttl_seconds: 2
      |
      |log_path: "${pathToStr(instDir.resolve("log"))}"
      |
      |network_broadcast_address: $instAddress
      |
      |network_console_address: $instAddress
      |network_console_port: 7777
      |network_coordinator_http_address: $instAddress
      |network_admin_http_address: $instAddress
      |network_read_timeout_ms: 60000
      |network_write_timeout_ms: ${networkWriteTimeout * 1000}
      |network_stream_timeout_ms: 2000
      |network_round_trip_time_ms: 150
      |
      |shutdown_grace_period_seconds: 1
      |
      |storage_data_path: "${pathToStr(instDir)}"
      |storage_sync_on_shutdown: $syncOnShutdown
      |storage_dual_write_index_cfs: $withDualWrite
      |storage_enable_qos: true
      |storage_new_read_timeout_ms: 600000
      |storage_load_snapshot_timeout_seconds: $LoadSnapshotTimeoutSeconds
      |
      |flags_path: ${pathToStr(flagsPath)}
      |
      |index2cf_validation_ratio: $index2CFValidationRatio
      |
      |jwks_cache_refresh_rate_seconds: 300
      |jwks_cache_expire_rate_seconds: 3600
      |
      |jwk_encryption_key_store: ${RSAPEMGenerator
                     .genPEM(instDir.resolve("keys"))
                     .toString}
      |jwk_encryption_password: ""
      |jwk_encryption_kid: "1234"
      |
      |runtime_processors: 1
      |
      |trace_probability: 0.01
      |trace_secret: randomsecretfortracing
      |
      |export_path: "${pathToStr(exportPath)}"
      |""".stripMargin

    if (withSSL || withAdminSSL || withApiSSL) {
      val (keyFile, trustFile) = copySSLFiles(instDir)

      if (withSSL) {
        config += s"""
        |peer_encryption_level: all
        |peer_encryption_password: secret
        |peer_encryption_key_file: "${pathToStr(keyFile)}"
        |peer_encryption_trust_file: "${pathToStr(trustFile)}"
        """.stripMargin
      }

      if (withAdminSSL) {
        config += s"""
        |admin_ssl_password: secret
        |admin_ssl_key_file: "${pathToStr(keyFile)}"
        |admin_ssl_trust_file: "${pathToStr(trustFile)}"
        """.stripMargin
      }

      if (withApiSSL) {
        config += s"""
        |http_ssl_password: secret
        |http_ssl_key_file: "${pathToStr(keyFile)}"
        |http_ssl_trust_file: "${pathToStr(trustFile)}"
        """.stripMargin
      }
    }

    opsLimits foreach { limits =>
      config += s"""
      |query_ops_rate_limits_enabled: true
      |query_max_read_ops_per_second: ${limits.read}
      |query_max_write_ops_per_second: ${limits.write}
      |query_max_compute_ops_per_second: ${limits.compute}
      """.stripMargin
    }

    burstSeconds foreach { burst =>
      config += s"""
      |query_ops_rate_limits_burst_seconds: $burst
      """.stripMargin
    }

    Files.write(configPath, config.getBytes("UTF-8"))
  }

  private def deleteDir(dir: Path) = {
    if (dir.toFile.isDirectory)
      Files.walkFileTree(
        dir,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes) = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException) = {
            if (exc eq null) {
              Files.delete(dir)
            }
            FileVisitResult.CONTINUE
          }
        }
      )
  }

  private def checkNotLaunched(i: Int) = synchronized {
    if (processes.contains(i)) {
      throw new IllegalStateException(s"Instance $i already running")
    }
  }

  /** Launches a node that was launched before but has since terminated
    * (preserving its data and config).
    *
    * @param i the instance index
    * @return the HttpClient for the instance
    */
  def relaunch(i: Int, apiVers: String) = {
    checkNotLaunched(i)
    if (!configFile(i).toFile.isFile) {
      throw new IllegalStateException(
        s"Can not find ${configFile(i)}; instance $i was probably not launched before."
      )
    }
    launchConfigured(i, apiVers)
  }

  val envVars = MMap.empty[String, String]

  def launchConfigured(
    i: Int,
    apiVers: String,
    withStdOutErr: Boolean = false): FaunaDB.Client = {
    val pargs = List.newBuilder[String]

    pargs ++= Seq("java", "-server", "-cp", classpath)

    // NOTE: as opposed to RunCorePlugin, we don't bother with configuring the
    // console and the JMX agent ports.
    // If they are needed, the functionality can be ported over.

    pargs ++= Seq(
      "-Djava.net.preferIPv4Stack=true",
      "-Dhttp.connection.timeout=2",
      "-Dhttp.connection-manager.timeout=2",
      "-Dhttp.socket.timeout=6",
      "-Dcassandra.ring_delay_ms=1", // defaults to 30000, making 2nd and subsequent node sleep for 30s during startup
      "-Dcassandra.skip_wait_for_gossip_to_settle=0", // disables 8s waiting for the gossip to settle
      s"-Djava.rmi.server.hostname=127.0.0.$i",
      "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.math=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/sun.net.www.protocol.http=ALL-UNNAMED",
      "--add-opens=java.base/sun.net.www.protocol.https=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "-XX:+UseG1GC",
      "-XX:MaxGCPauseMillis=200",
      "-XX:G1HeapRegionSize=1m",
      "-Xss256k",
      "-Xms256M",
      "-Xmx1536M",
      "-XX:-OmitStackTraceInFastThrow", // the stack is more important than the speed
      "-Dfauna.safe.access.providers=localhost" // For JWT.
    )

    if (sys.env.contains("CORE_DEBUG")) {
      println(s"attaching core debugger to 127.0.0.$i:5005")
      pargs += s"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.$i:5005"
    }

    pargs ++= Seq(
      "fauna.core.Service",
      "-c",
      configFile(i).toString
    )

    val pb = new JProcessBuilder(pargs.result(): _*)
    envVars foreach { case (k, v) =>
      pb.environment().put(k, v)
    }

    pb.redirectErrorStream(true)
    val p = pb.start()

    synchronized {
      processes = processes.updated(i, p)
    }

    new Thread(new Runnable {
      def run() = waitForTerminationAndRemove(p, i)
    }).start()

    if (withStdOutErr) {
      printProcessStream(p.getInputStream, s"$i:out")
      printProcessStream(p.getErrorStream, s"$i:err")
    }

    val instAddress = address(i)
    if (!waitForEndpoint(instAddress, coreAdminPort)) {
      throw new TimeoutException(
        s"Timed out waiting for $instAddress:$coreAdminPort to start"
      )
    } else {
      client(i, apiVers)
    }
  }

  private def printProcessStream(is: InputStream, name: String) = {
    new Thread(new Runnable {
      def run(): Unit = {
        try {
          val reader = new BufferedReader(new InputStreamReader(is))
          var line = reader.readLine()
          while (line != null) {
            println(s"[core:$name] %s" format line)
            line = reader.readLine()
          }
        } catch {
          // Ignore it; it just got asynchronously closed
          // FIXME: make ignoring dependent on known process termination
          case _: IOException => ()
        }
      }
    }).start()
  }

  private def getSysProp(key: String): String =
    Option(System.getProperty(key)) match {
      case Some(value) => value
      case None =>
        throw new IllegalStateException(
          s"$key system property not set! Your project should declare .enablePlugins(fauna.sbt.RunCoreConfigExportPlugin)"
        )
    }

  def waitForEndpoint(ip: String, port: Int = corePort): Boolean = {
    val deadline = System.currentTimeMillis() + coreStartupTimeout
    def timeLeft = (deadline - System.currentTimeMillis).toInt

    while (timeLeft > 0) {
      if (endpointListening(ip, timeLeft, port)) return true
      Thread.sleep(50)
    }
    false
  }

  def endpointListening(ip: String, timeout: Int, port: Int = corePort) = {
    val sock = new Socket()
    try {
      sock.connect(new InetSocketAddress(ip, port), timeout)
      sock.close()
      true
    } catch {
      case _: Throwable => false
    }
  }

  def adminCLI(i: Int, args: Iterable[String]): Int = {
    val pargs = List.newBuilder[String]

    pargs ++= Seq("java", "-server", "-cp", classpath)

    pargs ++= Seq(
      "fauna.tools.Admin",
      "-c",
      configFile(i).toString,
      "-k", // provide an explicit key to override shell env on dev machines
      FaunaDB.rootKey
    )

    pargs ++= args

    val pb = new JProcessBuilder(pargs.result(): _*)

    pb.redirectErrorStream(true)
    val p = pb.start()

    printProcessStream(p.getInputStream, s"admin_$i:out")
    printProcessStream(p.getErrorStream, s"admin_$i:err")

    p.waitFor()
    p.exitValue()
  }
}
