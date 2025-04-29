package fauna.qa.main

import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.qa._
import java.io.File
import java.lang.{ Process, ProcessBuilder }
import java.lang.ProcessBuilder.Redirect
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.Executors
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Failure

/** DriverCustomer extends the Customer trait and offers support for
  * running load through external processes - primarily this if for v10
  * drivers, but could be used for the v4 drivers or any other external
  * process like the Fauna CLI.
  *
  * This class doesn't store state for the driver queries; that needs to be
  * handled by the test code that is being run. For info on that, see
  * core/qa/DriverTestGenerators/README.md
  *
  * The test process stdout and stderr will be redirected to log files in the
  * same location as qa.log (/var/log/faunadb) with the pattern:
  *
  * {TestGenName}_{HostAddress}_{TimestampMicros}.log
  *
  * These log files are archived along with qa.log and qa.conf to the QA S3 bucket.
  */

class DriverCustomer(
  node: CoreNode,
  config: QAConfig,
  testGenName: String
) extends Customer {
  private implicit val ec = ExecutionContext.parasitic
  private[this] val log = getLogger()

  private val defaultDriverLanguage = "js"
  private val defaultDriverPath = "/usr/share/qa/DriverTestGenerators"

  private val procs = Seq.newBuilder[Process]
  private val unique = Timestamp(Instant.now()).micros.toString()
  private val logPath = Path
    .of(System.getProperty("logPath", "/var/log/faunadb/qa.log"))
    .getParent()
    .toString()
  private val logFileName = f"${testGenName}_${host.addr}_${unique}.log"
  private val logFile = new File(Path.of(logPath, logFileName).toString())

  // Create fixed thread pool of 1 for the long-running test task
  private val singleThreadContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

  @volatile private var closing = false

  def host = node.host

  def run(testGenerator: TestGenerator, schema: Schema.DB): Future[Unit] = {
    closing = false

    val secret = schema.adminKey.getOrElse("secret")
    val envVars = Map(
      "FAUNA_QAHOST" -> host.addr,
      "FAUNA_ENDPOINT" -> f"http://${host.addr}:8443",
      "FAUNA_SECRET" -> secret
    )

    runShellCommand("./run.sh", envVars, true)(singleThreadContext)
  }

  def runRequests(reqs: RequestIterator): Future[Unit] =
    throw new UnsupportedOperationException(
      "DriverCustomer doesn't support running requests directly.")

  def pause(): Unit = {
    log.info(f"Attempting to squelch host ${host.addr}")
    runShellCommand(f"touch ${host.addr}.pause") onComplete {
      case Failure(err) => log.error(f"Unable to squelch host ${host.addr}; $err")
      case _          => ()
    }
  }

  def continue(): Unit = {
    log.info(f"Attempting to un-squelch host ${host.addr}")
    runShellCommand(f"rm -f ${host.addr}.pause") onComplete {
      case Failure(err) => log.error(s"Unable to un-squelch host ${host.addr}; $err")
      case _          => ()
    }
  }

  def close(): Unit = {
    closing = true

    procs.result() foreach {
      case p if p.isAlive() =>
        // Kill the process group with the hyphen before "-$pid"
        val pid = p.pid()
        runShellCommand(s"kill -9 -- -$pid")
      case _                => ()
    }

    runShellCommand("rm -f *.pause") onComplete {
      case Failure(err) => log.error(s"Unable to clean .pause files; $err")
      case _          => ()
    }
  }

  def runValidationQuery(
    testGen: TestGenerator with ValidatingTestGenerator,
    schema: Schema.DB,
    minSnapTime: Timestamp,
    timeout: Duration
  ): Future[String] =
    throw new UnsupportedOperationException(
      "DriverCustomer doesn't support ValidatingTestGenerators.")

  private[this] def runShellCommand(
    cmd: String,
    envVars: Map[String, String] = Map.empty,
    shouldLog: Boolean = false
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val cwd = Path.of(
      config.testGenDriverPath getOrElse defaultDriverPath,
      testGenName,
      config.testGenDriverLang getOrElse defaultDriverLanguage)

    // Use 'setsid' to create a new process group
    val pb = new ProcessBuilder("setsid", "bash", "-c", cmd)
    pb.directory(cwd.toFile())
    envVars foreach { case (k, v) => pb.environment().put(k, v) }

    if (shouldLog) {
      pb.redirectOutput(Redirect.appendTo(logFile))
      pb.redirectError(Redirect.appendTo(logFile))
    }

    val proc = pb.start()
    procs += proc
    wrapProcessInFuture(proc, shouldLog)(ec)
  }

  private def wrapProcessInFuture(
    proc: Process,
    shouldLog: Boolean
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      val cmd = proc.info().commandLine().orElse("unknown")
      val exitCode = proc.waitFor()

      if (shouldLog) {
        exitCode match {
          case 0 =>
            log.info(
              s"Command '${cmd}' exited with exit code 0; log file: ${logFileName}")
          case 137 if closing =>
            log.warn(
              s"Command '${cmd}' terminated with SIGKILL by Worker; log file: ${logFileName}")
          case 137 =>
            val message =
              s"Command '${cmd}' unexpectedly terminated with SIGKILL; log file: ${logFileName}"
            log.error(message)
            throw new IllegalStateException(message)
          case _ =>
            val message =
              s"**FATAL** Command '${cmd}' failed with non-zero exit code (${exitCode}); log file: ${logFileName}"
            log.error(message)
            throw new IllegalStateException(message)
        }
      }
    }
}
