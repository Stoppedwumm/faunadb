package fauna.qa.main

import com.typesafe.config.ConfigFactory
import fauna.config.Loader
import fauna.lang.syntax._
import fauna.qa._
import fauna.qa.net._
import java.util.concurrent.atomic.AtomicReference

import fauna.qa.operator.Cmd

import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.{ Process, ProcessLogger }
import scala.util.{ Success, Try }

/**
  * The following network helper classes provide a nice class name for
  * logging as well as simpler creation due to not needing to provide
  * types for Req and Rep.
  */
class OperatorServer(port: Int, handler: OperatorReq => Future[OperatorRep])
    extends Server[OperatorReq, OperatorRep](port, handler)

class OperatorClient(host: Host, port: Int)
    extends Client[OperatorReq, OperatorRep](host, port)

case class OperatorClientGroup(clients: Vector[Client[OperatorReq, OperatorRep]])
    extends ClientGroup[OperatorReq, OperatorRep] {

  def limited(clients: Vector[Client[OperatorReq, OperatorRep]]) =
    copy(clients = clients)
}

object OperatorClientGroup {

  def apply(hosts: Vector[Host], port: Int): OperatorClientGroup =
    apply(hosts map { new OperatorClient(_, port) })
}

/**
  * The main Worker object started by a script on each core node machine.
  */
object Operator {

  @annotation.nowarn("cat=unused-params")
  def main(argv: Array[String]): Unit = {
    val config = QAConfig(ConfigFactory.load())

    val port = config.remotePort
    val worker = new Operator(port, config)

    getLogger().info(s"Starting Operator on $port")
    Await.result(worker.run(), Duration.Inf)
  }
}

/**
  * Operators receive and execute `Cmd`s on core nodes.
  */
class Operator(port: Int, config: QAConfig) {
  private[this] val runningP = Promise[Unit]()
  private[this] val log = getLogger()

  private[this] val server = new OperatorServer(
    port,
    {
      case msg: OperatorReq.RunCmd  => runCmd(msg)
      case OperatorReq.CreateConfig => createConfig()
      case OperatorReq.Reset        => reset()
    })

  implicit private val ec = ExecutionContext.global

  // Maintain a set of running processes so we can stop them when needed.
  private[this] val runningProcs = new AtomicReference[Set[Process]](Set.empty)

  val coreConfig: Map[String, Any] = Map(
    // TODO: Find a way to install a keystore and use DC level encryption
    "peer_encryption_level" -> "none",
    "auth_root_key" -> config.getString("fauna.db.root-key"),
    "storage_data_path" -> config.getString("fauna.db.data-path"),
    "log_path" -> config.getString("fauna.db.log-path"),
    "log_trace" -> config.getBoolean("fauna.db.log-trace"),
    "log_level" -> config.getString("fauna.db.log-level"),
    "log_rotate_count" -> 10,
    "network_admin_http_address" -> "0.0.0.0",
    "network_broadcast_address" -> config.getString("fauna.db.broadcast-addr"),
    "consensus_stall_restart_period_enable" -> config.opt("fauna.db.consensus-stall-restart-enable", _.toBoolean).getOrElse(false),
    "cluster_name" -> config.getString("fauna.db.cluster-name"),
    "cluster_region_group" -> "qa",
    "cluster_environment" -> "qa",
    "network_coordinator_http_address" -> "0.0.0.0",
    "network_console_port" -> 7777,
    "network_host_id" -> config.getString("fauna.db.network-id"),
    "network_listen_address" -> config.getString("fauna.db.listen-addr"),
    "network_round_trip_time_ms" -> config.getInt("fauna.db.round-trip-time"),
    "dogstatsd_host" -> "localhost",
    "dogstatsd_port" -> 8127,
    "trace_apm" -> "datadog",
    "trace_probability" -> config.getDouble("fauna.db.trace-probability"),
    "storage_enable_qos" -> config.getBoolean("fauna.db.enable-qos"),
    "storage_concurrent_reads" -> config.getInt("fauna.db.concurrent-reads"),
    "flags_path" -> "/etc/feature-flag-periodic.d/feature-flags.json",
    "health_check_timeout_ms" -> config.getInt("fauna.db.health-check-timeout")
  )

  val httpSslConfig: Map[String, Any] = config.httpSslKeyFile match {
    case Some(value) => Map("http_ssl_key_file" -> value)
    case None        => Map.empty[String, Any]
  }

  val queryLimitsConfig: Map[String, Any] = Map(
    "query_ops_rate_limits_enabled" -> true,
    "query_max_read_ops_per_second" -> config.queryReadLimit,
    "query_max_write_ops_per_second" -> config.queryWriteLimit,
    "query_max_compute_ops_per_second" -> config.queryComputeLimit
  )

  val finalConfig: Map[String, Any] =
    coreConfig ++ httpSslConfig ++ (config.enableQueryLimits match {
      case true  => queryLimitsConfig
      case false => Map.empty[String, Any]
    })

  def reset(): Future[OperatorRep] =
    Future {
      blocking {
        val legacy = runningProcs.getAndSet(Set.empty)
        log.info(s"Resetting. Destroying ${legacy.size} running processes")
        legacy foreach { _.destroy() }
        OperatorRep.Ready
      }
    }

  def runCmd(msg: OperatorReq.RunCmd): Future[OperatorRep] = {
    val cmd = msg.cmd
    log.info(s"Executing[$cmd]: ${cmd.shellString}")

    val procLog = new OpProcessLogger(cmd)

    val proc = cmd.run(procLog)
    runningProcs.accumulateAndGet(
      Set(proc),
      { (c, g) =>
        c ++ g
      })

    Future {
      blocking {
        val succeeded = Try(proc.exitValue()) match {
          case Success(0) => true
          case ret =>
            log.warn(s"runCmd failed: $ret")
            false
        }

        runningProcs.accumulateAndGet(
          Set(proc),
          { (c, g) =>
            c -- g
          })

        if (succeeded) {
          OperatorRep.Success(procLog.linesOut, procLog.linesErr)
        } else {
          OperatorRep.Failure(procLog.linesOut, procLog.linesErr)
        }
      }
    }
  }

  def createConfig(): Future[OperatorRep] =
    Future {
      val path = config.coreConfPath
      Loader.serialize(path, finalConfig) match {
        case Left(err) =>
          log.error(s"Could not create config at $path:\n$err")
          OperatorRep.Failure(Vector.empty, Vector.empty)
        case Right(_) =>
          log.info(s"Created config at $path")
          OperatorRep.Success(Vector.empty, Vector.empty)
      }
    }

  def run(): Future[Unit] = {
    server.start()
    runningP.future
  }

  def close(): Unit = {
    server.stop()
    runningP.setDone()
  }

  /**
    * Process logger that preserves standard- and error-output.
    */
  private class OpProcessLogger(
    cmd: Cmd,
    maxLinesOut: Int = 1024,
    maxLinesErr: Int = 1024)
      extends ProcessLogger {

    private var _outLines = Vector.empty[String]

    private var _errLines = Vector.empty[String]

    private var _outTruncated = false

    private var _errTruncated = false

    def linesOut = _outLines

    def linesErr = _errLines

    def isOutTruncated = _outTruncated

    def isErrTruncated = _errTruncated

    def out(str: => String): Unit = {
      log.info(s"STDOUT[$cmd]: $str")
      if (_outLines.size < maxLinesOut) {
        _outLines :+= str
      } else {
        _outTruncated = true
      }
    }

    def err(str: => String): Unit = {
      log.info(s"STDERR[$cmd]: $str")
      if (_errLines.size < maxLinesErr) {
        _errLines :+= str
      } else {
        _errTruncated = true
      }
    }

    def buffer[T](f: => T): T = f
  }
}
