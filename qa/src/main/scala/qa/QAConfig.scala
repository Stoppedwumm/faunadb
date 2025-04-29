package fauna.qa

import com.typesafe.config.Config
import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.Timestamp
import fauna.prop.PropConfig
import fauna.qa.operator.Operation
import java.nio.file.Path
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

case class Host(dc: String, addr: String)

object Host {
  implicit val Codec = CBOR.RecordCodec[Host]
}

case class CoreNode(
  host: Host,
  port: Int,
  replica: String,
  isActive: Boolean,
  role: CoreNode.Role) {

  def dc = host.dc
  def addr = host.addr
}

object CoreNode {

  sealed abstract class Role(override val toString: String)

  object Role {
    case object Data extends Role("data")
    case object DataLog extends Role("data+log")
    case object Compute extends Role("compute")

    implicit val Codec = CBOR.SumCodec[Role](
      CBOR.SingletonCodec(Data),
      CBOR.SingletonCodec(DataLog),
      CBOR.SingletonCodec(Compute)
    )
  }

  implicit val Codec = CBOR.RecordCodec[CoreNode]
}

object Hosts {
  def apply(nodes: Vector[CoreNode]): Vector[Host] = nodes map { _.host }
  def apply(nodes: Set[CoreNode]): Set[Host] = nodes map { _.host }
}

/** A QASetup encapsulates how to set up a cluster for a particular OpGenerator, and what the initial
  * active-state of each of the hosts should be after `toInit` has been run.  In the typical case, `toInit` will simply
  * spin up all the cluster nodes, so all hosts will be active.  But, in certain kinds of ops, we may
  * only want a partially-initialized cluster.
  *
  * TODO: This is a particular instance of a broader QAState data structure, that ought to be plumbed
  * through the system as the system does its thing.  Conceptually I keep thinking of running ops as folding over
  * a data structure like this, and if we were doing something like that, then the active/inactive sets could
  * subsume the squelched boolean living on the workers, maybe?  Should `ToInit` be of something more akin to
  * `OpRunnerState`?
  *
  * To that end: I'm not necessarily wild about the naming.
  */
case class QASetup(toInit: Vector[Operation], afterwards: Vector[CoreNode])

class QAConfig(private[QAConfig] val cfg: Config) {
  val DefaultTest = "Simple"
  val DefaultOp = "InitOnly"

  def opt(path: String): Option[String] =
    opt(path, identity)

  def opt[T](path: String, f: String => T): Option[T] =
    if (cfg.hasPath(path)) Some(f(cfg.getString(path))) else None

  def getBoolean(path: String) = cfg.getBoolean(path)
  def getDouble(path: String) = cfg.getDouble(path)
  def getInt(path: String) = cfg.getInt(path)
  def getString(path: String) = cfg.getString(path)
  def getDuration(path: String) = QAConfig.parseDuration(getString(path))

  val testRunID = opt("fauna.test.id")

  val coreVersion = opt("fauna.db.version")

  val coreCluster = cfg.getStringList("fauna.db.hosts").asScala.toVector map { str =>
    val (dc, host, port) = str.split(":") match {
      case Array(dc, host, port) => (dc, host, port.toInt)
      case Array(host, port)     => ("NoDC", host, port.toInt)
      case str =>
        throw new IllegalArgumentException(s"Invalid fauna.db.hosts $str.")
    }
    CoreNode(Host(dc, host), port, dc, isActive = false, CoreNode.Role.DataLog)
  }

  val coreConfPath = Path.of(getString("fauna.db.conf-path"))

  val clusterName = opt("fauna.db.cluster-name") getOrElse "Unknown"

  val rootKey = getString("fauna.db.root-key")

  val timeout = getDuration("fauna.test.timeout")

  val testDuration = getDuration("fauna.test.duration")

  // the amount of time cluster initialization may take before
  // reaching "data-ready"
  val readyTimeout = getDuration("fauna.ready-timeout")

  private val RateRegex = """([0-9]+) per (\w+)""".r

  val perCustomerRate = opt("fauna.test.per-customer-rate") match {
    case Some(RateRegex(value, unit)) => Rate.Limited(value.toInt, unit)
    case _                            => Rate.Max
  }

  //TODO: See comment block above.
  val propConfig = opt("fauna.test.prop-seed", { p =>
    PropConfig(seed = p.toLong)
  }).getOrElse(PropConfig())

  val qaCluster = cfg.getStringList("fauna.cluster.hosts").asScala.toVector map {
    str =>
      str.split(":") match {
        case Array(dc, host) => Host(dc, host)
        case Array(host)     => Host("NoDC", host)
        case str =>
          throw new IllegalArgumentException(s"Invalid fauna.cluster.hosts $str.")
      }
  }

  val clients = getInt("fauna.cluster.clients")

  val testGenNames = (opt("fauna.test-generator") getOrElse DefaultTest).split("\\.").toSeq

  val testGenDriverLang = opt("fauna.test-generator-driver-lang")
  val testGenDriverPath = opt("fauna.test-generator-driver-path")

  val opGenName = opt("fauna.op-generator") getOrElse DefaultOp

  def clientIDsByHost: Vector[(Host, Range)] = {
    val clientsPerGen = math.ceil(clients.toDouble / testGenNames.length).toInt
    val nGroups = math.ceil(clientsPerGen.toDouble / qaCluster.size).toInt
    val idRanges = (0 until clientsPerGen).grouped(nGroups)
    qaCluster.zip(idRanges)
  }

  val remotePort = getInt("fauna.remote-port")

  val annotationTags = {
    val tags = Vector.newBuilder[(String, String)]
    tags += ("cluster" -> clusterName)
    tags += ("type" -> "qa")
    tags += ("env" -> "qa")
    coreVersion foreach { v =>
      tags += ("version" -> v)
    }
    testRunID foreach { i =>
      tags += ("run-id" -> i)
    }
    tags.result()
  }

  def timer: Timer = QAConfig.Timer
  def rand = propConfig.rand

  def dashboardURLs(start: Timestamp, end: Timestamp) = {
    val path = "fauna.datadog.dashboards"
    if (cfg.hasPath(path)) {
      cfg.getConfig(path).entrySet.asScala.toVector map { e =>
        val name = e.getKey
        val template = e.getValue.unwrapped.toString
        val startMS = start.millis
        val endMS = (end + 1.minute).millis
        val url = template.format(clusterName, startMS, endMS)
        (name, url)
      }
    } else {
      Vector.empty
    }
  }

  // Support for limiting queries by max per-second ops
  val enableQueryLimits =
    opt("fauna.db.enable-query-limits", _.toBoolean) getOrElse false
  val queryReadLimit =
    opt("fauna.db.read-ops-limit", _.toDouble) getOrElse 10000.0
  val queryWriteLimit =
    opt("fauna.db.write-ops-limit", _.toDouble) getOrElse 2500.0
  val queryComputeLimit =
    opt("fauna.db.compute-ops-limit", _.toDouble) getOrElse 10000.0

  // Set limits at the tenant level; default to 1% of max cluster limits
  val enableQueryLimitsPerTenant =
    opt("fauna.db.enable-query-limits-per-tenant", _.toBoolean) getOrElse false
  val queryReadLimitPerTenant =
    opt("fauna.db.read-ops-limit-per-tenant", _.toDouble) getOrElse (queryReadLimit / 100)
  val queryWriteLimitPerTenant =
    opt("fauna.db.write-ops-limit-per-tenant", _.toDouble) getOrElse (queryWriteLimit / 100)
  val queryComputeLimitPerTenant =
    opt("fauna.db.compute-ops-limit-per-tenant", _.toDouble) getOrElse (queryComputeLimit / 100)

  val httpSslKeyFile = opt("fauna.db.http-ssl-key-file")
  val useHttps = httpSslKeyFile isDefined
}

object QAConfig {
  val Timer = new Timer(10)

  def parseDuration(str: String): FiniteDuration = {
    val Array(length, unit) = str.split("\\.")
    FiniteDuration(length.toLong, unit)
  }

  def apply(config: Config): QAConfig =
    new QAConfig(config)

  implicit val Codec = CBOR.AliasCodec[QAConfig, Config](new QAConfig(_), _.cfg)
}
