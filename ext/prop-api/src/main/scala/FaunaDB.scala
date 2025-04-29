package fauna.prop.api

import fauna.codex.json._
import fauna.limits.AccountLimits
import fauna.net.http._
import fauna.net.security.SSLConfig
import fauna.prop.api.DefaultQueryHelpers._
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.util.AsciiString
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.StringOps
import scala.concurrent.duration.FiniteDuration

object FaunaDB extends APIResponseHelpers {
  // These key-values should always be matched by ones in
  // RunCorePlugin.checkLocalCore,
  // and be consistent with values in CoreLauncher.
  private val apiPort = getEnv("FAUNADB_PORT", "8443")
  private val adminPort = getEnv("FAUNADB_ADMIN_PORT", "8444")
  val rootKey = getEnv("FAUNA_ROOT_KEY", "secret")

  type Headers = Seq[(AsciiString, AnyRef)]

  case class ClientConfig(
    version: String,
    host: String,
    ssl: Option[SSLConfig],
    headers: Headers, // Headers included in API requests. Not included in admin requests.
    timeout: FiniteDuration) // Admin request timeout.

  class Client(val cfg: ClientConfig) {
    val api = Http2Client(
      cfg.host,
      apiPort.toInt,
      headers = Seq(HTTPHeaders.FaunaDBAPIVersion -> cfg.version) ++ cfg.headers,
      ssl = cfg.ssl,
      enableEndpointIdentification = false
    )
    val admin = Http2Client(
      cfg.host,
      adminPort.toInt,
      headers = Seq(HTTPHeaders.FaunaDBAPIVersion -> cfg.version),
      ssl = cfg.ssl,
      responseTimeout = cfg.timeout,
      enableEndpointIdentification = false
    )

    def withVersion(version: String): Client =
      client(version, cfg.host, cfg.ssl, cfg.headers, cfg.timeout)

    def withHeaders(headers: Headers): Client =
      client(cfg.version, cfg.host, cfg.ssl, cfg.headers ++ headers, cfg.timeout)
  }

  val clients = new ConcurrentHashMap[ClientConfig, Client]()

  def client(
    version: String,
    host: String,
    ssl: Option[SSLConfig] = None,
    headers: Headers = Seq.empty,
    responseTimeout: FiniteDuration = HttpClient.DefaultResponseTimeout): Client =
    clients.computeIfAbsent(
      ClientConfig(version, host, ssl, headers, responseTimeout),
      new Client(_))

  def sourcePaths =
    new StringOps(
      Option(System.getProperty("source.config")).getOrElse(
        "")) split File.pathSeparatorChar toSeq

  def makeDB(
    name: String,
    api: HttpClient,
    apiVersion: String,
    adminKey: String = rootKey,
    container: Boolean = false,
    accountID: Option[Long] = None,
    limits: Option[AccountLimits] = None
  ): Database = {
    var db = api.get(s"/databases/$name", rootKey)

    if (db.statusCode == 404) {
      val account = accountID.fold(JSNull: JSValue) { JS(_) }
      val rateLimits = limits.fold(JSNull: JSValue) { lims =>
        val json = lims.toJSON

        MkObject(
          "read_ops" -> MkObject((json / "read_ops").as[JSObject].value: _*),
          "write_ops" -> MkObject((json / "write_ops").as[JSObject].value: _*),
          "compute_ops" -> MkObject((json / "compute_ops").as[JSObject].value: _*)
        )
      }

      db = api.query(
        CreateDatabase(
          MkObject(
            "name" -> name,
            "api_version" -> apiVersion,
            "container" -> container,
            "account" -> MkObject(
              "id" -> account,
              "limits" -> rateLimits
            )
          )
        ),
        adminKey
      )
    }

    if (db.statusCode == 200 || db.statusCode == 201) {
      val ref = Ref(s"databases/$name")
      val keys = api.query(
        MkObject(
          "server" -> CreateKey(MkObject("role" -> "server", "database" -> ref)),
          "client" -> CreateKey(MkObject("role" -> "client", "database" -> ref)),
          "admin" -> CreateKey(MkObject("role" -> "admin", "database" -> ref))
        ),
        adminKey
      )

      if (keys.statusCode == 200) {
        val sk = (keys.resource / "server" / "secret").as[String]
        val ck = (keys.resource / "client" / "secret").as[String]
        val ak = (keys.resource / "admin" / "secret").as[String]
        Database(db.resource, sk, ck, ak)
      } else {
        sys.error(s"Could not create keys:\n${keys.json}")
      }
    } else {
      System.err.println(s"Could not create DB:\n${db.json}")
      if (db.statusCode == 503) {
        throw ReadTimeoutException.INSTANCE
      } else {
        sys.error(s"Could not create DB:\n${db.json}")
      }
    }
  }

  private def getEnv(key: String, default: String) =
    Option(System.getenv(key)) getOrElse default
}

object Database {
  def apply(
    name: String,
    key: String,
    clientKey: String,
    adminKey: String,
    globalID: String): Database = {
    val ref = JSObject("id" -> name, "@ref" -> JSObject("id" -> "databases"))

    Database(
      JSObject("name" -> name, "@ref" -> ref, "global_id" -> globalID),
      key,
      clientKey,
      adminKey
    )
  }
}

case class Database(
  resource: JSObject,
  key: String,
  clientKey: String,
  adminKey: String
) extends APIResponseHelpers {

  def ts = resource.ts
  def ref = resource.ref
  def refObj = resource.refObj
  def path = resource.path
  def name = (resource / "name").as[String]
  def apiVersion = (resource / "api_version").as[String]
  def globalID = (resource / "global_id").as[String]
  def priority = (resource / "priority").as[Int]
}

case class Key(resource: JSObject) extends APIResponseHelpers {

  def ts = resource.ts
  def ref = resource.ref
  def refObj = resource.refObj
  def secret = (resource / "secret").as[String]
  def role = (resource / "role").as[String]
}

case class JWTToken(resource: JSObject) extends APIResponseHelpers {

  def token = (resource / "token").as[String]

}

case class Collection(resource: JSObject, database: Database)
    extends APIResponseHelpers {

  def ts = resource.ts
  def ref = resource.ref
  def refObj = resource.refObj
  def path = resource.path
  def name = (resource / "name").as[String]
}

case class Index(resource: JSObject, sourceClass: Collection)
    extends APIResponseHelpers {

  def ts = resource.ts
  def ref = resource.ref
  def refObj = resource.refObj
  def name = (resource / "name").as[String]
  def active = (resource / "active").as[Boolean]
  def sourceRef = (resource / "source" / "@ref").as[String]

  def terms = (resource / "terms").orElse(Seq.empty[JSValue]) map { o =>
    (o / "field").as[JSArray]
  }

  def values = (resource / "values").orElse(Seq.empty[JSValue]) map { o =>
    (o / "field").as[JSArray]
  }

  def partitions = (resource / "partitions").as[Long]
}

case class User(
  collection: Collection,
  resource: JSObject,
  password: String,
  email: String,
  tokenRes: JSObject,
  credential: JSObject,
  database: Database
) extends APIResponseHelpers {

  def ts = resource.ts
  def ref = resource.ref
  def refObj = resource.refObj
  def path = resource.path
  def id = resource.id

  def token: String = (tokenRes / "secret").as[String]
}

case class UserFunction(resource: JSObject, name: String)
    extends APIResponseHelpers {
  def refObj = resource.refObj
}

case class Role(resource: JSObject, database: Database, name: String)
    extends APIResponseHelpers {
  def refObj = resource.refObj
}

case class Membership private (resource: Collection, predicate: Option[JSValue])

object Membership {

  def apply(resource: Collection, predicate: JSValue = null): Membership =
    new Membership(resource, Option(predicate) map { QueryF(_) })
}

case class Privilege(
  resource: JSValue,
  read: RoleAction = RoleAction.Denied,
  write: RoleAction = RoleAction.Denied,
  create: RoleAction = RoleAction.Denied,
  createWithID: RoleAction = RoleAction.Denied,
  delete: RoleAction = RoleAction.Denied,
  historyRead: RoleAction = RoleAction.Denied,
  historyWrite: RoleAction = RoleAction.Denied,
  unrestrictedRead: RoleAction = RoleAction.Denied,
  call: RoleAction = RoleAction.Denied
)

object Privilege {

  def open(value: JSValue): Privilege = {
    Privilege(
      value,
      read = RoleAction.Granted,
      write = RoleAction.Granted,
      create = RoleAction.Granted,
      createWithID = RoleAction.Granted,
      delete = RoleAction.Granted,
      historyRead = RoleAction.Granted,
      historyWrite = RoleAction.Granted,
      unrestrictedRead = RoleAction.Granted,
      call = RoleAction.Granted
    )
  }
}

case class RoleAction private (config: JSValue)

object RoleAction {

  val Granted = RoleAction(true)
  val Denied = RoleAction(false)

  def apply(lambda: JSObject): RoleAction =
    new RoleAction(QueryF(lambda))

  def apply(fqlx: String): RoleAction =
    new RoleAction(JSString(fqlx))
}

case class AccessProviderRole private (role: Role, predicate: Option[JSValue])

object AccessProviderRole {

  def apply(role: Role, predicate: JSValue = null): AccessProviderRole =
    AccessProviderRole(role, Option(predicate) map { QueryF(_) })
}

case class AccessProvider(
  resource: JSObject,
  name: String,
  db: Database
) extends APIResponseHelpers {
  def refObj = resource.refObj
  def audience = (resource / "audience").as[String]
}
