package fauna.api.test

import fauna.codex.json._
import fauna.lang.syntax._
import fauna.net.http.BasicAuth
import fauna.net.http.HttpResponse
import fauna.prop.api._
import fauna.prop.Prop
import fauna.prop.PropConfig
import io.netty.util.AsciiString
import java.nio.charset.StandardCharsets
import org.scalactic.source.Position
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.control.NonFatal

final case class FQL(override val toString: String) extends AnyVal

object FQL {
  implicit def apply(str: String) = new FQL(str)
  implicit def apply(json: JSValue) = new FQL(json.toString)
  implicit def apply[A](prop: Prop[A])(implicit toFQL: A => FQL) = prop.map(toFQL)
}

final case class Key(override val toString: String) extends AnyVal

object Key {
  implicit def apply(db: Database) = new Key(db.adminKey)
  implicit def apply(str: String) = new Key(str)
}

final case class FQL2Params(
  arguments: JSObject = JSObject(),
  typecheck: Option[Boolean] = Some(true),
  // API specs use simple format (default for direct calls)
  format: Option[String] = None,
  // any extra headers to add. These will overwrite any other headers set from other
  // parameters.
  headers: Seq[(String, String)] = Seq.empty,
  performanceHints: Option[Boolean] = None
) {
  def allHeaders: Seq[(AsciiString, String)] = {
    val out = Seq.newBuilder[(AsciiString, String)]
    out.addAll(headers.map { case (k, v) => new AsciiString(k) -> v })
    typecheck.foreach { typecheck =>
      out += new AsciiString("x-typecheck") -> typecheck.toString
    }
    format.foreach { format =>
      out += new AsciiString("x-format") -> format
    }
    performanceHints.foreach { phs =>
      out += new AsciiString("x-performance-hints") -> phs.toString
    }
    out.result()
  }
  def body(query: FQL): JSValue =
    JSObject("query" -> JSString(query.toString), "arguments" -> arguments)
}

trait FQL2Helpers extends HttpSpecMatchers with Matchers with HttpResponseHelpers {
  // The client used by default.
  def client: FaunaDB.Client

  def queryOk(
    q: FQL,
    key: Key,
    params: FQL2Params = FQL2Params(),
    client: FaunaDB.Client = client
  )(implicit pos: Position): JSValue = {
    val fut = queryRaw(q, key, params)
    if (fut.statusCode != 200) {
      val resp = fut.json
      fail(
        s"expected successful query failed: \n ${(resp / "error")} \n ${(resp / "summary").asOpt[String]}"
      )
    }
    fut.json / "data"
  }
  def queryErr(
    q: FQL,
    key: Key,
    params: FQL2Params = FQL2Params(),
    client: FaunaDB.Client = client
  )(implicit pos: Position): JSValue = {
    val fut = queryRaw(q, key, params)
    fut should respond(400)
    fut.json
  }

  def queryRaw(
    q: FQL,
    key: Key,
    params: FQL2Params = FQL2Params(),
    client: FaunaDB.Client = client)(
    implicit pos: Position): Future[HttpResponse] = {
    val res =
      client.api.post(
        "/query/1",
        params.body(q),
        key.toString,
        headers = params.allHeaders)
    try {
      res
    } catch {
      // FIXME: use specific exception
      case _: Throwable =>
        fail(
          s"FQL query `query` unexpectedly failed. status : ${res.statusCode}, body : ${res.body
              .toString(StandardCharsets.UTF_8)}")
    }
  }

  def legacyQuery(
    q: JSValue,
    db: Database,
    client: FaunaDB.Client = client
  ): JSValue = {
    val res = client.api.query(q, BasicAuth(db.adminKey))
    res should respond(200, 201)
    res.json / "resource"
  }
}

abstract class FQL2APISpec
    extends AbstractHTTPSpec
    with JSGenerators
    with FQL2Helpers
    with HttpResponseHelpers {

  val RootDB =
    Database("__root__", "secret:server", "secret:client", "secret:admin", "0")

  // FIXME: this should not be required
  def apiVers = "UNSTABLE"

  val aUniqueDBName = for {
    name <- aUniqueName
    if queryOk(s"!Database.byName('db$name').exists()", RootDB).as[Boolean]
  } yield s"db$name"

  val aDatabase: Prop[Database] = aDatabase(typechecked = false, pprotected = false)

  val aTypecheckedDatabase: Prop[Database] =
    aDatabase(typechecked = true, pprotected = false)

  val aProtectedDatabase: Prop[Database] =
    aDatabase(typechecked = false, pprotected = true)

  def aDatabase(typechecked: Boolean, pprotected: Boolean): Prop[Database] =
    aUniqueDBName.map { name =>
      val res = queryOk(
        s"""|let db = Database.create({
            |  name: "$name",
            |  typechecked: $typechecked,
            |  protected: $pprotected
            |})
            |
            |{
            |  server: Key.create({ role: "server", database: "$name" }),
            |  client: Key.create({ role: "client", database: "$name" }),
            |  admin: Key.create({ role: "admin", database: "$name" }),
            |  global_id: db.global_id,
            |}
            |""".stripMargin,
        RootDB
      )

      Database(
        name,
        (res / "server" / "secret").as[String],
        (res / "client" / "secret").as[String],
        (res / "admin" / "secret").as[String],
        (res / "global_id").as[String]
      )
    }

  def anAccountDatabase: Prop[Database] =
    for {
      name <- aUniqueDBName
      id   <- Prop.long(1_000_000)
    } yield {
      val res = queryOk(
        s"""|let db = Database.create({
            |  name: "$name",
            |  account: { id: $id }
            |})
            |
            |{
            |  server: Key.create({ role: "server", database: "$name" }),
            |  client: Key.create({ role: "client", database: "$name" }),
            |  admin: Key.create({ role: "admin", database: "$name" }),
            |  global_id: db.global_id,
            |}
            |""".stripMargin,
        RootDB,
        FQL2Params(typecheck = Some(false))
      )

      Database(
        name,
        (res / "server" / "secret").as[String],
        (res / "client" / "secret").as[String],
        (res / "admin" / "secret").as[String],
        (res / "global_id").as[String]
      )
    }

  def aFunc(
    db: Database,
    body: String = "x => x",
    role: String = null): Prop[String] = {
    for {
      name <- aUniqueIdentifier
    } yield {
      queryOk(
        s"""|Function.create({
            | name: "$name",
            | body: "$body",
            | role: ${Option(role) getOrElse "null"}
            |})""".stripMargin,
        db
      )

      name
    }
  }

  def mkCollection(db: Database, name: String, indexes: Seq[FQL] = Seq.empty)(
    implicit pos: Position) =
    try {
      queryOk(
        s"""|Collection.create({
            |  name: "$name",
            |  indexes: {${indexes.mkString(", ")}}
            |})
            |""".stripMargin,
        db
      )
    } catch {
      case NonFatal(err) =>
        fail(s"Failed to create collection `$name`: ${err.getMessage}", err)
    }

  // Makes an FQL10 collection that supports long-ago historical queries, as used
  // in some tests.
  def mkHistoryCollection(db: Database, name: String)(implicit pos: Position) =
    try {
      queryOk(
        s"""|Collection.create({
            |  name: "$name",
            |  history_days: 1000000000
            |})
            |""".stripMargin,
        db
      )
    } catch {
      case NonFatal(err) =>
        fail(s"Failed to create collection `$name`: ${err.getMessage}", err)
    }

  def aCollection(db: Database, indexes: Prop[FQL]*)(
    implicit pos: Position): Prop[String] =
    aCollection(db, indexes.sequence)

  def aCollection(db: Database, indexes: Prop[Seq[FQL]])(
    implicit pos: Position): Prop[String] =
    for {
      name <- aUniqueIdentifier
      idxs <- indexes
    } yield {
      mkCollection(db, name, idxs)
      name
    }

  def aHistoryCollection(db: Database)(implicit pos: Position) =
    for {
      name <- aUniqueIdentifier
    } yield {
      mkHistoryCollection(db, name)
      name
    }

  def anIndex(
    name: Prop[FQL] = aUniqueIdentifier,
    terms: Prop[Seq[FQL]] = Prop.const(Seq.empty),
    values: Prop[Seq[FQL]] = Prop.const(Seq.empty)
  ): Prop[FQL] = {
    def toFields(values: Seq[FQL]): String =
      values.map { v => s"{ field: '$v' }" }.mkString(", ")

    for {
      idxName   <- name
      idxTerms  <- terms
      idxValues <- values
    } yield {
      s"""|$idxName: {
          |  terms: [${toFields(idxTerms)}],
          |  values: [${toFields(idxValues)}]
          |}""".stripMargin
    }
  }

  def updateSchemaOk(db: Database, files: (String, String)*)(
    implicit pos: Position) = {
    val res = api.upload("/schema/1/update", files, db.adminKey)
    res should respond(OK)
  }

  def updateSchemaOk(secret: String, files: (String, String)*)(
    implicit pos: Position) = {
    val res = api.upload("/schema/1/update", files, secret)
    res should respond(OK)
  }

  case class User(id: String, password: String, token: String, database: Database)

  case class Membership private (resource: String, predicate: Option[String])

  object Membership {
    def apply(resource: String, predicate: String = null): Membership =
      new Membership(resource, Option(predicate))
  }

  case class Privilege(
    resource: String,
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

    def open(resource: String): Privilege = {
      Privilege(
        resource,
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

  case class RoleAction private (config: String)

  object RoleAction {
    val Granted = new RoleAction("true")
    val Denied = new RoleAction("false")

    def apply(predicate: String): RoleAction =
      new RoleAction(s"""|<<+END
            |$predicate
            |END""".stripMargin)
  }

  def aUserOfCollection(db: Database, coll: String): Prop[User] = {
    for {
      user <- aDocument(db, coll)
      pass <- Prop.string(1, 40, jsChars)
    } yield {
      val token = queryOk(
        s"""|let cred = Credentials.create({
            |  document: $coll.byId('$user')!,
            |  password: '$pass'
            |})
            |
            |cred.login('$pass')""".stripMargin,
        db
      )

      val secret = (token / "secret").as[String]

      User(user, pass, secret, db)
    }
  }

  def aKey(db: Database, role: Prop[String]) = {
    for {
      r <- role
    } yield {
      queryOk(s"Key.create({role: '$r'})", db)
    }
  }

  def aKeySecret(db: Database, role: String) =
    for {
      key <- aKey(db, Prop.const(role))
    } yield {
      (key / "secret").as[String]
    }

  def aRole(
    db: Database,
    memberships: Seq[Membership] = Seq.empty,
    privileges: Seq[Privilege] = Seq.empty) = {
    for {
      aRole <- aUniqueIdentifier
    } yield {
      val membershipStr = memberships map { membership =>
        val predicate = membership.predicate match {
          case Some(body) =>
            s"""|<<+END
                |$body
                |END""".stripMargin

          case None =>
            "null"
        }

        s"""|{
            |  resource: \"${membership.resource}\",
            |  predicate: $predicate
            |}""".stripMargin
      }

      val privilegesStr = privileges map { privilege =>
        s"""|{
            |  resource: \"${privilege.resource}\",
            |  actions: {
            |    read: ${privilege.read.config},
            |    write: ${privilege.write.config},
            |    create: ${privilege.create.config},
            |    create_with_id: ${privilege.createWithID.config},
            |    delete: ${privilege.delete.config},
            |    history_write: ${privilege.historyWrite.config},
            |    history_read: ${privilege.historyRead.config},
            |    unrestricted_read: ${privilege.unrestrictedRead.config},
            |    call: ${privilege.call.config}
            |  }
            |}""".stripMargin
      }

      queryOk(
        s"""|Role.create({
            |  name: "$aRole",
            |  membership: ${membershipStr.mkString("[", ",", "]")},
            |  privileges: ${privilegesStr.mkString("[", ",", "]")}
            |})""".stripMargin,
        db
      )

      aRole
    }
  }

  // FIXME: change this generator once we support non-ascii on literals
  override def jsChars = Prop.choose('a' to 'z')

  val ADocumentBatchSize = 4000

  def aDocument(
    db: Database,
    coll: String,
    data: Prop[FQL] = jsObject
  )(implicit pos: Position): Prop[String] = {
    new Prop[String] {
      def run(conf: PropConfig) =
        data.run(conf) map { fields =>
          queryOk(s"$coll.create($fields).id", db).as[String]
        }

      override def times(n: Int) =
        if (n == 0) {
          Prop.const(Nil)
        } else {
          new Prop[Seq[String]] {
            def run(conf: PropConfig) =
              data.times(n).run(conf) map { fields =>
                fields
                  .grouped(ADocumentBatchSize)
                  .flatMap { batch =>
                    val arr = batch.mkString("[", ",", "]")
                    queryOk(
                      s"$arr.fold([], (a, d) => a.append($coll.create(d).id))",
                      db
                    ).as[Seq[String]]
                  }
                  .toSeq
              }
          }
        }
    }
  }
}
