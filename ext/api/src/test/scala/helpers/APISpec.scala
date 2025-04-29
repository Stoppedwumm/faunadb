package fauna.api.test

import fauna.codex.json._
import fauna.flags.test.FlagProps
import fauna.net.http.{ AuthType, BasicAuth, BearerAuth, HttpResponse }
import fauna.net.security.JWT
import fauna.net.util.URIEncoding
import fauna.prop.api._
import fauna.prop.Prop
import fauna.repo._
import org.scalactic.source.Position
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.control.NonFatal

abstract class AbstractHTTPSpec
    extends PropSpec
    with HttpResponseHelpers
    with Matchers
    with HttpSpecMatchers
    with Eventually
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  def apiVers: String

  var enableCoreStdOut = false

  val rootKey = FaunaDB.rootKey

  // Enable feature flags for API tests for the root account.
  val flags = FeatureFlags(
    version = 1,
    Vector(
      FlagProps(
        "account_id",
        0,
        Map(
          "enable_data_exporter" -> true
        )
      ))
  )

  private[this] var id: Int = _
  private[this] var _client: FaunaDB.Client = _
  def client = _client
  def clusterInstanceID: Int = id

  def api = client.api
  def admin = client.admin

  override protected def beforeAll() = {
    val CoreLauncher.IDAndClient(i, cl) =
      CoreLauncher.launchOneNodeCluster(
        apiVers,
        withStdOutErr = enableCoreStdOut,
        flags = Some(flags))
    id = i
    _client = cl

    eventually(timeout(scaled(15.seconds)), interval(1.seconds)) {
      cl.api.get("/ping?scope=write", rootKey) should respond(OK)
    }
  }

  override protected def afterAll() = CoreLauncher.delete(id)

  // Test defintion DSL

  // during registration

  var _path: Option[String] = None

  // endpoint registration

  def withPath(path: String)(f: => Unit) = {
    _path = Some(path)

    try {
      f
    } finally {
      _path = None
    }
  }

  protected class PathBuilder(path: Seq[String]) {
    def /(body: => Unit) = withPath(pathString)(body)
    def -(body: => Unit) = withPath(pathString)(body)

    private def pathString = path.mkString("", "/", "")
  }

  implicit def stringToPathBuilder(str: String) = new PathBuilder(Seq(str))

  // test registration

  override def test(name: String, testTags: Tag*)(f: => Any)(
    implicit pos: Position) =
    super.test((_path.toSeq :+ name) mkString ": " trim, testTags: _*)(f)(pos)
}

trait TaskHelpers { self: AbstractHTTPSpec =>

  def waitForTaskExecution(): Unit =
    try {
      val tl = admin.get("/admin/tasks/list", rootKey)
      tl should respond(OK)

      if ((tl.json / "tasks").as[Seq[JSValue]].nonEmpty) {
        Thread.sleep(1000)
        waitForTaskExecution()
      }
    } catch {
      case NonFatal(e) =>
        sys.error(s"Error waiting for tasks: ${e.getMessage}")
    }
}

abstract class FQL1APISpec
    extends AbstractHTTPSpec
    with APIGenerators
    with APISpecMatchers
    with APIResponseHelpers
    with TaskHelpers {

  def aDatabase: Prop[Database]

  // http helpers

  val trace = Seq("X-TRACE" -> "")

  // seed for examples

  val openPermissions =
    JSObject("read" -> "public", "write" -> "public", "create" -> "public")
}

trait API20Spec extends FQL1APISpec with Query20Helpers {
  def apiVers = "2.0"
  val aDatabase = aDatabase(apiVers)
}

trait API21Spec extends API20Spec {
  override def apiVers = "2.1"
}

trait API27Spec extends FQL1APISpec with Query27Helpers {
  def apiVers = "2.7"
  val aDatabase = aDatabase(apiVers)

  // the container flag is backwards compatible across all api versions
  // type annotation required here because aContainerDB is not on the APIClient trait
  val aContainerDB: Prop[Database] = Prop.long flatMap { aContainerDB(apiVers, _) }
}

trait API212Spec extends API27Spec {
  override def apiVers = "2.12"
}

trait API3Spec extends API212Spec {
  override def apiVers = "3"
}

trait API4Spec extends API3Spec {
  override def apiVers = "4"
}

trait API5Spec extends API4Spec {
  override def apiVers = "5"
}

trait APIUnstableSpec extends API27Spec {
  override def apiVers = "UNSTABLE"
}

trait RESTSpec extends API20Spec {
  def collection(path: String, key: String): Seq[JSValue] = {
    @annotation.tailrec
    def coll0(cursor: Option[JSValue], prev: Seq[JSValue]): Seq[JSValue] = {
      val res = cursor match {
        case Some(c) =>
          // the collection is ascending, so cursor using after
          api.get(path, key, query = s"after=${URIEncoding.encode(c.toString)}")
        case None => api.get(path, key)
      }
      res should respond(OK)
      val cur = (res.resource / "data").as[Seq[JSValue]]

      (res.resource / "after").asOpt[JSArray] match {
        case Some(a) =>
          // these cursors will always be single element arrays
          coll0(Some(a.get(0)), prev ++ cur)
        case None => prev ++ cur
      }
    }

    coll0(None, Nil)
  }

}

trait FQL1QuerySpec extends AbstractQueryHelpers { self: FQL1APISpec =>

  def runRawQuery(q: JSValue, auth: AuthType): Future[HttpResponse] =
    client.api.query(q, auth)
  def runRawQuery(q: JSValue, jwt: JWT): Future[HttpResponse] =
    runRawQuery(q, BearerAuth(jwt.getToken))
  def runRawQuery(q: JSValue, jwt: JWTToken): Future[HttpResponse] =
    runRawQuery(q, BearerAuth(jwt.token))
  def runRawQuery(q: JSValue, user: User): Future[HttpResponse] =
    runRawQuery(q, BasicAuth(user.token))

  def runQuery(q: JSValue, auth: AuthType): JSValue = {
    val res = runRawQuery(q, auth)
    res should respond(200, 201)
    res.json / "resource"
  }

  def runQuery(q: JSValue, db: Database): JSValue = runQuery(q, BasicAuth(db.key))
  def runQuery(q: JSValue, u: User): JSValue = runQuery(q, BasicAuth(u.token))
  def runQuery(q: JSValue, jwt: JWT): JSValue = runQuery(q, BearerAuth(jwt.getToken))
  def runQuery(q: JSValue, jwt: JWTToken): JSValue =
    runQuery(q, BearerAuth(jwt.token))

  // Assertions

  def qassert(q: JSValue, jwt: JWT) =
    if (runQuery(q, jwt) != JSTrue) {
      throw new TestFailedException(s"$q did not equal true.", 2)
    }

  def qassert(q: JSValue, db: Database) =
    if (runQuery(q, db) != JSTrue) {
      throw new TestFailedException(s"$q did not equal true.", 2)
    }

  def qassert(q: JSValue, u: User) =
    if (runQuery(q, u) != JSTrue) {
      throw new TestFailedException(s"$q did not equal true.", 2)
    }

  def qassert(q: JSValue, token: String) =
    if (runQuery(q, token) != JSTrue) {
      throw new TestFailedException(s"$q did not equal true.", 2)
    }

  def qassertRuns(
    q: JSValue,
    keys: Seq[(String, Boolean)],
    pos: JSArray = JSArray()) =
    keys foreach { case (k, hasAccess) =>
      if (hasAccess) {
        runQuery(q, k)
      } else {
        qassertErr(q, "permission denied", pos, k)
      }
    }

  def qequals(qa: JSValue, qb: JSValue, jwt: JWT) =
    qassert(JSObject("equals" -> JSArray(qa, qb)), jwt)

  def qequals(qa: JSValue, qb: JSValue, db: Database) =
    qassert(JSObject("equals" -> JSArray(qa, qb)), db)

  def qequals(qa: JSValue, qb: JSValue, u: User) =
    qassert(JSObject("equals" -> JSArray(qa, qb)), u)

  def qequals(qa: JSValue, qb: JSValue, token: String) =
    qassert(JSObject("equals" -> JSArray(qa, qb)), token)

  private[this] def qassertErr0(js: JSValue, code: String, pos: JSArray) = {
    val err = (js / "errors" / 0).asOpt[JSObject] getOrElse {
      throw new TestFailedException(s"Response $js was not an error.", 5)
    }

    if ((err / "position") != pos) {
      throw new TestFailedException(s"$err did not have position $pos", 3)
    }

    if ((err / "code") != JSString(code)) {
      throw new TestFailedException(s"$err did not have code `$code`", 3)
    }

    err
  }

  def qassertErr(q: JSValue, code: String, pos: JSArray, db: Database) =
    qassertErr0(runRawQuery(q, db.key).json, code, pos)

  def qassertErr(q: JSValue, code: String, pos: JSArray, u: User) =
    qassertErr0(runRawQuery(q, u.token).json, code, pos)

  def qassertErr(q: JSValue, code: String, pos: JSArray, token: String) =
    qassertErr0(runRawQuery(q, token).json, code, pos)

  def qassertErr(
    q: JSValue,
    code: String,
    desc: String,
    pos: JSArray,
    key: AuthType): JSObject = {
    val err = qassertErr0(runRawQuery(q, key).json, code, pos)
    if ((err / "description") != JSString(desc)) {
      throw new TestFailedException(s"$err did not have description `$desc`", 2)
    }
    err
  }

  def qassertErr(
    q: JSValue,
    code: String,
    desc: String,
    pos: JSArray,
    jwt: JWT): JSObject =
    qassertErr(q, code, desc, pos, BearerAuth(jwt.getToken))

  def qassertErr(
    q: JSValue,
    code: String,
    desc: String,
    pos: JSArray,
    db: Database): JSObject =
    qassertErr(q, code, desc, pos, db.key)

  def qassertBad(q: JSValue, desc: String, db: Database): Unit = {
    val js = runRawQuery(q, db.key).json
    val err = (js / "errors" / 0).asOpt[JSObject] getOrElse {
      throw new TestFailedException(s"Response $js was not a bad request.", 5)
    }

    if ((err / "code") != JSString("bad request")) {
      throw new TestFailedException(s"$err was not a bad request", 3)
    }

    if ((err / "description") != JSString(desc)) {
      throw new TestFailedException(s"$err did not have description `$desc`", 2)
    }

  }

  def collection(
    q: JSValue,
    db: Database,
    size: Long = DefaultPageSize,
    cursor: PaginateCursor = null) = {
    @annotation.tailrec
    def coll0(cursor: Option[PaginateCursor], prev: Seq[JSValue]): Seq[JSValue] = {
      val ascending = cursor match {
        case Some(Before(_)) => false
        case _               => true
      }

      val paginate =
        cursor.fold(Paginate(q, size = size))(Paginate(q, _, size = size))
      val res = runQuery(paginate, db)
      val curr = (res / "data").as[Seq[JSValue]]

      if (ascending) {
        (res / "after").asOpt[JSValue] match {
          case Some(a) => coll0(Some(After(a)), prev ++ curr)
          case None    => prev ++ curr
        }
      } else {
        (res / "before").asOpt[JSValue] match {
          case Some(a) => coll0(Some(After(a)), curr ++ prev)
          case None    => curr ++ prev
        }
      }
    }

    coll0(Option(cursor), Vector.empty)
  }

  def events(
    q: JSValue,
    db: Database,
    size: Long = DefaultPageSize,
    ascending: Boolean = false) = {
    @annotation.tailrec
    def asc0(after: PaginateCursor, prev: Seq[JSValue]): Seq[JSValue] = {
      val res = runQuery(Paginate(q, after, events = true, size = size), db)
      val resources = prev ++ (res / "data").as[Seq[JSValue]]

      (res / "after").asOpt[JSValue] match {
        case Some(a) => asc0(After(Quote(a)), resources)
        case None    => resources
      }
    }

    @annotation.tailrec
    def desc0(before: PaginateCursor, prev: Seq[JSValue]): Seq[JSValue] = {
      val res = runQuery(Paginate(q, before, events = true, size = size), db)
      val resources = (res / "data").as[Seq[JSValue]] ++ prev

      (res / "before").asOpt[JSValue] match {
        case Some(b) => desc0(Before(Quote(b)), resources)
        case None    => resources
      }
    }

    if (ascending) {
      asc0(After(0L), Seq.empty)
    } else {
      desc0(Before(Long.MaxValue), Seq.empty)
    }
  }

}

trait QueryAPI20Spec extends API20Spec with FQL1QuerySpec
trait QueryAPI21Spec extends API21Spec with FQL1QuerySpec
trait QueryAPI27Spec extends API27Spec with FQL1QuerySpec
trait QueryAPI212Spec extends API212Spec with FQL1QuerySpec
trait QueryAPI3Spec extends API3Spec with FQL1QuerySpec
trait QueryAPI4Spec extends API4Spec with FQL1QuerySpec
trait QueryAPI5Spec extends API5Spec with FQL1QuerySpec
trait QueryAPIUnstableSpec extends APIUnstableSpec with FQL1QuerySpec
