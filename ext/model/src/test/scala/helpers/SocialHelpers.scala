package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.{ AdminPermissions, Auth, RootAuth }
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.prop.api.{ Database => _, Key => _, _ }
import fauna.repo._
import fauna.repo.query.Query
import scala.util.Random

object SocialHelpers extends SocialHelpersVersioned(APIVersion.Default)
object SocialHelpersV3 extends SocialHelpersVersioned(APIVersion.V3)
object SocialHelpersV4 extends SocialHelpersVersioned(APIVersion.V4)
object SocialHelpersUnstable extends SocialHelpersVersioned(APIVersion.Unstable)

class SocialHelpersVersioned(apiVersion: APIVersion) extends DefaultQueryHelpers {
  def ID(subID: Long, clsID: CollectionID) = DocID(SubID(subID), clsID)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  case class Inst(id: DocID, cls: String) {
    def ref = s"classes/$cls/${id.subID.toLong}"
    def docRef = s"@doc/$cls/${id.subID.toLong}"
    def refObj = MkRef(ClassRef(cls), id.subID.toLong)
  }

  case class Tok(id: TokenID, scope: ScopeID, instance: Option[DocID], secret: String) {
    def credentials = Query(instance) flatMapT { Credentials.getByDocument(scope, _) }
  }

  def evalQuery(auth: Auth, ts: Timestamp, q: JSValue) =
    EvalContext.write(auth, ts, apiVersion).parseAndEvalTopLevel(JSON.parse[Literal](q.toByteBuf))

  def evalQuery(auth: Auth, q: JSValue): Query[Either[List[Error], Literal]] =
    Query.snapshotTime flatMap { evalQuery(auth, _, q) }

  def runQuery(auth: Auth, ts: Timestamp, q: JSValue) =
    evalQuery(auth, ts, q) map {
      case Right(r)   => r
      case Left(errs) => sys.error(s"$errs $q")
    }

  def runQuery(auth: Auth, q: JSValue): Query[Literal] =
    Query.snapshotTime flatMap { runQuery(auth, _, q) }

  def newScope: Query[ScopeID] =
    newScope(RootAuth)

  def newScope(auth: Auth): Query[ScopeID] =
    newScope(auth, Random.nextInt().toString)

  def newScope(auth: Auth, accountID: AccountID): Query[ScopeID] =
    newScope(auth, Random.nextInt().toString, accountID = Some(accountID.toLong))

  def newScope(auth: Auth, ver: APIVersion): Query[ScopeID] =
    newScope(auth, Random.nextInt().toString, ver = ver)

  def newScope(auth: Auth, name: String, ver: APIVersion = apiVersion, accountID: Option[Long] = None): Query[ScopeID] =
    runQuery(
      auth,
      Clock.time,
      CreateDatabase(
        MkObject(
          "name" -> name,
          "account" -> (accountID map { id => MkObject("id" -> id) } getOrElse JSNull),
          "api_version" -> ver.toString))) map {
      case VersionL(v, _) => v.data(Database.ScopeField)
      case r              => sys.error(s"Unexpected: $r")
    }

  def mkCollection(auth: Auth, params: JSValue) =
    runQuery(auth, Clock.time, CreateCollection(params)) map {
      case VersionL(v, _) => v.docID.as[CollectionID]
      case r              => sys.error(s"Unexpected: $r")
    }

  def toIndex(r: Literal) = Query(r) flatMap {
    case VersionL(v, _) =>
      fauna.model.Index.getUncached(v.parentScopeID, v.id.as[IndexID]) map { _.get }
    case r => sys.error(s"Unexpected: $r")
  }

  def mkIndex(auth: Auth, name: String, source: String, terms: List[List[String]], values: List[List[String]] = Nil, active: Boolean = true) =
    newIndex(auth, name, Left(List(ClassRef(source))), terms, values, active)

  def allSourcesIndex(auth: Auth, name: String, terms: List[List[String]] = Nil, values: List[List[String]] = Nil, active: Boolean) =
    newIndex(auth, name, Right("_"), terms, values, active)

  def multiSourceIndex(auth: Auth, name: String, sources: List[JSObject] = Nil, terms: List[List[String]] = Nil, values: List[List[String]] = Nil) =
    newIndex(auth, name, Left(sources), terms, values)

  private def newIndex(auth: Auth, name: String, sources: Either[List[JSObject], String], terms: List[List[String]], values: List[List[String]], active: Boolean = true): Query[fauna.model.Index] = {
    val source = sources match {
      case Right(str) => JSString(str)
      case Left(srcs) => JSArray(srcs: _*)
    }

    runQuery(auth, Clock.time,
      CreateIndex(
        MkObject(
          "name" -> name,
          "source" -> source,
          "active" -> active,
          "terms"  -> (terms map { p => MkObject("field" -> p) }),
          "values" -> (values map { p => MkObject("field" -> p) })))) flatMap toIndex
  }

  def mkDoc(auth: Auth, cls: String, params: JSObject = MkObject()) =
    runQuery(auth, CreateF(ClassRef(cls), params)) map {
      case VersionL(v, _) => Inst(v.docID, cls)
      case r              => sys.error(s"Unexpected: $r")
    }

  def mkRole(auth: Auth, name: String, privileges: Seq[JSValue], membership: Seq[JSValue] = Seq.empty) =
    runQuery(auth, CreateRole(MkObject(
      "name" -> name,
      "privileges" -> privileges,
      "membership" -> membership
    ))) map {
      case VersionL(v, _) => v.id.as[RoleID]
      case r              => sys.error(s"Unexpected: $r")
    }

  def mkAccessProvider(
    auth: Auth,
    name: String,
    issuer: String,
    jwksUri: String,
    roles: Seq[JSValue]) =
    runQuery(
      auth,
      CreateAccessProvider(
        MkObject(
          "name" -> name,
          "issuer" -> issuer,
          "jwks_uri" -> jwksUri,
          "roles" -> roles
        ))) map {
      case VersionL(v, _) => v.id.as[AccessProviderID]
      case r              => sys.error(s"Unexpected: $r")
    }

  def mkPerson(auth: Auth, params: JSObject = MkObject()) =
    mkDoc(auth, "people", params = params)

  def mkPost(auth: Auth, author: Inst, isShare: Boolean = false) =
    mkDoc(auth, "posts", MkObject("data" -> MkObject(
      "author" -> author.refObj, "is_share" -> isShare)))

  def mkRoleKey(role: JSValue, auth: Auth = RootAuth) =
    makeKey(MkObject("role" -> role), auth)

  def mkKey(db: String, role: String = "server", auth: Auth = RootAuth) =
    makeKey(MkObject("role" -> role, "database" -> DatabaseRef(db)), auth)

  def mkCurrentDBKey(role: JSValue, auth: Auth = RootAuth) =
    makeKey(MkObject("role" -> role), auth)

  private def makeKey(params: JSValue, auth: Auth) =
    runQuery(auth, Clock.time,
      CreateKey(params)) flatMap {
      case VersionL(v, _) =>
        Key.getLatest(v.parentScopeID, v.id.as[KeyID]) map { key =>
          val secret = v.data(Key.SecretField)
          (secret.get, key.get)
        }
      case r => sys.error(s"Unexpected: $r")
    }

  def mkFollow(auth: Auth, follower: Inst, followee: Inst) =
    mkDoc(auth, "follows", MkObject("data" -> MkObject(
      "follower" -> follower.refObj,
      "followee" -> followee.refObj)))

  def mkUnfollow(auth: Auth, follower: Inst, followee: Inst) =
    runQuery(auth, DeleteF(Select(0, Paginate(Match(
      IndexRef("follows_by_people"),
      JSArray(follower.refObj, followee.refObj))))))

  def socialSetup(ctx: RepoContext, auth: Auth) = {
    ctx ! mkCollection(auth, MkObject("name" -> "people"))
    ctx ! mkCollection(auth, MkObject("name" -> "follows"))
    ctx ! mkCollection(auth, MkObject("name" -> "posts"))
    ctx ! runQuery(auth, Clock.time, Get(ClassRef("posts")))
    ctx ! mkIndex(auth,
        "posts_by_author",
        "posts",
        List(List("data", "author")))
    ctx ! mkIndex(auth,
        "follows_by_follower",
        "follows",
        List(List("data", "follower")),
        List(List("data", "followee")))
    ctx ! mkIndex(auth,
        "follows_by_people",
        "follows",
        List(List("data", "follower"), List("data", "followee")))

    ctx ! mkRole(
      auth.withPermissions(AdminPermissions),
      "posts_perms",
      Seq(
        MkObject(
          "resource" -> ClassRef("posts"),
          "actions" -> MkObject("create" -> true, "write" -> true, "read" -> true))),
      Seq(MkObject("resource" -> ClassRef("people")))
    )
  }

  def getInstance(auth: Auth, ref: JSObject, ts: Timestamp) =
    runQuery(auth, ts, Get(ref)) map {
      case VersionL(v, _) => v
      case r              => sys.error(s"Unexpected: $r")
    }

  def getMinimal(auth: Auth, set: JSValue, ts: Timestamp = Timestamp.MaxMicros) =
    runQuery(auth, ts, Get(set)) map {
      case VersionL(v, _) => v
      case r              => sys.error(s"Unexpected: $r")
    }

  def followsFor(user: Inst): JSObject =
    followsFor(user.refObj)

  def followsFor(obj: JSValue): JSObject =
    Match(IndexRef("follows_by_follower"), JSArray(obj))

  def loginAs(auth: Auth, inst: Inst, password: String, ttl: Option[Timestamp] = None) =
    runQuery(auth, Clock.time, Login(inst.refObj, MkObject("password" -> password, "ttl" -> ttl.map { t => Time(t.toString) }))) map {
      case VersionL(v, _) =>
        Tok(v.docID.as[TokenID],
          auth.scopeID,
          v.data(Token.DocumentField),
          v.data(Token.SecretField) getOrElse "broken")
      case r              => sys.error(s"Unexpected: $r")
    }

  def logoutAs(auth: Auth, removeAll: Boolean) =
    runQuery(auth, Clock.time, Logout(removeAll)) map {
      case TrueL  => true
      case FalseL => false
      case r      => sys.error(s"Unexpected: $r")
    }

  def updatePassword(auth: Auth, inst: Inst, oldPass: String, newPass: String) =
    runQuery(auth, Clock.time, Update(inst.refObj,
      MkObject(
        "credentials" -> JSObject(
          "current_password" -> oldPass,
          "password" -> newPass)))) map {
      case VersionL(_, _) => inst
      case r              => sys.error(s"Unexpected: $r")
    }

  def postsFor(obj: JSValue): JSObject =
    Match(IndexRef("posts_by_author"), JSArray(obj))

  def postsFor(auth: Auth, user: Inst): Query[Seq[SetEventL]] =
    events(auth, postsFor(user.refObj)) map {
      _.elems map { _.asInstanceOf[SetEventL] }
    }

  def timelineFor(user: Inst) =
    Join(followsFor(user), Lambda("author" ->
      Match(IndexRef("posts_by_author"), JSArray(Var("author")))))

  def events(auth: Auth, set: JSValue, size: Int = Everything, cursor: PaginateCursor = NoCursor) = {
    val q = Paginate(set, cursor, size = size, events = true)

    runQuery(auth: Auth, Timestamp.MaxMicros, q) map {
      case pg @ PageL(_, _, _, _) => pg
      case r              => sys.error(s"Unexpected: $r")
    }
  }

  def collection(auth: Auth, set: JSValue, size: Int = Everything, ts: Timestamp = Timestamp.MaxMicros, cursor: PaginateCursor = NoCursor) = {
    def coll(cur: PaginateCursor, prev: PageL): Query[PageL] = {
      val page = {
        val p = Paginate(set, cur, size = size)
        if (ts == Timestamp.MaxMicros) p else At(ts.micros, p)
      }

      runQuery(auth, Timestamp.MaxMicros, page) flatMap {
        case PageL(es, _, None, Some(c)) =>
          RenderContext(auth, APIVersion.V21, ts) flatMap { rc =>
            rc.render(c) flatMap { buf =>
              val after = After(JSON.parse[JSValue](buf))
              assert(after != cur)
              coll(after, prev.copy(elems = es ++ prev.elems))
            }
          }
        case PageL(es, _, _, _) => Query(prev.copy(elems = es ++ prev.elems))
        case r                  => sys.error(s"Unexpected: $r")
      }
    }
    coll(cursor, PageL(Nil, Nil, None, None))
  }

  def validTS(r: Literal) = r match {
    case SetEventL(e)             => e.ts.validTS
    case DocEventL(e)             => e.ts.validTS
    case CursorL(Left(e: EventL)) => e.event.ts.validTS
    case _                        => sys.error("event expected")
  }
}
