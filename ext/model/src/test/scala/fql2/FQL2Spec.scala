package fauna.model.test

import fauna.ast.{ EvalContext, Literal, ValidationError }
import fauna.atoms.{ APIVersion, AccountID, CollectionID, ScopeID }
import fauna.auth.{ Auth, RootAuth, ServerPermissions, SystemAuth }
import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.flags.{ Feature, Value => FFValue }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ Cache, Collection, Database }
import fauna.model.gc.MVTProvider
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.FQLInterpreter.TypeMode
import fauna.model.runtime.Effect
import fauna.model.schema.{
  Result => SchemaResult,
  SchemaError,
  SchemaManager,
  SchemaSource,
  SchemaStatus
}
import fauna.model.schema.fsl.SourceFile
import fauna.net.security.{ JWK, JWKProvider, JWKSError }
import fauna.prop.api.DefaultQueryHelpers
import fauna.prop.Generators
import fauna.repo.query.{ Query, State }
import fauna.repo.store.CacheStore
import fauna.repo.test.CassandraHelper
import fauna.repo.values.Value
import fauna.repo.RepoContext
import fauna.storage.doc.ValidationException
import fauna.util.BCrypt
import fql.ast.{ Expr, Src, TypeExpr }
import fql.ast.display._
import fql.color.ColorKind
import fql.parser.Parser
import fql.schema.DiffRender
import java.net.URL
import org.scalactic.source.Position
import org.scalatest.time._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.ClassTag

case class Output(res: Result[SuccessOutput], logs: InfoWarns, state: Option[State])

case class SuccessOutput(
  value: Value,
  ts: Timestamp,
  ty: TypeExpr,
  logs: InfoWarns) {
  def typeStr: String = ty.display
}

trait FQL2Spec extends Spec with Generators {

  // Lower the limit for tests to speed them up and
  // prevent timeouts and transactions that are too big.
  val TestMaxStackFrames = 256

  implicit val ctx =
    CassandraHelper.context(
      scope = "model",
      invalidateOpt = Some(Cache.invalidate),
      fqlxMaxStackFrames = TestMaxStackFrames,
      mvtProvider = MVTProvider
    )

  override implicit val patienceConfig = {
    val maxRefreshRate =
      ctx.cacheContext.schema.refreshRate.max(
        ctx.cacheContext.schema2.refreshRate
      )

    PatienceConfig(
      timeout = Span(maxRefreshRate.toSeconds, Seconds),
      interval = Span(100, Millis)
    )
  }

  def newScope = ScopeID(ctx.nextID())
  def newAuth = Auth.forScope(newScope)
  def newDB: Auth = newChildDB(RootAuth)
  def newDB(accountID: Long): Auth = newTenantRoot(RootAuth, accountID)
  def newDB(typeChecked: Boolean = true) = newChildDB(RootAuth, typeChecked)
  def newChildDB(auth: Auth, typeChecked: Boolean = true) = {
    val name = s"db${ctx.nextID().toString}"

    evalOk(
      auth,
      s"""|Database.create({
          |  name: "$name",
          |  typechecked: $typeChecked
          |})""".stripMargin
    )

    loadAuth(getDB(auth.scopeID, name).value.scopeID)
  }

  def loadAuth(scope: ScopeID) = {
    val db = (ctx ! Database.getUncached(scope)).get
    SystemAuth(db.scopeID, db, ServerPermissions)
  }

  def newCustomerTenantRoot(auth: Auth): Auth = {
    newTenantRoot(auth, 10000)
  }

  def newInternalTenantRoot(auth: Auth): Auth = {
    newTenantRoot(auth, 5)
  }

  private def newTenantRoot(auth: Auth, accountID: Long): Auth = {
    val name = s"db${ctx.nextID().toString}"

    evalOk(
      auth,
      s"""|Database.create({
          |  name: "$name",
          |  account: { id: $accountID }
          |})""".stripMargin,
      typecheck = false
    )

    val db = getDB(auth.scopeID, name).value

    SystemAuth(db.scopeID, db, ServerPermissions)
  }

  def getDB(scope: ScopeID, name: String) = ctx ! {
    Database.idByName(scope, name) flatMapT {
      Database.getUncached(scope, _)
    }
  }

  // NB: Must have a different name than `fql2.ValueOps`, or it will conflict
  // and just not work.
  implicit class ValueTestOps(val value: Value) {
    def to[A <: Value](implicit ct: ClassTag[A], pos: Position): A =
      value match {
        case ct(a) => a
        case other =>
          fail(s"Expected type ${ct.runtimeClass}, got ${other.getClass}")
      }
  }

  implicit def tokenToAuth(token: String): Auth =
    (ctx ! Auth.fromAuth(token, List(BCrypt.hash("secret")))).value

  def evalAllPages(auth: Auth, query: String) = {
    val elems = Vector.newBuilder[Value]

    def eval0(page: Value): Vector[Value] = {
      elems ++= (page / "data").as[Vector[Value]]
      (page / "after") match {
        case _: Value.Null => elems.result()
        case cursor        => eval0(evalPage(auth, cursor))
      }
    }

    eval0(evalOk(auth, query))
  }

  def evalPage(
    auth: Auth,
    cursor: Value
  )(implicit pos: Position): Value = {
    val c = cursor match {
      case v: Value.SetCursor => Value.SetCursor.toBase64(v, None).toString
      case _                  => fail(s"not a set cursor: $cursor")
    }

    evalPage(auth, c)
  }

  def evalPage(auth: Auth, cursor: String)(implicit pos: Position): Value = {
    evalOk(auth, s"Set.paginate('$cursor')")
  }

  def parseOk(query: String): Expr =
    Parser.query(query).getOr { err =>
      fail(s"Failed to parse: $err")
    }

  def evalOk(
    auth: Auth,
    query: String,
    // FIXME: enable checking in all tests!
    typecheck: Boolean = true,
    globalCtx: FQLInterpreter.VarCtx = Map.empty,
    effect: Effect = Effect.Write)(implicit pos: Position): Value =
    evalRes(auth, query, typecheck, globalCtx, effect).value

  def evalOk(token: String, query: String)(implicit pos: Position): Value =
    evalRes(token, query).value

  def evalRes(
    auth: Auth,
    query: String,
    typecheck: Boolean = true,
    globalCtx: FQLInterpreter.VarCtx = Map.empty,
    effect: Effect = Effect.Write,
    performanceHintsEnabled: Boolean = false
  )(implicit pos: Position): SuccessOutput = {
    eval(
      auth,
      query,
      typecheck,
      globalCtx,
      effect,
      performanceHintsEnabled).res match {
      case Result.Ok(res) => res
      case Result.Err(e) =>
        e match {
          case e: QueryRuntimeFailure =>
            e.abortReturn.foreach { abort => fail(s"query aborted:\n$abort") }
          case _ => ()
        }
        fail(
          s"unexpected eval fail:\n${e.errors.map(_.renderWithSource(Map.empty)).mkString("\n\n")}")
    }
  }

  def evalErr(token: String, query: String)(implicit pos: Position): QueryFailure = {
    val auth = (ctx ! Auth.fromAuth(token, List(BCrypt.hash("secret")))).value
    evalErr(auth, query)
  }

  def evalErr(
    auth: Auth,
    query: String,
    typecheck: Boolean = true,
    globalCtx: FQLInterpreter.VarCtx = Map.empty,
    effect: Effect = Effect.Write)(implicit pos: Position): QueryFailure =
    eval(auth, query, typecheck, globalCtx, effect = effect).res match {
      case Result.Ok(_) =>
        fail(s"unexpected eval success. The following query succeeded:\n$query")
      case Result.Err(e) => e
    }

  def renderErr(
    auth: Auth,
    query: String,
    typecheck: Boolean = true,
    globalCtx: FQLInterpreter.VarCtx = Map.empty,
    effect: Effect = Effect.Write)(implicit pos: Position) = {
    evalErr(auth, query, typecheck, globalCtx, effect).errors
      .map(_.renderWithSource(Map.empty))
      .mkString("\n\n")
  }

  def eval(token: String, query: String): Output = {
    val auth = (ctx ! Auth.fromAuth(token, List(BCrypt.hash("secret")))).value
    eval(auth, query)
  }

  def eval(
    auth: Auth,
    query: String,
    typecheck: Boolean = true,
    globalCtx: FQLInterpreter.VarCtx = Map.empty,
    effect: Effect = Effect.Write,
    performanceHintsEnabled: Boolean = false
  ): Output = {
    val res: Result[Expr] = Parser.query(query)
    res match {
      case Result.Ok(expr) =>
        eval(
          auth,
          expr,
          if (typecheck) TypeMode.InferType else TypeMode.Disabled,
          globalCtx,
          effect,
          performanceHintsEnabled)
      case res @ Result.Err(_) =>
        Output(res, InfoWarns.empty, None)
    }
  }

  def eval(
    auth: Auth,
    expr: Expr,
    typeMode: TypeMode,
    globalCtx: FQLInterpreter.VarCtx,
    effect: Effect,
    performanceHintsEnabled: Boolean): Output = {
    val intp = new FQLInterpreter(
      auth,
      Effect.Limit(effect, "model tests"),
      performanceDiagnosticsEnabled = performanceHintsEnabled)
    val q =
      intp
        .evalWithTypecheck(expr, globalCtx, typeMode)
        .flatMap { res =>
          intp.runPostEvalHooks().flatMap { postEvalRes =>
            intp.infoWarns.map { infoWarns =>
              postEvalRes match {
                case Result.Ok(_)        => (res, infoWarns)
                case err @ Result.Err(_) => (err, infoWarns)
              }
            }
          }
        }
        .flatMap { case (res, info) =>
          Query.state.map { state => (res, info, state) }
        }
        .onlyWriteWhen {
          case (Result.Ok(_), _, _) => true
          case _                    => false
        }

    ctx !! q match {
      case RepoContext.Result(ts, (res, infoWarns, state)) =>
        val out = res.map { case (v, tpe) => SuccessOutput(v, ts, tpe, infoWarns) }
        Output(out, infoWarns, Some(state))
    }
  }

  class QueryFailedException[T](val value: T) extends Exception

  implicit class QueryOps[T](q: Query[T]) {
    def onlyWriteWhen[U](f: T => Boolean): Query[T] =
      q.flatMap { v =>
        if (f(v)) {
          Query.value(v)
        } else {
          Query.fail(new QueryFailedException(v))
        }
      }.recover { case e: QueryFailedException[_] =>
        e.value.asInstanceOf[T]
      }
  }

  def getDocFields(auth: Auth, doc: Value)(implicit pos: Position) =
    doc match {
      case d: Value.Doc =>
        val intp = new FQLInterpreter(auth, Effect.Limit(Effect.Read, "model tests"))
        (ctx ! ReadBroker.getAllFields(intp, d))
          .getOrElse(fail(s"failed to read doc fields"))
      case _ => fail(s"$doc is not a Value.Doc")
    }

  def mkKey(auth: Auth, role: String): String = {
    val key = evalOk(
      auth,
      s"""|Key.create({
          |  role: \"$role\"
          |})""".stripMargin)

    (getDocFields(auth, key) / "secret").as[String]
  }

  def mkRoleAuth(auth: Auth, role: String): Auth = {
    val secret = mkKey(auth, role)
    (ctx ! Auth.fromAuth(secret, List(BCrypt.hash("secret")))).value
  }

  def mkColl(auth: Auth, name: String): CollectionID = {
    evalOk(auth, s"""Collection.create({name: "$name"})""")
    (ctx ! Collection.idByNameActive(auth.scopeID, name)).value
  }

  def jwkProvider(jwksUri: String, key: JWK) =
    new JWKProvider {
      def getJWK(url: URL, kid: Option[String]): Future[JWK] = {
        if (jwksUri == url.toString && key.kid == kid) {
          Future.successful(key)
        } else {
          Future.failed(new JWKSError(url, "wrong uri or key"))
        }
      }
    }

  def updateSchema(auth: Auth, files: (String, String)*) =
    updateSchema0(auth, overrideMode = true, pin = false, files: _*)

  def updateSchemaPin(auth: Auth, files: (String, String)*) =
    updateSchema0(auth, overrideMode = true, pin = true, files: _*)

  def updateSchemaNoOverride(auth: Auth, files: (String, String)*) =
    updateSchema0(auth, overrideMode = false, pin = false, files: _*)

  // Once in a blue moon there's a test that would like to control how this query
  // is run instead of just running it.
  def updateSchema0Q(
    auth: Auth,
    overrideMode: Boolean,
    pin: Boolean,
    files: (String, String)*) = {
    val intp = new FQLInterpreter(auth)
    val q = for {
      _ <-
        if (pin) {
          SchemaStatus.pin(intp)
        } else {
          Query.value(())
        }

      sources = files map { case (name, content) =>
        SourceFile(name, SourceFile.Ext.FSL, content)
      }

      updateRes <- SchemaManager.update(intp, sources, overrideMode = overrideMode)
      res <- updateRes match {
        case SchemaResult.Ok(res) =>
          intp.runPostEvalHooks().map {
            case Result.Ok(_) => SchemaResult.Ok(res)
            case Result.Err(err) =>
              SchemaResult.Err(SchemaError.QueryFailure(err))
          }
        case res @ SchemaResult.Err(_) => Query.value(res)
      }
    } yield res

    q.onlyWriteWhen { _.isOk }
  }

  def updateSchema0(
    auth: Auth,
    overrideMode: Boolean,
    pin: Boolean,
    files: (String, String)*) =
    ctx ! updateSchema0Q(auth, overrideMode, pin, files: _*)

  def schemaContent(auth: Auth, filename: String) = {
    val source = ctx ! SchemaSource.get(auth.scopeID, filename)
    source map { _.content }
  }

  // This is a partial re-implementation of SchemaResponse, mostly for debugging.
  def renderSchemaErrors(errors: Iterable[SchemaError], ctx: Map[Src.Id, String])(
    implicit strs: ListBuffer[String]): Unit = {
    errors.foreach {
      case SchemaError.SchemaSourceErrors(errs, files) =>
        val ctx0 = ctx ++ files.view.map { f => f.src -> f.content }.toMap
        renderSchemaErrors(errs, ctx0)
      case SchemaError.FQLError(err) => strs += err.renderWithSource(ctx)
      case SchemaError.QueryFailure(err) =>
        strs += err.errors.map(_.renderWithSource(ctx)).mkString("\n\n")
      case other => strs += other.toString
    }
  }

  def renderSchemaErrors(
    errors: Iterable[SchemaError],
    files: Seq[(String, String)]): String = {
    val buf = ListBuffer.empty[String]
    renderSchemaErrors(errors, files.map { case (k, v) => Src.Id(k) -> v }.toMap)(
      buf)
    buf.mkString("\n")
  }

  def updateSchemaPinOk(auth: Auth, files: (String, String)*)(
    implicit pos: Position) = {
    val res = updateSchemaPin(auth, files: _*) match {
      case SchemaResult.Ok(v) => v
      case SchemaResult.Err(e) => {
        fail(s"unexpected error updating schema:\n${renderSchemaErrors(e, files)}")
      }
    }

    // NB: Invalidate both, so that `SchemaStatus.forScope` sees the latest.
    val parentScope =
      ctx ! Database.forScope(auth.scopeID).map(_.get.parentScopeID)
    ctx ! CacheStore.invalidateScope(parentScope)
    ctx ! CacheStore.invalidateScope(auth.scopeID)

    res
  }

  def updateSchemaOk(
    auth: Auth,
    files: (String, String)*)(implicit pos: Position) = {
    updateSchema(auth, files: _*) match {
      case SchemaResult.Ok(v) => v
      case SchemaResult.Err(e) => {
        fail(s"unexpected error updating schema:\n${renderSchemaErrors(e, files)}")
      }
    }
  }

  def updateSchemaErr(
    auth: Auth,
    files: (String, String)*)(implicit pos: Position) = {
    updateSchema(auth, files: _*) match {
      case SchemaResult.Ok(_) =>
        fail(s"updating schema passed when it should have failed")
      case SchemaResult.Err(e) => renderSchemaErrors(e, files)
    }
  }

  def validateSchemaOk(auth: Auth, schema: (String, String)*): String = {
    ctx.cacheContext.invalidateAll()
    ctx ! SchemaManager
      .validate(
        auth.scopeID,
        schema.map { case (name, content) => SourceFile.FSL(name, content) })
      .map {
        case SchemaResult.Ok(res) =>
          val beforeSrc = res.before.view map { file => file.src -> file.content }
          val afterSrc = schema.view map { case (name, content) =>
            Src.Id(name) -> content
          }

          DiffRender.renderSemantic(
            beforeSrc.toMap,
            afterSrc.toMap,
            res.diffs,
            ColorKind.None)

        case SchemaResult.Err(errs) => fail(errs.toString)
      }
  }

  def validateStagedSchemaOk(auth: Auth, schema: (String, String)*): String = {
    ctx ! SchemaManager
      .validate(
        auth.scopeID,
        schema.map { case (name, content) => SourceFile.FSL(name, content) },
        staged = true)
      .map {
        case SchemaResult.Ok(res) =>
          val beforeSrc = res.before.view map { file => file.src -> file.content }
          val afterSrc = schema.view map { case (name, content) =>
            Src.Id(name) -> content
          }

          DiffRender.renderSemantic(
            beforeSrc.toMap,
            afterSrc.toMap,
            res.diffs,
            ColorKind.None)

        case SchemaResult.Err(errs) => fail(errs.toString)
      }
  }

  def withAccountFlag[T](ff: Feature[AccountID, _], value: FFValue)(f: => T): T = {
    CassandraHelper.ffService.withAccount(ff, value) {
      // Refresh to see new feature flags.
      ctx.cacheContext.invalidateAll()

      try {
        f
      } finally {
        // Refresh to see old feature flags.
        ctx.cacheContext.invalidateAll()
      }
    }
  }
}

trait FQL2WithV4Spec extends FQL2Spec with DefaultQueryHelpers {
  class V4QueryFailedException(val errors: List[fauna.ast.Error]) extends Exception

  def evalV4(auth: Auth, q: JSValue) =
    ctx ! Query.snapshotTime
      .flatMap { ts =>
        EvalContext
          .write(auth, ts, APIVersion.V4)
          .parseAndEvalTopLevel(JSON.parse[Literal](q.toByteBuf))
      }
      .onlyWriteWhen { _.isRight }

  def evalV4Ok(auth: Auth, q: JSValue): Literal =
    evalV4(auth, q) match {
      case Right(lit) => lit
      case Left(errs) => fail(s"unexpected error: ${errs.mkString("\n")}")
    }

  def evalV4Err(auth: Auth, q: JSValue): List[fauna.ast.Error] =
    evalV4(auth, q) match {
      case Right(lit) => fail(s"unexpected success, with value: ${lit}")
      case Left(errs) => errs
    }

  def validateV4ErrorResponse(
    response: List[fauna.ast.Error],
    err: ValidationException) = {
    response.length shouldBe 1
    response.head shouldBe a[ValidationError]
    response.head match {
      case ValidationError(errs, _) =>
        errs.length shouldBe 1
        errs.head shouldEqual err
      case f =>
        fail(s"Unexpected error type $f, expected ValidationError")
    }
  }
}
