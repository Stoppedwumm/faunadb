package fauna.model

import fauna.ast.{ EvalContext, LambdaL }
import fauna.atoms._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model.runtime.fql2.{ FQLInterpreter, Result }
import fauna.model.runtime.fql2.FQLInterpCtx
import fauna.model.schema.{ SchemaCollection, SchemaItemView }
import fauna.model.Key.RoleFieldType
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.util.ReferencesValidator
import fql.ast.{ Src, TypeExpr }
import fql.parser.Parser
import fql.typer.{ Type, TypeScheme }

case class UserFunction(
  id: UserFunctionID,
  scopeID: ScopeID,
  name: String,
  alias: Option[String],
  lambda: LambdaWrapper,
  role: Option[Key.Role])

object UserFunction extends ExceptionLogging {
  def apply(vers: Version.Live, body: LambdaWrapper): UserFunction =
    UserFunction(
      vers.id.as[UserFunctionID],
      vers.parentScopeID,
      SchemaNames.findName(vers),
      SchemaNames.findAlias(vers),
      body,
      vers.data(RoleField))

  val RoleField = Field[Option[Key.Role]]("role")
  val BodyField = Field[LambdaWrapper.Src]("body")
  val UserSigField = Field[Option[String]]("signature")
  val InternalSigField = Field[Option[String]]("internal_sig")

  val VersionValidator =
    Document.DataValidator +
      SchemaNames.NameField.validator +
      RoleField.validator +
      BodyField.validator +
      UserSigField.validator +
      InternalSigField.validator

  def LiveValidator(ec: EvalContext) =
    VersionValidator +
      ReferencesValidator(ec) +
      Key.RoleNameValidator(ec, RoleField.path)

  private def parseUserSig(vers: Version.Live): Option[TypeExpr.Lambda] =
    vers.data(UserSigField) flatMap { sig =>
      val name = SchemaNames.findName(vers)
      Parser.typeExpr(sig, Src.UserFunc(name)).toOption collect {
        case lambda: TypeExpr.Lambda => lambda
      }
    }

  private def parseInternalSig(vers: Version.Live): TypeScheme =
    vers.data(InternalSigField) match {
      case None => Type.Any.typescheme
      case Some(sig) =>
        FQLInterpreter.prepareLambdaSig(sig) match {
          case Result.Ok(sch) => sch
          case Result.Err(e) =>
            squelchAndLogException {
              throw new IllegalStateException(
                s"UDF internal sig is invalid on ${vers.parentScopeID}, ${vers.id}, $e")
            }
            Type.Any.typescheme
        }
    }

  private def prepareBody(vers: Version.Live, name: String): Query[LambdaWrapper] =
    vers
      .data(BodyField)
      .fold(
        { src =>
          FQLInterpreter
            .prepareUserFunc(Src.UserFunc(name), src, allowShortLambda = false) map {
            case Result.Ok(lambda) =>
              LambdaWrapper(
                lambda,
                parseUserSig(vers),
                parseInternalSig(vers),
                src
              )
            case Result.Err(e) =>
              throw new IllegalStateException(
                s"Invalid lambda body on ${vers.parentScopeID}, ${vers.id} $e")
          }
        },
        { qry => Query.value(LambdaWrapper(LambdaL(vers.parentScopeID, qry))) }
      )

  def get(scope: ScopeID, id: UserFunctionID): Query[Option[UserFunction]] =
    getItem(scope, id).map(_.flatMap(_.active))

  def getItem(
    scope: ScopeID,
    id: UserFunctionID): Query[Option[SchemaItemView[UserFunction]]] =
    Cache.functionByID(scope, id)

  def getItemUncached(
    scope: ScopeID,
    id: UserFunctionID): Query[Option[SchemaItemView[UserFunction]]] =
    SchemaCollection.UserFunction(scope).schemaVersState(id).flatMapT {
      _.flatMapSimplified { vers =>
        val name = SchemaNames.findName(vers)
        prepareBody(vers, name).map(UserFunction(vers, _))
      }.map(Some(_))
    }

  def idByName(ctx: FQLInterpCtx, name: String): Query[Option[UserFunctionID]] =
    ctx.env.idByName[UserFunctionID](ctx.scopeID, name)

  def idByNameActive(scope: ScopeID, name: String): Query[Option[UserFunctionID]] =
    Cache.functionIDByName(scope, name).map(_.flatMap(_.active))

  def idByAlias(ctx: FQLInterpCtx, alias: String): Query[Option[UserFunctionID]] =
    ctx.env.idByAlias[UserFunctionID](ctx.scopeID, alias)

  def idByAliasActive(scope: ScopeID, alias: String): Query[Option[UserFunctionID]] =
    Cache.functionIDByAlias(scope, alias).map(_.flatMap(_.active))

  def idByIdentifier(
    ctx: FQLInterpCtx,
    name: String): Query[Option[UserFunctionID]] =
    idByName(ctx, name).orElseT(idByAlias(ctx, name))

  def idByNameUncached(
    scope: ScopeID,
    name: String): Query[Option[UserFunctionID]] = {
    SchemaNames.idByNameStagedUncached[UserFunctionID](scope, name)
  }

  def idByAliasUncached(
    scope: ScopeID,
    alias: String): Query[Option[UserFunctionID]] = {
    SchemaNames.idByAliasStagedUncached[UserFunctionID](scope, alias)
  }

  def idByIdentifierUncached(
    scope: ScopeID,
    name: String): Query[Option[UserFunctionID]] =
    idByNameUncached(scope, name).orElseT(idByAliasUncached(scope, name))

  def getAll(scope: ScopeID): PagedQuery[Iterable[UserFunction]] =
    UserFunctionID.getAllUserDefined(scope) collectMT { get(scope, _) }

  def getAllUncached(scope: ScopeID): PagedQuery[Iterable[UserFunction]] =
    UserFunctionID.getAllUserDefined(scope) collectMT {
      getItemUncached(scope, _).map(_.flatMap(_.active))
    }
}
