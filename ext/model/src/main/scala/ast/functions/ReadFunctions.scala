package fauna.ast

import fauna.atoms._
import fauna.auth.{ Auth, AuthLike, JWTLogin, KeyLogin, LoginAuth, TokenLogin }
import fauna.model.runtime.Effect
import fauna.model.SchemaSet
import fauna.repo.query.Query

object ExistsFunction extends QFunction {
  val effect = Effect.Read

  def apply(
    res: UnresolvedIdentifierL,
    tsOpt: Option[AbstractTimeL],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val ts = ec.getValidTime(tsOpt)

    res match {
      case SetL(set) =>
        ReadAdaptor.exists(ec, set, ts, pos)
      case RefL(scope, CollectionID(DatabaseID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Databases(scope), ts, pos)
      case RefL(scope, CollectionID(KeyID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Keys(scope), ts, pos)
      case RefL(scope, CollectionID(TokenID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Tokens(scope), ts, pos)
      case RefL(scope, CollectionID(CredentialsID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Credentials(scope), ts, pos)
      case RefL(scope, CollectionID(CollectionID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Collections(scope), ts, pos)
      case RefL(scope, CollectionID(IndexID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Indexes(scope), ts, pos)
      case RefL(scope, CollectionID(TaskID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.Tasks(scope), ts, pos)
      case RefL(scope, CollectionID(UserFunctionID.collID)) =>
        ReadAdaptor.exists(ec, SchemaSet.UserFunctions(scope), ts, pos)
      case RefL(scope, id) =>
        ReadAdaptor.exists(ec, scope, id, ts)
      case UnresolvedRefL(_) =>
        Query.value(Right(FalseL))
    }
  }
}

object GetFunction extends QFunction {
  val effect = Effect.Read

  def apply(
    ref: IdentifierL,
    tsOpt: Option[AbstractTimeL],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val ts = ec.getValidTime(tsOpt)

    ref match {
      case RefL(scope, id) =>
        ReadAdaptor.getInstance(ec, scope, id, ts, pos)
      case SetL(set) =>
        ReadAdaptor.getSet(ec, set, ts, pos)
    }
  }
}

object FirstFunction extends QFunction {
  val effect = Effect.Read

  def apply(v: Literal, ec: EvalContext, pos: Position): Query[R[Literal]] =
    v match {
      case ArrayL(elems) if elems.isEmpty =>
        Query(Left(List(EmptyArrayArgument(pos at "first"))))
      case ArrayL(elems) => Query(Right(elems.head))
      case SetL(set) =>
        ReadAdaptor.paginate(
          set = set,
          cursorOpt = None,
          tsOpt = None,
          sizeOpt = Some(1),
          sourcesOpt = None,
          ascending = true,
          ecRaw = ec,
          pos = pos,
          verb = Some("first")) map {
          case Right(PageL(vs, _, _, _)) if vs.isEmpty =>
            Left(List(EmptySetArgument(pos at "first")))
          case Right(PageL(vs, _, _, _)) =>
            Right(vs.head)
          case l @ Left(_) => l
        }
      case _ =>
        Query(
          Left(
            List(
              InvalidArgument(List(Type.Set, Type.Array), v.rtype, pos at "first"))))
    }
}

object LastFunction extends QFunction {
  val effect = Effect.Read

  def apply(v: Literal, ec: EvalContext, pos: Position): Query[R[Literal]] =
    v match {
      case ArrayL(elems) if elems.isEmpty =>
        Query(Left(List(EmptyArrayArgument(pos at "last"))))
      case ArrayL(elems) =>
        Query(Right(elems.last))
      case SetL(set) =>
        ReadAdaptor.paginate(
          set = set,
          cursorOpt = None,
          tsOpt = None,
          sizeOpt = Some(1),
          sourcesOpt = None,
          ascending = false,
          ecRaw = ec,
          pos = pos,
          verb = Some("last")) map {
          case Right(PageL(vs, _, _, _)) if vs.isEmpty =>
            Left(List(EmptySetArgument(pos at "last")))
          case Right(PageL(vs, _, _, _)) => Right(vs.head)
          case l @ Left(_)               => l
        }
      case _ =>
        Query(Left(
          List(InvalidArgument(List(Type.Set, Type.Array), v.rtype, pos at "last"))))
    }
}

object KeyFromSecretFunction extends QFunction {
  val effect = Effect.Read

  def apply(secret: String, ec: EvalContext, pos: Position): Query[R[Literal]] = {
    def refFromAuth(authOpt: Option[Auth]): Option[RefL] = {
      authOpt flatMap {
        case LoginAuth.Source(KeyLogin(key)) =>
          Some(RefL(key.parentScopeID, key.id.toDocID))
        case LoginAuth.Source(TokenLogin(token)) =>
          Some(RefL(token.scopeID, token.id.toDocID))
        case _ => None
      }
    }

    def dbRefFromJWT(authOpt: Option[Auth]): Query[R[ObjectL]] = {
      authOpt match {
        case Some(la @ LoginAuth.Source(JWTLogin(_, _))) =>
          val db = la.database
          val refL = RefL(db.parentScopeID, db.id)
          ec.auth.checkReadPermission(refL.scope, refL.id) map {
            if (_) {
              Right(ObjectL("database" -> refL))
            } else {
              Left(List(PermissionDenied(Right(refL), pos)))
            }
          }
        case _ => Query(Left(List(KeyNotFound(secret, pos))))
      }
    }

    if (AuthLike.isJWT(secret)) {
      Query.jwkProvider.flatMap { jwkProvider =>
        Auth.fromJwt(secret, jwkProvider) flatMap dbRefFromJWT
      }
    } else {
      val authOptQ = Auth.lookup(secret)
      authOptQ flatMap { authOpt =>
        refFromAuth(authOpt) match {
          case Some(ref) =>
            GetFunction(ref, None, ec, pos) map {
              // swallow permission denied
              _.left map { _ =>
                List(KeyNotFound(secret, pos))
              }
            }
          case _ =>
            Query(Left(List(KeyNotFound(secret, pos))))
        }
      }
    }
  }
}
