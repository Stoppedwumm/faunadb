package fauna.ast

import fauna.atoms._
import fauna.auth._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.Effect
import fauna.net.security.{ JWT, JWTFields }
import fauna.repo.{ WriteFailure, WriteFailureException }
import fauna.repo.query.Query
import fauna.storage.doc.Diff
import java.time.Instant
import scala.annotation.unused

/** Returns the current identity.
  * For JWT it returns the 'sub' field of the payload.
  * For Fauna tokens, returns the document ref of the token.
  * For Fauna keys it returns an error.
  *
  * @since 4
  */
object CurrentIdentityFunction extends QFunction {
  val effect = Effect.Read

  private val SubPath = List(Right(JWTFields.Subject))

  def apply(@unused n: Unit, ec: EvalContext, pos: Position): Query[R[Literal]] =
    ec.auth match {
      case LoginAuth.Source(JWTLogin(_, payload)) =>
        ReadAdaptor.select(ec, SubPath, all = false, payload, None, pos) mapLeftT {
          _ => List(MissingIdentityError(pos))
        }

      case LoginAuth.Identity(id) =>
        Query.value(Right(RefL(ec.auth.scopeID, id)))

      case _ =>
        Query.value(Left(List(MissingIdentityError(pos))))
    }
}

/** Returns True if CurrentIdentity() returns a value or False if it returns an error.
  * @since 4
  */
object HasCurrentIdentityFunction extends QFunction {
  val effect = Effect.Read

  def apply(n: Unit, ec: EvalContext, pos: Position): Query[R[Literal]] =
    CurrentIdentityFunction(n, ec, pos) map { res =>
      Right(BoolL(res.isRight))
    }
}

object IdentifyFunction extends QFunction {
  val effect = Effect.Read

  private def identify(
    scope: ScopeID,
    id: DocID,
    password: String,
    pos: Position): Query[R[Literal]] =
    ReadAdaptor.getCredentials(scope, id, pos) map {
      case Right(creds) => Right(BoolL(creds matches password))
      case _            => Right(FalseL)
    }

  def apply(
    ref: IdentifierL,
    password: String,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    ref match {
      case RefL(scope, id) => identify(scope, id, password, pos)
      case SetL(set) =>
        ReadAdaptor.setForRead(ec.auth, set, pos at "identify") flatMapT {
          // ensure identify cannot check in the past
          _.snapshotHead(ec.resetValidTime) flatMap {
            case Some((scope, id)) => identify(scope, id, password, pos)
            case None =>
              Query(Right(FalseL))
          }
        }
    }
}

object LoginFunction extends QFunction {
  val effect = Effect.Write

  private def login(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    params: Diff,
    pos: Position): Query[R[Literal]] = {
    val credsQ = ReadAdaptor.getCredentials(scope, id, pos)

    val passQ = Query(Credentials.PasswordField.read(params.fields)) mapLeftT {
      errs =>
        List(ValidationError(errs, pos))
    }

    (credsQ, passQ) parT {
      case (creds, Some(pw)) if creds matches pw =>
        val d = params.update(Token.DocumentField -> Some(creds.documentID))
        val auth = EvalAuth(ec.auth.scopeID, ServerPermissions)
        WriteAdaptor(TokenID.collID).create(ec.copy(auth = auth), None, d, pos)
      case _ =>
        Query(Left(List(AuthenticationFailed(pos))))
    }
  }

  def apply(
    ref: IdentifierL,
    params: Diff,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    ref match {
      case RefL(scope, id) =>
        login(ec, scope, id, params, pos)
      case SetL(set) =>
        ReadAdaptor.setForRead(ec.auth, set, pos at "login") flatMapT {
          // ensure login cannot use passwords from the past
          _.snapshotHead(ec.resetValidTime) flatMap {
            case Some((scope, id)) =>
              login(ec, scope, id, params, pos)
            case None =>
              Query(Left(List(AuthenticationFailed(pos))))
          }
        }
    }
}

object LogoutFunction extends QFunction {
  val effect = Effect.Write

  private val toTrueR: Any => R[Literal] = { _ => Right(TrueL) }

  def apply(
    removeAll: Boolean,
    ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] =
    if (removeAll) {
      ec.auth match {
        case LoginAuth.Identity(id) =>
          Token.removeByDocument(ec.auth.scopeID, id, ec.snapshotTime) map toTrueR
        case _ => Query(Right(FalseL))
      }
    } else {
      ec.auth match {
        case LoginAuth.Source(TokenLogin(tok)) =>
          Token
            .removeToken(ec.scopeID, tok.id)
            .map { _ => Right(TrueL) }
            .recover {
              // `Logout` can be called multiple times, in which case the document
              // won't exist. Just ignore the error if it doesn't exist.
              case WriteFailureException(WriteFailure.DocNotFound(_, _)) =>
                Right(TrueL)
            }
        case _ => Query(Right(FalseL))
      }
    }
}

/** Used to issue JSON Web Tokens.
  *
  * Arguments:
  *
  * subject - A document or role reference.
  *           If omitted the current identity or the current role will be used.
  * expireIn - The number of seconds the token should be valid for.
  *            Defaults to 1800 seconds.
  *
  * @since 4
  */
object IssueAccessJWTFunction extends QFunction {
  val effect = Effect.Write

  private[this] val MinSeconds = 10L
  private[this] val MaxSeconds = 7200L
  private[this] val DefaultSeconds = 1800L

  def apply(
    ref: RefL,
    expireIn: Option[Long],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    expireIn match {
      case Some(value) =>
        if (value < MinSeconds || value > MaxSeconds) {
          return Query.value(
            Left(
              List(
                BoundsError(
                  "expire in",
                  s">= $MinSeconds and <= $MaxSeconds",
                  pos at "expire_in"))))
        }

      case _ => ()
    }

    if (!canIssueJWT(ec)) {
      return Query.value(Left(List(PermissionDenied(Right(ref), pos))))
    }

    getScope(ec, ref) flatMap {
      case Some(scope) =>
        Query.internalJWK flatMap { case (jwk, algorithm) =>
          val issuer =
            JWTToken.CanonicalDBUrlPrefix + Database.encodeGlobalID(ec.scopeID)
          val audience =
            JWTToken.CanonicalDBUrlPrefix + Database.encodeGlobalID(ec.scopeID)
          val issuedAt = Instant.now().getEpochSecond
          val expireAt = issuedAt + expireIn.getOrElse(DefaultSeconds)

          val jwt = JWT.createToken(
            jwk,
            algorithm,
            issuer,
            audience,
            issuedAt,
            expireAt,
            scope)

          Query.value(
            Right(
              ObjectL(
                "token" -> StringL(jwt.getToken)
              )))
        }
      case None =>
        Query.value(Left(List(InvalidJWTScopeError(pos))))
    }
  }

  private def getScope(ec: EvalContext, ref: RefL): Query[Option[String]] =
    ref match {
      case RefL(scope, doc @ DocID(subID, UserCollectionID(coll))) =>
        ReadAdaptor.exists(ec, scope, doc, ec.getValidTime(None)) flatMap {
          case Right(TrueL) =>
            SchemaNames.lookupCachedName(ec.scopeID, coll.toDocID) mapT { name =>
              s"@doc/$name/${subID.toLong}"
            }

          case _ =>
            Query.none
        }

      case RefL(_, id @ DocID(_, RoleID.collID)) =>
        SchemaNames.lookupCachedName(ec.scopeID, id) mapT { name =>
          s"@role/$name"
        }

      case _ =>
        Query.none
    }

  private def canIssueJWT(ec: EvalContext) = {
    ec.auth match {
      case LoginAuth.Source(KeyLogin(key)) =>
        key.role match {
          case Key.AdminRole  => true
          case Key.ServerRole => true
          // todo: create role permissions to control JWT creation
          case Key.UserRoles(_)       => false
          case Key.ServerReadOnlyRole => false
          case Key.ClientRole         => false
        }
      case LoginAuth.Source(RootLogin)      => true
      case LoginAuth.Source(TokenLogin(_))  => false
      case LoginAuth.Source(JWTLogin(_, _)) => false
      case RootAuth                         => true
      case _                                => false
    }
  }
}

/** Returns the current token/key reference or an object representing
  * the payload of the JWT used in the request.
  *
  * @since 4
  */
object CurrentTokenFunction extends QFunction {
  val effect = Effect.Read

  def apply(@unused n: Unit, ec: EvalContext, pos: Position): Query[R[Literal]] =
    ec.auth match {
      case LoginAuth.Source(KeyLogin(key)) if ec.scopeID == key.parentScopeID =>
        Query.value(Right(RefL(key.parentScopeID, key.id.toDocID)))

      case LoginAuth.Source(TokenLogin(token)) =>
        Query.value(Right(RefL(token.scopeID, token.id.toDocID)))

      case LoginAuth.Source(JWTLogin(_, payload)) =>
        Query.value(Right(payload))

      case _ =>
        Query.value(Left(List(InvalidTokenError(pos))))
    }
}

/** Returns True if CurrentToken() returns a value or False case it returns a error.
  *
  * @since 4
  */
object HasCurrentTokenFunction extends QFunction {
  val effect = Effect.Pure

  def apply(n: Unit, ec: EvalContext, pos: Position): Query[R[Literal]] =
    CurrentTokenFunction(n, ec, pos) map {
      case Right(_) => Right(TrueL)
      case Left(_)  => Right(FalseL)
    }
}
