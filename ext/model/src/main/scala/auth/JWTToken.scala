package fauna.auth

import fauna.ast._
import fauna.atoms.{ GlobalDatabaseID, GlobalID, RoleID, ScopeID }
import fauna.codex.json._
import fauna.exec.ImmediateExecutionContext
import fauna.lang.syntax._
import fauna.model._
import fauna.net.security._
import fauna.repo.query.Query
import java.time.Instant
import java.util.regex.Pattern
import scala.util.{ Failure, Success }

/** Terminology
  *
  * JWT = JSON Web Token
  * JOSE = JSON Object Signing and Encryption
  * JWS = JSON Web Signature (rfc7515)
  * JWE = JSON Web Encryption (rfc7516)
  * JWK = JSON Web Key (rfc7517)
  * JWKS = JSON Web Key Set
  */

object JWTToken {
  // FIXME: find the correct way to get that string
  val CanonicalDBUrlPrefix = "https://db.fauna.com/db/"
  val CanonicalDBUrlPrefixLength = CanonicalDBUrlPrefix.length
  val SplitClaimPattern = Pattern.compile("\\s+")

  def canonicalDBUrl(globalID: GlobalID): String =
    s"$CanonicalDBUrlPrefix${Database.encodeGlobalID(globalID)}"

  def canonicalDBUrl(db: Database): String =
    canonicalDBUrl(db.globalID)

  def parseGlobalID(jwt: JWT): Option[String] = {
    val audiences = jwt.getAudience filter { aud =>
      aud startsWith CanonicalDBUrlPrefix
    }

    Option.when(audiences.sizeIs == 1) {
      audiences.head.substring(CanonicalDBUrlPrefixLength)
    }
  }

  def decodeGlobalID(globalID: Option[String]): Option[GlobalDatabaseID] =
    globalID flatMap { globalID =>
      Database.decodeGlobalID(globalID) filterNot { _ == GlobalDatabaseID.MinValue }
    }

  def fromToken(token: String, jwkProvider: JWKProvider): Query[Option[Auth]] =
    try {
      val jwt = JWT(token)

      val now = Instant.now()

      if (!JWTValidator.basicValidations(jwt, now)) {
        return Query.none
      }

      val databaseQ = Query.value(decodeGlobalID(parseGlobalID(jwt))) flatMapT {
        id =>
          Database.forGlobalID(id)
      }

      databaseQ flatMapT { db =>
        (jwt.getIssuer match {
          case None => Query.none
          case Some(issuer) if issuer == canonicalDBUrl(db) =>
            Query.internalJWK map { _ =>
              None
            }
          case Some(issuer) =>
            AccessProvider.getByIssuer(db.scopeID, issuer) flatMapT { provider =>
              val jwkQ = Query.stats flatMap { stats =>
                Query.future {
                  implicit val ec = ImmediateExecutionContext
                  jwkProvider.getJWK(provider.jwksUri, jwt.getKeyId) andThen {
                    case Success(_) => stats.incr("JWK.Lookup.Success")
                    case Failure(_) => stats.incr("JWK.Lookup.Failure")
                  }
                }
              }

              jwkQ flatMap { jwk =>
                if (jwk.verify(jwt)) {
                  getJWTAuth(jwt, provider)
                } else {
                  Query.none
                }
              }
            }
        }).recoverWith {
          case _: JWKException => Query.none
          case _: JWTException => Query.none
        }
      }
    } catch {
      case _: JWTException =>
        Query.none
    }

  private def getJWTAuth(
    jwt: JWT,
    accessProvider: AccessProvider): Query[Option[Auth]] = {
    Database.forScope(accessProvider.scopeID) flatMapT { db =>
      val payload = toLiteral(jwt.payload)
      val source = JWTLogin(jwt, payload)

      getRoles(accessProvider, payload, source) flatMap { roles =>
        if (roles.isEmpty) {
          Query.none
        } else {
          RoleEvalContext.lookup(accessProvider.scopeID, roles).flatMap { context =>
            db.account.map { account =>
              val auth = LoginAuth(
                accessProvider.scopeID,
                db,
                account.limiter,
                db,
                RolePermissions(context),
                source,
                None)

              Some(auth)
            }
          }
        }
      }
    }
  }

  def toLiteral(value: JSValue): Literal =
    value match {
      case JSLong(value)      => LongL(value)
      case JSDouble(value)    => DoubleL(value)
      case JSString(value)    => StringL(value)
      case JSRawString(value) => StringL(value.toString)
      case array: JSArray     => ArrayL(array.value.toList map toLiteral)
      case obj: JSObject =>
        ObjectL(obj.value.toList map { case (k, v) => (k, toLiteral(v)) })
      case JSNull  => NullL
      case JSTrue  => TrueL
      case JSFalse => FalseL
      case _       => NullL
    }

  private def getRoles(
    accessProvider: AccessProvider,
    payload: Literal,
    source: LoginSource): Query[Set[RoleID]] = {
    val rolesQ = Query.value(accessProvider.roles) selectMT {
      case AccessProviderRole.Parsed(_, None) =>
        Query.True

      case AccessProviderRole.Parsed(_, Some(predicate)) =>
        evalPredicate(accessProvider.scopeID, payload, source, predicate)
    }

    rolesQ mapT { _.role } map { _.toSet }
  }

  private def evalPredicate(
    scopeID: ScopeID,
    payload: Literal,
    source: LoginSource,
    lambda: LambdaWrapper): Query[Boolean] = {
    lambda.evalPredicate(scopeID, Left(payload), source) map { _ == Right(TrueL) }
  }
}
