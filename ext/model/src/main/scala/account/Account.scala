package fauna.model.account

import fauna.atoms._
import fauna.flags.{ AccountFlags, Feature, Value => FFValue }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model.{ Cache, Database }
import fauna.model.schema.SchemaCollection
import fauna.repo.query.Query
import fauna.repo.service.rateLimits.OpsLimiter
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import scala.reflect.ClassTag

final case class Account(
  accountID: AccountID,
  flags: AccountFlags,
  limiter: OpsLimiter)

object Account extends ExceptionLogging {
  def get(accountID: AccountID): Query[Account] = Cache.accountByID(accountID)

  def forScope(scope: ScopeID): Query[Option[Account]] =
    Database.forScope(scope).flatMapT { db => get(db.accountID).map(Some(_)) }

  def flagForAccount[V <: FFValue: ClassTag](
    account: AccountID,
    feat: Feature[AccountID, V]): Query[V#T] =
    Account.get(account).map(_.flags.get(feat))

  def flagForScope[V <: FFValue: ClassTag](
    scope: ScopeID,
    feat: Feature[AccountID, V]): Query[V#T] =
    Account.forScope(scope).flatMap {
      case Some(account) => Query.value(account.flags.get(feat))
      case None =>
        Account.default.map { account =>
          val value = account.flags.get(feat)

          squelchAndLogException(new IllegalStateException(
            s"Failed to lookup flag $feat for scope $scope, as it does not exist. Using the value for the default account $value"))

          value
        }
    }

  def getUncached(accountID: AccountID): Query[Account] = {
    if (accountID == AccountID.Root) {
      Account.defaultUncached
    } else {
      Account.getSettingsUncached(accountID).flatMap {
        case Some(settings) =>
          if (settings.id != Some(accountID)) {
            squelchAndLogException(new IllegalStateException(
              s"Account ID in settings did not match the account ID looked up: ${settings.id} != $accountID"))
          }

          for {
            repo    <- Query.repo
            flags   <- repo.accountFlags(accountID, settings.ffProps)
            limiter <- repo.limiters.get(accountID, flags, settings.limits)
          } yield Account(accountID, flags, limiter)

        case None =>
          squelchAndLogException(
            new IllegalStateException(
              s"Account $accountID does not exist, returning the default account"))

          Account.defaultUncached
      }
    }
  }

  def default: Query[Account] = Account.get(AccountID.Root)

  private def defaultUncached: Query[Account] = for {
    repo  <- Query.repo
    flags <- repo.accountFlags(AccountID.Root, Map.empty)
  } yield Account(AccountID.Root, flags, PermissiveOpsLimiter)

  /** Returns an empty account. Should only be used by unit tests (that don't
    * need feature flags), or when producing the initial auth for the auth
    * lookup query.
    *
    * Instead, prefer `Account.default`, which will lookup the flags for the
    * default account correctly.
    *
    * In practice, the main difference is that flags defined for the root
    * account (ie, flags with the property `account: 0`) will be skipped with
    * this "empty" account.
    */
  val Empty = Account(AccountID.Root, AccountFlags.forRoot, PermissiveOpsLimiter)

  private def getSettingsUncached(
    accountID: AccountID): Query[Option[AccountSettings]] =
    Database.scopeByAccountID(accountID).flatMapT { scope =>
      Database.getUncached(scope).flatMapT { db =>
        SchemaCollection.Database(db.parentScopeID).get(db.id).flatMapT { doc =>
          val settings =
            doc.data(Database.AccountField).map(AccountSettings.fromData)

          Query.value(settings)
        }
      }
    }
}
