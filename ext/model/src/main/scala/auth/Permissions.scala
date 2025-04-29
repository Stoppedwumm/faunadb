package fauna.auth

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.repo.query._

sealed trait Permissions {

  private[auth] def checkAction(
    scope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean]

  // FIXME: this is only used once, in the perm check for collection reads.
  // That should be ported over to checkAction and this path removed.
  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean]

  private[auth] def isInScope(parent: ScopeID, child: ScopeID): Query[Boolean] = {
    if (parent == child) {
      Query.True
    } else {
      Database.forScope(child) existsT { _ hasAncestor parent }
    }
  }
}

case object NullPermissions extends Permissions {

  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] = Query.False

  private[auth] def checkAction(
    scope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = Query.False
}

case object AdminPermissions extends Permissions {

  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] = Query.True

  private[auth] def checkAction(
    curScope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = {

    action match {
      case ReadInstance(scope, _, _)       => isInScope(curScope, scope)
      case HistoryRead(scope, _)           => isInScope(curScope, scope)
      case ReadIndex(scope, _, _)          => isInScope(curScope, scope)
      case CallFunction(scope, _, _)       => isInScope(curScope, scope)
      case WriteInstance(scope, _, _, _)   => Query.value(curScope == scope)
      case HistoryWrite(scope, _, _, _, _) => Query.value(curScope == scope)
      case CreateInstance(scope, _)        => Query.value(curScope == scope)
      case CreateInstanceWithID(scope, _)  => Query.value(curScope == scope)
      case DeleteInstance(scope, _)        => Query.value(curScope == scope)
      case _: UnrestrictedIndexRead        => Query.False
      case _: UnrestrictedCollectionRead   => Query.False
      case ReadSchema(scope)               => isInScope(curScope, scope)
      case WriteSchema(scope)              => isInScope(curScope, scope)
      case Logging(scope)                  => isInScope(curScope, scope)
    }
  }
}

sealed trait AbstractServerPermissions extends Permissions {

  private val RootClassIDs = Set(
    DatabaseID.collID,
    KeyID.collID,
    RoleID.collID,
    AccessProviderID.collID
  )

  protected def canRead(
    curScope: ScopeID,
    scope: ScopeID,
    id: DocID): Query[Boolean] = {

    if (!(RootClassIDs contains toClassID(id))) {
      isInScope(curScope, scope)
    } else {
      Query.False
    }
  }

  protected def canWrite(
    curScope: ScopeID,
    scope: ScopeID,
    id: DocID): Query[Boolean] = {

    if (!(RootClassIDs contains toClassID(id)) && curScope == scope) {
      Query.True
    } else {
      Query.False
    }
  }

  private def toClassID(id: DocID): CollectionID =
    id.asOpt[CollectionID] getOrElse id.collID
}

case object ServerPermissions extends AbstractServerPermissions {

  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] = Query.True

  private[auth] def checkAction(
    curScope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = {

    action match {
      case ReadInstance(scope, id, _)     => canRead(curScope, scope, id)
      case HistoryRead(scope, id)         => canRead(curScope, scope, id)
      case WriteInstance(scope, id, _, _) => canWrite(curScope, scope, id)
      case CreateInstance(scope, ver) =>
        canWrite(curScope, scope, ver.collID.toDocID)
      case CreateInstanceWithID(scope, ver) => canWrite(curScope, scope, ver.id)
      case DeleteInstance(scope, id)        => canWrite(curScope, scope, id)
      case HistoryWrite(scope, id, _, _, _) => canWrite(curScope, scope, id)
      case CallFunction(scope, _, _)        => isInScope(curScope, scope)
      case ReadIndex(scope, _, _)           => isInScope(curScope, scope)
      case _: UnrestrictedIndexRead         => Query.False
      case _: UnrestrictedCollectionRead    => Query.False
      case ReadSchema(scope)                => isInScope(curScope, scope)
      case WriteSchema(_)                   => Query.False
      case Logging(scope)                   => isInScope(curScope, scope)
    }
  }
}

case object ServerReadOnlyPermissions extends AbstractServerPermissions {

  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] = {
    import ActionNames._

    name match {
      case Read | HistoryRead | SchemaRead | Logging => Query.True
      case _                                         => Query.False
    }
  }

  private[auth] def checkAction(
    curScope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = {

    action match {
      case ReadInstance(scope, id, _) => canRead(curScope, scope, id)
      case HistoryRead(scope, id)     => canRead(curScope, scope, id)
      case ReadIndex(scope, _, _)     => isInScope(curScope, scope)
      case ReadSchema(scope)          => isInScope(curScope, scope)
      case Logging(scope)             => isInScope(curScope, scope)
      case _                          => Query.False
    }
  }
}

/** Used by permission system to execute read-only predicates with udf calls */
private[auth] case object InternalReadOnlyPermissions
    extends AbstractServerPermissions {

  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] = {
    import ActionNames._

    name match {
      case Read | Call | UnrestrictedRead => Query.True
      case _                              => Query.False
    }
  }

  private[auth] def checkAction(
    curScope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = {

    action match {
      case ReadInstance(scope, id, _)         => canRead(curScope, scope, id)
      case CallFunction(scope, _, _)          => Query.value(curScope == scope)
      case UnrestrictedIndexRead(sp, id, _)   => canRead(curScope, sp, id.toDocID)
      case UnrestrictedCollectionRead(sp, id) => canRead(curScope, sp, id.toDocID)
      case _                                  => Query.False
    }
  }
}

sealed trait RoleBasedPermissions extends Permissions {
  val context: RoleEvalContext
}

final case class InstancePermissions(instanceID: DocID, context: RoleEvalContext)
    extends RoleBasedPermissions {

  /** These are default, non-revocable permissions which apply before ABAC */
  private def checkBaselinePerms(scope: ScopeID, action: RoleEvalAction) =
    if (scope != action.scope) {
      Query.False
    } else {
      def isAssociatedToken(s: ScopeID, tokID: TokenID) =
        Token.get(s, tokID).map(_.flatMap(_.document) == Some(instanceID))

      def isAssociatedCredential(s: ScopeID, credID: CredentialsID) =
        Credentials.get(s, credID).map(_.map(_.documentID) == Some(instanceID))

      action match {
        // documents can read associated credentials
        case ReadInstance(s, CredentialsID(id), _) => isAssociatedCredential(s, id)
        // documents can read and delete associated tokes
        case ReadInstance(s, TokenID(id), _) => isAssociatedToken(s, id)
        case DeleteInstance(s, TokenID(id))  => isAssociatedToken(s, id)

        case _ => Query.False
      }
    }

  override private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] = {
    if (context.roles.nonEmpty) {
      Query.value(context.hasAction(resource, name))
    } else {
      Query.False
    }
  }

  override private[auth] def checkAction(
    curScope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = {

    checkBaselinePerms(curScope, action).flatMap {
      case true => Query.True
      case false =>
        if (curScope == action.scope && context.roles.nonEmpty) {
          context.eval(action, Some(instanceID), source)
        } else {
          Query.False
        }
    }
  }
}

object RolePermissions {

  def lookup(scope: ScopeID, roles: Set[RoleID]): Query[RolePermissions] = {
    RoleEvalContext.lookup(scope, roles) map {
      RolePermissions(_)
    }
  }
}

final case class RolePermissions(context: RoleEvalContext)
    extends RoleBasedPermissions {

  private[auth] def hasAction(
    scope: ScopeID,
    resource: DocID,
    name: String): Query[Boolean] =
    Query.value(context.hasAction(resource, name))

  private[auth] def checkAction(
    curScope: ScopeID,
    action: RoleEvalAction,
    source: LoginSource): Query[Boolean] = {

    if (curScope == action.scope) {
      context.eval(action, None, source)
    } else {
      Query.False
    }
  }
}
