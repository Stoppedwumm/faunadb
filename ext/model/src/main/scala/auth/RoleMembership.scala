package fauna.auth

import fauna.ast._
import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.repo.query._

final case class ParsedMembership(
  scopeID: ScopeID,
  roleID: RoleID,
  predicate: Option[LambdaWrapper]) {

  def canAssign(id: DocID, source: LoginSource): Query[Boolean] = {
    predicate match {
      case None         => Query.True
      case Some(lambda) => evalPredicate(id, lambda, source)
    }
  }

  private def evalPredicate(
    id: DocID,
    lambda: LambdaWrapper,
    source: LoginSource): Query[Boolean] =
    Query.withSpan("role.eval") {
      lambda.evalPredicate(scopeID, Left(RefL(scopeID, id)), source, Some(id)) map {
        _ == Right(TrueL)
      }
    }
}

object ParsedMembership {
  def parse(
    scope: ScopeID,
    role: RoleID,
    membership: Membership): Query[ParsedMembership] = {

    membership match {
      case Membership(_, None) =>
        Query.value(ParsedMembership(scope, role, None))

      case Membership(_, Some(lambda)) =>
        LambdaWrapper.parsePredicate(scope, lambda) map { parsed =>
          ParsedMembership(scope, role, Some(parsed))
        }
    }
  }
}

object RoleMembership {

  def byCollectionUncached(
    scope: ScopeID,
    id: Either[String, CollectionID]
  ): Query[Option[Set[ParsedMembership]]] = {
    def parseMembership(roleID: RoleID) = {
      val memberships =
        Role.get(scope, roleID) flatMapT { role =>
          role.membership
            .filter { _.resource == id }
            .map { ParsedMembership.parse(scope, roleID, _) }
            .sequence
            .map { Some(_) }
        }
      memberships getOrElseT Iterable.empty
    }

    val rolesQ = Role.byMembership(scope, id).takeT(Role.MaxRolesPerCollection)
    val parsedQ = rolesQ.flattenT flatMapT { parseMembership(_) }
    parsedQ map { ps => Some(ps.toSet) }
  }

  def lookupRoles(
    scope: ScopeID,
    id: DocID,
    source: LoginSource): Query[Set[RoleID]] = {
    val memberships = lookupMembership(scope, id)

    Query.timing("RoleMembership.Eval.Time") {
      val assigned = memberships selectMT { _.canAssign(id, source) }
      assigned mapT { _.roleID } map { _.toSet }
    }
  }

  private def lookupMembership(
    scope: ScopeID,
    id: DocID): Query[Set[ParsedMembership]] = {

    // different roles can have the same membership
    // v4 roles index membership by collID
    // v10 roles index membership by name
    // we need to get membership by both terms to not miss any role
    val membershipByNameQ = Collection.get(scope, id.collID) flatMapT { coll =>
      Cache.roleMembershipByCollection(scope, Left(coll.name))
    } getOrElseT Set.empty

    val membershipByCollIDQ =
      Cache.roleMembershipByCollection(scope, Right(id.collID)) getOrElseT Set.empty

    (membershipByNameQ, membershipByCollIDQ) par {
      case (membershipByName, membershipByCollID) =>
        Query.value(membershipByName ++ membershipByCollID)
    }
  }
}
