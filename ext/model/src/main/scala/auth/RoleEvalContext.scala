package fauna.auth

import fauna.ast._
import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.fql2.{
  GlobalContext,
  UserFunction => RuntimeUserFunction
}
import fauna.model.runtime.fql2.stdlib.CollectionCompanion
import fauna.model.runtime.Effect
import fauna.repo.query._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.{ Buffer, Map => MMap, Set => MSet }

private sealed trait RolePolicy {

  def estimatedEvalCost: Int

  def eval(
    action: RoleEvalAction,
    id: Option[DocID],
    source: LoginSource): Query[Boolean]
}

private case object StaticPolicy extends RolePolicy {

  val estimatedEvalCost: Int = 0

  def eval(
    action: RoleEvalAction,
    id: Option[DocID],
    source: LoginSource): Query[Boolean] =
    Query.True
}

private final case class DynamicPolicy(predicate: LambdaWrapper) extends RolePolicy {

  // If the predicate lambda has not been parsed yet, estimatedCallMaxEffect
  // returns Write. This is fine because the Tracker ordering is dynamic based
  // on other internal heuristics.
  //
  // Calls to user defined functions have write effect.
  def estimatedEvalCost = predicate.estimatedCallMaxEffect match {
    case Effect.Pure | Effect.Observation => 1
    case Effect.Read                      => 2
    case Effect.Write                     => 3
  }

  def eval(
    action: RoleEvalAction,
    id: Option[DocID],
    source: LoginSource): Query[Boolean] = {
    val args = predicate match {
      case LambdaWrapper.FQL4(_) =>
        action.lambdaArgs(predicate.arity.numRequiredArgs)
      case LambdaWrapper.FQLX(_, _, _, _) =>
        action.fqlxLambdaArgs(predicate.arity.numRequiredArgs)
    }
    predicate.evalPredicate(
      action.scope,
      args,
      source,
      id,
      action.snapshotTime
    ) map { _ == Right(TrueL) }
  }
}

private object RolePolicy {
  def parse(
    scope: ScopeID,
    action: RoleAction,
    roleName: String,
    actionName: String): Query[Option[RolePolicy]] = {
    action match {
      case StaticRoleAction(true) => Query.some(StaticPolicy)
      case DynamicRoleAction(lambda) =>
        LambdaWrapper.parsePredicate(
          scope,
          lambda,
          Some(s"$roleName:$actionName")) map { l =>
          Some(DynamicPolicy(l))
        }
      case _ => Query.none
    }
  }
}

/** Tracks the number of times a giving policy returned authorized (true).
  * Policies that return the most number of authorized responses are ordered first
  * for better chances of hitting the authorized response within a few queries.
  */
private final class Tracker(private val policy: RolePolicy)
    extends Ordered[Tracker] {

  private val authorizedCount = new AtomicInteger(0)

  def eval(
    action: RoleEvalAction,
    id: Option[DocID],
    source: LoginSource): Query[Boolean] = {
    policy.eval(action, id, source) map { authorized =>
      if (authorized) {
        authorizedCount.updateAndGet { old =>
          // Cache reload resets this counter later
          if (old == Integer.MAX_VALUE) old else old + 1
        }
      }
      authorized
    }
  }

  def compare(other: Tracker): Int = {
    val thisCounter = authorizedCount.get()
    val otherCounter = other.authorizedCount.get()

    Integer.compare(otherCounter, thisCounter) match { // reversed
      case 0 =>
        // Must not return 0 to avoid being excluded from sets
        if (policy.estimatedEvalCost < other.policy.estimatedEvalCost) -1 else 1
      case n => n
    }
  }
}

/** Optimized [[RolePolicy]] evaluation set.
  *
  * It closes the set for writes at the first [[StaticPolicy]] added as
  * its evaluation always returns true.
  *
  * Evaluation rules:
  *   - If empty, returns to false;
  *   - If the set is closed, returns true;
  *   - If a single [[DynamicPolicy]] is present, evaluates the giving policy;
  *   - If multiple [[DynamicPolicy]] are present, track their return state
  *     and evaluate them in the most efficient order.
  *
  * Not suitable for concurrent modifications.
  */
private final class PolicySet {

  private val policies = MSet.empty[RolePolicy]
  private val trackers = Buffer.empty[Tracker]
  private var closed = false

  def +=(other: PolicySet): Unit =
    other.policies foreach { this += _ }

  def +=(policy: RolePolicy): Unit = {
    if (!closed) {
      policy match {
        case StaticPolicy =>
          trackers.clear()
          policies.clear()
          policies += policy
          closed = true

        case DynamicPolicy(_) =>
          if (policies.add(policy)) {
            // Only start tracking return states when needed
            if (policies.size == 2) {
              policies foreach {
                trackers += new Tracker(_)
              }
            } else if (policies.size > 2) {
              trackers += new Tracker(policy)
            }
          }
      }
    }
  }

  def nonEmpty = policies.nonEmpty

  def eval(
    action: RoleEvalAction,
    id: Option[DocID],
    source: LoginSource): Query[Boolean] = {
    if (closed) {
      Query.True

    } else if (policies.isEmpty) {
      Query.False

    } else if (policies.size == 1) {
      policies.head.eval(action, id, source)

    } else {
      // Sort every time because the order of trackers can change at each eval
      val evalQ = trackers.sorted map { _.eval(action, id, source) }

      // Stops at the first authorized response.
      evalQ.foldLeft(Query.False) { (queryA, queryB) =>
        queryA flatMap { if (_) Query.True else queryB }
      }
    }
  }
}

/** Searches policies to evaluate based on the target action. The lookup key is
  * a tuple containing an document id and the desired action, which maps to
  * role's [[fauna.model.Privilege]] actions.
  */
final class RoleEvalContext private (
  val roles: Set[Role],
  private val table: MMap[(DocID, String), PolicySet]) {

  def roleIDs = roles map { _.id }

  def hasAction(resource: DocID, name: String): Boolean =
    table.get((resource, name)).exists(_.nonEmpty)

  def eval(
    action: RoleEvalAction,
    id: Option[DocID],
    source: LoginSource): Query[Boolean] = {
    Query.timing("RoleEvalContext.Eval.Time") {
      table.get((action.resource, action.name)) match {
        case Some(policies) =>
          policies.eval(action, id, source)
        case None =>
          Query.False
      }
    }
  }

  override def hashCode(): Int = 8191 * roles.hashCode()

  // The lookup table is derived from the roles set.
  // If the roles are equal, the lookup tables must also be.
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: RoleEvalContext => roles == other.roles
      case _                      => false
    }
  }
}

object RoleEvalContext {

  val NullContext = new RoleEvalContext(Set.empty, MMap.empty)

  def lookup(
    scope: ScopeID,
    resource: DocID,
    source: LoginSource): Query[RoleEvalContext] =
    RoleMembership.lookupRoles(scope, resource, source) flatMap {
      lookup(scope, _)
    }

  def lookup(scope: ScopeID, roles: Set[RoleID]): Query[RoleEvalContext] =
    if (roles.isEmpty) {
      Query.value(NullContext)
    } else {
      Cache.roleECByRoles(scope, roles) getOrElseT NullContext
    }

  def getECByRolesUncached(
    scope: ScopeID,
    roleIDs: Set[RoleID]): Query[Option[RoleEvalContext]] = {
    val roleQs = roleIDs map { Role.get(scope, _) }
    roleQs.sequence flatMap { roles =>
      buildContext(scope, roles.flatten.toSet)
    }
  }

  private def buildContext(
    scope: ScopeID,
    roles: Set[Role]): Query[Option[RoleEvalContext]] = {
    if (roles.isEmpty) {
      return Query.none
    }

    val actions = for {
      role                 <- roles
      privilege            <- role.privileges
      (actionName, action) <- privilege.actions.getOrElse(Nil)
    } yield {
      (privilege.resource, role.name, actionName, action)
    }

    actions
      .map { case (resource, roleName, actionName, action) =>
        RolePolicy.parse(scope, action, roleName, actionName) flatMapT { policy =>
          val docIDQ = resource match {
            case Left(name) =>
              GlobalContext.lookupSingletonOrUDFActive(scope, name) flatMap {
                case Some(s: CollectionCompanion) =>
                  Query.value(Some(s.collID.toDocID))

                case Some(f: RuntimeUserFunction) =>
                  Query.value(Some(f.funcID.toDocID))

                case _ =>
                  Query.value(None)
              }

            case Right(docID) =>
              Query.value(Some(docID))
          }

          docIDQ mapT { docID =>
            (docID, actionName, policy)
          }
        }
      }
      .sequence
      .map { actions =>
        val table = MMap.empty[(DocID, String), PolicySet]

        actions.flatten foreach { case (resource, actionName, policy) =>
          val key = (resource, actionName)
          val set = table.getOrElseUpdate(key, new PolicySet)
          set += policy
        }

        Some(new RoleEvalContext(roles, table))
      }
  }
}
