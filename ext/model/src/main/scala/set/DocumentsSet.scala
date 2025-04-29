package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.model.schema.NativeIndex
import fauna.repo._
import fauna.repo.query._
import fauna.storage._
import fauna.storage.index._
import fauna.storage.ir._

case class DocumentsSet(
  scopeID: ScopeID,
  collID: CollectionID,
  isFiltered: Boolean = true)
    extends EventSet {

  val set =
    collID match {
      case DatabaseID.collID       => SchemaSet.Databases(scopeID)
      case CollectionID.collID     => SchemaSet.Collections(scopeID)
      case IndexID.collID          => SchemaSet.Indexes(scopeID)
      case UserFunctionID.collID   => SchemaSet.UserFunctions(scopeID)
      case RoleID.collID           => SchemaSet.Roles(scopeID)
      case AccessProviderID.collID => SchemaSet.AccessProviders(scopeID)
      case KeyID.collID            => SchemaSet.Keys(scopeID)

      // exclude credentials and tokens, given they are not configuration docs like
      // above.
      case CredentialsID.collID | TokenID.collID | _ =>
        val terms = Vector(IndexTerm(DocIDV(collID.toDocID)))
        IndexSet(NativeIndex.DocumentsByCollection(scopeID), terms)
    }

  def count = set.count

  def shape: EventSet.Shape = set.shape

  def isComposite: Boolean = false

  def filteredForRead(auth: Auth): Query[Option[EventSet]] = {
    auth.checkUnrestrictedReadPermission(scopeID, collID) map { allowed =>
      if (allowed) Some(this.copy(isFiltered = false)) else Some(this)
    }
  }

  def snapshot(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean): PagedQuery[Iterable[EventSet.Elem[Event]]] =
    set.snapshot(ec, from, to, size, ascending)

  def history(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean): PagedQuery[Iterable[EventSet.Elem[Event]]] =
    set.history(ec, from, to, size, ascending)

  def sparseSnapshot(
    ec: EvalContext,
    keys: Vector[Event],
    ascending: Boolean): PagedQuery[Iterable[EventSet.Elem[Event]]] =
    set.sparseSnapshot(ec, keys, ascending)

  def sortedValues(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    ascending: Boolean): PagedQuery[Iterable[EventSet.Elem[Event]]] =
    set.sortedValues(ec, from, to, size, ascending)
}
