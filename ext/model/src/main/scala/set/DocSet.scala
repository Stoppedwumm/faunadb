package fauna.model

import fauna.ast.EvalContext
import fauna.atoms._
import fauna.auth.Auth
import fauna.lang.{ Page, Timestamp }
import fauna.lang.syntax._
import fauna.repo._
import fauna.repo.query.Query
import fauna.storage._
import EventSet._

sealed class DocEventSet(scope: ScopeID, id: DocID) extends EventSet {

  def count = 1

  // NOTE:
  //   The Versions CF schema orders action ascending - the
  //   opposite of SortedIndex/HistoricalIndex - therefore the min/max
  //   values must have actions ordered opposite of
  //   Event.{MinValue/MaxValue}.
  override val minValue =
    SetEvent(
      Resolved(Timestamp.Epoch),
      ScopeID.MinValue,
      DocID.MinValue,
      SetAction.MinValue)

  override val maxValue =
    SetEvent(Unresolved, ScopeID.MaxValue, DocID.MaxValue, SetAction.MaxValue)

  // Document "read" and "history_read" permissions are separate.
  // If [[filteredForRead]] allows to read this set, it should not be
  // filtered based on Document "read" permissions later.
  def isFiltered: Boolean = false

  def isComposite: Boolean = false

  def filteredForRead(auth: Auth) =
    auth.checkHistoryReadPermission(scope, id) map { if (_) Some(this) else None }

  private def toVersionID(evt: Event) =
    VersionID(evt.ts.validTS, evt.action.toDocAction)

  def history(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    asc: Boolean): PagedQuery[Iterable[Elem[Event]]] =
    RuntimeEnv.Default
      .Store(scope)
      .versions(id, toVersionID(from), toVersionID(to), size, asc) mapValuesT { v =>
      selfElem(v.event)
    }

  def sortedValues(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    asc: Boolean) =
    history(ec, from, to, size, asc)

  def snapshot(ec: EvalContext, from: Event, to: Event, size: Int, asc: Boolean) = {
    val (min, max) = if (asc) (from, to) else (to, from)

    val q = if (id >= min.docID && id <= max.docID) {
      RuntimeEnv.Default.Store(scope).get(id, ec.validTime) mapT { v =>
        SetEvent(ec.validTime, v.parentScopeID, v.id, Add, v.ttl)
      }
    } else {
      Query.none
    }

    q map { id => Page(id map { selfElem(_) }) }
  }

  def sparseSnapshot(ec: EvalContext, keys: Vector[Event], asc: Boolean) = {
    val slice = keys collect {
      case k if k.docID == id =>
        selfElem(SetEvent(ec.validTime, scope, id, Add, k.ttl))
    }

    val q = if (slice.nonEmpty) {
      RuntimeEnv.Default.Store(scope).get(id, ec.validTime) mapT { _ =>
        Page[Query](slice)
      }
    } else {
      Query.none
    }

    q map { _.getOrElse(Page[Query](List.empty)) }
  }

  override def snapshotHead(ec: EvalContext) =
    RuntimeEnv.Default.Store(scope).get(id, ec.validTime) mapT { v =>
      (v.parentScopeID, v.docID)
    }

  def shape = EventSet.Shape.Single
}

case class DocSet(scope: ScopeID, id: DocID) extends DocEventSet(scope, id)

case class DocSingletonSet(scope: ScopeID, id: DocID)
    extends DocEventSet(scope, id) {
  override def history(
    ec: EvalContext,
    from: Event,
    to: Event,
    size: Int,
    asc: Boolean) =
    super.history(ec, from, to, size, asc) collectT {
      case Elem(v, _) if (v.action == Update) =>
        None

      case Elem(v, _) =>
        val action = v.action.toSetAction
        Some(selfElem(SetEvent(v.ts.validTS, v.scopeID, v.docID, action, v.ttl)))
    }
}
