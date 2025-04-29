package fauna.storage

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.storage.index._
import fauna.storage.doc._
import fauna.storage.ir._

/*
 * An event is some action taken on (or on behalf of) a document at
 * a point in time. They are partitioned into two classes: document
 * and set events.
 *
 * Document events are actions taken directly on a document -
 * creates, updates, deletes.
 *
 * Set events are actions taken as a *result* of instance events -
 * addition to or removal from a set.
 */
sealed abstract class Event {
  def scopeID: ScopeID
  def docID: DocID
  def ts: BiTimestamp
  def action: Action
  def tuple: IndexTuple
  def ttl: Option[Timestamp]

  def validTSV = ts.validTSOpt match {
    case None     => TransactionTimeV.Micros
    case Some(ts) => LongV(ts.micros)
  }

  def isCreate: Boolean = action.isCreate
  def isDelete: Boolean = action.isDelete

  def withDocID(id: DocID): Event
  def at(validTS: Timestamp, action: Action): Event
  def at(validTS: Timestamp): Event
  def at(action: Action): Event

  def toSetEvent: SetEvent

  def compareByDocID(other: Event): Int = {
    def compare0(c: Int, next: => Int) =
      if (c == 0) next else c

    compare0(docID.compare(other.docID),
      compare0(ts.compareValid(other.ts),
        action.compare(other.action)))
  }
}

object Event {
  val MinValue = SetEvent(Resolved(Timestamp.Epoch), ScopeID.MinValue, DocID.MinValue, SetAction.MaxValue)
  val MaxValue = SetEvent(Unresolved, ScopeID.MaxValue, DocID.MaxValue, SetAction.MinValue)

  val ord: Ordering[Event] =
    new Ordering[Event] {
      def compare(a: Event, b: Event): Int = (a, b) match {
        case (a: SetEvent, b: SetEvent) => SetEvent.ord.compare(a, b)
        case (a: DocEvent, b) => a.compareByDocID(b)
        case (a, b: DocEvent) => a.compareByDocID(b)
      }
    }

  object SnapshotOrd extends Ordering[Event] {
    def compare(a: Event, b: Event): Int = {
      def compare0(c: Int, next: => Int) =
        if (c == 0) next else c

      val cmp = (a, b) match {
        case (a: SetEvent, b: SetEvent) => SetEvent.compareValues(a.terms, b.terms)
        case (a, b)                     => a.docID compare b.docID
      }

      compare0(cmp,
        compare0(a.ts compareValid b.ts,
          compare0(a.action compare b.action,
            a.ts compareTransaction b.ts)))
    }
  }

  object HistoricalOrd extends Ordering[Event] {
    def compare(a: Event, b: Event): Int = {
      def compare0(c: Int, next: => Int) =
        if (c == 0) next else c

      compare0(a.ts compareValid b.ts,
        compare0(a.action compare b.action,
          compare0(a.docID compare b.docID,
            compare0(a.tuple compare b.tuple,
              a.ts compareTransaction b.ts))))
    }
  }
}

case class DocEvent(
  ts: BiTimestamp,
  scopeID: ScopeID,
  docID: DocID,
  action: DocAction,
  _diff: MapV) extends Event {

  def diff: Diff =
    Diff(_diff)

  // TODO: Might be nice to have a constant.
  def ttl: Option[Timestamp] = _diff.get(List("ttl")) collect {
    case TimeV(ts) => ts
  }

  def diffStream: DataStream =
    IRValueDataStream(_diff)

  lazy val tuple = IndexTuple(scopeID, docID, ttl = ttl)
  def withDocID(id: DocID): Event = copy(docID = id)
  def at(validTS: Timestamp, a: Action) = copy(ts = AtValid(validTS), action = a.toDocAction)
  def at(validTS: Timestamp) = copy(ts = AtValid(validTS))
  def at(a: Action) = copy(action = a.toDocAction)

  lazy val toSetEvent = SetEvent(ts, scopeID, docID, action, Vector.empty, ttl)
}

object DocEvent {
  def apply(validTS: Timestamp, scopeID: ScopeID, docID: DocID, action: DocAction, diff: MapV): DocEvent =
    DocEvent(AtValid(validTS), scopeID, docID, action, diff)
}

case class SetEvent(
  ts: BiTimestamp,
  scopeID: ScopeID,
  docID: DocID,
  action: SetAction,
  values: Vector[IndexTerm],
  ttl: Option[Timestamp] = None)
    extends Event {

  lazy val valuesWithDocID = IndexTerm(docID) +: values
  lazy val terms = if (values.isEmpty) Vector(IndexTerm(docID)) else values
  lazy val tuple = IndexTuple(scopeID, docID, values, ttl)
  def withDocID(id: DocID): Event = copy(docID = id)
  def at(validTS: Timestamp, a: Action) = copy(ts = AtValid(validTS), action = a.toSetAction)
  def at(validTS: Timestamp) = copy(ts = AtValid(validTS))
  def at(a: Action) = copy(action = a.toSetAction)

  def toSetEvent = this
}

object SetEvent {
  implicit val ord: Ordering[SetEvent] =
    new Ordering[SetEvent] {
      def compare(a: SetEvent, b: SetEvent): Int = {
        def compare0(c: Int, next: => Int) =
          if (c == 0) {
            next
          } else {
            c
          }

        compare0(a.ts compareValid b.ts,
          compare0(a.docID compare b.docID,
            compare0(b.action compare a.action,
              compareValues(a.terms, b.terms)
            )
          )
        )
      }
    }

  def apply(ts: BiTimestamp, scopeID: ScopeID, docID: DocID, action: Action, values: Vector[IndexTerm], ttl: Option[Timestamp]): SetEvent =
    action match {
      case Create | Update => SetEvent(ts, scopeID, docID, Add, values, ttl)
      case Delete          => SetEvent(ts, scopeID, docID, Remove, values, ttl)
      case Add             => SetEvent(ts, scopeID, docID, Add, values, ttl)
      case Remove          => SetEvent(ts, scopeID, docID, Remove, values, ttl)
    }

  def apply(ts: BiTimestamp, scopeID: ScopeID, docID: DocID, action: Action, values: Vector[IndexTerm]): SetEvent =
    apply(ts, scopeID, docID, action, values, None)

  def apply(ts: BiTimestamp, scopeID: ScopeID, docID: DocID, action: Action): SetEvent =
    apply(ts, scopeID, docID, action, Vector.empty, None)

  def apply(ts: BiTimestamp, scopeID: ScopeID, docID: DocID, action: Action, ttl: Option[Timestamp]): SetEvent =
    apply(ts, scopeID, docID, action, Vector.empty, ttl)

  def apply(validTS: Timestamp, scopeID: ScopeID, docID: DocID, action: Action, values: Vector[IndexTerm]): SetEvent =
    apply(AtValid(validTS), scopeID, docID, action, values, None)

  def apply(validTS: Timestamp, scopeID: ScopeID, docID: DocID, action: Action, values: Vector[IndexTerm], ttl: Option[Timestamp]): SetEvent =
    apply(AtValid(validTS), scopeID, docID, action, values, ttl)

  def apply(validTS: Timestamp, scopeID: ScopeID, docID: DocID, action: Action): SetEvent =
    apply(AtValid(validTS), scopeID, docID, action, Vector.empty, None)

  def apply(validTS: Timestamp, scopeID: ScopeID, docID: DocID, action: Action, ttl: Option[Timestamp]): SetEvent =
    apply(AtValid(validTS), scopeID, docID, action, Vector.empty, ttl)

  def compareValues(a: Vector[IndexTerm], b: Vector[IndexTerm]): Int = {
    val aSize = a.size
    val bSize = b.size
    val minSize = math.min(aSize, bSize)
    var i = 0
    while (i < minSize) {
      val cmp = a(i) compare b(i)
      if (cmp != 0) return cmp
      i += 1
    }
    aSize - bSize
  }

}
