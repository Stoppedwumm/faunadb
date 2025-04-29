package fauna.api.test

import fauna.codex.json._
import fauna.prop._
import fauna.prop.api._
import fauna.repo.DefaultPageSize
import scala.collection.mutable.{ Set => MSet, ListBuffer }
import scala.collection.immutable.SortedSet

trait SetSpec extends QueryAPI21Spec {

  val DefaultRange = 100 to 500

  // No need for `sizeOpt` here unlike `pageDescending`.
  // Used for asserting that paginating the entire history with a particular page size sees all events.
  def pageAscending[T](expected: Iterable[T], reversed: Boolean): Seq[T] = {
    if (reversed) {
      expected.toSeq
    } else {
      expected.toSeq.reverse
    }
  }

  // Need `sizeOpt` to reverse each page in that ordering
  def pageDescending[T](expected: Iterable[T], sizeOpt: Option[Int], reversed: Boolean): Seq[T] = {
    val coll = if (reversed) {
      expected.toSeq.reverse
    } else {
      expected.toSeq
    }

    val size = sizeOpt getOrElse DefaultPageSize
    coll.grouped(size).flatMap { _.reverse }.toSeq
  }

  // for debugging
  def printNiceError[T](
    dir: String,
    evts: Iterable[T],
    ctrl: Iterable[T],
    expected: Iterable[T],
    sizeOpt: Option[Int]): Unit = {

    println(s"\n==== $dir: $sizeOpt")
    evts foreach println
    println("----")
    ctrl foreach println
    println("----")
    expected foreach println
    println("====\n")
  }

  case class Event(
    source: JSObject,
    resource: JSObject,
    ts: Long,
    action: String,
    values: Option[JSObject] = None) {

    def toSetEvent: Event = {
      action match {
        case "create" => copy(action = "add")
        case "delete" => copy(action = "remove")
        case other    => sys.error(s"Unexpected action $other")
      }
    }
  }

  object Event {

    def fromInst(src: JSObject, inst: JSObject) =
      Event(SetRef(src), inst.refObj, inst.ts, "create")

    def fromJS(js: JSObject) = {
      Event(
        (js / "sources" / 0).as,
        (js / "value" / "instance").as,
        (js / "value" / "ts").as,
        (js / "value" / "action").as,
        (js / "value" / "values" / 0).asOpt)
    }
  }

  def toSetEvents(events: Iterable[Event]): Iterable[Event] =
    events map { _.toSetEvent }

  def sortEvents(events: Iterable[Event]): Seq[Event] = {
    events.toSeq sortBy { e =>
      (e.ts * -1, if (e.action == "create" || e.action == "add") 0 else 1)
    }
  }

  def normalizeEvents(events: Iterable[Event], compress: Boolean): Seq[Event] = {
    sortEvents((sortEvents(events) groupBy { _.resource }).values flatMap { evs =>
      val buf = ListBuffer(evs.toSeq: _*)
      var i = 0
      while (buf.size > (i + 1)) {
        if (compress && buf(i).action == buf(i + 1).action) {
          buf.remove(i)
        } else {
          i += 1
        }
      }
      buf.toList
    })
  }

  def historicalJoin(source: Seq[Event], target: Seq[Event]) = {
    val ord = Ordering.by[(Long, String), (Long, Int)] {
      case (t, "create") => (t, 0)
      case (t, _)        => (t, 1)
    }

    val seed = Map.empty[JSObject, SortedSet[(Long, String)]]
    val ranges = source.foldLeft(seed) { (ranges, event) =>
      val resource = event.values getOrElse event.resource

      val set = ranges.getOrElse(resource, SortedSet.empty(ord))
      val range = set + (event.ts -> event.action)
      ranges + (resource -> range)
    }

    sortEvents(target) filter { event =>
      val target = (event.source / "@set" / "terms").as[JSObject]
      ranges.get(target) flatMap { r =>
        r.rangeTo(event.ts -> "delete").lastOption map { _._2 == "create" }
      } getOrElse false
    }
  }

  def eventsForP(
    cls: Collection,
    q: JSValue,
    inst: JSValue,
    tsSet: MSet[JSLong]): Prop[Event] = {
    val tsP = Prop.int(1 to 10000) map { ts => JSLong(inst.ts + ts) }
    val uniqueTSP = Prop.unique(tsP, tsSet)
    anEvent(cls, Some(inst.refObj), ts = uniqueTSP) map { e =>
      Event(SetRef(q), (e / "instance").as, (e / "ts").as, (e / "action").as)
    }
  }

  def eventsP(
    db: Database,
    events: Range = DefaultRange): Prop[(Collection, JSValue, Set[Event])] =
    for {
      cls <- aCollection(db)
      set = Events(Documents(cls.refObj))
      inst <- aDocument(cls)
      addEvent = Event.fromInst(set, inst)
      eSize <- Prop.int(events)
      tsSet = MSet.empty[JSLong]
      events <- eventsForP(cls, set, inst, tsSet) * eSize
    } yield (cls, set, toSetEvents(addEvent +: events).toSet)

  def collectEvents(
    db: Database,
    set: JSValue,
    sizeOpt: Option[Int],
    cursorField: String) = {

    val before = cursorField == "before"
    val size = sizeOpt map { JS(_) } getOrElse JSNull

    @annotation.tailrec
    def gather(cursor: JSValue, prev: Seq[JSObject]): Seq[Event] = {
      val c = if (before) Before(Quote(cursor)) else After(Quote(cursor))
      val query = Paginate(set, c, size = size, sources = true)

      val res = runQuery(query, db)
      val events = prev ++ (res / "data").as[Seq[JSObject]]

      (res / cursorField).asOpt[JSValue] match {
        case Some(JSNull) | None => events map { Event.fromJS(_) }
        case Some(c) if c == cursor => throw new Exception("cursor loop detected")
        case Some(cursor) => gather(cursor, events)
      }
    }

    val startTS = if (before) Long.MaxValue else 0
    gather(JSObject("ts" -> startTS), Seq.empty)
  }

  def eventsDescending(db: Database, set: JSValue, sizeOpt: Option[Int]) =
    collectEvents(db, set, sizeOpt, "before")

  def eventsAscending(db: Database, set: JSValue, sizeOpt: Option[Int]) =
    collectEvents(db, set, sizeOpt, "after")

  def validateEventsDescending(
    db: Database,
    set: JSValue,
    expected: Iterable[Event],
    sizeOpt: Option[Int]): Unit = {
    val evts = eventsDescending(db, set, sizeOpt)
    val ctrl = pageDescending(expected, sizeOpt, reversed = false)
    if (evts != ctrl) {
      printNiceError("Descending", evts, ctrl, expected, sizeOpt)
      fail(s"Descending $sizeOpt did not match")
    }
  }

  def validateEventsAscending(
    db: Database,
    set: JSValue,
    expected: Iterable[Event],
    sizeOpt: Option[Int]): Unit = {
    val evts = eventsAscending(db, set, sizeOpt)
    val ctrl = pageAscending(expected, reversed = false)
    if (evts != ctrl) {
      printNiceError("Ascending", evts, ctrl, expected, sizeOpt)
      fail(s"Ascending $sizeOpt did not match")
    }
  }

  def validateEvents(db: Database, set: JSValue, expected: Iterable[Event], compress: Boolean = true): Unit = {
    val normalized = normalizeEvents(expected, compress)
    val pageSize = (normalized.size / 5) + 1

    val sizes = List(None, Some(4000), Some(pageSize), Some(1)).distinct

    sizes foreach { validateEventsDescending(db, set, normalized, _) }
    sizes foreach { validateEventsAscending(db, set, normalized, _) }
  }

  case class Document(ts: Long, ref: JSObject) extends Ordered[Document] {
    def compare(other: Document) =
      ts compare other.ts match {
        case 0 =>
          (ref / "@ref" / "id").as[String] compare (other.ref / "@ref" / "id").as[String]
        case cmp => cmp
      }
  }

  object Document {
    def fromJS(obj: JSObject): Document = Document(obj.ts, obj.refObj)
  }

  def collP(
    db: Database,
    insts: Range = DefaultRange): Prop[(Collection, JSValue, Set[Document])] =
    for {
      cls <- aCollection(db)
      set = Documents(cls.refObj)
      instCount <- Prop.int(insts)
      insts <- someDocuments(instCount, cls)
      refs = insts map { Document.fromJS(_) } toSet
    } yield (cls, set, refs)

  def collectRefs(
    db: Database,
    set: JSValue,
    sizeOpt: Option[Int],
    cursorField: String,
    cursorValue: JSValue) = {

    val before = cursorField == "before"
    val size = sizeOpt map { JS(_) } getOrElse JSNull

    @annotation.tailrec
    def gather(cursor: JSValue, prev: Seq[JSValue]): Seq[JSValue] = {
      val c = if (before) Before(Quote(cursor)) else After(Quote(cursor))
      val query: JSObject = Paginate(set, c, size = size)

      val res = runQuery(query, db)
      val data = prev ++ (res / "data").as[Seq[JSValue]]

      (res / cursorField).asOpt[JSValue] match {
        case Some(JSNull) | None => data
        case Some(c) if c == cursor => throw new Exception("cursor loop detected")
        case Some(cursor) => gather(cursor, data)
      }
    }

    gather(cursorValue, Seq.empty)
  }

  def collDescending(db: Database, set: JSValue, sizeOpt: Option[Int], reversed: Boolean) =
    collectRefs(db, set, sizeOpt, "before", if (reversed) 0 else JSNull)

  def collAscending(db: Database, set: JSValue, sizeOpt: Option[Int], reversed: Boolean) =
    collectRefs(db, set, sizeOpt, "after", if (reversed) JSNull else 0)

  def validateCollDescending(
    db: Database,
    set: JSValue,
    expected: Iterable[JSObject],
    sizeOpt: Option[Int],
    reversed: Boolean): Unit = {
    val ctrl = pageDescending(expected, sizeOpt, reversed)
    var attemptsMade = 0
    while (true) {
      val coll = collDescending(db, set, sizeOpt, reversed)
      if (coll == ctrl) {
        if (attemptsMade > 0) {
          cancel(s"Descending $sizeOpt succeeded after $attemptsMade attempts")
        } else {
          return
        }
      } else if (attemptsMade < 3) {
        attemptsMade += 1
      } else {
        printNiceError("Descending", coll, ctrl, expected, sizeOpt)
        fail(s"Descending $sizeOpt did not match after $attemptsMade attempts")
      }
    }
  }

  def validateCollAscending(
    db: Database,
    set: JSValue,
    expected: Iterable[JSObject],
    sizeOpt: Option[Int],
    reversed: Boolean): Unit = {
    val ctrl = pageAscending(expected, reversed)
    var attemptsMade = 0
    while (true) {
      val coll = collAscending(db, set, sizeOpt, reversed)
      if (coll == ctrl) {
        if (attemptsMade > 0) {
          cancel(s"Ascending $sizeOpt succeeded after $attemptsMade attempts")
        } else {
          return
        }
      } else if (attemptsMade < 3) {
        attemptsMade += 1
      } else {
        printNiceError("Ascending", coll, ctrl, expected, sizeOpt)
        fail(s"Ascending $sizeOpt did not match after $attemptsMade attempts")
      }
    }
  }

  def validateCollection(db: Database, set: JSValue, expected: Iterable[Document], reversed: Boolean = false): Unit = {
    val normalized = expected.toSeq.sorted.reverseIterator map { _.ref } toSeq
    val pageSize = (normalized.size / 5) + 1

    val sizes = List(None, Some(4000), Some(pageSize), Some(1)).distinct

    sizes foreach { validateCollDescending(db, set, normalized, _, reversed) }
    sizes foreach { validateCollAscending(db, set, normalized, _, reversed) }
  }
}
