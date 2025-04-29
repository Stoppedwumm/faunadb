package fauna.model.runtime.stream

import fauna.auth.Auth
import fauna.lang.{ Page, TimeBound, Timing }
import fauna.model.runtime.fql2.{ IndexSet, SingletonSet, ValueSet }
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.repo.PagedQuery

object Feed {
  @inline def MaxPageSize = ValueSet.MaximumElements
  @inline def DefaultPageSize = ValueSet.DefaultPageSize
  @inline def DefaultTimeout = Subscription.StreamQueryTimeout

  final case class Page(
    events: Seq[(Event, Event.Metrics)],
    cursor: Value.EventSource.Cursor,
    hasNext: Boolean,
    stats: Event.Metrics
  )
}

final class Feed(auth: Auth, source: Value.EventSource) {

  require(source.cursor.nonEmpty, "cursor is undefined")

  def poll(pageSize: Int, deadline: TimeBound): Query[Feed.Page] = {
    val start = Timing.start
    val filter = new EventFilter(source.watchedFields)
    val (set, transformer) = EventTransformer.extract(source.set)
    val replayer = new EventReplayer(auth, filter, transformer)

    Query.snapshotTime.flatMap { snapshotTS =>
      val eventsQ = Query.withMetrics {
        val replayQ =
          set match {
            case SingletonSet(doc: Value.Doc) =>
              replayer.replay(
                auth.scopeID,
                doc.id,
                from = source.cursor.get,
                to =
                  None, // no limit, storage won't return values above the query's snapshot time.
                pageSize = pageSize
              )

            case set: IndexSet =>
              replayer.replay(
                set,
                from = source.cursor.get,
                to =
                  None, // no limit, storage won't return values above the query's snapshot time.
                pageSize = pageSize
              )
            case other =>
              Query.fail(
                new IllegalArgumentException(s"invalid index source found: $other"))
          }

        collectEvents(replayQ, pageSize, deadline)
      }

      eventsQ map { case ((events, cursor, hasMore), stateMetrics) =>
        // NB. Polling on a source that returns no events should rollup the next
        // cursor to the query's snapshot time so that continuously polling on the
        // same feed is not limitted by the collection's retention period.
        //
        // Note that some events may produce no cursors and are typically errors that
        // are not associated with any documents. In those cases the current cursor
        // is preserved to avoid skipping events in between given cursor and the
        // query's snapshot time.
        val next =
          cursor match {
            case Some(cursor) if hasMore => cursor
            case None if events.nonEmpty => source.cursor.get
            case _ => Value.EventSource.Cursor.MinValue.copy(ts = snapshotTS)
          }
        val metrics = Event.Metrics(stateMetrics, start.elapsedMillis)
        Feed.Page(events, next, hasMore, metrics)
      }
    }
  }

  private def collectEvents(
    eventsQ: PagedQuery[Iterable[Event.Processed]],
    pageSize: Int,
    deadline: TimeBound
  ): Query[(
    Seq[(Event, Event.Metrics)],
    Option[Value.EventSource.Cursor],
    Boolean)] = {
    // NB. Change feeds are read-only queries and this method uses sequential
    // semantics. There should be no reason for the query to retry any branches,
    // therefore, using mutable state here should be safe.
    val events = Vector.newBuilder[(Event, Event.Metrics)]
    events.sizeHint(pageSize)

    var cursor = Option.empty[Value.EventSource.Cursor]
    var collected = 0

    @inline def emit(event: (Event, Event.Metrics)) = {
      events += event
      collected += 1
    }

    /** If an error is encountered while iterating, this method will return false, indicating that iteration should
      * not continue. Otherwise it will return true.
      */
    def collectIter(iter: Iterator[Event.Processed]): Boolean = {
      while (iter.hasNext && collected < pageSize) {
        val processed = iter.next()
        // NB. Do not override a previously known cursor.
        cursor = processed.cursor.orElse(cursor)
        processed.lift match {
          case None                       => () // ignore
          case Some((_: Event.Status, _)) => () // ignore
          case Some(event @ (_: Event.ErrorEvent, _)) =>
            emit(event)
            return false // stop
          case Some(other) => emit(other)
        }
      }
      true
    }

    def collectEvents0(
      page: Page[Query, Iterable[Event.Processed]]
    ): Query[(
      Seq[(Event, Event.Metrics)],
      Option[Value.EventSource.Cursor],
      Boolean)] = {
      // NB. Uses an iterator instead of page utils to short-circuit at the page size
      val iter = page.value.iterator
      val canContinue = collectIter(iter)

      if (
        collected < pageSize &&
        canContinue &&
        page.next.isDefined &&
        deadline.hasTimeLeft
      ) {
        val next = page.next.get
        next() flatMap { collectEvents0(_) }
      } else {
        val hasMore = iter.hasNext || page.next.isDefined
        Query.value((events.result(), cursor, hasMore))
      }
    }

    eventsQ flatMap { collectEvents0(_) }
  }
}
