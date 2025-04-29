package fauna.ast

import fauna.model._
import fauna.model.runtime.Effect
import fauna.repo.query._

/** Pagination is controlled via a cursor and a direction which is defined
  * by the "ascending" argument. The "ascending" argument points backward in
  * the pagination order if its value is "false", it points forward otherwise.
  * The contents of each page are returned in ascending order. Backward pagination
  * is exclusive, and forward is inclusive.
  *
  * Arrays are naturally ordered lexically by their values, ascending. Therefore,
  * the first page of a set will contain the lowest values. Following successive
  * forward pages will paginate through the set in ascending value order. Following
  * backward pages will paginate in descending order.
  *
  * Events use "ts" as their natural order, sorted ascending. Therefore, the first
  * page of a set's events includes the earliest events in that set. Following
  * successive forward pages will paginate forward in time. Following backward pages
  * will paginate backward in time.
  */
object PaginateFunction {

  object PaginateAfter extends QFunction {
    val effect: Effect = Effect.Read

    def apply(
      setL: IdentifierL,
      after: Option[Literal],
      ts: Option[AbstractTimeL],
      size: Option[Long],
      events: Option[Boolean],
      sources: Option[Boolean],
      ec: EvalContext,
      pos: Position): Query[R[Literal]] = {

      val set = toEventSet(setL, events)

      def paginate(cursor: Option[Cursor]): Query[R[Literal]] =
        ReadAdaptor.paginate(
          set,
          cursor,
          ts,
          size,
          sources,
          ascending = true,
          ec,
          pos,
          Some("paginate"))

      after map {
        CursorParser.lowerBoundCursor(set, _, ec.scopeID, ec.apiVers, pos at "after")
      } match {
        case None                => paginate(None)
        case Some(Right(cursor)) => paginate(Some(cursor))
        case Some(Left(errors))  => Query.value(Left(errors))
      }
    }
  }

  object PaginateBefore extends QFunction {
    val effect: Effect = Effect.Read

    def apply(
      setL: IdentifierL,
      before: Literal,
      ts: Option[AbstractTimeL],
      size: Option[Long],
      events: Option[Boolean],
      sources: Option[Boolean],
      ec: EvalContext,
      pos: Position): Query[R[Literal]] = {

      val set = toEventSet(setL, events)

      def paginate(cursor: Option[Cursor]): Query[R[Literal]] =
        ReadAdaptor.paginate(
          set,
          cursor,
          ts,
          size,
          sources,
          ascending = false,
          ec,
          pos,
          Some("paginate"))

      CursorParser.lowerBoundCursor(
        set,
        before,
        ec.scopeID,
        ec.apiVers,
        pos at "before") match {
        case Right(cursor) => paginate(Some(cursor))
        case Left(errors)  => Query.value(Left(errors))
      }
    }
  }

  object PaginateCursor extends QFunction {
    val effect: Effect = Effect.Read

    def apply(
      setL: IdentifierL,
      cursor: Literal,
      ts: Option[AbstractTimeL],
      size: Option[Long],
      events: Option[Boolean],
      sources: Option[Boolean],
      ec: EvalContext,
      pos: Position): Query[R[Literal]] = {

      val set = toEventSet(setL, events)

      def paginate(cursor: Option[Cursor], ascending: Boolean): Query[R[Literal]] =
        ReadAdaptor.paginate(
          set,
          cursor,
          ts,
          size,
          sources,
          ascending,
          ec,
          pos,
          Some("paginate"))

      CursorParser.parseCursorObj(
        set,
        cursor,
        ec.scopeID,
        ec.apiVers,
        pos at "cursor") match {
        case Right((cursor, ascending)) => paginate(cursor, ascending)
        case Left(errors)               => Query.value(Left(errors))
      }
    }
  }

  private def toEventSet(
    refOrSetL: IdentifierL,
    events: Option[Boolean]): EventSet = {

    val set = refOrSetL match {
      case RefL(scope, id) => DocSet(scope, id)
      case SetL(set)       => set
    }

    if (!set.shape.isHistorical && events.contains(true)) {
      HistoricalSet(set)
    } else {
      set
    }
  }
}
