package fauna.model.runtime.stream

import fauna.lang.syntax._
import fauna.model.runtime.fql2.{
  FQLInterpCtx,
  FQLInterpreter,
  ProjectedSet,
  Result,
  ValueSet,
  WhereFilterSet
}
import fauna.model.runtime.fql2.serialization.FQL2ValueMaterializer
import fauna.repo.query.Query
import fauna.repo.values.Value
import fauna.storage.{ Create, Delete, Update }
import scala.annotation.tailrec
import scala.collection.immutable.ArraySeq

object EventTransformer {

  sealed trait Transformation {
    def transform(ctx: FQLInterpCtx, value: Value): Query[Result[Option[Value]]]
  }

  final class Filter(
    pred: Value.Func,
    stackTrace: FQLInterpreter.StackTrace = FQLInterpreter.StackTrace.empty)
      extends Transformation {
    def transform(ctx: FQLInterpCtx, value: Value) =
      WhereFilterSet.evalPredicate(ctx, pred, value, value, stackTrace)
  }

  final class Project(
    fn: Value.Func
  ) extends Transformation {
    def transform(ctx: FQLInterpCtx, value: Value): Query[Result[Option[Value]]] =
      ctx
        .evalApply(fn, ArraySeq(value))
        .mapT(Some(_))
  }

  @inline def empty = new EventTransformer(Seq.empty)

  def extract[A](source: ValueSet): (ValueSet, EventTransformer) = {
    @tailrec
    def extract0(
      source: ValueSet,
      transformations: List[Transformation]
    ): (ValueSet, EventTransformer) =
      source match {
        case set: WhereFilterSet =>
          val filter = new EventTransformer.Filter(set.predicate)
          extract0(set.inner, filter :: transformations)

        case set: ProjectedSet =>
          val projection = new EventTransformer.Project(set.fn)
          extract0(set.inner, projection :: transformations)

        case other =>
          (other, new EventTransformer(transformations))

      }
    extract0(source, Nil)
  }
}

final class EventTransformer(transformations: Seq[EventTransformer.Transformation]) {
  import Result._

  final def transform(
    ctx: FQLInterpCtx,
    value: StreamValue): Query[Result[Option[Event]]] = {

    def materialize0(eventType: EventType.Data, data: Value, isCurr: Boolean) =
      if (isCurr) {
        value.evalCurr(ctx) { (ctx, _) =>
          materialize(ctx, eventType, data, value.cursor)
        }
      } else {
        value.evalPrev(ctx) { (ctx, _) =>
          materialize(ctx, eventType, data, value.cursor)
        }
      }

    def currQ = value.evalCurr(ctx) { applyTransformations(_, _) }
    def prevQ = value.evalPrev(ctx) { applyTransformations(_, _) }

    value.action match {
      case Create =>
        currQ flatMapT {
          case Some(curr) => materialize0(EventType.Add, curr, true)
          case None       => Ok(None).toQuery
        }
      case Update =>
        (currQ, prevQ) parT {
          case (None, None)          => Ok(None).toQuery
          case (Some(curr), None)    => materialize0(EventType.Add, curr, true)
          case (Some(curr), Some(_)) => materialize0(EventType.Update, curr, true)
          case (None, Some(prev))    => materialize0(EventType.Remove, prev, false)
        }
      case Delete =>
        prevQ flatMapT {
          case Some(prev) => materialize0(EventType.Remove, prev, false)
          case None       => Ok(None).toQuery
        }
    }
  }

  private def applyTransformations(
    ctx: FQLInterpCtx,
    doc: Value.Doc): Query[Result[Option[Value]]] = {

    val seedQ: Query[Result[Option[Value]]] =
      Ok(Option(doc)).toQuery

    /** If we fail permissions here then returning None will
      * ensure we end up with the correct stream event.
      * This can happen in the case of an Update, where a document can be updated such that permissions either:
      * - revokes access to read the document
      * In this scenario returning None for the current version will show the stream event as a Remove.
      * - grants access to read the document
      * In this scenario returning None for a permission error on the previous version will show the stream
      * event as an Add.
      */
    ctx.auth.checkReadPermission(ctx.scopeID, doc.id, ctx.systemValidTime).flatMap {
      case false => Ok(None).toQuery
      case true =>
        transformations.foldLeft(seedQ) { case (valueQ, tr) =>
          valueQ flatMapT {
            case Some(value) => tr.transform(ctx, value)
            case None        => Ok(None).toQuery
          }
        }
    }
  }

  private def materialize(
    ctx: FQLInterpCtx,
    eventType: EventType.Data,
    value: Value,
    cursor: Value.EventSource.Cursor
  ): Query[Result[Option[Event]]] = {
    FQL2ValueMaterializer.materialize(ctx, value) mapT { materialized =>
      Some(Event.Data(eventType, materialized, cursor))
    }
  }
}
