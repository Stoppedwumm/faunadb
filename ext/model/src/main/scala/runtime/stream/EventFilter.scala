package fauna.model.runtime.stream

import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpCtx, ReadBroker }
import fauna.repo.query.Query
import fauna.repo.schema.Path
import fauna.repo.values.Value
import fauna.storage.{ Create, Delete, Update }
import fql.ast.{ Name, Span }

object EventFilter {
  val empty = new EventFilter(Seq.empty)
}

/** Decides if a document version should be part of the stream output. The rules
  * applied are:
  *
  * 1. Discard values based on ABAC rules;
  * 2. If no watched fields, all remaining values are kept;
  * 3. If watching for specific fields:
  *   3.1. Creates and deletes where watched fields are present are kept;
  *   3.2. Updates changing at least one of the watched fields are kept;
  *   3.3. Otherwise discard the value.
  *
  * Note that only metadata and/or stored fields are considered for comparisons as
  * the intent is to mimic what's possible to filter using an index. Future
  * improvements might include pushing this logic to data nodes.
  */
final class EventFilter(watchedFields: Seq[Path]) {

  def keep(ctx: FQLInterpCtx, value: StreamValue): Query[Boolean] =
    (value.action match {
      case Create => ctx.auth.checkReadPermission(ctx.scopeID, value.docID)
      /** We do an OR check here because an update can result in either a Update or Remove event. When it is an Update
        * permissions on the current version are required, for a Remove permissions on the prior version are required.
        * In the EventTransformer we turn permissions errors when rendering the current or prior version into None,
        * which will end up creating the correct stream event for the consumer.
        */
      case Update =>
        (
          ctx.auth.checkReadPermission(ctx.scopeID, value.docID),
          ctx.auth.checkReadPermission(
            ctx.scopeID,
            value.docID,
            Some(value.cursor.ts.prevNano))) par { case (curCheck, prevCheck) =>
          Query.value(curCheck || prevCheck)
        }
      case Delete =>
        ctx.auth.checkReadPermission(
          ctx.scopeID,
          value.docID,
          Some(value.cursor.ts.prevNano))
    }).flatMap {
      case false                      => Query.False
      case _ if watchedFields.isEmpty => Query.True
      case _ if value.isDelete        => value.evalPrev(ctx)(hasWatchedFields)
      case _                          => watchedFieldsChanged(ctx, value)
    }

  private def hasWatchedFields(ctx: FQLInterpCtx, doc: Value.Doc): Query[Boolean] =
    watchedFields.foldLeft(Query.True) { (accQ, path) =>
      accQ flatMap { hasField =>
        if (hasField) {
          extract(ctx, path, doc) map { _.nonEmpty }
        } else {
          Query.False
        }
      }
    }

  private def watchedFieldsChanged(
    ctx: FQLInterpCtx,
    value: StreamValue): Query[Boolean] =
    watchedFields.foldLeft(Query.False) { (accQ, path) =>
      accQ flatMap { changed =>
        if (changed) {
          Query.True
        } else {
          val currQ = value.evalCurr(ctx) { extract(_, path, _) }
          val prevQ = value.evalPrev(ctx) { extract(_, path, _) }
          (currQ, prevQ) par { (currV, prevV) => Query.value(currV != prevV) }
        }
      }
    }

  private def extract(
    ctx: FQLInterpCtx,
    path: Path,
    doc: Value.Doc): Query[Option[Value]] =
    path.elements match {
      case Left(_) :: _ | Nil   => Query.none
      case Right(field) :: rest =>
        // NB. Extracted from index building logic: extract metadata or stored
        // fields at the top level, then project the remaining path form the top
        // level field, if any. This routine does not allow watching across FKs.
        // See the top level comment for details.
        ReadBroker.getField(ctx, doc, Name(field, Span.Null)) flatMap { rValue =>
          rValue.liftValue match {
            case None => Query.none
            case Some(value) =>
              ReadBroker.extractValue(
                ctx,
                computed = false,
                value = value,
                path = rest
              )
          }
        }
    }

}
