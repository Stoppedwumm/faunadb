package fauna.storage

import fauna.atoms._
import fauna.exec.ImmediateExecutionContext
import fauna.flags.EnableMVTReadHints
import fauna.lang._
import fauna.storage.cassandra.{ CollectionStrategy, ColumnFamilySchema }
import io.netty.buffer.Unpooled
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.control.NoStackTrace
import scala.util.Success

package object api {

  /** Type alias to commonly used page types. */
  private[api] type MultiPage[A] = PagedFuture[Iterable[A]]

  // Helper function to setup C* predicates in various read ops.
  private[api] def setupPredicates(
    schema: ColumnFamilySchema,
    fromPred: Predicate,
    toPred: Predicate,
    order: Order) = {

    schema.validatePredicate(fromPred)
    schema.validatePredicate(toPred)

    val (fromC, toC) =
      order match {
        case Order.Ascending  => (Predicate.GTE, Predicate.LTE)
        case Order.Descending => (Predicate.LTE, Predicate.GTE)
      }

    val from =
      schema
        .encodePrefix(fromPred, fromC)
        .getOrElse(Unpooled.EMPTY_BUFFER)

    val to =
      schema
        .encodePrefix(toPred, toC)
        .getOrElse(Unpooled.EMPTY_BUFFER)

    (fromPred.rowKey, from, to)
  }

  private[api] def withMVTHint[A](
    ctx: Storage.Context,
    scopeID: ScopeID,
    collID: CollectionID,
    minValidTS: Timestamp)(fn: => Future[A]): Future[A] = {
    implicit val ec = ImmediateExecutionContext
    fn andThen { case Success(_) =>
      ctx.hostFlags() collect {
        case Some(flags) if flags.get(EnableMVTReadHints) =>
          CollectionStrategy.hints.put(scopeID, collID, minValidTS)
      }
    }
  }
}

package api {

  /** Thrown by read ops asserting that its valid time is at or above coll's MVT. */
  final case class ReadValidTimeBelowMVT(
    validTS: Timestamp,
    mvt: Timestamp,
    collID: CollectionID)
      extends Exception(
        s"The valid time of $validTS is below collection's $collID mvt: $mvt.")
      with NoStackTrace

  object ReadValidTimeBelowMVT {

    @inline def maybeThrow(
      validTS: Timestamp,
      mvt: Timestamp,
      collID: CollectionID) =
      if (validTS < mvt) throw ReadValidTimeBelowMVT(validTS, mvt, collID)

    @inline def maybeThrow(validTS: Timestamp, mvtMap: MVTMap) =
      mvtMap.findInvalid(validTS) foreach { case (collID, mvt) =>
        throw ReadValidTimeBelowMVT(validTS, mvt, collID)
      }
  }

  private[api] object MultiPage {

    def empty[A]: MultiPage[A] =
      MultiPage(Page[Future](Iterable.empty[A]))

    implicit def apply[A](iter: Iterable[A]): MultiPage[A] =
      Future.successful(Page[Future](iter))

    implicit def apply[A](page: Page[Future, Iterable[A]]): MultiPage[A] =
      Future.successful(page)
  }
}
