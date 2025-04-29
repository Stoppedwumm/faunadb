package fauna.repo.cache

import fauna.logging.ExceptionLogging
import fauna.repo.{ RepoContext, SchemaContentionException }
import fauna.repo.query.{ QFail, Query }
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

object QueryLoadingValue {
  sealed trait UpdateRes
  object UpdateRes {
    case object NotPresent extends UpdateRes
    case object Updated extends UpdateRes
    case object Invalidated extends UpdateRes
  }

  val Init = Success(None)
}

/** A cache value that can reload itself.
  *
  * The cache value is loaded by a single thread. Any other thread attempting to
  * read the value while it's being loaded will block until the load is complete.
  * If the result is empty all awaiting threads will see an empty value.
  * Subsequent reads of the empty value will retrigger the load process.
  *
  * The value stored in the `AtomicReference` may be one of:
  * - A `Success(None)` could mean either:
  *   - The value has not been loaded yet.
  *   - The query has been ran, and returned a None.
  * - A `Success(Some)`, which means the value has been loaded.
  * - A `Failure`, which means the query failed and the next attempt to read
  *   this value will reload it.
  * - An incomplete promise, which means that a thread is currently loading the
  *   value. That thread will complete the promise once it is done loading.
  *
  * Note that the complete promise storing a failure is distinct from a `None`. The
  * failure causes the second read in `QueryCache` to propogate the exception, which
  * is necessary to avoid spinning on a failing query.
  */
final class QueryLoadingValue[K, V](key: K, queryForKey: K => Query[Option[V]])
    extends AtomicReference[Any](QueryLoadingValue.Init)
    with ExceptionLogging {

  import QueryLoadingValue._

  private def query: Query[Option[V]] = QFail.guard(queryForKey(key))

  final def cachedValue: Either[Future[Option[V]], Try[Option[V]]] =
    get() match {
      case p: Promise[_] => Left(p.asInstanceOf[Promise[Option[V]]].future)
      case t             => Right(t.asInstanceOf[Try[Option[V]]])
    }

  final def load(repo: RepoContext)(
    implicit ec: ExecutionContext): Future[Option[V]] =
    repo.result(query) transform { t =>
      t.failed.foreach {
        case _: SchemaContentionException => ()
        case err =>
          logException(
            new IllegalStateException(s"Failed to load value for key $key", err))
      }

      val res = t match {
        case Success(v)     => if (v.value.isDefined) Success(v.value) else Init
        case t @ Failure(_) => t.asInstanceOf[Try[Option[V]]]
      }

      // satisfy any promise waiters w/ the loaded value. We also propagate any
      // exception here.
      getAndSet(res) match {
        case p: Promise[_] =>
          p.asInstanceOf[Promise[Any]].complete(res)
        case _ => ()
      }

      res
    }

  final def tryLoad(repo: RepoContext, prev: Any)(
    implicit ec: ExecutionContext): Option[Future[Option[V]]] = {

    val p = Promise[Option[V]]()
    if (compareAndSet(prev, p)) {
      Some(load(repo))
    } else {
      None
    }
  }

  @annotation.tailrec
  final def updateIfPresent(f: V => Option[V]): UpdateRes =
    get() match {
      case prev @ Success(Some(v)) =>
        val (next, res) = f(v.asInstanceOf[V]) match {
          case v @ Some(_) => (Success(v), UpdateRes.Updated)
          case None        => (Init, UpdateRes.Invalidated)
        }
        if (compareAndSet(prev, next)) {
          res
        } else {
          updateIfPresent(f)
        }
      case _ =>
        UpdateRes.NotPresent
    }
}
