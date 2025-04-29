package fauna.exec

import fauna.lang.syntax._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class AsyncQueue[T] {
  private[this] val latch = new AsyncLatch[Boolean]
  private[this] val queue = ListBuffer.empty[T]

  private def getBatch() =
    synchronized {
      val b = queue.result()
      queue.clear()
      b
    }

  override def toString =
    synchronized { queue.mkString("AsyncQueue(", ", ", ")") }

  def add(values: Iterable[T]) = {
    synchronized { queue ++= values }
    latch.signal(true)
  }

  def size = synchronized { queue.size }

  /**
    * Contending calls to poll result in one thread winning the entire
    * pending batch, and all others receiving Nil.
    */
  def poll(idle: FiniteDuration): Future[Option[List[T]]] = {
    val batch = getBatch()

    if (batch.nonEmpty) {
      Future.successful(Some(batch))
    } else {
      implicit val ec = ImmediateExecutionContext
      Timer.Global.completedAfter(latch.await, idle)(FutureFalse) flatMap {
        case true  => poll(idle)
        case false => FutureNone
      }
    }
  }

  def subscribe[R](idle: FiniteDuration)(f: Option[List[T]] => Future[Option[R]])(implicit ec: ExecutionContext): Future[R] =
    poll(idle) flatMap {
      f(_) flatMap {
        case None    => subscribe(idle)(f)
        case Some(r) => Future.successful(r)
      }
    }

  def fold[S, R](seed: S, idle: FiniteDuration)
    (f: (S, Option[List[T]]) => Future[Either[S, R]])(implicit ec: ExecutionContext): Future[R] = {

    var s = seed
    subscribe(idle) { ev =>
      f(s, ev) flatMap {
        case Right(r) => Future.successful(Some(r))
        case Left(s0) =>
          s = s0
          FutureNone
      }
    }
  }
}
