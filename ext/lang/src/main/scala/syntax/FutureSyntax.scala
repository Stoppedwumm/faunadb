package fauna.lang

import java.util.concurrent.atomic.AtomicInteger
import language.experimental.macros
import language.implicitConversions
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.reflect.macros._
import scala.util.{ Failure, Success, Try }

trait FutureSyntax {
  val FutureTrue = Future.successful(true)
  val FutureFalse = Future.successful(false)
  val FutureNone = Future.successful(None)

  final def GuardFuture[T](fut: Future[T]): Future[T] =
    macro FutureSyntax.GuardFutureImpl[T]

  implicit def futureOps[T](f: Future[T]) = new FutureSyntax.FutureOps(f)

  implicit def promiseOps[T](p: Promise[T]) = new FutureSyntax.PromiseOps(p)
}

object FutureSyntax {
  private val toUnit: Try[Any] => Future[Unit] = {
    case Success(_) => Future.unit
    case t          => Future.fromTry(t.asInstanceOf[Try[Unit]])
  }

  final class FutureOps[T](val f: Future[T]) extends AnyVal {
    def unit: Future[Unit] = f.transformWith(toUnit)(ExecutionContext.parasitic) // XXX: can't access IEC

    def ensure(run: => Unit)(implicit executor: ExecutionContext): Future[T] = {
      f onComplete { _ => run }
      f
    }

    def before[S](next: => Future[S])(implicit ev: f.type <:< Future[Unit], executor: ExecutionContext): Future[S] = {
      val p = Promise[S]()
      ev(f) onComplete {
        case Success(_) => p completeWith next
        case Failure(t) => p failure t
      }
      p.future
    }
  }

  final class PromiseOps[T](val p: Promise[T]) extends AnyVal {
    def setDone()(implicit ev: p.type <:< Promise[Unit]) =
      ev(p).trySuccess(())
  }

  def GuardFutureImpl[T](c: blackbox.Context)(fut: c.Tree): c.Tree = {
    import c.universe._

    q"""try { $fut } catch {
      case _root_.scala.util.control.NonFatal(e) =>
        _root_.scala.concurrent.Future.failed(e)
    }"""
  }

  /**
    * Returns the first future of the list to successfully complete.
    * If all futures fail the resulting future fails with the last exception.
    * Be careful, you probably only want to use this with futures which actually finish,
    * otherwise in case of failing futures the caller may be left waiting indefinitely,
    * and additionally a Promise will be leaked.
    */
  final def firstSuccessOf[T](futures: Seq[Future[T]]): Future[T] = {
    futures match {
      case Nil => Future.never
      case f :: Nil => f
      case _ =>
        implicit val ec = ExecutionContext.parasitic // XXX: can't access IEC

        val waiters = new AtomicInteger(futures.size)

        val p = Promise[T]()
        futures foreach {
          _.onComplete {
            case f @ Failure(_) =>
              // effectively fails the promise with the last seen failure
              if (waiters.decrementAndGet == 0) p.tryComplete(f)
            case s @ Success(_) =>
              p.tryComplete(s)
          }
        }
        p.future
    }
  }
}
