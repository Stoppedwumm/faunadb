package fauna

import fauna.lang.Local
import scala.collection.immutable.ListMap
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Try

package object trace {

  private val localTracer = new Local[PrintTracer]

  def isTracingEnabled = localTracer().isDefined

  def traceOut(msg: => Any) = if (isTracingEnabled) println(msg)

  def traceMsg(msg: => Any) = localTracer() foreach { _.println(msg) }

  def traceError(e: Throwable, label: String = "Error", stackTraceLength: Int = 50) =
    if (isTracingEnabled) {
      traceMsg(s"$label: $e, ${e.getMessage}")
      if (stackTraceLength > 0) traceMsg(e.getStackTrace.take(stackTraceLength) map { "  "+_ } mkString "\n")
    }

  def traceValue[T](value: T) = {
    localTracer() foreach { _.println(value) }
    value
  }

  def traceFuture[T](f: => Future[T])(filter: (CharSequence, Try[T]) => Future[T]): Future[T] = {
    implicit val ec = ExecutionContext.parasitic // XXX: can't access IEC

    localTracer.let(new PrintTracer) {
      f transformWith { r =>
        val log = localTracer().get.log
        filter(log, r)
      }
    }
  }

  def traceBlocking[T](report: CharSequence => Unit)(f: => T): T = {
    implicit val ec = ExecutionContext.parasitic // XXX: can't access IEC
    Await.result(traceFuture(Future(f)) { (l, f) => report(l); Future.fromTry(f) }, Duration.Inf)
  }

  def printTrace[T](f: => T) = traceBlocking(println)(f)

  def printTraceF[T](f: => Future[T]) = traceFuture(f) { (l, f) => println(l); Future.fromTry(f) }
}

package trace {
  class PrintTracer {
    private var _threadLogs = ListMap.empty[Long, StringBuilder]

    def println(msg: Any): Unit = {
      val s = msg.toString
      val tid = 0L // Thread.currentThread.getId

      val log = synchronized {
        _threadLogs.getOrElse(tid, {
          val b = new StringBuilder
          _threadLogs += (tid -> b)
          b
        })
      }

      if (s eq null)
        log.append("NULL\n")
      else try {
        log.append(s)
        if (s.last != '\n') log.append("\n")
      } catch {
        case _: ArrayIndexOutOfBoundsException => log.append("???")
      }
    }

    def log: CharSequence = {
      synchronized { _threadLogs.values.toList } match {
        case Nil => ""
        case l   => l reduceLeft { _ append _ }
      }
    }

    def clear(): Unit = synchronized { _threadLogs = ListMap.empty[Long, StringBuilder] }
  }
}
