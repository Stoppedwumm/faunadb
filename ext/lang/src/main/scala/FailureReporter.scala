package fauna.lang

import java.io.PrintStream

object FailureReporter {
  object Stderr extends PrintStreamFailureReporter(System.err)

  object Default extends FailureReporter {
    @volatile private[this] var _reporter: FailureReporter = Stderr

    def set(r: FailureReporter): Unit = _reporter = r

    def report(thread: Option[Thread], ex: Throwable) = _reporter.report(thread, ex)
  }
}

trait FailureReporter extends Thread.UncaughtExceptionHandler {
  def report(thread: Option[Thread], ex: Throwable): Unit

  def uncaughtException(thread: Thread, ex: Throwable) = report(Some(thread), ex)
}

class PrintStreamFailureReporter(err: PrintStream) extends FailureReporter {

  def report(thread: Option[Thread], ex: Throwable) = {
    thread foreach { t =>
      err.println(s"Uncaught exception on thread ${t.getName}:")
    }

    ex.printStackTrace(err)
  }
}

class LogFailureReporter(log: (String, Throwable) => Unit) extends FailureReporter {

  def report(thread: Option[Thread], ex: Throwable) = {
    val msg = thread match {
      case Some(t) => s"Uncaught exception on thread ${t.getName}:"
      case None    => s"Uncaught exception:"
    }

    log(msg, ex)
  }
}

