package fauna.logging

import fauna.lang.FailureReporter
import java.util.concurrent.atomic.AtomicBoolean
import language.experimental.macros
import org.apache.logging.log4j.LogManager
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.macros._
import scala.util.Try

object ExceptionLogFailureReporter extends FailureReporter {
  def report(thread: Option[Thread], e: Throwable) =
    ExceptionLogging.log.error(ExceptionMessage(thread, e))
}

object ExceptionLogging {
  lazy val log = LogManager.getLogger("exception")

  private[this] val overrideSquelch = new AtomicBoolean(false)

  /** Always squelch exceptions in `thnk`.
    */
  def alwaysSquelch(thnk: => Unit): Unit = {
    // No one else can use `alwaysSquelch` while we're using it.
    if (!overrideSquelch.compareAndSet(false, true)) {
      throw new IllegalStateException("alwaysSquelch is already in use")
    }

    try {
      thnk
    } finally {
      overrideSquelch.set(false)
    }
  }

  /** This should be private to `ExceptionLogging`, but is public for macros to
    * consume it.
    */
  def isOverrideSquelch: Boolean = overrideSquelch.get
}

class ExceptionLoggingMacros(val c: blackbox.Context) {
  import c.universe._

  private def isReleaseBuild =
    Option(System.getProperty("fauna.release"))
      .flatMap(_.toBooleanOption)
      .getOrElse(false)

  private def squelchBase(onDebug: c.Tree, onRelease: c.Tree): c.Tree = {
    if (isReleaseBuild) {
      onRelease
    } else {
      q"""{
        if (_root_.fauna.logging.ExceptionLogging.isOverrideSquelch) {
          ${onRelease}
        } else {
          ${onDebug}
        }
      }"""
    }
  }

  def squelchAndLogException1(thnk: c.Tree): c.Tree =
    squelchBase(
      thnk,
      q"""{
        try {
          ${thnk}
        } catch {
          case e: _root_.java.lang.Throwable =>
            ${c.prefix.tree}.logException(e)
        }
      }"""
    )

  def squelchAndLogException2(thnk: c.Tree, ctch: c.Tree): c.Tree =
    squelchBase(
      thnk,
      q"""{
        try {
          ${thnk}
        } catch {
          case e: _root_.java.lang.Throwable =>
            val alt = ${ctch}.applyOrElse(e, { e: _root_.java.lang.Throwable => throw e })
            ${c.prefix.tree}.logException(e)
            alt
        }
      }"""
    )
}

trait ExceptionLogging {
  protected def exceptionLog = ExceptionLogging.log

  protected def squelchAndLogException(thnk: => Unit): Unit =
    macro ExceptionLoggingMacros.squelchAndLogException1

  protected def squelchAndLogException[T](
    thnk: => T,
    ctch: PartialFunction[Throwable, T]): T =
    macro ExceptionLoggingMacros.squelchAndLogException2

  protected def logException(e: Throwable): Unit =
    ExceptionLogFailureReporter.report(None, e)

  protected def logException(t: Try[_]): Unit =
    if (t.isFailure) logException(t.failed.get)

  protected def logException[T](
    f: Future[T]
  )(implicit ec: ExecutionContext): Future[T] = {
    f onComplete logException
    f
  }
}
