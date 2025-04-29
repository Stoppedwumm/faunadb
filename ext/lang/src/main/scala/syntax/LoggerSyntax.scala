package fauna.lang

import language.experimental.macros
import org.apache.logging.log4j.{ LogManager, Logger => SLogger }
import scala.reflect.macros._

case class Logger(inner: SLogger) extends AnyVal {
  def name = inner.getName

  @inline def isTraceEnabled: Boolean = inner.isTraceEnabled
  def trace(message: Any): Unit = macro LoggingMacros.trace1
  def trace(message: Any, t: Throwable): Unit = macro LoggingMacros.trace2

  @inline def isDebugEnabled: Boolean = inner.isDebugEnabled
  def debug(message: Any): Unit = macro LoggingMacros.debug1
  def debug(message: Any, t: Throwable): Unit = macro LoggingMacros.debug2

  @inline def isInfoEnabled: Boolean = inner.isInfoEnabled
  def info(message: Any): Unit = macro LoggingMacros.info1
  def info(message: Any, t: Throwable): Unit = macro LoggingMacros.info2

  @inline def isWarnEnabled: Boolean = inner.isWarnEnabled
  def warn(message: Any): Unit = macro LoggingMacros.warn1
  def warn(message: Any, t: Throwable): Unit = macro LoggingMacros.warn2

  @inline def isErrorEnabled: Boolean = inner.isErrorEnabled
  def error(message: Any): Unit = macro LoggingMacros.error1
  def error(message: Any, t: Throwable): Unit = macro LoggingMacros.error2
}

trait LoggingSyntax {
  def getLogger(): Logger = macro LoggingMacros.getLogger

  def getLogger(scope: String): Logger = Logger(LogManager.getLogger(scope))
}

class LoggingMacros(val c: blackbox.Context) {
  import c.universe._

  def getLogger(): c.Tree =
    q"""_root_.fauna.lang.Logger(
      _root_.org.apache.logging.log4j.LogManager.getLogger(this.getClass.getName))"""

  def trace1(message: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isTraceEnabled) $logger.trace($message.toString)
    }"""
  }

  def trace2(message: c.Tree, t: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isTraceEnabled) $logger.trace($message.toString, $t)
    }"""
  }

  def debug1(message: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isDebugEnabled) $logger.debug($message.toString)
    }"""
  }

  def debug2(message: c.Tree, t: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isDebugEnabled) $logger.debug($message.toString, $t)
    }"""
  }

  def info1(message: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isInfoEnabled) $logger.info($message.toString)
    }"""
  }

  def info2(message: c.Tree, t: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isInfoEnabled) $logger.info($message.toString, $t)
    }"""
  }

  def warn1(message: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isWarnEnabled) $logger.warn($message.toString)
    }"""
  }

  def warn2(message: c.Tree, t: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isWarnEnabled) $logger.warn($message.toString, $t)
    }"""
  }

  def error1(message: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isErrorEnabled) $logger.error($message.toString)
    }"""
  }

  def error2(message: c.Tree, t: c.Tree): c.Tree = {
    val logger = TermName(c.freshName("logger"))
    q"""{
      val $logger = ${c.prefix.tree}.inner
      if ($logger.isErrorEnabled) $logger.error($message.toString, $t)
    }"""
  }
}
