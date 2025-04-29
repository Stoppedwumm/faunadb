package fauna.logging

import java.nio.file.{ Files, Paths }
import org.apache.logging.log4j._
import org.apache.logging.log4j.core.appender._
import org.apache.logging.log4j.core.appender.rolling.{
  DefaultRolloverStrategy,
  SizeBasedTriggeringPolicy
}
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.LoggerContext
import scala.io.Source

package object test {

  def resetRootLogger() = {
    val root = LoggerContext
      .getContext(false)
      .getConfiguration
      .getRootLogger
    root.getAppenders().keySet.forEach(root.removeAppender)
    root.setLevel(Level.WARN)
    root
  }

  def flushRootLog() =
    LoggerContext
      .getContext(false)
      .getConfiguration
      .getRootLogger
      .getAppenders
      .values
      .forEach {
        case a: AbstractOutputStreamAppender[_] => a.getManager.flush()
        case _                                  =>
      }

  def enableFileLogging(fileName: String): Unit = {

    Files.deleteIfExists(Paths.get(fileName))

    val ctx = LoggerContext.getContext(false)
    val cfg = ctx.getConfiguration()

    val layout = PatternLayout.newBuilder().withPattern("%d{ISO8601} %m%n").build()
    val strategy = DefaultRolloverStrategy
      .newBuilder()
      .withConfig(cfg)
      .withMin("1")
      .withMax(1_000_000.toString)
      .build()

    val p = SizeBasedTriggeringPolicy.createPolicy(s"100 MB")
    p.start()

    val appender = RollingFileAppenderBridge.newAppender(
      "test",
      layout,
      fileName,
      true,
      p,
      strategy)
    appender.start()

    val root = resetRootLogger()
    root.addAppender(appender, null, null)
    ctx.updateLoggers()

    LogManager.getRootLogger()
  }

  def setupConsoleLogger(): Logger = {
    val layout = PatternLayout.createDefaultLayout
    val appender = ConsoleAppender.createDefaultAppenderForLayout(layout)
    appender.start()

    val root = resetRootLogger()
    root.addAppender(appender, Level.WARN, root.getFilter)
    LoggerContext.getContext(false).updateLoggers()

    LogManager.getRootLogger()
  }

  def withFileLogging(f: => Unit): String =
    try {
      val prefix = getClass.getSimpleName
      val dir = Files.createTempDirectory(prefix)
      val logfile = dir.resolve("test.log").toString
      enableFileLogging(logfile)

      f

      // flush and return log contents
      flushRootLog()
      Source.fromFile(logfile).mkString
    } finally {
      setupConsoleLogger()
    }

  def setLogTrace() = {
    val ctx = LoggerContext.getContext(false)
    val cfg = ctx.getConfiguration()
    cfg.getRootLogger().setLevel(Level.TRACE)
    ctx.updateLoggers()
  }
}
