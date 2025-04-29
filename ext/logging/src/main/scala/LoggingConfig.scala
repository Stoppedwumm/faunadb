package fauna.logging

import java.nio.file.{ InvalidPathException, Paths }
import org.apache.logging.log4j._
import org.apache.logging.log4j.core.{ ErrorHandler, LogEvent, LoggerContext }
import org.apache.logging.log4j.core.appender._
import org.apache.logging.log4j.core.appender.rolling._
import org.apache.logging.log4j.core.config._
import org.apache.logging.log4j.core.layout._
import scala.jdk.CollectionConverters._

object LoggingConfig {
  var slowQueryThreshold = 0
}

case class LoggingConfig(
  level: String,
  path: String,
  pattern: String,
  rotate_count: Int,
  rotate_size_mb: Int,
  slow_query_ms: Int) {

  private val lowPriorityCount =
    if (rotate_count > 0) {
      (rotate_count / 2.0).ceil.toInt
    } else rotate_count

  private[this] def JSONLayout =
    PatternLayout.newBuilder().withPattern("%m%n").build()

  private[this] def CustomLayout =
    PatternLayout.newBuilder().withPattern(pattern).build()

  private[this] def SizePolicy = {
    val p = SizeBasedTriggeringPolicy.createPolicy(s"$rotate_size_mb MB")
    p.start()
    p
  }

  private def rolloverStrategy(cfg: Configuration, maxCount: Int) =
    DefaultRolloverStrategy
      .newBuilder()
      .withConfig(cfg)
      .withMin("1")
      .withMax((maxCount max 1).toString)
      .build()

  private def makeLogger(ctx: LoggerContext, name: String, isJSON: Boolean) = {
    val log = ctx.getLogger(name)
    log.setAdditive(false)
    log.addAppender(makeAppender(ctx.getConfiguration(), name, isJSON))
    log
  }

  private def makeAppender(cfg: Configuration, name: String, isJSON: Boolean) = {
    if (path ne null) {
      val filename = s"$path/$name.log"
      val layout = if (isJSON) {
        JSONLayout
      } else {
        CustomLayout
      }

      // HAX. if we want overrides for more than query, formalize this.
      val strategy = name match {
        case "query" if rotate_count > 0 => rolloverStrategy(cfg, rotate_count)
        case _                           => rolloverStrategy(cfg, lowPriorityCount)
      }

      val rfAppender = RollingFileAppenderBridge.newAppender(
        name,
        layout,
        filename,
        true,
        SizePolicy,
        strategy)

      cfg.addAppender(rfAppender)
      rfAppender.start()

      val ref = AppenderRef.createAppenderRef(rfAppender.getName, null, null)

      val app = AsyncAppender
        .newBuilder()
        .setName(s"async-$name")
        .setAppenderRefs(Array(ref))
        .setConfiguration(cfg)
        .build()

      // Exceptions on the exception logger can't log to the exception
      // logger.
      if (name != "exception") {
        val handler = new ErrorHandler with ExceptionLogging {
          def error(msg: String, event: LogEvent, t: Throwable): Unit =
            error(msg, t)

          def error(msg: String, t: Throwable): Unit = {
            val ex = new RuntimeException(msg, t)
            logException(ex)
          }

          def error(msg: String): Unit =
            logException(new RuntimeException(msg))
        }

        app.setHandler(handler)
      }

      app.start()
      cfg.addAppender(app)

      app

    } else {
      val app = NullAppender.createAppender(name)
      app.start()
      cfg.addAppender(app)
      app
    }
  }

  def configure(
    rootName: Option[String],
    jsonChildren: Seq[String] = Nil,
    children: Seq[String] = Nil,
    debugConsole: Boolean = false): Logger = {
    LoggingConfig.slowQueryThreshold = slow_query_ms

    val ctx = LoggerContext.getContext(false)
    val cfg = ctx.getConfiguration()
    val rootLog = cfg.getRootLogger()

    rootLog.getAppenders().keySet.asScala foreach { rootLog.removeAppender(_) }
    rootLog.setLevel(Level.toLevel(level, Level.INFO))

    rootName foreach { name =>
      val dir = Paths.get(path).toAbsolutePath.toFile
      if (!dir.exists && !dir.mkdirs()) {
        throw new InvalidPathException(path, "Unable to create log directory")
      }
      rootLog.addAppender(makeAppender(cfg, name, false), null, null)
      children foreach { n => makeLogger(ctx, n, false) }
      jsonChildren foreach { n => makeLogger(ctx, n, true) }
    }

    if (debugConsole) {
      val console = ConsoleAppender.createDefaultAppenderForLayout(CustomLayout)
      rootLog.addAppender(console, Level.DEBUG, rootLog.getFilter)
    }

    ctx.updateLoggers()
    LogManager.getRootLogger()
  }
}
