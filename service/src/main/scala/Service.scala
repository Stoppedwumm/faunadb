package fauna.core

import fauna.api.{ APIServer, FaunaApp }
import fauna.lang.FailureReporter
import fauna.logging.ExceptionLogFailureReporter
import fauna.util.REPLServer
import io.netty.util.internal.logging._
import org.apache.logging.log4j.Logger
import scala.util.control.NonFatal

object Service extends FaunaApp("Service") with APIServer {
  var replS: Option[REPLServer] = None

  override def setupLogging(): Option[Logger] = {
    try {
      InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE)

      val logs = Seq.newBuilder[String]

      logs ++= Seq("exception", "slow", "tasks")

      if (config.log_queries) {
        logs += "query"
      }

      if (config.log_streams) {
        logs += "stream"
      }

      if (config.log_trace) {
        logs += "trace"
      }

      if (config.log_limiters) {
        logs += "limiter"
      }

      val rootLogger = config.logging.configure(Some("core"), logs.result())

      FailureReporter.Default.set(ExceptionLogFailureReporter)
      Thread.setDefaultUncaughtExceptionHandler(ExceptionLogFailureReporter)

      Some(rootLogger)
    } catch {
      case NonFatal(e) =>
        e.printStackTrace()
        System.err.println(s"Could not configure logging: ${e.getMessage}")
        None
    }
  }

  start {
    if (config.network_console_port > 0) {
      val repl = Console(
        () => apiInst,
        config.network_console_address,
        config.network_console_port)
      repl.start()
      replS = Some(repl)
    }
  }

  stop {
    replS foreach { _.stop() }
  }
}
