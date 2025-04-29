package fauna.logging;

import org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.Layout;

/**
 * A nice bridge between Log4J's craziness and Scalac's quirks.
 *
 * The method signature for RollingRandomAccessFileAppender.newBuilder
 * confuses the Scala compiler such that it thinks the return type is
 * Nothing. So we create a nicer bridge.
 */
public class RollingFileAppenderBridge {
  public static RollingRandomAccessFileAppender newAppender(
      final String name,
      final Layout<String> layout,
      final String filename,
      final boolean append,
      final TriggeringPolicy policy,
      final RolloverStrategy strategy) {

      return RollingRandomAccessFileAppender
        .newBuilder()
        .setName(name)
        .setLayout(layout)
        .withFileName(filename)
        .withFilePattern(filename + ".%i")
        .withAppend(append)
        .withPolicy(policy)
        .withStrategy(strategy)
        .withImmediateFlush(false)
        .build();
    }
}
