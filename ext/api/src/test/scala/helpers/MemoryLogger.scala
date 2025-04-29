package fauna.api.test

import org.apache.logging.log4j.message.Message
import org.apache.logging.log4j.spi.AbstractLogger
import org.apache.logging.log4j.{ Level, Marker }

/**
  * A mock implementation of a log4j Logger, which accumulates log
  * messages in memory for testing.
  */
@annotation.nowarn("cat=unused")
class MemoryLogger extends AbstractLogger {

  private val level = Level.ALL

  var messages = List.empty[String]

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: Message,
    err: Throwable
  ): Boolean =
    level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: CharSequence,
    err: Throwable
  ): Boolean =
    level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: Object,
    err: Throwable
  ): Boolean =
    level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    err: Throwable
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(lvl: Level, mark: Marker, msg: String): Boolean =
    level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p1: Object*
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object,
    p4: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object,
    p4: Object,
    p5: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object,
    p4: Object,
    p5: Object,
    p6: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object,
    p4: Object,
    p5: Object,
    p6: Object,
    p7: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object,
    p4: Object,
    p5: Object,
    p6: Object,
    p7: Object,
    p8: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def isEnabled(
    lvl: Level,
    mark: Marker,
    msg: String,
    p0: Object,
    p1: Object,
    p2: Object,
    p3: Object,
    p4: Object,
    p5: Object,
    p6: Object,
    p7: Object,
    p8: Object,
    p9: Object
  ): Boolean = level.intLevel() >= lvl.intLevel()

  override def getLevel(): Level = level

  override def logMessage(
    fqcn: String, // "fully-qualified class name"
    lvl: Level,
    mark: Marker,
    msg: Message,
    err: Throwable
  ): Unit = messages = msg.getFormattedMessage() :: messages

  /**
    * Clear accumulated log messages.
    */
  def reset() = messages = List.empty
}
