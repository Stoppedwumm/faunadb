package fauna.stats

import fauna.lang.syntax._
import java.io.{ File, FileOutputStream }
import java.lang.management.ManagementFactory
import java.nio.channels.Channels
import java.nio.ByteBuffer
import java.util.{ Timer, TimerTask }

class CSVStats(filename: String, bufferSize: Int = 5 * 1024 * 1024)
    extends StatsRecorder {

  private val log = getLogger

  private val buffer = ByteBuffer.allocate(bufferSize)
  private val copy = ByteBuffer.allocate(bufferSize)
  private val timer = new Timer(true)

  private val os = new FileOutputStream(new File(filename))
  private val chan = Channels.newChannel(os)

  def set(key: String, value: Double): Unit = writeMetric(s"s,$key,$value")
  def set(key: String, value: String): Unit = writeMetric(s"s,$key,$value")
  def count(key: String, count: Long, tags: StatTags = StatTags.Empty): Unit =
    writeMetric(s"""c,$key,$count,"${tags.str}"""")
  def timing(key: String, value: Long): Unit = writeMetric(s"t,$key,$value")

  def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
    writeMetric(s"""d,$key,$value,"${tags.str}"""")

  def incr(key: String): Unit = count(key, 1)
  def decr(key: String): Unit = count(key, -1)

  def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit =
    writeMetric(s"""e,${level.str},"$title","$text","${tags.str}"""")

  private def writeToFile(): Unit = {
    copy.clear()
    synchronized {
      buffer.flip()
      copy.put(buffer)
      buffer.clear()
    }

    copy.flip()
    chan.write(copy)
    os.flush()
  }

  private val task = new TimerTask {
    def run(): Unit = writeToFile()
  }

  timer.schedule(task, 30 * 1000, 30 * 1000)

  private def writeMetric(metric: String): Unit = {
    val timestamp = System.currentTimeMillis()
    val jvmAge = ManagementFactory.getRuntimeMXBean().getUptime()

    val data = (s"$timestamp,$jvmAge," + metric + "\n").toUTF8Bytes
    synchronized {
      if (buffer.remaining() >= data.length) {
        buffer.put(data)
      } else {
        log.info("Buffer underrun")
      }
    }
  }
}
