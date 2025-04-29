package fauna.stats

import java.lang.management.{ BufferPoolMXBean, ManagementFactory }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class JVMMetrics {
  case class ThreadStat(state: String, percentage: Double)
  case class GCStat(name: String, time: Duration, runs: Long)

  val runtime = ManagementFactory.getRuntimeMXBean
  val threads = ManagementFactory.getThreadMXBean
  val mem = ManagementFactory.getMemoryMXBean
  val pools = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala
  val gcs = ManagementFactory.getGarbageCollectorMXBeans.asScala

  def uptime = runtime.getUptime.milliseconds

  def threadCount = threads.getThreadCount

  def threadStates = {
    val infos = threads.getThreadInfo(threads.getAllThreadIds) filter { _ ne null }
    val grouped = infos groupBy { _.getThreadState }
    grouped map {
      case (state, ts) =>
        ThreadStat(state.toString, ts.length.toDouble / infos.length.toDouble)
    }
  }

  def heapUsed = mem.getHeapMemoryUsage.getUsed

  def heapCommitted = mem.getHeapMemoryUsage.getCommitted

  def nonHeapUsed = mem.getNonHeapMemoryUsage.getUsed

  def nonHeapCommitted = mem.getNonHeapMemoryUsage.getCommitted

  def garbageCollectors = {
    gcs map { gc =>
      val name = gc.getName.replace('_', '.').replace(' ', '.')
      GCStat(name, gc.getCollectionTime.milliseconds, gc.getCollectionCount)
    }
  }

  def snap(stats: StatsRecorder): Unit = {
    stats.set("JVM.Uptime", uptime.toSeconds)

    stats.set("JVM.Threads", threadCount)
    threadStates foreach { stat =>
      val name = stat.state.toLowerCase.capitalize
      stats.set(s"JVM.Threads.$name", stat.percentage)
    }

    stats.set("JVM.HeapUsed", heapUsed)
    stats.set("JVM.HeapCommitted", heapCommitted)
    stats.set("JVM.NonHeapUsed", nonHeapUsed)
    stats.set("JVM.NonHeapCommitted", nonHeapCommitted)

    pools foreach { pool =>
      val poolName = if (pool.getName == "mapped - 'non-volatile memory'") {
        "Mapped.NonVolatileMemory"
      } else {
        pool.getName.capitalize
      }
      val name = s"JVM.Pool.${poolName}"
      stats.set(s"$name.Count", pool.getCount)
      stats.set(s"$name.Bytes.Used", pool.getMemoryUsed)
      stats.set(s"$name.Bytes.Total", pool.getTotalCapacity)
    }

    garbageCollectors foreach { stat =>
      val name = s"JVM.GC.${stat.name}"
      stats.set(s"$name.Time", stat.time.toMillis)
      stats.set(name, stat.runs)
    }
  }
}
