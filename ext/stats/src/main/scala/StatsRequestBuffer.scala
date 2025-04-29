package fauna.stats

import fauna.codex.json.{ JSObject, JSValue }
import fauna.codex.json2.JSONWriter
import fauna.lang.UnboundedAverage
import io.netty.util.{
  AbstractReferenceCounted,
  IllegalReferenceCountException,
  ReferenceCounted
}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{ AtomicReference, LongAdder }
import java.util.function.{ BiConsumer, Function => JFunction }
import scala.annotation.tailrec

object StatsRequestBuffer {

  final class Span(
    val buffer: StatsRequestBuffer,
    report: StatsRequestBuffer => Unit)
      extends AbstractReferenceCounted {
    def touch(hint: Any): ReferenceCounted = this
    protected def deallocate(): Unit = report(buffer)
  }

  object Reporter {

    def apply(buffer: => StatsRequestBuffer)(
      report: StatsRequestBuffer => Unit): Reporter = new Reporter(buffer, report)
  }

  final class Reporter private (
    buffer: => StatsRequestBuffer,
    report: StatsRequestBuffer => Unit) {

    private[this] val curr =
      new AtomicReference(new Span(buffer, report))

    @tailrec
    def span: Span =
      try {
        val span = curr.get
        span.retain()
        span
      } catch {
        case _: IllegalReferenceCountException =>
          span
      }

    def flush(): Unit = {
      val next = new Span(buffer, report)
      val old = curr.getAndSet(next)
      old.release()
    }
  }
}

final class StatsRequestBuffer(private[this] val whitelist: Set[String] = Set.empty)
    extends StatsRecorder {

  private[this] val newAddr = new JFunction[String, LongAdder] {
    def apply(k: String): LongAdder = new LongAdder
  }

  private[this] val newHisto = new JFunction[String, UnboundedAverage] {
    def apply(k: String) = new UnboundedAverage
  }

  private[this] val sValues = new ConcurrentHashMap[String, String]
  private[this] val dValues = new ConcurrentHashMap[String, Double]
  private[this] val timings = new ConcurrentHashMap[String, UnboundedAverage]
  private[this] val distros = new ConcurrentHashMap[String, UnboundedAverage]
  private[this] val counts = new ConcurrentHashMap[String, LongAdder]

  private[this] def record(key: String): Boolean =
    whitelist.isEmpty || whitelist.contains(key)

  private[this] def toJson[T](
    map: ConcurrentHashMap[String, T]
  )(f: T => JSValue): JSObject = {
    val obj = JSObject.newBuilder
    val consumer = new BiConsumer[String, T] {
      def accept(k: String, v: T): Unit = obj += ((k, f(v)))
    }
    map forEach consumer
    obj.result()
  }

  def set(key: String, value: Double): Unit =
    if (record(key)) dValues.put(key, value)

  def set(key: String, value: String): Unit =
    if (record(key)) sValues.put(key, value)

  def getD(key: String): Double = {
    dValues.getOrDefault(key, 0.0)
  }

  def count(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
    if (value != 0 && record(key)) counts.computeIfAbsent(key, newAddr).add(value)

  def countOpt(key: String): Option[Long] =
    Option(counts.get(key)) map { _.longValue }

  def countOrZero(key: String): Long =
    counts.get(key) match {
      case null => 0L
      case a => a.longValue()
    }

  def incr(key: String) =
    count(key, 1)

  def decr(key: String) =
    count(key, -1)

  def timing(key: String, value: Long): Unit =
    if (record(key)) timings.computeIfAbsent(key, newHisto).sample(value)

  def distribution(key: String, value: Long, tags: StatTags = StatTags.Empty): Unit =
    if (record(key)) distros.computeIfAbsent(key, newHisto).sample(value)

  def timingOpt(key: String): Option[Long] =
    Option(timings.get(key)) map { _.toLong }

  def event(level: StatLevel, title: String, text: String, tags: StatTags): Unit = ()

  /**
    * Passes each key/value pair in this buffer to the provided
    * function, typically for output to a log or HTTP response.
    */
  def output(out: (String, Any) => Unit): Unit = {
    sValues forEach { (k, v) => out(k, v) }
    dValues forEach { (k, v) => out(k, v.toLong) }
    timings forEach { (k, v) => out(k, v.toLong) }
    distros forEach { (k, v) => out(k, v.toLong) }
    counts forEach { (k, v) => out(k, v.longValue) }
  }

  def toJson: JSObject =
    JSObject(
      (toJson(sValues)(identity) ++
        toJson(dValues)(_.toLong) ++
        toJson(timings)(_.toLong) ++
        toJson(distros)(_.toLong) ++
        toJson(counts)(_.longValue)).value.sortBy(_._1): _*
    )

  /**
    * Emits all stats within this buffer to the provided `JSONWriter`
    * as a set of key/value pairs.
    *
    * Assumes the caller has opened a JSON object with
    * `JSONWriter.writeObjectStart()`.
    */
  def toJson(out: JSONWriter): Unit =
    toJson.writeTo(out.buf, pretty = false)
}
