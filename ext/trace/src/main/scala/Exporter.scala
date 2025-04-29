package fauna.trace

import com.lmax.disruptor.{ ExceptionHandler, SleepingWaitStrategy }
import com.lmax.disruptor.dsl.{ Disruptor, ProducerType }
import fauna.codex.json._
import fauna.lang.{ NamedPoolThreadFactory, Service }
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import java.util.concurrent.{
  CountDownLatch,
  RejectedExecutionException
}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.{ ArrayBuffer, ListBuffer }

/**
  * Exporters receive finished spans for sampling, processing, and
  * distribution to logs and APMs.
  */
abstract class Exporter {
  /**
    * Export a single span's data.
    */
  def exportOne(data: Span.Data): Unit

  /**
    * Export a batch of span data.
    */
  def exportAll(data: Vector[Span.Data]): Unit =
    data foreach { exportOne(_) }
}

/**
  * Maintains finished spans in memory for testing.
  */
final class MemoryExporter extends Exporter {
  private[this] val finished = ListBuffer.empty[Span.Data]

  def exportOne(data: Span.Data): Unit = synchronized {
    finished += data
  }

  def finishedSpans: Seq[Span.Data] = synchronized {
    finished.toSeq
  }

  def reset(): Unit = synchronized {
    finished.clear()
  }
}

/**
  * Exports finished spans to trace.log. This exporter is handy for
  * debugging, but is not particularly efficient. Caveat executor.
  */
final class LogExporter(stats: Tracer.Stats) extends Exporter {
  private[this] val log = getLogger("trace")

  def exportOne(data: Span.Data): Unit = {
    val js = JSObject.newBuilder

    js += "trace_id" -> data.traceID
    js += "span_id" -> data.spanID
    js += "flags" -> data.flags

    data.parentID foreach { parent =>
      js += "parent_id" -> parent
    }

    js += "name" -> data.name.toString
    js += "status" -> data.status
    js += "kind" -> data.kind
    js += "start" -> data.startTime.toString
    js += "duration" -> (data.finishNanos - data.startNanos)

    val attrs = JSArray.newBuilder

    data.attributes forEach {
      case (key, value) =>
        attrs += JSObject(
          "key" -> key.toString,
          "value" -> attrToJSValue(value))
    }

    js += "attributes" -> attrs.result()

    val resource = JSObject.newBuilder

    data.resource foreach {
      case (key, value) =>
        resource += key.toString -> value.toString
    }

    js += "resource" -> resource.result()

    log.info(js.result().toString.stripLineEnd)
    stats.incrSpansExported()
  }

  private def attrToJSValue(value: Span.Value[_]): JSValue =
    value match {
      case Span.StringValue(v)  => JSString(v.toString)
      case Span.LongValue(v)    => JSLong(v)
      case Span.DoubleValue(v)  => JSDouble(v)
      case Span.BooleanValue(v) => JSBoolean(v)
      case Span.ArrayValue(v)   => JSArray(v map { attrToJSValue(_) } toSeq)
    }

}

/**
  * An Exporter which uses an LMAX Disruptor ring buffer to move
  * export() work off producer threads.
  */
final class QueueExporter(
  bufferSize: Int,
  exporters: Vector[Exporter],
  stats: Tracer.Stats)
    extends Exporter
    with Service
    with ExceptionLogging {

  // Decidedly un-Scala, but this allows Disruptor to pre-allocate
  // events
  private final class Event(
    var data: Span.Data = null,
    var shutdown: Boolean = false) {

    // pre-emptively drop the reference to Span.Data, so it may be
    // freed before the ring buffer comes back around
    def clear(): Unit =
      data = null
  }

  private[this] val buffer = new ArrayBuffer[Span.Data](bufferSize)

  private[this] val log = getLogger

  private[this] val disruptor =
    new Disruptor(() => new Event,
      bufferSize,
      new NamedPoolThreadFactory("TraceExporter", makeDaemons = true),
      ProducerType.MULTI,
      new SleepingWaitStrategy)

  private[this] val isShutdown = new AtomicBoolean(true)
  private[this] val shutdown = new CountDownLatch(1)

  disruptor.handleEventsWith({ (event, _, end) =>
    if (event.shutdown) {
      if (buffer.nonEmpty) {
        flush()
      }

      shutdown.countDown()
    } else {
      buffer += event.data

      if (end || buffer.size >= bufferSize) {
        flush()
      }
    }

    event.clear()
  })

  disruptor.setDefaultExceptionHandler(new ExceptionHandler[Event] {
    def handleEventException(ex: Throwable, sequence: Long, event: Event) = {
      stats.incrSpansDropped()
      logException(ex)
    }

    def handleOnStartException(ex: Throwable) =
      logException(ex)

    def handleOnShutdownException(ex: Throwable) =
      logException(ex)
  })

  def isRunning: Boolean = !isShutdown.get

  def start(): Unit =
    if (isShutdown.compareAndSet(true, false)) {
      disruptor.start()
    }

  def stop(graceful: Boolean): Unit =
    if (isShutdown.compareAndSet(false, true)) {
      var sent = enqueue(null, true)

      // once isShutdown == true no producers may enqueue; this loop
      // will eventually succeed
      while (!sent) {
        sent = enqueue(null, true)
      }

      try {
        shutdown.await()
        disruptor.shutdown()
      } catch {
        case _: InterruptedException =>
          log.warn("Interrupted during shutdown, spans may not have exported.")
      }
    }

  def exportOne(data: Span.Data): Unit =
    if (!enqueue(data)) {
      stats.incrSpansDropped()
      log.debug(s"Unable to queue event for spanID ${data.spanID.toHexString} in traceID ${data.traceID.toHexString}. Queue full?")
    }

  private def enqueue(data: Span.Data, shutdown: Boolean = false): Boolean =
    if (isRunning || shutdown) {
      disruptor.getRingBuffer.tryPublishEvent(
        { (event: Event, _: Long, data: Span.Data) =>
          event.data = data
          event.shutdown = event.shutdown || shutdown // once shutdown, always shutdown
        },
        data)
    } else {
      log.warn("Attempting to enqueue trace events to shutdown exporter.")
      false
    }

  private def flush(): Unit = {
    val spans = buffer.toVector
    exporters foreach { e =>
      try {
        e.exportAll(spans)
      } catch {
        case ex: RejectedExecutionException =>
          log.warn(s"Rejected export of ${spans.size} spans: ${ex.getMessage}")
          stats.incrSpansDropped(spans.size)
      }
    }

    buffer.clear()
  }
}
