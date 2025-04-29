package fauna.trace

import fauna.lang.Timestamp
import fauna.lang.clocks._
import fauna.lang.syntax._
import io.netty.util.AsciiString
import java.util.{ Collections, LinkedHashMap, Map => JMap }
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Random

object Span {

  val MaxAttributes = 128 // XXX: wild-ass guess

  def newBuilder(
    name: AsciiString,
    tracer: Tracer,
    resource: Resource = Resource.Empty): Builder =
    new Builder(
      name,
      tracer,
      resource = resource,
      parent = tracer.activeSpan map { Left(_) })

  /**
    * Builds a new span with the provided `name`, optionally parented
    * by either a local or remote span.
    *
    * This class is not thread safe.
    */
  final class Builder(
    private[this] val name: AsciiString,
    private[this] val tracer: Tracer,
    private[this] var parent: Option[Either[Span, TraceContext]],
    private[this] var kind: Kind = Kind.Default,
    private[this] var resource: Resource = Resource.Empty,
    private[this] var isSampled: Boolean = false,
    private[this] var secret: String = "") {

    def withKind(kind: Kind): Builder = {
      this.kind = kind
      this
    }

    def withResource(resource: Resource): Builder = {
      this.resource = resource
      this
    }

    def withParent(span: Span): Builder = {
      this.parent = Some(Left(span))
      this
    }

    def withParent(span: Option[Span]): Builder = {
      this.parent = span map { Left(_) }
      this
    }

    def withParent(ctx: TraceContext): Builder = {
      this.parent = Some(Right(ctx))
      this
    }

    def ignoreParent(): Builder = {
      this.parent = None
      this
    }

    def enableSampling(): Builder = {
      this.isSampled = true
      this
    }

    def withSecret(secret: String): Builder = {
      this.secret = secret
      this
    }

    @inline
    def start()(implicit rnd: Random = ThreadLocalRandom.current()): Span = {
      val (span, _) = startWithContext()
      span
    }

    def startWithContext()(implicit rnd: Random = ThreadLocalRandom.current()): (Span, TraceContext) = {
      val traceID = parent match {
        case None             => TraceID.randomID
        case Some(Left(span)) => span.traceID
        case Some(Right(ctx)) => ctx.traceID
      }

      val spanID = SpanID.randomID

      val parentCtx = parent map {
        case Left(span) => span.context
        case Right(ctx) => ctx
      }

      val shouldSample =
        isSampled || tracer.shouldSample(traceID, parentCtx, secret)

      // Generate a random trace context for every request, in order to use it
      // in the query logs.
      val ctx = new TraceContext(
        traceID,
        spanID,
        TraceFlags.Default.withSampled(shouldSample))

      val span = if (shouldSample) {
        new RecordingSpan(name, kind, resource, ctx, parent, tracer)
      } else {
        new NoopSpan(tracer)
      }

      tracer.stats.incrSpansStarted()

      // default attributes
      if (span.isSampled) {
        val th = Thread.currentThread()
        span.addAttribute(Attributes.Thread.ID, th.getId())
        span.addAttribute(Attributes.Thread.Name, th.getName())
      }

      (span, ctx)
    }
  }

  sealed trait Value[T] {
    def value: T
  }

  case class StringValue(value: AsciiString) extends Value[AsciiString]
  case class LongValue(value: Long) extends Value[Long]
  case class DoubleValue(value: Double) extends Value[Double]
  case class BooleanValue(value: Boolean) extends Value[Boolean]
  case class ArrayValue[S <: Value[_]](value: Array[S]) extends Value[Array[S]]

  // LHM capacity and load factor are set to avoid resizing
  final class Attributes(capacity: Int)
      extends LinkedHashMap[AsciiString, Value[_]](capacity + 1, 1, /* accessOrder */true) {

    private[this] var total = 0

    // only use this variant of put() to maintain total
    def putAttribute(key: AsciiString, value: Value[_]): Unit = {
      total += 1
      put(key, value)
    }

    def droppedSize: Int =
      total - size()

    // called *after* insertion of a new element; add +1 capacity to
    // account for this
    override def removeEldestEntry(eldest: JMap.Entry[AsciiString, Value[_]]): Boolean =
      size() > capacity
  }

  /**
    * Immutable representation of a Span's data for export.
    */
  final class Data(
    val traceID: TraceID,
    val spanID: SpanID,
    val flags: TraceFlags,
    val parentID: Option[SpanID],
    val name: AsciiString,
    val status: Status,
    val kind: Kind,
    val resource: Resource,
    val operation: AsciiString,
    val startTime: Timestamp,
    val startNanos: Long,
    val finishNanos: Long,
    val attributes: JMap[AsciiString, Value[_]])
}

/**
  * A span represents a timed unit of work in a trace. Spans start as
  * soon as they are constructed, and are finished when finish() is
  * called.
  *
  * Spans may not be active for the entirety of their duration. For
  * example, a Runnable may create a span, but the span is only active
  * for the period the Runnable is executing.
  *
  * Spans may also record attributes (key/value pairs) during their
  * execution for later analysis.
  */
trait Span extends Any {

  def name: AsciiString

  /**
    * A span may be either unrooted, in which case it is the root
    * span, or have a parent. If a parent exists, it may either be a
    * local Span, or a remote span (TraceContext).
    */
  def parent: Option[Either[Span, TraceContext]]

  def context: TraceContext

  /**
    * Marks this span as finished, and exports it (if necessary).
    */
  def finish(): Unit

  def traceID: TraceID = context.traceID
  def spanID: SpanID = context.spanID

  def parentID: Option[SpanID] =
    parent map {
      case Left(span) => span.spanID
      case Right(ctx) => ctx.spanID
    }

  /**
    * Sets the status of the span, overriding the default status of
    * OK. May be called multiple times, but only the last call
    * will be recorded.
    */
  def setStatus(status: Status): Unit

  /**
    * Sets the operation of the span, which typically refers to a more
    * specific action than a Span's name. For example, a Span named
    * "row.read" might have an operation value "read $key" with a
    * specific row key in $key.
    *
    * If an operation is not set when a Span is finished, the Span's
    * name will be used.
    *
    * May be called multiple times, but only the last call will be
    * recorded.
    */
  def setOperation(value: AsciiString): Unit
  def setOperation(value: CharSequence): Unit

  def addAttribute(key: CharSequence, value: CharSequence): Unit
  def addAttribute(key: AsciiString, value: CharSequence): Unit
  def addAttribute(key: AsciiString, value: AsciiString): Unit

  def addAttribute(key: CharSequence, value: Long): Unit
  def addAttribute(key: AsciiString, value: Long): Unit

  def addAttribute(key: CharSequence, value: Double): Unit
  def addAttribute(key: AsciiString, value: Double): Unit

  def addAttribute(key: CharSequence, value: Boolean): Unit
  def addAttribute(key: AsciiString, value: Boolean): Unit

  def addAttribute(key: CharSequence, value: Array[CharSequence]): Unit
  def addAttribute(key: AsciiString, value: Array[CharSequence]): Unit
  def addAttribute(key: AsciiString, value: Array[AsciiString]): Unit

  def addAttribute(key: CharSequence, value: Array[Long]): Unit
  def addAttribute(key: AsciiString, value: Array[Long]): Unit

  def addAttribute(key: CharSequence, value: Array[Double]): Unit
  def addAttribute(key: AsciiString, value: Array[Double]): Unit

  def addAttribute(key: CharSequence, value: Array[Boolean]): Unit
  def addAttribute(key: AsciiString, value: Array[Boolean]): Unit

  /**
    * Sets Resource labels for this span. Each call will merge the
    * provided Resource with any pre-existing Resource associated with
    * this span, according to the rules defined here:
    * https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/sdk-resource.md#merge
    */
  def addResource(resource: Resource): Unit

  def failed(ex: Throwable): Unit =
    setStatus(Status.forThrowable(ex))

  def onFailure(f: Future[_])(implicit ec: ExecutionContext): Unit =
    if (isSampled) { // prevent allocation when not sampled
      f.failed foreach { failed(_) }
    }

  /**
    * Indicates whether a parent span MAY have recorded trace data, in
    * which case this span MUST record trace data.
    *
    * Callers may use this to avoid trace event overhead.
    */
  def isSampled: Boolean =
    context.isSampled
}

object NoopSpan {
  final val Name = new AsciiString("NoopSpan")
  final val Context =
    TraceContext(
      new TraceID(0x0, 0x0),
      new SpanID(0x0),
      TraceFlags.Default)
}

/**
  * An unsampled Span, which retains nothing.
  */
final class NoopSpan(val tracer: Tracer) extends AnyVal with Span {

  def name: AsciiString = NoopSpan.Name
  def parent: Option[Either[Span, TraceContext]] = None
  def context: TraceContext = NoopSpan.Context

  def setStatus(status: Status): Unit = ()

  def setOperation(value: AsciiString): Unit = ()
  def setOperation(value: CharSequence): Unit = ()

  def addAttribute(key: CharSequence, value: CharSequence): Unit = ()
  def addAttribute(key: AsciiString, value: CharSequence): Unit = ()
  def addAttribute(key: AsciiString, value: AsciiString): Unit = ()

  def addAttribute(key: CharSequence, value: Long): Unit = ()
  def addAttribute(key: AsciiString, value: Long): Unit = ()

  def addAttribute(key: CharSequence, value: Double): Unit = ()
  def addAttribute(key: AsciiString, value: Double): Unit = ()

  def addAttribute(key: CharSequence, value: Boolean): Unit = ()
  def addAttribute(key: AsciiString, value: Boolean): Unit = ()

  def addAttribute(key: CharSequence, value: Array[CharSequence]): Unit = ()
  def addAttribute(key: AsciiString, value: Array[CharSequence]): Unit = ()
  def addAttribute(key: AsciiString, value: Array[AsciiString]): Unit = ()

  def addAttribute(key: CharSequence, value: Array[Long]): Unit = ()
  def addAttribute(key: AsciiString, value: Array[Long]): Unit = ()

  def addAttribute(key: CharSequence, value: Array[Double]): Unit = ()
  def addAttribute(key: AsciiString, value: Array[Double]): Unit = ()

  def addAttribute(key: CharSequence, value: Array[Boolean]): Unit = ()
  def addAttribute(key: AsciiString, value: Array[Boolean]): Unit = ()

  def addResource(resource: Resource): Unit = ()

  def finish(): Unit =
    tracer.stats.incrSpansFinished()

  override def toString = "NoopSpan"
}

/**
  * A span which retains records for later processing, sampling, and
  * exporting to an APM.
  */
final class RecordingSpan(
  val name: AsciiString,
  val kind: Kind,
  private[this] var resource: Resource, // synchronized on this
  val context: TraceContext,
  val parent: Option[Either[Span, TraceContext]],
  tracer: Tracer,
  clock: DeadlineClock = SystemDeadlineClock) extends Span {

  require(context.isSampled,
    "unsampled spans MUST NOT record")

  private[this] val log = getLogger

  private[this] val startTime: Timestamp = Clock.time

  private[this] val startNanos: Long = clock.nanos

  // synchronized on this
  private[this] var finishNanos: Long = _

  // synchronized on this
  private[this] var isFinished: Boolean = false

  // synchronized on this
  private[this] var status: Status = Status.Default

  // synchronized on this
  private[this] var operation: AsciiString = name

  // synchronized on this
  private[this] val attributes = new Span.Attributes(Span.MaxAttributes)

  /**
    * Returns the duration of this span in nanoseconds. If the span
    * has not finished, the duration since its start is returned.
    */
  def durationNanos: Long = synchronized {
    if (isFinished) {
      finishNanos - startNanos
    } else {
      clock.nanos - startNanos
    }
  }

  def setStatus(status: Status): Unit = synchronized {
    if (isFinished) {
      log.trace("Calling setStatus() on a finished Span")
    } else {
      this.status = status
    }
  }

  def setOperation(value: AsciiString): Unit = synchronized {
    if (isFinished) {
      log.trace("Calling setOperation() on a finished Span")
    } else {
      this.operation = value
    }
  }

  def setOperation(value: CharSequence): Unit =
    setOperation(new AsciiString(value))

  def addAttribute(key: CharSequence, value: CharSequence): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: CharSequence): Unit =
    addAttribute(key, new AsciiString(value))

  def addAttribute(key: AsciiString, value: AsciiString): Unit =
    addAttribute(key, Span.StringValue(value))

  def addAttribute(key: CharSequence, value: Long): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Long): Unit =
    addAttribute(key, Span.LongValue(value))

  def addAttribute(key: CharSequence, value: Double): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Double): Unit =
    addAttribute(key, Span.DoubleValue(value))

  def addAttribute(key: CharSequence, value: Boolean): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Boolean): Unit =
    addAttribute(key, Span.BooleanValue(value))

  def addAttribute(key: CharSequence, value: Array[CharSequence]): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Array[CharSequence]): Unit = {
    val vs = value map { v => new AsciiString(v) }
    addAttribute(key, vs)
  }

  def addAttribute(key: AsciiString, value: Array[AsciiString]): Unit = {
    val vs = value map { v => Span.StringValue(v) }
    addAttribute(key, Span.ArrayValue(vs))
  }

  def addAttribute(key: CharSequence, value: Array[Long]): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Array[Long]): Unit = {
    val vs = value map { Span.LongValue(_) }
    addAttribute(key, Span.ArrayValue(vs))
  }

  def addAttribute(key: CharSequence, value: Array[Double]): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Array[Double]): Unit = {
    val vs = value map { Span.DoubleValue(_) }
    addAttribute(key, Span.ArrayValue(vs))
  }

  def addAttribute(key: CharSequence, value: Array[Boolean]): Unit =
    addAttribute(new AsciiString(key), value)

  def addAttribute(key: AsciiString, value: Array[Boolean]): Unit = {
    val vs = value map { Span.BooleanValue(_) }
    addAttribute(key, Span.ArrayValue(vs))
  }

  def addResource(resource: Resource): Unit = synchronized {
    if (isFinished) {
      log.trace("Calling addResource() on a finished Span")
    } else {
      this.resource += resource
    }
  }

  def finish(): Unit = synchronized {
    if (isFinished) {
      log.trace("Calling finish() on a finished Span")
    } else {
      finishNanos = clock.nanos
      isFinished = true
      tracer.stats.incrSpansFinished()
      tracer.exportOne(this.toData)
    }
  }

  def toData: Span.Data =
    new Span.Data(
      traceID,
      spanID,
      context.flags,
      parentID,
      name,
      status,
      kind,
      resource,
      operation,
      startTime,
      startNanos,
      finishNanos,
      Collections.unmodifiableMap(attributes))

  override def toString =
    s"RecordingSpan(name = $name, " +
      s"ts = $startTime, " +
      s"start = $startNanos, " +
      s"finish = $finishNanos, " +
      s"attributes = ${attributes.size} (${attributes.droppedSize} dropped)" +
      ")"

  private def addAttribute(key: AsciiString, value: Span.Value[_]): Unit =
    synchronized {
      if (isFinished) {
        log.trace("Calling addAttribute() on a finished Span")
      } else {
        attributes.putAttribute(key, value)
      }
    }
}
