package fauna.trace

import fauna.lang.syntax._
import fauna.stats.StatsRecorder
import io.netty.util.AsciiString
import java.util.concurrent.CopyOnWriteArrayList
import java.util.{ Set => JSet }
import scala.util.control.NonFatal

object Tracer {
  case class Stats(recorder: StatsRecorder) {
    def incrSpansStarted(): Unit =
      recorder.incr("Tracing.Spans.Started")

    def incrSpansFinished(): Unit =
      recorder.incr("Tracing.Spans.Finished")

    def incrSpansDropped(count: Int = 1): Unit =
      recorder.count("Tracing.Spans.Dropped", count)

    def incrSpansExported(): Unit =
      recorder.incr("Tracing.Spans.Exported")
  }

}

sealed abstract class Tracer {
  def resource: Resource
  def stats: Tracer.Stats
  def sampler: Sampler

  def addExporter(exporter: Exporter): Unit
  def exportOne(data: Span.Data): Unit

  /**
    * Returns the currently active Span, if any.
    */
  def activeSpan: Option[Span] =
    Scope.activeSpan

  def buildSpan(name: String): Span.Builder =
    buildSpan(new AsciiString(name))

  def buildSpan(name: AsciiString): Span.Builder =
    Span.newBuilder(name, this, resource)

  /**
    * Activates the given Span, returning a Scope which must be closed
    * once the Span's execution has completed.
    */
  def activate(span: Span): Option[Scope] =
    Scope.open(span)

  /**
    * Used by EC to capture an active scope when a Future is created
    * for re-activation when that Future is executed.
    *
    * Each retainActive() call MUST have a corresponding Scope.close().
    */
  def retainActive(): Option[Scope] =
    Scope.retainActive()

  def shouldSample(
    traceID: TraceID,
    parent: Option[TraceContext],
    secret: String): Boolean =
    sampler.shouldSample(traceID, parent, secret)

  /**
    * Starts and activates a new Span for the duration of `f`,
    * parented by the current active Span. If no Span is active, no
    * new Span is started.
    */
  def withSpan[T](name: String)(f: => T): T = {
    activeSpan match {
      case None    => f
      case Some(_) =>
        val span = buildSpan(name).start()
        val scope = activate(span)

        try {
          f
        } catch {
          case NonFatal(e) =>
            span.setStatus(Status.forThrowable(e))
            throw e
        } finally {
          scope foreach { _.close() }
        }
    }
  }

  /**
    * Starts a new Span for the duration of `f`, parented by the
    * provided context. If no context is provided, no new Span is
    * started.
    */
  def withTraceContext[T](name: String, ctx: Option[TraceContext])(f: => T): T =
    ctx match {
      case None    => f
      case Some(t) =>
        val span = buildSpan(name)
          .withParent(t)
          .start()

        val scope = activate(span)
        try {
          f
        } catch {
          case NonFatal(e) =>
            span.setStatus(Status.forThrowable(e))
            throw e
        } finally {
          scope foreach { _.close() }
        }
    }

}

object GlobalTracer {
  val instance = new GlobalTracer()

  @volatile private var tracer = new SamplingTracer(Sampler.Default)
  @volatile private var isRegistered = false
  @volatile private var headerSecret = Option.empty[String]

  def registerIfAbsent(mkTracer: => (Option[String], SamplingTracer)): Boolean =
    synchronized {
      if (!isRegistered) {
        val (secret, t) = mkTracer
        tracer = t
        headerSecret = secret
        isRegistered = true
        true
      } else {
        false
      }
    }
}

final class GlobalTracer private(samplerOverride: Option[Sampler] = None) extends Tracer {
  def resource: Resource =
    GlobalTracer.tracer.resource

  def stats: Tracer.Stats =
    GlobalTracer.tracer.stats

  def sampler: Sampler =
    samplerOverride.getOrElse(GlobalTracer.tracer.sampler)

  def headerSecret: Option[String] =
    GlobalTracer.headerSecret

  def exportOne(data: Span.Data): Unit =
    GlobalTracer.tracer.exportOne(data)

  def addExporter(exporter: Exporter): Unit =
    GlobalTracer.tracer.addExporter(exporter)

  def withSampler(sampler: Sampler): Tracer =
    new GlobalTracer(Some(sampler))
}

object SamplingTracer {
  def apply(probability: Double, stats: Tracer.Stats): SamplingTracer =
    new SamplingTracer(new ProbabilitySampler(probability), stats = stats)

  def apply(probability: Double,
            resource: Resource,
            stats: Tracer.Stats,
            tracedSecrets: JSet[String]): SamplingTracer = {
    val sampler =
      if (probability > 0.0) {
        new AggregateSampler(
          new ProbabilitySampler(probability),
          new SecretSampler(tracedSecrets)
        )
      } else {
        new SecretSampler(tracedSecrets)
      }

    new SamplingTracer(sampler, resource, stats)
  }
}

final class SamplingTracer(
  val sampler: Sampler,
  val resource: Resource = Resource.Empty,
  val stats: Tracer.Stats = Tracer.Stats(StatsRecorder.Null))
    extends Tracer {

  private[this] val log = getLogger
  private[this] val exporters = new CopyOnWriteArrayList[Exporter]

  def addExporter(exporter: Exporter): Unit =
    exporters.add(exporter)

  def exportOne(data: Span.Data): Unit = {
    if (exporters.size > 0) {
      exporters forEach { e => e.exportOne(data) }
    } else {
      log.trace("No exporter configured for sampled traces?")
      stats.incrSpansDropped()
    }
  }

}
