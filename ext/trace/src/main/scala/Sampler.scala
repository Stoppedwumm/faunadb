package fauna.trace

import java.util.{ Set => JSet }

object Sampler {

  /**
    * The default sampler continues sampling whenever it is enabled,
    * but does not start any new samples.
    */
  val Default = new Sampler {
    def shouldSample(
      traceID: TraceID,
      parent: Option[TraceContext],
      secret: String): Boolean =
      parent exists { _.isSampled }
  }
}

trait Sampler {

  /**
    * Called when a span is built to decide whether to sample the
    * provided span.
    *
    * This method is called with every span created; it must be fast.
    */
  def shouldSample(
    traceID: TraceID,
    parent: Option[TraceContext],
    secret: String): Boolean
}

final class ProbabilitySampler(probability: Double)
    extends Sampler {

  require(probability >= 0.0 && probability <= 1.0,
    "probability must be between [0.0, 1.0]")

  // Presuming trace IDs' lower 64 bits are randomly distributed,
  // convert the probability into an upper bound, and simply compare
  // the abs of each trace ID's low bits to the bound. This also works
  // on systems (DataDog) which only use 64 bit trace IDs.
  private[this] val upperBound =
    probability match {
      case 0.0 => Long.MinValue
      case 1.0 => Long.MaxValue
      case pct => (pct * Long.MaxValue).toLong
    }

  def shouldSample(
    traceID: TraceID,
    parent: Option[TraceContext],
    secret: String): Boolean = {

    parent match {
      // if parent is sampled, continue sampling
      case Some(ctx) if ctx.isSampled => true
      case _ =>
        Math.abs(traceID.toLong) <= upperBound
    }
  }
}

final class SecretSampler(tracedSecrets: JSet[String]) extends Sampler {
  def shouldSample(traceID: TraceID,
                   parent: Option[TraceContext],
                   secret: String): Boolean =
    tracedSecrets.contains(secret)
}

final class AggregateSampler(samplers: Sampler*) extends Sampler {
  def shouldSample(traceID: TraceID,
                   parent: Option[TraceContext],
                   secret: String): Boolean =
    samplers exists { _.shouldSample(traceID, parent, secret) }
}

