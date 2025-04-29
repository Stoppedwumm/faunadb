package fauna.trace.test

import fauna.trace._

class SamplerSpec extends Spec {
  val tracer = new SamplingTracer(Sampler.Default)

  "Sampler" - {
    "defaults" in {
      val dropped = tracer.buildSpan("dropped").start()

      dropped.isSampled should be (false)

      val parent = tracer.buildSpan("sampled")
        .enableSampling()
        .start()

      parent.isSampled should be (true)

      val child = tracer.buildSpan("sampled")
        .withParent(parent)
        .start()

      child.isSampled should be (true)
    }
  }
}
