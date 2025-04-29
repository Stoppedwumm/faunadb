package fauna.lang.test

import fauna.lang._
import scala.util.Random

class RandomSampleSpec extends Spec {
  "RandomSample" - {
    "must be non-empty" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        RandomSample.choose(Vector.empty)
      }
    }

    "chooses from one sample" in {
      val item = new Object
      RandomSample.choose(Vector((item, 42.0))) should equal(item)
    }

    "works" in {
      val rnd = new Random(42)
      val samples = Vector.tabulate(100) { i =>
        (i, rnd.nextDouble())
      }

      // #theydidthemath
      RandomSample.choose(samples)(rnd) should equal(27)
    }
  }
}
