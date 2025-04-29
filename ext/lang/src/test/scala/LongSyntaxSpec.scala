package fauna.lang.test

import fauna.lang.syntax._

class LongSyntaxTest extends Spec {
  "saturated add" - {
    "adds" in {
      10L.saturatedAdd(20L) should equal(30L)
    }

    "saturates on overflow" in {
      Long.MaxValue.saturatedAdd(1) should equal (Long.MaxValue)
    }

    "saturates on underflow" in {
      Long.MinValue.saturatedAdd(-1) should equal(Long.MinValue)
    }
  }
}
