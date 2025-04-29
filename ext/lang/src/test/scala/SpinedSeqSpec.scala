package fauna.lang.test

import fauna.lang.SpinedSeq

class SpinedSeqSpec extends Spec {
  "SpinedSeq" - {
    "empty" in {
      SpinedSeq.empty.isEmpty should be(true)
    }

    "append" in {
      val buf = SpinedSeq.empty[Integer]

      (0 until 128) foreach { i =>
        buf.append(i)
        buf.sizeIs == i + 1
        buf.last == i
      }
    }
  }
}
