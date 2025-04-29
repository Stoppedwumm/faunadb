package fauna.tx.test

import fauna.tx.log._

class TailIndexSpec extends Spec {
  "TailIndex" - {
    "empty index returns min value" in {
      1 to 10 foreach { i =>
        (new TailIndex(0L))(i) should equal (0)
      }
    }

    "indexes powers of 2" in {
      val index = new TailIndex(0L)

      1 to 100000 foreach { i =>
        index.add(i, i)
      }

      1 to 100000 foreach { i =>
        (index(i) <= i) should equal (true)
      }

      val tiers = Seq(16, 128, 1024, 8192)

      tiers foreach { tier =>
        (1 to 100000).reverse filter { _ % tier == 0 } take 128 foreach { i =>
          index(i) should equal (i)
        }
      }
    }

    "disallows non-increasing adds" in {
      val index = new TailIndex(0L)

      index.add(32, 32)

      an[IllegalArgumentException] should be thrownBy index.add(32, 32)
      an[IllegalArgumentException] should be thrownBy index.add(16, 16)
    }

    "dropAfter does not break add" in {
      val index = new TailIndex(0L)

      1 to 16000 foreach { i =>
        index.add(i, i)
      }

      index.dropAfter(16000)

      16001 to 17000 foreach { i =>
        index.add(i, i)
      }
    }
  }
}
