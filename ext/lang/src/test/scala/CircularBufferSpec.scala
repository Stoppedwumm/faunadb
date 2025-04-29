package fauna.lang.test

import fauna.lang.CircularBuffer

class CircularBufferSpec extends Spec {
  "CircularBuffer" - {
    "empty" in {
      val buf = CircularBuffer[Int](10)

      buf.size should equal (0)
      buf.capacity should equal (10)
      buf.isEmpty should equal (true)
      a[NoSuchElementException] should be thrownBy buf.head
      a[NoSuchElementException] should be thrownBy buf.last

      an[ArrayIndexOutOfBoundsException] should be thrownBy buf(0)
      an[ArrayIndexOutOfBoundsException] should be thrownBy buf(-1)
      an[ArrayIndexOutOfBoundsException] should be thrownBy { buf(0) = 1 }
      an[ArrayIndexOutOfBoundsException] should be thrownBy { buf(-1) = 1 }
      an[ArrayIndexOutOfBoundsException] should be thrownBy buf.remove(0)
      an[ArrayIndexOutOfBoundsException] should be thrownBy buf.remove(-1)
    }

    "add" - {
      val buf = CircularBuffer[Int](10)

      "up to capacity" in {
        for (i <- 1 to 10) {
          buf += i
          buf.size should equal (i)
          buf.capacity should equal (10)
        }

        buf.toSeq should equal (1 to 10)
      }

      "past capacity" in {
        for (i <- 11 to 20) {
          buf += i
          buf.size should equal (10)
          buf.capacity should equal (10)

          buf.toSeq should equal (1 to i drop (i - 10))
        }
      }
    }

    "shrink" in {
      val arr = new Array[Int](10)
      val buf = new CircularBuffer(arr, 0)

      for (i <- 1 to 10) {
        buf += i
      }

      buf.shrink(5)

      arr.toList should equal (List(1, 2, 3, 4, 5, 0, 0, 0, 0, 0))
      buf.toSeq should equal (1 to 5)
      an[ArrayIndexOutOfBoundsException] should be thrownBy buf(5)
    }

    "remove" in {
      val arr = new Array[Int](10)
      val buf = new CircularBuffer(arr, 0)

      for (i <- 1 to 10) {
        buf += i

        // try to remove the next element
        an[ArrayIndexOutOfBoundsException] should be thrownBy buf.remove(i)
      }

      arr.toList should equal (1 to 10)

      for (i <- 0 to 4) {
        buf.remove(i)
      }

      // ensure that the backing array slots are reset to the null value.
      arr.toList should equal (List(2, 4, 6, 8, 10, 0, 0, 0, 0, 0))
      buf.toSeq should equal (2 to 10 by 2)
      buf.size should equal (5)

      for (i <- 11 to 20) {
        buf += i
      }

      arr.toList should equal (List(16, 17, 18, 19, 20, 11, 12, 13, 14, 15))
      buf.toSeq should equal (11 to 20)
      buf.size should equal (10)

      for (i <- 0 to 4) {
        buf.remove(i)
      }

      arr.toList should equal (List(0, 0, 0, 0, 0, 12, 14, 16, 18, 20))
      buf.toSeq should equal (12 to 20 by 2)
      buf.size should equal (5)
    }

    "filtered" in {
      val arr = new Array[Int](10)
      val buf = new CircularBuffer(arr, 0)

      for (i <- 1 to 10) {
        buf += i
      }

      buf filtered { _ % 2 == 0 }

      arr.toList should equal (List(2, 4, 6, 8, 10, 0, 0, 0, 0, 0))
      buf.toSeq should equal (2 to 10 by 2)
    }
  }
}
