package fauna.tx.test

import fauna.tx.transaction._

class ContiguousQueueSpec extends Spec {
  "Contiguousqueue" - {
    val q = new ContiguousQueue[String](100, 100, 100)

    "new" in {
      q.lastIdx should equal (100)
      q.peek should equal (null)
      q.poll should equal (null)
    }

    "drops preceding adds" in {
      q.add(80, 100, "foo")

      q.lastIdx should equal (100)
      q.peek should equal (null)
      q.poll should equal (null)
    }

    "emits contiguous adds" in {
      q.add(100, 110, "bar")
      q.lastIdx should equal (110)
      q.peek should equal (ContiguousQueue.Entry(100, 110, "bar"))
      q.poll should equal (ContiguousQueue.Entry(100, 110, "bar"))
      q.poll should equal (null)
    }

    "buffers non-contiguous adds" in {
      q.add(120, 130, "baz")
      q.lastIdx should equal (110)
      q.poll should equal (null)

      q.add(110, 120, "qux")
      q.lastIdx should equal (130)
      q.poll should equal (ContiguousQueue.Entry(110, 120, "qux"))
      q.poll should equal (ContiguousQueue.Entry(120, 130, "baz"))
      q.poll should equal (null)
    }

    "drops overlapped buffered adds" in {
      q.add(135, 145, "x")
      q.add(140, 150, "b")
      q.add(145, 155, "x")
      q.add(150, 160, "c")
      q.lastIdx should equal (130)
      q.poll should equal (null)

      q.add(130, 140, "a")
      q.lastIdx should equal (160)
      q.poll should equal (ContiguousQueue.Entry(130, 140, "a"))
      q.poll should equal (ContiguousQueue.Entry(140, 150, "b"))
      q.poll should equal (ContiguousQueue.Entry(150, 160, "c"))
      q.poll should equal (null)
    }
  }
}
