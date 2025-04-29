package fauna.net.test

import fauna.net.gossip.FinitePriorityDeque

class FinitePriorityDequeSpec extends Spec {

  "FinitePriorityDequeSpec" - {

    "push and pop elements" in {
      val queue = new FinitePriorityDeque[String](10, 3)
      queue.push("a")
      queue.pop().fold(fail("Must retrieve element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("a")
      }
    }

    "push, pop and replace elements" in {
      val queue = new FinitePriorityDeque[String](10, 3)
      queue.push("b")
      queue.pop().fold(fail("Must retrieve pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("b")

        queue.replace(elem)
        queue.pop().fold(fail("Must retrieve replaced element")) { elem =>
          queue.pop() should equal (None)
          elem.priority should equal(0)
          elem.value should equal("b")
        }
      }

    }

    "push, pop and requeue elements" in {
      val queue = new FinitePriorityDeque[String](10, 3)
      queue.push("c")
      queue.pop().fold(fail("Must retrieve pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("c")

        assert(queue.requeue(elem), "Must accept requeued element")
        queue.pop().fold(fail("Must retrive requeued element")) { elem =>
          elem.priority should equal(1)
          elem.value should equal("c")
        }
      }
    }

    "drop elements with too low priority" in {
      val queue = new FinitePriorityDeque[String](10, 1)
      queue.push("d")
      queue.pop().fold(fail("Must retrive pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("d")

        assert(!queue.requeue(elem), "Must drop requeued element")
        queue.pop() should equal(None)
      }
    }

    "drop elements to make space" in {
      val queue = new FinitePriorityDeque[String](1, 2)
      queue.push("e")

      queue.push("f")
      queue.pop().fold(fail("Must retrive pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("f")
        queue.pop() should be (None)
      }
    }

    "retrieve elements in LIFO order" in {
      val queue = new FinitePriorityDeque[String](2, 1)
      queue.push("e")
      queue.push("f")

      queue.pop().fold(fail("Must retrive pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("f")
      }

      queue.pop().fold(fail("Must retrive pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("e")
      }
    }

    "must not cause priority inversion when dropping elements" in {
      val queue = new FinitePriorityDeque[String](1, 2)
      queue.push("e")

      queue.pop().fold(fail("Must retrive pushed element")) { elem =>
        elem.priority should equal(0)
        elem.value should equal("e")

        queue.push("f")
        assert(!queue.requeue(elem),
               "Must reject insertion if dropping element with higher priority")
      }
    }

    "must only drop elements when they have reached max priority (if there is space)" in {
      val queue = new FinitePriorityDeque[String](2, 3)
      queue.push("e")
      val chain =
        for {
          pop0 <- queue.pop()
          req0 = queue.requeue(pop0)
          pop1 <- queue.pop()
          req1 = queue.requeue(pop1)
          pop2 <- queue.pop()
          req2 = queue.requeue(pop2)
        } yield {
          pop0.priority should equal(0)
          pop1.priority should equal(1)
          pop2.priority should equal(2)

          req2 should equal(false)
        }

      chain should be(a[Some[_]])
    }
  }

}
