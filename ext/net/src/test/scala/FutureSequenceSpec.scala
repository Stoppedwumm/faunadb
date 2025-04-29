package fauna.net.test

import fauna.lang.syntax._
import fauna.net.util.FutureSequence
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

class FutureSequenceSpec extends Spec {

  "FutureSequenceSpec" - {

    "Sane args" in {
      a[IllegalArgumentException] should be thrownBy FutureSequence(0)
    }

    "Protects an context that is not threadsafe" in {
      val sequence = FutureSequence()

      @volatile var failed = false

      var list = List.empty[Int]
      var running = false

      (1 to 10) foreach { i =>
        sequence(Future {
          if (running) failed = true
          list = list :+ i
          running = true
          Thread.sleep(20)
          running = false
        })
      }

      val complete = Promise[Unit]()
      sequence(Future { complete.setDone() })
      Await.ready(complete.future, Duration.Inf)

      failed should equal (false)
      list should equal ((1 to 10).toList)
    }

    "Fails work if the queue is too full." in {
      val sequence = FutureSequence(5)

      (1 to 5) foreach { _ =>
        sequence(Future { Thread.sleep(100) })
      }

      val rv = sequence(Future { () })
      rv.isCompleted should equal (true)
      rv.value.get.failed.get.getMessage should equal (s"Max of 5 futures currently pending.")
    }
  }
}
