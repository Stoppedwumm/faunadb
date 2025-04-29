package fauna.exec.test

import fauna.exec.ImmediateExecutionContext
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class IECSpec extends Spec {

  implicit val ec = ImmediateExecutionContext

  def await[A](f: Future[A]): A = Await.result(f, 10.seconds)

  "IEC" - {
    "blocking twice doesn't deadlock" in {
      val future2 = Future {
        val future1 = Future { 3 }
        await(future1) + 2
      }
      await(future2) shouldBe 5
    }
    "blocking a bunch doesn't crash because of a recycled object" in {
      val future2 = Future {
        val a = Future { 2 } // start trampoline
        await(Future { 3 }) // calls block context
        await(a map { _ + 3 }) // calls block context again, crashes
      }
      await(future2) shouldBe 5
    }
    "blocking multiple times doesn't crash" in {
      val future2 = Future {
        val a = Future { 2 } // start single trampoline
        val b = Future { 3 } // borrows a RecycledQueue from the pool
        val c = Future { await(Future(4)) } // blocks within the queue
        await(Future.sequence(Seq(a, b, c))) // blocks within the queue
      }
      await(future2) shouldBe List(2, 3, 4)
    }
  }
}
