package fauna.tx.test

import fauna.lang.syntax._
import fauna.tx.log._
import java.nio.file._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

class TestAdder(dir: Path) {
  val log = Log.open[Int](dir, None, "adder", fileSize = 1 * 1024)
  val state = SnapshottedState.open[TX, Int, Int](log, dir, "adder", TX.MinValue, 0) { (_, a, b) => a + b }

  def get = state.get

  def close(): Unit = state.close()

  def sync() = Await.result(state.sync(5.seconds), 10.seconds)

  def +=(i: Int) = Await.result(log.add(i, 5.seconds), 10.seconds)
  def -=(i: Int) = this += (i * -1)
}

class LoggedStateSpec extends Spec {

  "SnapshottedState" - {
    "works" in {
      val dir = aTestDir()
      val adder = new TestAdder(dir)

      1 to 100000 foreach { _ => adder += 1 }
      adder.sync()

      adder.get should equal (100000)

      adder.close()
      dir.deleteRecursively()
    }

    "reads state from persisted log" in {
      val dir = aTestDir()
      val adder = new TestAdder(dir)

      adder += 1
      adder += 1

      adder.sync()
      adder.close()

      val adder2 = new TestAdder(dir)

      adder2.sync()

      adder2.get should equal (2)
      adder2.close()

      dir.deleteRecursively()
    }

    "saves state" in {
      val dir = aTestDir()
      val adder = new TestAdder(dir)

      1 to 100 foreach { adder += _ }

      adder.sync()
      adder.get should equal ((1 to 100 foldLeft 0) { _ + _ })

      adder.state.persistedIdx should equal (TX(100))

      101 to 150 foreach { adder += _ }

      adder.sync()
      adder.get should equal ((1 to 150 foldLeft 0) { _ + _ })

      adder.state.persistedIdx should equal (TX(100))

      adder.state.save()
      adder.state.persistedIdx should equal (TX(150))

      adder.close()
      dir.deleteRecursively()
    }

    "doesn't regress on restart" in {
      val sum = (1 to 150 foldLeft 0) { _ + _ }
      1 to 100 foreach { _ =>
        val dir = aTestDir()

        val a1 = {
          val adder = new TestAdder(dir)

          1 to 150 foreach { adder += _ }

          val fut = adder.state.subscribe {
            Future.successful(adder.get != sum)
          }
          Await.result(fut, 10 seconds)
          adder.state.persistedIdx should equal (TX(100))
          // Don't close it yet, as that saves state.
          // We want the second part of the test to have
          // persisted state be older than last committed log entry.
          adder
        }

        {
          val adder = new TestAdder(dir)
          adder.get should equal (sum)
          adder.state.persistedIdx should equal (TX(100))
          adder.close()
        }
        // Close the first one now too.
        a1.close()
        dir.deleteRecursively()
      }
    }

    "subscriptions initially sync" in {
      val dir = aTestDir()
      val adder = new TestAdder(dir)

      1 to 100 foreach { adder += _ }

      val done = adder.state.subscribe {
        adder.get should equal ((1 to 100 foldLeft 0) { _ + _ })
        Future.successful(false)
      }

      Await.result(done, 30.seconds)

      adder.close()
      dir.deleteRecursively()
    }

    "subscriptions are level-triggered" in {
      val dir = aTestDir()
      val adder = new TestAdder(dir)

      var check = false

      val done = adder.state.subscribe({
        adder.synchronized {
          if (check) {
            adder.get should equal ((1 to 100 foldLeft 0) { _ + _ })
            Future.successful(false)
          } else {
            Future.successful(true)
          }
        }
      }, 10.millis)

      adder.synchronized {
        1 to 100 foreach { adder += _ }
        adder.sync()
        check = true
      }

      Await.result(done, 30.seconds)

      adder.close()
      dir.deleteRecursively()
    }
  }
}
