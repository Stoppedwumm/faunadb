package fauna.exec.test

import fauna.exec.Timer
import fauna.lang.syntax._
import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

class TimerSpec extends Spec {
  "Timer should" - {
    "schedule tasks repeatedly" in {
      val t = Timer.Global
      var i = 0
      val p = Promise[Unit]()
      val t1 = System.nanoTime()
      t.scheduleRepeatedly(200.millis, i < 4) {
        i = i + 1
        if (i == 4) {
          p.setDone()
        }
      }
      Await.ready(p.future, 1000.millis)
      val d = (System.nanoTime() - t1).nanos.toMillis
      // Not less than 3x200, but not more than 3x300
      d >= 600 should equal (true)
      d <= 900 should equal (true)

      Thread.sleep(300)
      // Stopped at 4
      i should equal (4)
    }
  }
}
