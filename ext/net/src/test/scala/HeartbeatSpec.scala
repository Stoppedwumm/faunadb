package fauna.net.test

import fauna.net.Heartbeat
import scala.concurrent.duration._

class HeartbeatSpec extends Spec {

  "Heartbeat" - {
    "beats" in {
      var beats: Int = 0

      val hb = Heartbeat(10.millis) { beats += 1 }
      Thread.sleep(100) // Be very generous, as test runs are CPU intensive.
      hb.stop()

      beats > 1 should equal (true)
    }

    "implements service" in {
      var beats: Int = 0

      val hb = Heartbeat(10.millis) { beats += 1 }

      hb.isRunning should equal (true)

      Thread.sleep(100)
      beats > 1 should equal (true)

      hb.start()
      hb.isRunning should equal (true)
      hb.start()
      hb.start()
      hb.isRunning should equal (true)

      hb.stop()
      hb.isRunning should equal (false)

      val prev = beats
      Thread.sleep(100)
      beats should equal (prev)

      hb.stop()
      hb.stop()
      hb.isRunning should equal (false)

      hb.start()
      hb.isRunning should equal (true)

      Thread.sleep(100)
      beats > prev should equal (true)
    }
  }
}
