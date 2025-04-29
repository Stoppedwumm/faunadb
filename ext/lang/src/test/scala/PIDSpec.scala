package fauna.lang.test

import fauna.lang._

class PIDSpec extends Spec {

  "PID" - {

    "adjust around the set point" in {
      val pid = new PID(20, 1, 1)
      pid.setPoint = 0.5
      pid(0.5) shouldBe 0 // no change needed
      pid(0.9).sign shouldBe -1 // decreasing force
      pid(0.1).sign shouldBe 1 // increasing force
    }

    "converges to a set point" in {
      val pid = new PID(1, 0.1, 0.1)
      pid.setPoint = 50

      def run(cv: Double) = {
        var _cv = cv
        for (_ <- 1 to 500) {
          val mv = pid(_cv)
          _cv += mv
        }
        _cv
      }

      run(50) shouldBe 50d // no change needed
      run(70) shouldBe 50d // converges from a high value
      run(30) shouldBe 50d // converges from a low value
    }

    "limits output" in {
      val pid = new PID(20, 1, 1, limit = PID.window(0.1))
      pid.setPoint = 0.5
      pid(0.9) shouldBe -0.1
      pid(0.1) shouldBe 0.1
    }

    "prevents windup" in {
      val pid1 = new PID(20, 1, 1)
      val pid2 = new PID(20, 1, 1, windup = PID.window(0.1))
      pid1.setPoint = 0.5
      pid2.setPoint = 0.5
      pid1(0.9) should be < pid2(0.9)
    }
  }
}
