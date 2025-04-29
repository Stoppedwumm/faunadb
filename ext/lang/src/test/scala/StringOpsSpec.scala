package fauna.lang.test

import fauna.lang.syntax._
import scala.util.Random

class StringOpsSpec extends Spec {

  "secureEquals" - {
    "compare strings" in {
      val str = Random.nextString(2048)
      val ops = new StringOps(str)
      ops.secureEquals(str) should equal(true)
      ops.secureEquals(str.dropRight(1)) should equal(false)
      ops.secureEquals(str + "!") should equal(false)
      ops.secureEquals(str.init.appended((str.last + 1).toChar)) should equal(false)
    }
  }
}
