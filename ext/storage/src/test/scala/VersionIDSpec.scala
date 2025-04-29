package fauna.storage.test

import fauna.lang._
import fauna.lang.clocks._
import fauna.storage._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter

class VersionIDSpec extends AnyFreeSpec with Matchers with BeforeAndAfter {
  import Timestamp._

  var now: Timestamp = _

  before { now = Clock.time }

  "incr" - {
    "should increment only the action if a create" in {
      VersionID(now, Create).saturatingIncr shouldBe VersionID(now, Delete)
      VersionID(now, Update).saturatingIncr shouldBe VersionID(now, Delete)
    }

    "should increment timestamp and action if a delete" in {
      VersionID(now, Delete).saturatingIncr shouldBe VersionID(now.nextMicro, Create)
    }

    "should not overflow" in {
      VersionID(Max, Delete).saturatingIncr shouldBe VersionID(MaxMicros, Delete)
      VersionID(MaxMicros, Delete).saturatingIncr shouldBe
        VersionID(MaxMicros, Delete)
    }
  }

  "decr" - {
    "should decrement only the action if a delete" in {
      VersionID(now, Delete).saturatingDecr shouldBe VersionID(now, Create)
      VersionID(now, Update).saturatingDecr shouldBe VersionID(now, Create)
    }

    "should decrement timestamp and action if a create" in {
      VersionID(now, Create).saturatingDecr shouldBe VersionID(now.prevMicro, Delete)
    }

    "should not underflow" in {
      VersionID(Min, Create).saturatingDecr shouldBe VersionID(Epoch, Create)
      VersionID(Epoch, Create).saturatingDecr shouldBe VersionID(Epoch, Create)
    }
  }
}
