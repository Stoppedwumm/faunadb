package fauna.model.test

import fauna.lang._
import fauna.model.stream._

class EventTypeSpec extends Spec {

  "EventType" - {

    "identifies event type based on bi-temporality" in {
      val ts1 = Timestamp.ofMicros(1)
      val ts2 = Timestamp.ofMicros(2)
      EventType(txnTS = ts2, validTS = ts2) shouldBe EventType.Version
      EventType(txnTS = ts2, validTS = ts1) shouldBe EventType.HistoryRewrite
    }
  }
}
