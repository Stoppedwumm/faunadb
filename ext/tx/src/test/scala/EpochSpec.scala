package fauna.tx.test

import fauna.lang.Timestamp
import fauna.tx.transaction._

class EpochTimestampSpec extends Spec {
  "ceilTimestamp, floorTimestamp" in {
    val e = scala.util.Random.nextLong()

    Epoch(e).floorTimestamp should equal (Timestamp.ofMillis(e * 10))
    Epoch(e).ceilTimestamp should equal (Epoch(e + 1).floorTimestamp.prevNano)
  }

  "epoch.timestamp ordered by tx" in {
    val epoch = Epoch(0)

    epoch.timestamp(0, 1) should equal (epoch.floorTimestamp)
    epoch.timestamp(0, 100) should equal (epoch.floorTimestamp)
    epoch.timestamp(0, 1000) should equal (epoch.floorTimestamp)
    epoch.timestamp(0, 10000) should equal (epoch.floorTimestamp)
    epoch.timestamp(0, Epoch.NanosPerEpoch) should equal (epoch.floorTimestamp)

    epoch.timestamp(9, 10) should equal (Timestamp.ofMillis(9))
    epoch.timestamp(99, 100) should equal (Timestamp.ofMicros(9900))
    epoch.timestamp(999, 1000) should equal (Timestamp.ofMicros(9990))
    epoch.timestamp(9999, 10000) should equal (Timestamp.ofMicros(9999))
    epoch.timestamp(Epoch.NanosPerEpoch - 1, Epoch.NanosPerEpoch) should equal (Timestamp.ofNanos(Epoch.NanosPerEpoch - 1))
    epoch.timestamp(Epoch.NanosPerEpoch - 1, Epoch.NanosPerEpoch) should equal (epoch.ceilTimestamp)
  }
}
