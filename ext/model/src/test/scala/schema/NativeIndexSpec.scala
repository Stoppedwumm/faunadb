package fauna.model.test

import fauna.model.schema.NativeIndex
import fauna.model.test.SocialHelpers
import fauna.repo.cache.MemoryMeterWeigher
import fauna.repo.test.CassandraHelper
import fauna.storage.index.NativeIndexID
import java.util.concurrent.atomic.AtomicReference

class NativeIndexSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  "NativeIndex" - {
    "should be ignored by memory meter" in {
      val scope = ctx ! newScope
      val key = new Object
      // NB. Native indexes have different shapes. Here we use any arbitrary index as
      // a baseline since if the memory meter ignores it, they will all report the
      // same number of bytes.
      val value =
        new AtomicReference[Any](
          Some(
            NativeIndex.DocumentsByCollection(scope)
          ))

      val meter = new MemoryMeterWeigher(accuracy = 1)
      val baseline = meter(key, value)

      val sizes =
        NativeIndexID.All map { idx =>
          value.set(NativeIndex(scope, idx))
          meter(key, value)
        }
      all(sizes) shouldBe baseline
    }
  }
}
