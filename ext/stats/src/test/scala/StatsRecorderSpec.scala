package fauna.stats.test

import fauna.prop._
import fauna.stats._

class StatsRecorderSpec extends Spec {

  prop("can filter metrics") {
    for {
      allowed <- Prop.string
      disallowed <- Prop.string
      value <- Prop.int
      stats = new StatsRequestBuffer
      filtered = stats.filtered(Set(allowed))
    } {
      val tags = StatTags(Set("tag" -> "value"))

      filtered.count(allowed, value)
      stats.countOpt(allowed).value shouldBe value

      filtered.count(allowed, value, tags)
      stats.countOpt(allowed).value shouldBe 2L*value

      filtered.count(disallowed, value)
      filtered.count(disallowed, value, tags)
      stats.countOpt(disallowed) shouldBe empty
    }
  }
}
