package fauna.stats.test

import fauna.prop._
import fauna.stats._

class StatTagsSpec extends Spec {
  val ValidChars = """abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-:./\"""

  prop("starts with a letter") {
    for {
      key <- Prop.alphaString(minSize = 2, maxSize = 200)
      value <- Prop.string(maxSize = 200)
    } {
      StatTags(Set(key -> value))
    }
  }

  prop("valid continuation chars") {
    for {
      first <- Prop.alphaString(1, 1)
      rest <- Prop.string(minSize = 0, maxSize = 198, chars = ValidChars)
      last <- Prop.string(1, 1, ValidChars filterNot { _ == ':' })
      value <- Prop.string(maxSize = 200)
    } {
      StatTags(Set(s"$first$rest$last" -> value))
    }
  }

  prop("must not end with ':'") {
    for {
      first <- Prop.alphaString(1, 1)
      rest <- Prop.string(minSize = 0, maxSize = 198, chars = ValidChars)
      value <- Prop.string(maxSize = 200)
    } {
      an[IllegalArgumentException] shouldBe thrownBy {
        StatTags(Set(s"$first$rest:" -> value))
      }
    }
  }
}
