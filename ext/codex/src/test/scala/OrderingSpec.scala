package fauna.codex.test

import com.ibm.icu.text.Collator
import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.prop._
import fauna.prop.test.PropSpec
import org.scalatest.matchers.should.Matchers

object OrderingSpec {
  val collator = Collator.getInstance.freeze
}

class OrderingSpec extends PropSpec(1000, 100000) with Matchers {
  private def betweenTwoStrings(s1: String, s2: String) = {
    val str1 = CBOR.encode(s1)
    val str2 = CBOR.encode(s2)
    val bytes1 = CBOR.encode(s1.toUTF8Buf)
    val bytes2 = CBOR.encode(s2.toUTF8Buf)

    withClue(s"Testing '${s1}' and '${s2}' : ") {
      val c = OrderingSpec.collator.compare(s1, s2)
      val o1 = CBOROrdering.compare(str1, str2)
      val o2 = CBOROrdering.compare(str2, str1)
      val b1 = CBOROrdering.compare(bytes1, bytes2)
      val b2 = CBOROrdering.compare(bytes2, bytes1)

      o1 should equal (-o2)
      b1 should equal (-b2)

      if (c == 0) {
        // When ICU deems strings equal, we use binary ordering to establish total ordering
        o1 should equal (b1)
      } else {
        // Otherwise we must be compatible with ICU ordering
        o1 should equal (c)
      }
    }
  }

  prop("CBOR compares empty strings") {
    // Old CBOROrdering did the wrong thing here when comparing a combining
    // character to an empty string. The character should sort after, not be equal
    for {
      s1 <- Prop.string(0, 32)
    } {
      val s2 = ""
      betweenTwoStrings(s1, s2)
    }
  }

  prop("CBOR compares single-character strings") {
    for {
      s1 <- Prop.string(1, 1)
      s2 <- Prop.string(1, 1)
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares alphanumeric strings") {
    for {
      s1 <- Prop.alphaNumString(1, 4096)
      s2 <- Prop.alphaNumString(1, 4096)
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares strings") {
    for {
      s1 <- Prop.string
      s2 <- Prop.string
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares identical strings") {
    for {
      s <- Prop.string(1, 32)
    } { betweenTwoStrings(s, s) }
  }

  prop("CBOR compares shared prefixed strings") {
    // Old CBOROrdering did the wrong thing here when comparing a bytestring
    // that is a prefix of another. The longer string should sort after, not be equal
    for {
      s1 <- Prop.alphaString(1, 32)
      s2 <- Prop.alphaString(1, 32)
    } { betweenTwoStrings(s1, s1 + s2) }
  }

  prop("CBOR compares small strings") {
    for {
      s1 <- Prop.alphaString(1, 16)
      s2 <- Prop.alphaString(1, 16)
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares large strings") {
    for {
      s1 <- Prop.string(1, 4096)
      s2 <- Prop.string(1, 4096)
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares equal length strings") {
    for {
      s1 <- Prop.alphaString(203, 203)
      s2 <- Prop.alphaString(203, 203)
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares strings with only case differences") {
    for {
      s1 <- Prop.string(1, 16, Seq('a', 'A', 'b', 'B'))
      s2 <- Prop.string(1, 16, Seq('a', 'A', 'b', 'B'))
    } { betweenTwoStrings(s1, s2) }
  }

  prop("CBOR compares substrings with only case differences") {
    for {
      s1 <- Prop.string(4, 4, Seq('d', 'D'))
      prefix2 <- Prop.string(4, 4, Seq('d', 'D'))
      s2 <- Prop.alphaString(4, 4)
    } { betweenTwoStrings(s1, prefix2 + s2) }
  }

  prop("CBOR compares any two encoded values") {
    for {
      a <- Value.random()
      b <- Value.random()
    } {
      val encA = CBOR.encode(a)
      val encB = CBOR.encode(b)

      val cmpA = CBOROrdering.compare(encA, encB)
      val cmpB = CBOROrdering.compare(encB, encA)

      cmpA.sign should be (-1 * cmpB.sign)
    }
  }
}
