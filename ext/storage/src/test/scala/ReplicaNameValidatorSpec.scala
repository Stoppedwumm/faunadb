package fauna.storage.test

import fauna.storage.ReplicaNameValidator
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ReplicaNameValidatorSpec extends AnyFreeSpec with Matchers {
  "ReplicaNameValidator" - {
    "works" in {
      ReplicaNameValidator.isValid("ABCabc123-_") shouldEqual true
    }

    "capital ASCII letters" in {
      ReplicaNameValidator.isValid("ABCDEFGHIJKLMNOPQRSTUVWYXZ") shouldEqual true
    }

    "lower case ASCII letters" in {
      ReplicaNameValidator.isValid("abcdefghijklmnopqrstuvwyxz") shouldEqual true
    }

    "numbers" in {
      ReplicaNameValidator.isValid("1abcdef") shouldEqual true
    }

    "letter followed by all numbers" in {
      ReplicaNameValidator.isValid("a1234567890") shouldEqual true
    }

    "non-ASCII characters" in {
      ReplicaNameValidator.isValid("مركزمعلوماتالرياض") shouldEqual true
      ReplicaNameValidator.isValid("מרכזבחיפה") shouldEqual true
      ReplicaNameValidator.isValid("北京數據中心") shouldEqual true
    }

    "region specifiers" in {
      ReplicaNameValidator.isValid("us/east-1") shouldEqual true
    }

    /* Testing items which generally are bad if seen in a path string */

    "letters and allowed punctuation" in {
      ReplicaNameValidator.isValid("a-b_c") shouldEqual true
      ReplicaNameValidator.isValid("-ab_c") shouldEqual false
      ReplicaNameValidator.isValid("_ca-b") shouldEqual false
    }

    /* Negative tests */

    "letters and asterisk" in {
      ReplicaNameValidator.isValid("a*b") shouldEqual false
    }

    "letters and space" in {
      ReplicaNameValidator.isValid("a b") shouldEqual false
    }

    "letters and colon" in {
      ReplicaNameValidator.isValid("a:b") shouldEqual false
    }

    "letters and dot" in {
      ReplicaNameValidator.isValid("a.b") shouldEqual false
    }

    "slash in front" in {
      ReplicaNameValidator.isValid("/ab") shouldEqual false
    }

    "two slashes" in {
      ReplicaNameValidator.isValid("ab/bb/c") shouldEqual false
    }

    "letters and backslash" in {
      ReplicaNameValidator.isValid("a\\b") shouldEqual false
    }

    "test null input" in {
      ReplicaNameValidator.isValid(null) shouldEqual false
    }

    "test empty input" in {
      ReplicaNameValidator.isValid("") shouldEqual false
    }
  }
}
