package fauna.trace.test

import fauna.trace.QueryLogTags
import scala.collection.immutable.SeqMap

class QueryLogTagsSpec extends Spec {
  "x-fauna-tags header" - {
    "empty tags if not sent" in {
      QueryLogTags.fromHTTPHeader(None) should be(Right(QueryLogTags(SeqMap.empty)))
    }

    "tags should be parsed correctly" in {
      QueryLogTags.fromHTTPHeader(Some("hello=world,foo=bar")) should be(
        Right(QueryLogTags(SeqMap("hello" -> "world", "foo" -> "bar"))))
    }

    "valid/invalid tags should cause errors" in {
      def checkOk(tags: String, map: SeqMap[String, String]) = {
        QueryLogTags.fromHTTPHeader(Some(tags)) should be(Right(QueryLogTags(map)))
      }
      def checkError(tags: String, description: String) = {
        QueryLogTags.fromHTTPHeader(Some(tags)) should be(Left(description))
      }

      checkOk("a=b", SeqMap("a" -> "b"))
      checkOk("foo=3", SeqMap("foo" -> "3"))
      checkOk("aB_3=3", SeqMap("aB_3" -> "3"))
      checkOk("baz=bleh,hello=world", SeqMap("baz" -> "bleh", "hello" -> "world"))
      checkError("a=b=c", "invalid tags")
      checkError("a==c", "invalid tags")
      checkError("a", "invalid tags")
      checkError("a,b=c", "invalid tags")
      checkError("a=b,a=c", "duplicate key")
      checkError(s"hello=world,foo=${"a" * 3000}", "header is too long")

      // Edge cases
      checkOk("", SeqMap.empty)
      checkError(",", "invalid tags")
      checkError(",,", "invalid tags")

      // Keys
      checkError("?=2", "invalid tags")
      checkError("a =2", "invalid tags")
      checkError("a #=2", "invalid tags")
      checkError("a=#", "invalid tags")
      checkOk("hello=2", SeqMap("hello" -> "2"))
      checkOk("a21_aasdf=2", SeqMap("a21_aasdf" -> "2"))

      checkOk(s"${"a" * 40}=2", SeqMap("a" * 40 -> "2"))
      checkError(s"${"a" * 41}=2", "invalid key")

      // Values
      checkOk(s"a=${"a" * 80}", SeqMap("a" -> "a" * 80))
      checkError(s"a=${"a" * 81}", "invalid value")

      // Total count
      val tagsStr = ((0 until 25) map { i: Int => s"$i=a" }).mkString(",")
      val tags = SeqMap.from((0 until 25) map { i: Int => (i.toString, "a") })
      checkOk(s"$tagsStr", tags)
      checkError(s"$tagsStr,25=a", "too many tags")
    }
  }
}
