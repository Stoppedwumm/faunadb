package fauna.lang.test

import fauna.lang._
import org.scalactic.source.Position
import scala.jdk.CollectionConverters._

class LRUSpec extends Spec {

  class Value(val str: String) extends LRU.Pinnable { var isPinned = false }

  def unpinneable = LRU.unpinneable[String, Value](maxCapacity = 3)
  def pinneable = LRU.pinneable[String, Value](maxCapacity = 3)

  def withLRU(mkCache: => LRU[String, Value])(test: LRU[String, Value] => Any) = {
    val cache = mkCache
    cache.put("foo", new Value("foo"))
    cache.put("bar", new Value("bar"))
    cache.put("baz", new Value("baz"))
    test(cache)
  }

  "LRU" - {
    "unpinneable" - {
      "tracks last recent used" in withLRU(unpinneable) { cache =>
        assertValues(cache, "foo", "bar", "baz")
        cache.get("bar")
        assertValues(cache, "foo", "baz", "bar")
        cache.get("foo")
        assertValues(cache, "baz", "bar", "foo")
      }

      "evits last recent used when at capacity" in withLRU(unpinneable) { cache =>
        assertValues(cache, "foo", "bar", "baz")
        cache.put("fuzz", new Value("fuzz"))
        assertValues(cache, "bar", "baz", "fuzz")
      }
    }

    "pinneable" - {
      "prefer evicting unpinned" in withLRU(pinneable) { cache =>
        cache.get("foo").isPinned = true
        cache.get("bar").isPinned = true
        cache.get("baz").isPinned = false
        assertValues(cache, "foo", "bar", "baz")

        cache.put("fuzz", new Value("fuzz"))
        assertValues(cache, "foo", "bar", "fuzz")
      }

      "evict eldest if all values are pinned" in withLRU(pinneable) { cache =>
        cache.get("foo").isPinned = true
        cache.get("bar").isPinned = true
        cache.get("baz").isPinned = true
        assertValues(cache, "foo", "bar", "baz")

        cache.put("fuzz", new Value("fuzz"))
        assertValues(cache, "bar", "baz", "fuzz")
      }
    }
  }

  private def assertValues[V](cache: LRU[String, Value], expected: String*)(
    implicit pos: Position) = {
    val actual = cache.values.asScala.view.map { _.str }.toSeq
    actual should contain.theSameElementsInOrderAs(expected)
  }
}
