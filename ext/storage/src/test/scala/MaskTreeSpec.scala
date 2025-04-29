package fauna.storage.test

import fauna.storage.doc._
import fauna.storage.ir._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MaskTreeSpec extends AnyFreeSpec with Matchers {

  "MaskTree" - {
    val m = MaskTree(List("foo", "bar"))
    val d = MapV("foo" -> MapV("bar" -> "good!", "bad" -> "bad!"))

    "selects and rejects correctly" in {
      m.select(d) should equal (MapV("foo" -> MapV("bar" -> "good!")))
      m.reject(d) should equal (MapV("foo" -> MapV("bad" -> "bad!")))
    }

    "selects across array boundaries" in {
      val m2 = MaskTree(List("foo"), List("bar"))
      val d2 = MapV("foo" -> ArrayV(MapV("bar" -> "good!", "bad" -> "bad!")))
      val d3 = MapV("foo" -> ArrayV(MapV("bar" -> "good!", "bad" -> "bad!"), MapV("bar" -> "very good!", "foo" -> 2)))
      m2.select(d2) should equal (MapV("foo" -> ArrayV(MapV("bar" -> "good!"))))
      m2.select(d3) should equal (MapV("foo" -> ArrayV(MapV("bar" -> "good!"), MapV("bar" -> "very good!"))))
    }

    "mask array" in {
      val m3 = MaskTree(List("foo"), List("bar"))
      val m4 = m.merge(m3)
      val d3 = MapV("foo" -> ArrayV(MapV("bar" -> "good!", "bad" -> "bad!"), MapV("bar" -> "soap"), 3))
      val d4 = MapV("foo" -> MapV("bar" -> "good!", "bad" -> "bad!"))
      m3.select(d3) should equal (MapV("foo" -> ArrayV(MapV("bar" -> "good!"), MapV("bar" -> "soap"), 3)))
      m3.reject(d3) should equal (MapV("foo" -> ArrayV(MapV("bad" -> "bad!"), MapV(), 3)))
      m4.select(d3) should equal (MapV("foo" -> ArrayV(MapV("bar" -> "good!"), MapV("bar" -> "soap"), 3)))
      m4.reject(d3) should equal (MapV("foo" -> ArrayV(MapV("bad" -> "bad!"), MapV(), 3)))
      m4.select(d4) should equal (MapV("foo" -> MapV("bar" -> "good!")))
      m4.reject(d4) should equal (MapV("foo" -> MapV("bad" -> "bad!")))

      val m5 = MaskTree(List("foo"), List("bar"))
      val m6 = MaskTree(List("baz"), List("cuz"))
      val d5 = MapV("foo" -> ArrayV(MapV("bar" -> 2)), "baz" -> ArrayV(MapV("cuz" -> 4)))
      m5.reject(d5) should equal (MapV("foo" -> ArrayV(MapV()), "baz" -> ArrayV(MapV("cuz" -> 4))))
      m5.merge(m6).reject(d5) should equal (MapV("foo" -> ArrayV(MapV()), "baz" -> ArrayV(MapV())))
    }

    "term validation" in {
      val name = MaskTree(List("name"))
      val unique = MaskTree(List("unique"))
      val termPath = MaskTree(List("terms"), List("path"))
      val valPath = MaskTree(List("values"), List("path"))
      val valReverse = MaskTree(List("values"), List("reverse"))
      val mask = name merge unique merge termPath merge valPath merge valReverse
      val d1 = MapV("name" -> "idx1", "unique" -> true, "terms" -> ArrayV(MapV("path" -> "data.foo")))
      val d2 = MapV("name" -> "idx1", "unique" -> true, "values" -> ArrayV(
        MapV("path" -> "data.foo", "irrelevant" -> "data"),
        MapV("reverse" -> true),
        MapV("omit" -> "this")))
      val d3 = MapV("name" -> "idx1", "unique" -> true, "values" -> ArrayV(
        MapV("path" -> "data.foo"),
        MapV("reverse" -> true),
        MapV()))
      mask.select(d1) should equal (d1)
      mask.select(d2) should equal (d3)
      mask.reject(d2) should equal (MapV("values"-> ArrayV(MapV("irrelevant" -> "data"), MapV(), MapV("omit" -> "this"))))
    }
  }

}
