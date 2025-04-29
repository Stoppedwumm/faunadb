package fauna.storage.test

import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ConcretePathSpec extends AnyFreeSpec with Matchers {
  "works for single element paths" in {
    ConcretePath("a").toDataPath shouldBe Seq("a")

    // Weird, but valid for internal collections.
    ConcretePath("data").toDataPath shouldBe Seq("data")
  }

  "works for data paths" in {
    ConcretePath("data", "a").toDataPath shouldBe Seq("data", "a")
    ConcretePath("data", "fooo").toDataPath shouldBe Seq("data", "fooo")
  }

  "works for nested paths" in {
    ConcretePath("data", "a", "b").toDataPath shouldBe Seq("data", "a", "b")
    ConcretePath("a", "b", "c").toDataPath shouldBe Seq("a", "b", "c")
  }

  "disallows empty paths" in {
    val ex = the[IllegalStateException] thrownBy ConcretePath().toDataPath
    ex.toString should include("Unsupported path")
  }

  "insert" - {
    "inserts a field" in {
      Data(MapV())
        .insert(ConcretePath("foo"), 5) shouldBe Data(MapV("foo" -> 5))

      Data(MapV("foo" -> 3))
        .insert(ConcretePath("bar"), 5) shouldBe Data(MapV("foo" -> 3, "bar" -> 5))

      Data(MapV("foo" -> MapV("bar" -> 3)))
        .insert(ConcretePath("foo", "qux"), 5) shouldBe Data(
        MapV("foo" -> MapV("bar" -> 3, "qux" -> 5)))
    }

    "blows up if the parent doesn't exist" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV())
          .insert(ConcretePath("foo", "bar"), 5)
      }
      e.getMessage should include(
        "Refusing to overwrite non-struct parent of nested field")
    }

    "blows up if the parent isn't a struct" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV("foo" -> 3))
          .insert(ConcretePath("foo", "bar"), 5)
      }
      e.getMessage should include(
        "Refusing to overwrite non-struct parent of nested field")
    }

    "creates 'data' if it doesn't exist" in {
      Data(MapV())
        .insert(ConcretePath("data", "foo"), 5) shouldBe Data(
        MapV("data" -> MapV("foo" -> 5)))
    }

    "blows up if the parent isn't a struct within 'data'" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV())
          .insert(ConcretePath("data", "foo", "bar"), 5)
      }
      e.getMessage should include(
        "Refusing to overwrite non-struct parent of nested field")
    }
  }

  "remove" - {
    "removes a field" in {
      Data(MapV("foo" -> 3))
        .remove(ConcretePath("foo")) shouldBe Data(MapV())
      Data(MapV("foo" -> MapV("bar" -> 3)))
        .remove(ConcretePath("foo", "bar")) shouldBe Data(MapV("foo" -> MapV()))
    }

    "doesn't remove a field if it doesn't exist" in {
      Data(MapV())
        .remove(ConcretePath("foo")) shouldBe Data(MapV())
    }

    "blows up if the parent doesn't exist" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV()).remove(ConcretePath("foo", "bar"))
      }
      e.getMessage should include(
        "Refusing to remove path whose parent is not a struct")
    }

    "blows up if the parent isn't a struct" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV("foo" -> 3)).remove(ConcretePath("foo", "bar"))
      }
      e.getMessage should include(
        "Refusing to remove path whose parent is not a struct")
    }

    "doesn't blow up if `data` is missing" in {
      Data(MapV()).remove(ConcretePath("data", "bar")) shouldBe Data(MapV())
    }

    "removes `data` if it is the only parent" in {
      Data(MapV("data" -> MapV("foo" -> 3)))
        .remove(ConcretePath("data", "foo")) shouldBe Data(MapV())

      Data(MapV("data" -> MapV("foo" -> 3, "bar" -> 4)))
        .remove(ConcretePath("data", "foo")) shouldBe Data(
        MapV("data" -> MapV("bar" -> 4)))
    }
  }

  "get" - {
    "looks up a field" in {
      Data(MapV("foo" -> MapV("bar" -> 3)))
        .get(ConcretePath("foo")) shouldBe Some(MapV("bar" -> 3))
      Data(MapV("foo" -> MapV("bar" -> 3)))
        .get(ConcretePath("foo", "bar")) shouldBe Some(LongV(3))
    }

    "returns None if the field doesn't exist" in {
      Data(MapV())
        .get(ConcretePath("foo")) shouldBe None
      Data(MapV("foo" -> MapV()))
        .get(ConcretePath("foo", "bar")) shouldBe None
    }

    "blows up if the parent of a field doesn't exist" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV())
          .get(ConcretePath("foo", "bar")) shouldBe None
      }
      e.getMessage should include("Cannot read field whose parent is not a struct")
    }

    "blows up if the parent of a field is not a struct" in {
      val e = the[IllegalStateException] thrownBy {
        Data(MapV("foo" -> 3))
          .get(ConcretePath("foo", "bar")) shouldBe None
      }
      e.getMessage should include("Cannot read field whose parent is not a struct")
    }

    "can read fields directly in `data` when `data` doesn't exist" in {
      Data(MapV())
        .get(ConcretePath("data", "foo")) shouldBe None
    }
  }
}
