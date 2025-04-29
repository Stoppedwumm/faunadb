package fauna.repo.test

import fauna.atoms._
import fauna.storage.doc._
import fauna.storage.ir._

class DataSpec extends Spec {
  "merge" - {
    "combines two Datas and preserves order" in {
      val data1 = MapV("foo" -> "bar").toData
      val data2 = MapV("baz" -> "qux").toData

      (data1 merge data2) should equal (MapV("foo" -> "bar", "baz" -> "qux").toData)
      (data2 merge data1) should equal (MapV("baz" -> "qux", "foo" -> "bar").toData)
    }

    "conflicts go to rhs" in {
      val data1 = MapV("foo" -> "bar").toData
      val data2 = MapV("foo" -> "qux").toData

      (data1 merge data2) should equal (MapV("foo" -> "qux").toData)
      (data2 merge data1) should equal (MapV("foo" -> "bar").toData)
    }

    "is recursive" in {
      val data1 = MapV("foo" -> MapV("foo" -> "bar")).toData
      val data2 = MapV("foo" -> MapV("baz" -> "qux")).toData

      (data1 merge data2) should equal (MapV("foo" -> MapV("foo" -> "bar", "baz" -> "qux")).toData)
      (data2 merge data1) should equal (MapV("foo" -> MapV("baz" -> "qux", "foo" -> "bar")).toData)
    }
  }

  "andThen (aka merge diffs)" - {
    val diff1 = MapV("foo" -> MapV("bar" -> NullV, "baz" -> "a")).toDiff
    val diff2 = MapV("foo" -> MapV("bar" -> "b", "baz" -> "a")).toDiff
    val diff3 = MapV("foo" -> MapV("baz" -> "b")).toDiff

    "diff andThen diff == diff" in {
      diff1 andThen diff1 should equal (diff1)
      diff2 andThen diff2 should equal (diff2)
      diff3 andThen diff3 should equal (diff3)
    }

    "data.patch(diff1 andThen diff2) == data.patch(diff1).patch(diff2)" in {
      val data = MapV("foo" -> MapV("bar" -> "og"), "qux" -> 1).toData

      data.patch(diff1 andThen diff2) should equal (data.patch(diff1).patch(diff2))
      data.patch(diff2 andThen diff1) should equal (data.patch(diff2).patch(diff1))

      data.patch(diff1 andThen diff3) should equal (data.patch(diff1).patch(diff3))
      data.patch(diff3 andThen diff1) should equal (data.patch(diff3).patch(diff1))

      data.patch(diff2 andThen diff3) should equal (data.patch(diff2).patch(diff3))
      data.patch(diff3 andThen diff2) should equal (data.patch(diff3).patch(diff2))
    }
  }

  "patch" - {
    "non-null is same as merge" in {
      val data = MapV("foo" -> "bar").toData
      val diff = MapV("foo" -> "qux").toDiff

      (data patch diff) should equal (MapV("foo" -> "qux").toData)
    }

    "null semantics" in {
      val data = MapV("foo" -> ArrayV("bar", "baz")).toData

      val removeField = MapV("foo" -> NullV).toDiff
      (data patch removeField) should equal (Data.empty)

      val emptyArray = MapV("foo" -> ArrayV(NullV)).toDiff
      (data patch emptyArray) should equal (MapV("foo" -> ArrayV.empty).toData)

      val emptyArrayMulti = MapV("foo" -> ArrayV(NullV, NullV)).toDiff
      (data patch emptyArrayMulti) should equal (MapV("foo" -> ArrayV.empty).toData)

      val emptyObject = MapV("foo" -> MapV("bar" -> NullV)).toDiff
      (data patch emptyObject) should equal (MapV("foo" -> MapV.empty).toData)

      val queryV = MapV("foo" -> MapV("bar" -> QueryV(APIVersion.Default, MapV("lambda" -> "x", "expr" -> MapV("baz" -> NullV)))))
      (data patch queryV.toDiff) should equal (queryV.toData)
    }
  }

  "diffTo" - {
    "empty diffTo rhs == rhs" in {
      val data = MapV("foo" -> MapV("bar" -> 1), "baz" -> "qux").toData

      (Data.empty diffTo data).fields should equal (data.fields)
    }

    "lhs diffTo empty == lhs with nulls" in {
      val data = MapV("foo" -> MapV("bar" -> 1), "baz" -> "qux").toData

      (data diffTo Data.empty) should equal (MapV("foo" -> NullV, "baz" -> NullV).toDiff)
    }

    val a = MapV("foo" -> 1, "bar" -> MapV("baz" -> 2, "quz" -> 3)).toData
    val b = MapV("foob" -> 1, "bar" -> MapV("baz" -> 4, "quzb" -> 3)).toData

    "a patch (a diffTo b) == b" in {
      (a patch (a diffTo b)) should equal (b)
    }

    "a patch (b diffTo a) == a" in {
      (a patch (b diffTo a)) should equal (a)
    }
  }

  "inverse" - {
    "data == (data patch diff) patch (data inverse diff)" in {
      val data1 = MapV("foo" -> MapV("bar" -> "a", "baz" -> "b"), "qux" -> false).toData
      val data2 = MapV("foo" -> MapV("bar" -> DocID(SubID(1), CollectionID(2)), "baz" -> "b"), "qux" -> true).toData

      val diff = data1 diffTo data2

      (data1 patch diff) should equal (data2)
      (data2 patch (data1 inverse diff)) should equal (data1)
      (data1 patch diff) patch (data1 inverse diff) should equal (data1)
    }
  }

  "subtract" - {
    val diff1 = MapV("foo" -> MapV("bar" -> NullV, "baz" -> "a")).toDiff
    val diff2 = MapV("foo" -> MapV("bar" -> "b", "baz" -> "a")).toDiff
    val diff3 = MapV("foo" -> MapV("baz" -> "b")).toDiff

    "diff subtract diff == Diff.empty" in {
      diff1 subtract diff1 should equal (Diff.empty)
      diff2 subtract diff2 should equal (Diff.empty)
      diff3 subtract diff3 should equal (Diff.empty)
    }

    "returns the set of changes that are not already included in rhs" in {
      diff1 subtract diff2 should equal (MapV("foo" -> MapV("bar" -> NullV)).toDiff)
      diff1 subtract diff3 should equal (MapV("foo" -> MapV("bar" -> NullV, "baz" -> "a")).toDiff)
    }
  }
}
