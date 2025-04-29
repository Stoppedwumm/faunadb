package fauna.model.test

import fauna.ast._
import fauna.auth.Auth
import fauna.prop.Generators
import fauna.repo.test.CassandraHelper

class ReverseSpec extends Spec with Generators {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)

  "Reverse" - {
    "index" in {
      val collName = aName.sample
      val idxName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      ctx ! mkIndex(auth, idxName, collName, List.empty, List(List("data", "value")))

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("value" -> Var("i"))))),
          (1 to 100).toList
        )
      )

      val expected = (1 to 100).map { LongL(_) }

      val idx = IndexRef(idxName)

      (ctx ! collection(auth, Match(idx))).elems shouldBe expected
      (ctx ! collection(auth, Reverse(Match(idx)))).elems shouldBe expected.reverse

      //reverse the reverse
      (ctx ! collection(auth, Reverse(Reverse(Match(idx))))).elems shouldBe expected
    }

    "intersection" in {
      val collName = aName.sample
      val idxName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      ctx ! mkIndex(auth, idxName, collName, List(List("data", "set")), List(List("data", "value")))

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 1, "value" -> Var("i"))))),
          (0 to 900).toList
        )
      )

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 2, "value" -> Var("i"))))),
          (400 to 800).toList
        )
      )

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 3, "value" -> Var("i"))))),
          (0 to 700).toList
        )
      )

      val set = Intersection(
        Reverse(Match(IndexRef(idxName), 1)),
        Reverse(Match(IndexRef(idxName), 2)),
        Reverse(Match(IndexRef(idxName), 3))
      )

      val expected = (400 to 700).map { LongL(_) }.reverse
      (ctx ! collection(auth, set)).elems shouldBe expected
    }

    "difference" in {
      val collName = aName.sample
      val idxName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      ctx ! mkIndex(auth, idxName, collName, List(List("data", "set")), List(List("data", "value")))

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 1, "value" -> Var("i"))))),
          (0 to 100).toList
        )
      )

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 2, "value" -> Var("i"))))),
          (70 to 90).toList
        )
      )

      ctx ! runQuery(auth,
        Foreach(
          Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 3, "value" -> Var("i"))))),
          (50 to 60).toList
        )
      )

      val set = Difference(
        Reverse(Match(IndexRef(idxName), 1)),
        Reverse(Match(IndexRef(idxName), 2)),
        Reverse(Match(IndexRef(idxName), 3))
      )

      val range0 = 0 to 49
      val range1 = 61 to 69
      val range2 = 91 to 100

      val expected = (range0 ++ range1 ++ range2).map { LongL(_) }
      (ctx ! collection(auth, set)).elems shouldBe expected.reverse
    }

    "union" - {
      "same source collection" in {
        val collName = aName.sample
        val idxName = aName.sample

        ctx ! mkCollection(auth, MkObject("name" -> collName))
        ctx ! mkIndex(auth, idxName, collName, List(List("data", "set")), List(List("data", "value")))

        ctx ! runQuery(auth,
          Foreach(
            Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 1, "value" -> Var("i"))))),
            (0 to 300).toList
          )
        )

        ctx ! runQuery(auth,
          Foreach(
            Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 2, "value" -> Var("i"))))),
            (300 to 800).toList
          )
        )

        ctx ! runQuery(auth,
          Foreach(
            Lambda("i" -> CreateF(ClassRef(collName), MkObject("data" -> MkObject("set" -> 3, "value" -> Var("i"))))),
            (800 to 1000).toList
          )
        )

        val set = Union(
          Reverse(Match(IndexRef(idxName), 1)),
          Reverse(Match(IndexRef(idxName), 2)),
          Reverse(Match(IndexRef(idxName), 3))
        )

        val expected = (0 to 1000).map { LongL(_) }.reverse
        (ctx ! collection(auth, set)).elems shouldBe expected
      }

      "different source collection" in {
        val Seq(coll0, coll1, idx0, idx1) = (aName * 4).sample
        ctx ! mkCollection(auth, MkObject("name" -> coll0))
        ctx ! mkCollection(auth, MkObject("name" -> coll1))
        ctx ! mkIndex(auth, idx0, coll0, Nil)
        ctx ! mkIndex(auth, idx1, coll1, Nil, List("data" :: "id" :: Nil))

        ctx ! runQuery(
          auth,
          Foreach(
            Lambda("_" -> CreateF(ClassRef(coll0))),
            (0 until 10).toList
          ))

        ctx ! runQuery(
          auth,
          Foreach(
            Lambda(
              "ref" -> CreateF(
                ClassRef(coll1),
                MkObject("data" -> MkObject(
                  "id" -> Var("ref")
                ))
              )),
            Select("data", Paginate(Documents(ClassRef(coll0)))))
        )

        val refs = (ctx ! collection(auth, Documents(ClassRef(coll0)))).elems

        // Causes a dangling foreign key. The output should be the same: the union of
        // all references that ever existed for the collection `coll0`.
        ctx ! runQuery(auth, DeleteF(Select("ref", Get(Documents(ClassRef(coll0))))))

        val set =
          Reverse(
            Union(
              Match(IndexRef(idx0)),
              Match(IndexRef(idx1))
            ))

        (ctx ! collection(auth, set)).elems should contain theSameElementsInOrderAs
          refs.reverse
      }
    }

    "join" in {
      val sourceColl = aName.sample
      val targetColl = aName.sample

      val sourceIdx = aName.sample
      val targetIdx = aName.sample

      val source = IndexRef(sourceIdx)
      val target = IndexRef(targetIdx)

      ctx ! mkCollection(auth, MkObject("name" -> sourceColl))
      ctx ! mkCollection(auth, MkObject("name" -> targetColl))

      ctx ! mkIndex(auth, sourceIdx, sourceColl, List.empty, List(List("data", "value")))
      ctx ! mkIndex(auth, targetIdx, targetColl, List(List("data", "term")), List(List("data", "value")))

      ctx ! runQuery(auth, CreateF(ClassRef(sourceColl), MkObject("data" -> MkObject("value" -> 0))))
      ctx ! runQuery(auth, CreateF(ClassRef(sourceColl), MkObject("data" -> MkObject("value" -> 1))))
      ctx ! runQuery(auth, CreateF(ClassRef(sourceColl), MkObject("data" -> MkObject("value" -> 2))))
      ctx ! runQuery(auth, CreateF(ClassRef(sourceColl), MkObject("data" -> MkObject("value" -> 3))))

      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 0, "value" -> 1))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 0, "value" -> 2))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 0, "value" -> 3))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 1, "value" -> 4))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 1, "value" -> 5))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 1, "value" -> 6))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 2, "value" -> 7))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 2, "value" -> 8))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 2, "value" -> 9))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 3, "value" -> 10))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 3, "value" -> 11))))
      ctx ! runQuery(auth, CreateF(ClassRef(targetColl), MkObject("data" -> MkObject("term" -> 3, "value" -> 12))))

      val join = Join(
        Match(source),
        Lambda("x" -> Reverse(Match(target, Var("x"))))
      )

      val page0 = ctx ! collection(auth, join)
      page0.elems shouldBe (1 to 12).map(LongL(_)).reverse

      val page1 = ctx ! collection(auth, Reverse(join))
      page1.elems shouldBe (1 to 12).map(LongL(_))
    }
  }
}
