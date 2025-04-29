package fauna.model.test

import fauna.ast._
import fauna.atoms.DocID
import fauna.auth.Auth
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.repo._
import fauna.repo.test.CassandraHelper

class IndexSetSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  def docID(r: Literal) = r match {
    case SetEventL(e) => e.docID
    case DocEventL(e) => e.docID
    case CursorL(Left(e: EventL)) => e.event.docID
    case _ => sys.error("event exported")
  }

  "IndexSetSpec" - {
    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    socialSetup(ctx, auth)

    ctx ! mkIndex(auth, "timeline", "posts", List(List("data", "author")), List(List("data", "is_share"), List("ref")))
    ctx ! mkIndex(auth, "shares", "posts", List(List("data", "author"), List("data", "is_share")))
    ctx ! runQuery(auth,
      CreateIndex(
        MkObject(
          "name" -> "reversed",
          "source" -> ClassRef("posts"),
          "active" -> true,
          "terms"  -> JSArray(MkObject("field" -> JSArray("data", "author"))),
          "values" -> JSArray(
            MkObject("field" -> JSArray("data", "is_share"), "reverse" -> true),
            MkObject("field" -> "ref")))))

    val alice = ctx ! mkPerson(auth)

    val first = ctx ! mkPost(auth, alice)
    val second = ctx ! mkPost(auth, alice, true)
    val third = ctx ! mkPost(auth, alice)

    val fourth = ctx ! mkPost(auth, alice, true)

    val conflict = ctx ! getInstance(auth, fourth.refObj, Clock.time)
    ctx ! runQuery(auth, Clock.time,
      InsertVers(third.refObj, conflict.ts.validTS.micros, "delete"))

    val fifth = ctx ! mkPost(auth, alice)
    val sixth = ctx ! mkPost(auth, alice, true)

    val timeline = Match(IndexRef("timeline"), alice.refObj)
    val shared = Match(IndexRef("shares"), (alice.refObj, true))
    val reversed = Match(IndexRef("reversed"), alice.refObj)

    "works" in {
      val posts = (ctx ! runQuery(auth, Paginate(timeline, size = 2))).asInstanceOf[PageL]
      posts.elems should equal (Seq(first.id, fifth.id) map { id => ArrayL(List(FalseL, RefL(scope, id))) })
      posts.before should equal (None)
      posts.after should equal (Some(CursorL(Right(ArrayL(List(TrueL, RefL(scope, second.id), RefL(scope, second.id)))))))

      val post = ctx ! getMinimal(auth, timeline)
      post.id should equal (first.id)

      val shares = (ctx ! runQuery(auth, Paginate(shared, size = 2))).asInstanceOf[PageL]
      shares.elems should equal (Seq(second.id, fourth.id) map { RefL(scope, _) })
      shares.before should equal (None)
      shares.after should equal (Some(CursorL(Right(ArrayL(List(RefL(scope, sixth.id)))))))

      val share = ctx ! getMinimal(auth, shared)
      share.docID should equal (second.id)

      val reverse = (ctx ! runQuery(auth, Paginate(reversed, size = 2))).asInstanceOf[PageL]
      reverse.elems should equal (Seq(second.id, fourth.id) map { id => ArrayL(List(TrueL, RefL(scope, id))) })
      reverse.before should equal (None)
      reverse.after should equal (Some(CursorL(Right(ArrayL(List(TrueL, RefL(scope, sixth.id), RefL(scope, sixth.id)))))))

      val postEvs = ctx ! events(auth, timeline, size = 2)
      postEvs.elems map docID should equal (Seq(first.id, second.id))

      val shareEvs = ctx ! events(auth, shared, size = 2)
      shareEvs.elems map docID should equal (Seq(second.id, fourth.id))

      val revEvs = ctx ! events(auth, reversed, size = 2)
      revEvs.elems map docID should equal (Seq(first.id, second.id))
    }

    "paginates compound collections" in {
      val after = After(fourth.refObj)
      val before = Before(fourth.refObj)

      val sharesA = (ctx ! runQuery(auth, Paginate(shared, cursor = after))).asInstanceOf[PageL]
      sharesA.elems should equal (Seq(fourth.id, sixth.id) map { RefL(scope, _) })
      sharesA.before should equal (Some(CursorL(Right(ArrayL(List(RefL(scope, fourth.id)))))))
      sharesA.after should equal (None)

      val sharesB = (ctx ! runQuery(auth, Paginate(shared, cursor = before))).asInstanceOf[PageL]
      sharesB.elems should equal (Seq(second.id) map { RefL(scope, _) })
      sharesB.before should equal (None)
      sharesB.after should equal (Some(CursorL(Right(ArrayL(List(RefL(scope, fourth.id)))))))
    }

    "paginates covered collections" in {
      val after = After((true, fourth.refObj))
      val before = Before((true, fourth.refObj))

      val postsA = (ctx ! runQuery(auth, Paginate(timeline, cursor = after))).asInstanceOf[PageL]
      postsA.elems should equal (Seq(fourth.id, sixth.id) map { id => ArrayL(List(TrueL, RefL(scope, id))) })
      postsA.before should equal (Some(CursorL(Right(ArrayL(List(TrueL, RefL(scope, fourth.id)))))))
      postsA.after should equal (None)

      val postsB = (ctx ! runQuery(auth, Paginate(timeline, cursor = before))).asInstanceOf[PageL]
      postsB.elems should equal (Seq(
        ArrayL(List(FalseL, RefL(scope, first.id))),
        ArrayL(List(FalseL, RefL(scope, fifth.id))),
        ArrayL(List(TrueL, RefL(scope, second.id)))))
      postsB.before should equal (None)
      postsB.after should equal (Some(CursorL(Right(ArrayL(List(TrueL, RefL(scope, fourth.id)))))))
    }

    "paginates reverse collections" in {
      val after = After((true, sixth.refObj, sixth.refObj))
      val before = Before((false, first.refObj, first.refObj))

      val posts = (ctx ! runQuery(auth, Paginate(reversed))).asInstanceOf[PageL]
      posts.elems should equal (Seq(
        ArrayL(List(TrueL, RefL(scope, second.id))),
        ArrayL(List(TrueL, RefL(scope, fourth.id))),
        ArrayL(List(TrueL, RefL(scope, sixth.id))),
        ArrayL(List(FalseL, RefL(scope, first.id))),
        ArrayL(List(FalseL, RefL(scope, fifth.id)))))
      posts.before should equal (None)
      posts.after should equal (None)

      val postsA = (ctx ! runQuery(auth, Paginate(reversed, cursor = after))).asInstanceOf[PageL]
      postsA.elems should equal (Seq(
        ArrayL(List(TrueL, RefL(scope, sixth.id))),
        ArrayL(List(FalseL, RefL(scope, first.id))),
        ArrayL(List(FalseL, RefL(scope, fifth.id)))))
      postsA.before should equal (Some(CursorL(Right(ArrayL(List(TrueL, RefL(scope, sixth.id), RefL(scope, sixth.id)))))))
      postsA.after should equal (None)

      val postsB = (ctx ! runQuery(auth, Paginate(reversed, cursor = before))).asInstanceOf[PageL]
      postsB.elems should equal (Seq(
        ArrayL(List(TrueL, RefL(scope, second.id))),
        ArrayL(List(TrueL, RefL(scope, fourth.id))),
        ArrayL(List(TrueL, RefL(scope, sixth.id)))))
      postsB.before should equal (None)
      postsB.after should equal (Some(CursorL(Right(ArrayL(List(FalseL, RefL(scope, first.id), RefL(scope, first.id)))))))
    }

    "paginates events" in {
      val after = After(MkObject(
        "ts" -> conflict.ts.validTS.micros,
        "action" -> "delete",
        "resource" -> second.refObj))
      val before = Before(MkObject(
        "ts" -> conflict.ts.validTS.micros,
        "action" -> "create",
        "resource" -> second.refObj))

      //full cursor
      val postsA = ctx ! events(auth, timeline, cursor = after)
      postsA.elems map docID should equal (Seq(third.id, fourth.id, fifth.id, sixth.id))
      postsA.before map docID should equal (Some(second.id))
      postsA.after map docID should equal (None)

      //long ts
      val postsA_1 = ctx ! events(auth, timeline, cursor = After(conflict.ts.validTS.micros))
      postsA_1.elems map docID should equal (Seq(fourth.id, fifth.id, sixth.id))
      postsA_1.before map docID should equal (Some(DocID.MinValue))
      postsA_1.after map docID should equal (None)

      //timestamp
      val postsA_2 = ctx ! events(auth, timeline, cursor = After(Epoch(conflict.ts.validTS.micros, "microsecond")))
      postsA_2.elems map docID should equal (Seq(fourth.id, fifth.id, sixth.id))
      postsA_2.before map docID should equal (Some(DocID.MinValue))
      postsA_2.after map docID should equal (None)

      val sharesA = ctx ! events(auth, shared, cursor = after)
      sharesA.elems map docID should equal (Seq(fourth.id, sixth.id))
      sharesA.before map docID should equal (Some(second.id))
      sharesA.after map docID should equal (None)

      val revA = ctx ! events(auth, timeline, cursor = after)
      revA.elems map docID should equal (Seq(third.id, fourth.id, fifth.id, sixth.id))
      revA.before map docID should equal (Some(second.id))
      revA.after map docID should equal (None)

      //full cursor
      val postsB = ctx ! events(auth, timeline, cursor = before)
      postsB.elems map docID should equal (Seq(first.id, second.id, third.id))
      postsB.before map docID should equal (None)
      postsB.after map docID should equal (Some(second.id))

      //long ts
      val postsB_1 = ctx ! events(auth, timeline, cursor = Before(conflict.ts.validTS.micros))
      postsB_1.elems map docID should equal (Seq(first.id, second.id, third.id))
      postsB_1.before map docID should equal (None)
      postsB_1.after map docID should equal (Some(DocID.MinValue))

      //timestamp
      val postsB_2 = ctx ! events(auth, timeline, cursor = Before(Epoch(conflict.ts.validTS.micros, "microsecond")))
      postsB_2.elems map docID should equal (Seq(first.id, second.id, third.id))
      postsB_2.before map docID should equal (None)
      postsB_2.after map docID should equal (Some(DocID.MinValue))

      val sharesB = ctx ! events(auth, shared, cursor = before)
      sharesB.elems map docID should equal (Seq(second.id))
      sharesB.before map docID should equal (None)
      sharesB.after map docID should equal (Some(second.id))

      val revB = ctx ! events(auth, reversed, cursor = before)
      revB.elems map docID should equal (Seq(first.id, second.id, third.id))
      revB.before map docID should equal (None)
      revB.after map docID should equal (Some(second.id))
    }

  }

  "BatchInsertionSpec" - {
    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    socialSetup(ctx, auth)

    ctx ! mkIndex(auth, "posts", "posts", Nil, Nil)

    val alice = ctx ! mkPerson(auth)
    val first = ctx ! mkPost(auth, alice)

    val RepoContext.Result(ts, _) = ctx !! (runQuery(auth, JSArray((0 until 15) map { _ =>
      CreateF(ClassRef("posts"),
        MkObject("data" -> MkObject("author" -> alice.refObj, "is_share" -> false)))
    })))

    val dos = (ctx ! collection(auth, Match(IndexRef("posts")))).elems map {
      case RefL(_, id) => s"classes/posts/${id.subID.toLong}"
      case _ => sys.error("ref expected")
    }

    ctx ! runQuery(auth, DeleteF(first.refObj))
    ctx ! mkPost(auth, alice)

    "works" in {
      val before = Before(MkObject(
        "ts" -> ts.micros,
        "action" -> "create",
        "resource" -> Ref(dos(3))))

      val evs = ctx ! events(auth, Match(IndexRef("posts")), cursor = before)
      evs.elems.size should equal (3)
    }

  }
}
