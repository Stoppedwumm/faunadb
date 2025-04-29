package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.model.RenderContext
import fauna.repo.test.CassandraHelper
import scala.util.Random

class IndexMVASpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  def page(auth: Auth, expr: JSValue) =
    (ctx ! runQuery(auth, expr)).asInstanceOf[PageL]

  "IndexMVASpec" - {
    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    ctx ! mkCollection(auth, MkObject("name" -> "folks"))

    Seq("mva" -> true, "no_mva" -> false).foreach { case (name, mva) =>
      ctx ! runQuery(
        auth,
        CreateIndex(
          MkObject(
            "name" -> name,
            "source" -> ClassRef("folks"),
            "terms" -> JSArray(
              MkObject("field" -> JSArray("data", "term"), "mva" -> mva)),
            "values" -> JSArray(
              MkObject("field" -> JSArray("data", "v1"), "mva" -> mva),
              MkObject("field" -> JSArray("data", "v2"), "mva" -> mva))
          ))
      )
    }

    def mkFolk(term: JSValue, v1: JSValue, v2: JSValue) =
      runQuery(
        auth,
        CreateF(
          ClassRef("folks"),
          MkObject("data" -> MkObject("term" -> term, "v1" -> v1, "v2" -> v2))))

    "non-MVA allows object terms" in {
      ctx ! mkFolk(MkObject("a" -> 1), 1, 1)

      val res1 = page(auth, Paginate(Match("no_mva", MkObject("a" -> 1))))
      res1.elems shouldEqual List(ArrayL(LongL(1), LongL(1)))

      ctx ! mkFolk(MkObject("b" -> 1, "a" -> 2), 2, 2)

      // lookup is successful regardless of object field order
      val res2 = page(auth, Paginate(Match("no_mva", MkObject("b" -> 1, "a" -> 2))))
      res2.elems shouldEqual List(ArrayL(LongL(2), LongL(2)))
      val res3 = page(auth, Paginate(Match("no_mva", MkObject("a" -> 2, "b" -> 1))))
      res3.elems shouldEqual List(ArrayL(LongL(2), LongL(2)))
    }

    "non-MVA allows object values (that sort correctly)" in {
      val expected = (1 to 10).map { i =>
        ArrayL(ObjectL("abcdef".split("").map(_ -> LongL(i)).toSeq: _*), LongL(i))
      }.toList

      (1 to 10).foreach { i =>
        val ns = "abcdef".split("")
        ctx ! mkFolk(
          1,
          MkObject(Random.shuffle(ns.toSeq).map(_ -> JSLong(i)): _*),
          i)
      }

      val res1 = page(auth, Paginate(Match("no_mva", 1))).elems
      res1 shouldEqual expected

      val res2 = {
        val b = List.newBuilder[Literal]
        var p = page(auth, Paginate(Match("no_mva", 1), size = 1))
        b ++= p.elems
        while (p.after.isDefined) {
          val cursor = {
            val lit = p.after.get
            val buf =
              ctx ! RenderContext.render(auth, APIVersion.V4, Clock.time, lit)
            val JSArray(obj: JSObject, i, r) = JS.parse(buf)
            // shuffle the object cursor in order to exercise index term
            // normalization
            val shuffled = JSObject(Random.shuffle(obj.value): _*)
            val js = JSArray(shuffled, i, r)
            After(Quote(js))
          }
          p = page(auth, Paginate(Match("no_mva", 1), size = 1, cursor = cursor))
          b ++= p.elems
        }
        b.result()
      }
      res2 shouldEqual expected
    }
  }
}
