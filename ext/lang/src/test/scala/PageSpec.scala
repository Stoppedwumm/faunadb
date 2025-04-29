package fauna.lang.test

import fauna.lang._
import fauna.lang.syntax._
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class Thunk[+A](a: => A) {
  def get: A = a

  def map[B](f: A => B) = new Thunk(f(a))

  def flatMap[B](f: A => Thunk[B]) = new Thunk(f(a).get)
}

object Thunk {
  def apply[A](a: => A) = new Thunk(a)

  implicit object MonadInstance extends Monad[Thunk] {
    def pure[A](a: A): Thunk[A] = Thunk(a)
    def map[A, B](m: Thunk[A])(f: A => B): Thunk[B] = m map f
    def flatMap[A, B](m: Thunk[A])(f: A => Thunk[B]): Thunk[B] = m flatMap f

    def accumulate[A, B](ms: Iterable[Thunk[A]], seed: B)(f: (B, A) => B): Thunk[B] = {
      def accum0(iter: List[Thunk[A]], seed: B): Thunk[B] = iter match {
        case Nil => pure(seed)
        case n :: rest => flatMap(n) { a => accum0(rest, f(seed, a)) }
      }

      accum0(ms.toList, seed)
    }
  }
}

object PagedThunk {
  def apply[T](t: T, next: Thunk[Page[Thunk, T]]) = Thunk(Page[Thunk](t, next))
  def apply[T](t: T) = Thunk(Page[Thunk](t))
}

class PageSpec extends Spec {

  "Page" - {
    "unfold" in {
      val f = Page.unfold(5) { s => Option((s, if (s > 0) Some(s - 1) else None)) }

      f flatMap { _.toSeq } should equal (Some(Seq(5, 4, 3, 2, 1, 0)))
    }

    "count" in {
      val page1 = Page[Option, Seq[Int]](Seq.fill(2)(1))
      val page2 = Page[Option, Seq[Int]](Seq.fill(3)(2), Some(page1))

      page1.count should be (Some(2))
      page2.count should be (Some(5))
    }

    "creates a page that has a next page" in {
      val p2 = Page[Option](2)
      val p1 = Page[Option](1, Option(p2))

      p2.hasNext should be (false)
      p1.hasNext should be (true)

      p1.toSeq should equal (Some(List(1, 2)))
    }

    "foreach" - {
      "is sequential" in {
        import scala.concurrent.ExecutionContext.Implicits.global
        val orderOrCompletion = Seq.newBuilder[Int]
        val delays = Page[Future](10, Future(Page[Future](0)))
        val foreachF =
          delays foreach { delay =>
            Future {
              Thread.sleep(delay)
              orderOrCompletion.synchronized {
                orderOrCompletion += delay
              }
            }
          }
        Await.result(foreachF, 1.second)
        // It should complete the first page element even though it has a bigger
        // delay so that `foreach` can preserve sequential semantics.
        orderOrCompletion.result() should contain.inOrderOnly(10, 0)
      }
    }

    "take/drop" - {
      val p = PagedThunk(Seq(1, 2, 3), PagedThunk(Seq(4, 5, 6), PagedThunk(Seq(7, 8))))

      "take" in {
        (p takeT 2).flattenT.get should equal (Seq(1, 2))
        (p takeT 2).headT.get should equal (Seq(1, 2))

        (p takeT 0).flattenT.get should equal (Nil)
        (p takeT 0).headT.get should equal (Nil)

        (p takeT 100).flattenT.get should equal (p.flattenT.get)
        (p takeT 100).headT.get should equal (Seq(1, 2, 3))
      }

      "takeWhile" in {
        (p takeWhileT { _ <= 2 }).flattenT.get should equal (Seq(1, 2))
        (p takeWhileT { _ <= 2 }).headT.get should equal (Seq(1, 2))

        (p takeWhileT { _ => false }).flattenT.get should equal (Nil)
        (p takeWhileT { _ => false }).headT.get should equal (Nil)

        (p takeWhileT { _ => true }).flattenT.get should equal (p.flattenT.get)
        (p takeWhileT { _ => true }).headT.get should equal (Seq(1, 2, 3))
      }

      "drop" in {
        (p dropT 2).flattenT.get should equal (Seq(3, 4, 5, 6, 7, 8))
        (p dropT 2).headT.get should equal (Seq(3))

        (p dropT 0).flattenT.get should equal (p.flattenT.get)
        (p dropT 0).headT.get should equal (Seq(1, 2, 3))

        (p dropT 100).flattenT.get should equal (Nil)
        (p dropT 100).headT.get should equal (Nil)
      }

      "dropWhile" in {
        (p dropWhileT { _ <= 2 }).flattenT.get should equal (Seq(3, 4, 5, 6, 7, 8))
        (p dropWhileT { _ <= 2 }).headT.get should equal (Seq(3))

        (p dropWhileT { _ => false }).flattenT.get should equal (p.flattenT.get)
        (p dropWhileT { _ => false }).headT.get should equal (Seq(1, 2, 3))

        (p dropWhileT { _ => true }).flattenT.get should equal (Nil)
        (p dropWhileT { _ => true }).headT.get should equal (Nil)
      }
    }

    "merge" - {
      val p1 = PagedThunk(Seq(1, 2, 3), PagedThunk(Seq(4, 5, 6)))
      val p2 = p1 mapValuesT { _ * 2 }
      val p3 = p2 mapValuesT { _ - 1 }
      val mp = Page.merge(Seq(p2, p3))

      "works" in {
        mp.flattenT.get should equal (1 to 12)
      }

      "dedups" in {
        p1.flattenT.get should equal (Page.merge(Seq(p1, p1)).flattenT.get)
      }

      "reduce works" in {
        val mp = Page.mergeReduce(Seq(p1, p2), ()) {
          case (Some((v, i)), _) if i == 0 => (List(v), ())
          case (_, _)                      => (Nil, ())
        }

        mp.flattenT.get should equal (p1.flattenT.get)
      }

      "avoids bear traps" in {
        // Specifically test that sharing of page tails does not leak
        // the internal mutable state of merge. The Thunk class is
        // required to test, because it recomputes its value when
        // calling get.
        val (head, t1, t2) = mp map { case Page(v, next) =>
          (v, next.get.apply().flattenT.get, next.get.apply().flattenT.get)
        } get

        t1 should equal (t2)

        (head ++ t2 ++ t2).toList.distinct.sorted should equal (mp.flattenT.get)
      }
    }

    "alignBy" in {
      val in3 = Seq(Page[Iterable](Seq(7, 8, 9, 10)))
      val in2 = Seq(Page[Iterable](Seq(3, 4, 5, 6), in3))
      val in1 = Seq(Page[Iterable](Seq(1, 2, 3, 3), in2))

      // elements on boundary are deferred.
      val out3 = Seq(Page[Iterable](Seq(6, 7, 8, 9, 10)))
      val out2 = Seq(Page[Iterable](Seq(3, 3, 3, 4, 5), out3))
      val out1 = Seq(Page[Iterable](Seq(1, 2), out2))

      (in1.alignByT { (a, b) => a == b } flattenT) should equal (out1.flattenT)
    }
  }
}
