package fauna.lang

import scala.collection.mutable.Buffer

trait PageSyntax {
  implicit class MPageOps[M[+_], A](val m: M[Page[M, A]])(implicit M: Monad[M]) {
    def appendT[A1 >: A](other: => M[Page[M, A1]]): M[Page[M, A1]] = M.map(m) { _ append other }

    def mapT[B](f: A => B): M[Page[M, B]] = M.map(m) { _ map f }

    def mapMT[B](f: A => M[B]): M[Page[M, B]] = M.flatMap(m) { _ mapM f }

    def flatMapT[B](f: A => M[Page[M, B]]): M[Page[M, B]] = M.flatMap(m) { _ flatMap f }

    def foreachT(f: A => M[Unit]): M[Unit] = M.flatMap(m) { _ foreach f }

    def foldLeftT[R](seed: R)(f: (R, A) => R): M[R] = M.flatMap(m) { _.foldLeft(seed)(f) }

    def foldLeftMT[R](seed: R)(f: (R, A) => M[R]): M[R] = M.flatMap(m) { _.foldLeftM(seed)(f) }

    def findT(f: A => Boolean): M[Option[A]] = M.flatMap(m) { _ find f }

    def toSeqT: M[Seq[A]] = M.flatMap(m) { _.toSeq }

    def headT = M.map(m) { _.value }
  }

  implicit class MIterablePageOps[M[+_], A](val m: M[Page[M, Iterable[A]]])(implicit M: Monad[M]) {
    def mapValuesT[B](f: A => B): M[Page[M, Iterable[B]]] = M.map(m) { _ mapValues f }

    def mapValuesMT[B](f: A => M[B]): M[Page[M, Iterable[B]]] =
      M.flatMap(m) { _ mapValuesM f }

    def flatMapValuesT[B](f: A => M[Iterable[B]]): M[Page[M, Iterable[B]]] = M.flatMap(m) { _ flatMapValues f }

    def flatMapPagesT[B](f: A => M[Page[M, Iterable[B]]]): M[Page[M, Iterable[B]]] =
      M.flatMap(m) { _ flatMapPages f }

    def forallT(f: A => Boolean): M[Boolean] = M.flatMap(m) { _ forall f }

    def forallMT(f: A => M[Boolean]): M[Boolean] = {
      def forall0(iterable: Iterable[A]) : M[Boolean] =
        iterable.headOption match {
          case Some(a) => M.flatMap(f(a)) {
            case true  => forall0(iterable.tail)
            case false => M.pure(false)
          }
          case None => M.pure(true)
        }
      M.flatMap(m) {
        page => M.flatMap(forall0(page.value)) {
          case true  => page.next match {
            case Some(nextPage) => nextPage().forallMT(f)
            case None           => M.pure(true)
          }
          case false => M.pure(false)
        }
      }
    }

    def foreachValueT(f: A => M[Unit]): M[Unit] = M.flatMap(m) { _ foreachValue f }

    def findValueT(f: A => Boolean): M[Option[A]] = M.flatMap(m) { _ findValue f }

    def flattenT: M[Seq[A]] = M.flatMap(m) { _.flatten }

    def collectT[B](f: A => Option[B]): M[Page[M, Iterable[B]]] = M.map(m) { _ collect f }

    def collectMT[B](f: A => M[Option[B]]): M[Page[M, Iterable[B]]] = M.flatMap(m) { _ collectM f }

    def foldLeftValuesT[R](seed: R)(f: (R, A) => R): M[R] = M.flatMap(m) { _.foldLeftValues(seed)(f) }

    def foldLeftValuesMT[R](seed: R)(f: (R, A) => M[R]): M[R] = M.flatMap(m) { _.foldLeftValuesM(seed)(f) }

    def reduceStreamT[B, S](seed: S)(f: (Option[A], S) => (List[B], S)): M[Page[M, Iterable[B]]] =
      M.flatMap(m) { _.reduceStream(seed, f) }

    def selectT(f: A => Boolean): M[Page[M, Iterable[A]]] = M.map(m) { _ select f }

    def selectMT(f: A => M[Boolean]): M[Page[M, Iterable[A]]] = M.flatMap(m) { _ selectM f }

    def rejectT(f: A => Boolean): M[Page[M, Iterable[A]]] = M.map(m) { _ reject f }

    def rejectMT(f: A => M[Boolean]): M[Page[M, Iterable[A]]] = M.flatMap(m) { _ rejectM f }

    def alignByT(f: (A, A) => Boolean): M[Page[M, Iterable[A]]] = M.flatMap(m) { _.alignBy(f) }

    def spansT(f: (A, A) => Boolean): M[Page[M, Iterable[A]]] = M.flatMap(m) { _.spans(f) }

    def splitT(count: Int): M[Page[M, Iterable[A]]] = M.map(m) { _.split(count) }

    def takeT(count: Int): M[Page[M, Iterable[A]]] = M.map(m) { _.take(count) }

    def takeWhileT(f: A => Boolean): M[Page[M, Iterable[A]]] = M.map(m) { _.takeWhile(f) }

    def dropT(count: Int): M[Page[M, Iterable[A]]] = M.flatMap(m) { _.drop(count) }

    def dropWhileT(f: A => Boolean): M[Page[M, Iterable[A]]] = M.flatMap(m) { _.dropWhile(f) }

    def initValuesT: M[Iterable[A]] = M.flatMap(m) { _ initValues }

    def headValueT: M[Option[A]] = M.flatMap(m) { _ headValue }

    def isEmptyT: M[Boolean] = M.flatMap(m) { _ isEmpty }

    def nonEmptyT: M[Boolean] = M.flatMap(m) { _ nonEmpty }

    def countT: M[Int] = M.flatMap(m) { _ count }
  }
}

object Page extends PageSyntax {
  def apply[M[+_], A](a: A): Page[M, A] =
    Page(a, None)

  def apply[M[+_], A](value: A, next: M[Page[M, A]]): Page[M, A] =
    Page(value, Some(() => next))

  // Curry the first type argument M where it cannot be decided.
  // e.g. Allows Page[Future](2) rather than Page[Future, Int](2)

  protected final class AppliedM[M[+_]] {
    def apply[A](a: A): Page[M, A] = Page[M, A](a)

    def apply[A](a: A, next: M[Page[M, A]]): Page[M, A] = Page[M, A](a, next)

    def unfold[S, A](seed: S)(f: S => M[(A, Option[S])])(implicit M: Monad[M]): M[Page[M, A]] =
      Page.unfold[M, S, A](seed)(f)
  }

  private val _apply = new AppliedM[Any]

  final def apply[M[+_]] = _apply.asInstanceOf[AppliedM[M]]

  def unfold[M[+_], S, A](seed: S)(f: S => M[(A, Option[S])])(implicit M: Monad[M]): M[Page[M, A]] =
    M.map(f(seed)) { case (value, nextSeed) => Page(value, nextSeed map { n => () => unfold(n)(f) }) }

  def merge[M[+_]: Monad, A: Ordering](pages: Seq[M[Page[M, Iterable[A]]]], fault: Int = 5) =
    pages match {
      case Seq(page) => page
      case pages     => mergeWithStrategy(pages, PageMergeStrategy.Dedup[A], fault)
    }

  def mergeBy[M[+_]: Monad, A: Ordering, B](pages: Seq[M[Page[M, Iterable[A]]]], fault: Int = 5)(f: List[(A, Int)] => List[B]) =
    mergeWithStrategy(pages, new PageMergeStrategy.DedupBy(f), fault)

  def mergeReduce[M[+_]: Monad, A: Ordering, B, S](pages: Seq[M[Page[M, Iterable[A]]]], state: S, fault: Int = 5)(f: (Option[(A, Int)], S) => (List[B], S)) =
    mergeWithStrategy(pages, new PageMergeStrategy.Function(state, f), fault)

  def mergeWithStrategy[M[+_], A, B, S](pages: Seq[M[Page[M, Iterable[A]]]], strategy: PageMergeStrategy[A, B, S], fault: Int = 5)(implicit M: Monad[M], ord: Ordering[A]) =
    new PageMerger(pages, strategy, fault, ord, M).result
}

/**
  * A paginator of values in a monad. Inherits underlying monad's
  * evaluation semantics.
  *
  * Page behaves like a stream: the (optional) continuation is
  * lazily-evaluated as the Page is consumed to yield additional
  * elements.
 */
case class Page[M[+_], +A](value: A, next: Option[() => M[Page[M, A]]]) {
  def hasNext: Boolean = next.isDefined

  def append[A1 >: A](other: => M[Page[M, A1]])(implicit M: Monad[M]): Page[M, A1] =
    Page(value, next map { n => () => M.map(n()) { _ append other } } orElse Some(() => other))

  def map[B](f: A => B)(implicit M: Monad[M]): Page[M, B] =
    Page(f(value), next map { n => () => M.map(n()) { _ map f } })

  def mapM[B](f: A => M[B])(implicit M: Monad[M]): M[Page[M, B]] =
    M.map(f(value)) { Page(_, next map { n => () => M.flatMap(n()) { _ mapM f } }) }

  def flatMap[B](f: A => M[Page[M, B]])(implicit M: Monad[M]): M[Page[M, B]] =
    M.map(f(value)) { b =>
      next match {
        case Some(next) => b append (M.flatMap(next()) { _ flatMap f })
        case None       => b
      }
    }

  def foreach(f: A => M[Unit])(implicit M: Monad[M]): M[Unit] =
    next match {
      case Some(next) => M.flatMap(f(value)) { _ => M.flatMap(next()) { _ foreach f } }
      case None       => f(value)
    }

  def foldLeft[S](seed: S)(f: (S, A) => S)(implicit M: Monad[M]): M[S] =
    next match {
      case Some(next) => M.flatMap(next()) { _.foldLeft(f(seed, value))(f) }
      case None       => M.pure(f(seed, value))
    }

  def foldLeftM[S](seed: S)(f: (S, A) => M[S])(implicit M: Monad[M]): M[S] =
    next match {
      case Some(next) => M.flatMap(f(seed, value)) { s => M.flatMap(next()) { _.foldLeftM(s)(f) } }
      case None       => f(seed, value)
    }

  def find(f: A => Boolean)(implicit M: Monad[M]): M[Option[A]] =
    if (f(value)) M.pure(Some(value)) else next match {
      case Some(next) => M.flatMap(next()) { _ find f }
      case None       => M.pure(None)
    }

  def toSeq(implicit M: Monad[M]): M[Seq[A]] = foldLeft(Vector.empty[A]) { _ :+ _ }

  def mapValues[B, C](f: B => C)(implicit ev: A <:< Iterable[B], M: Monad[M]): Page[M, Iterable[C]] =
    this map { _ map f }

  def mapValuesM[B, C](f: B => M[C])(
    implicit ev: A <:< Iterable[B],
    M: Monad[M]): M[Page[M, Iterable[C]]] =
    M.map(M.accumulate(value map f, Vector.empty[C]) { _ :+ _ }) { value0 =>
      Page(value0, next map { n => () => M.flatMap(n()) { _ mapValuesM f } })
    }

  def flatMapValues[B, C](f: B => M[Iterable[C]])(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[C]]] =
    M.map(M.accumulate(value map f, Vector.empty[C]) { _ ++ _ }) { newValue =>
      Page(newValue, next map { n => () => M.flatMap(n()) { _ flatMapValues f } })
    }

  def flatMapPages[B, C](f: B => M[Page[M, Iterable[C]]])(
    implicit ev: A <:< Iterable[B],
    M: Monad[M]): M[Page[M, Iterable[C]]] = {
    val pqs = ev(value).iterator.map(f).to(Vector)
    def loop(pqs: Vector[M[Page[M, Iterable[C]]]]): M[Page[M, Iterable[C]]] =
      if (pqs.isEmpty) {
        next match {
          case Some(thnk) => M.flatMap(thnk()) { _ flatMapPages f }
          case None       => M.pure(Page(Nil))
        }
      } else {
        pqs.head appendT loop(pqs.tail)
      }
    loop(pqs)
  }

  def forall[B](f: B => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Boolean] =
    (value forall f, next) match {
      case (false, _)         => M.pure(false)
      case (true, Some(next)) => M.flatMap(next()) { _ forall f }
      case (true, None)       => M.pure(true)
    }

  def foreachValue[B](f: B => M[Unit])(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Unit] =
    this foreach { iter => M.accumulate(iter map f, ()) { (_, _) => () } }

  def findValue[B](f: B => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Option[B]] =
    (value find f, next) match {
      case (Some(b), _)       => M.pure(Some(b))
      case (None, Some(next)) => M.flatMap(next()) { _ findValue f }
      case (None, None)       => M.pure(None)
    }

  def flatten[C](implicit ev: A <:< Iterable[C], M: Monad[M]): M[Seq[C]] =
    foldLeft(Vector.empty[C]) { (cs, a) => cs ++ ev(a) }

  def collect[B, C](f: B => Option[C])(implicit ev: A <:< Iterable[B], M: Monad[M]): Page[M, Iterable[C]] =
    this map { _ flatMap { f(_).toSeq } }

  def foldLeftValues[B, S](seed: S)(f: (S, B) => S)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[S] =
    foldLeft(seed) { (s, bs) => (bs foldLeft s)(f) }

  def foldLeftValuesM[B, S](seed: S)(f: (S, B) => M[S])(implicit ev: A <:< Iterable[B], M: Monad[M]): M[S] = {
    def fold0(state: S, s: LazyList[B]): M[S] =
      if (s.isEmpty) {
        M.pure(state)
      } else {
        M.flatMap(f(state, s.head)) { fold0(_, s.tail) }
      }

    foldLeftM(seed) { (s, bs) => fold0(s, bs.to(LazyList)) }
  }

  def reduceStream[B, C, S](seed: S, f: (Option[B], S) => (List[C], S))
    (implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[C]]] = {

    val buf = List.newBuilder[C]
    var state = seed

    ev(value) foreach { b =>
      val (cs, s) = f(Some(b), state)
      buf ++= cs
      state = s
    }

    next match {
      case None =>
        buf ++= f(None, state)._1
        M.pure(Page(buf.result(), None))

      case Some(m) =>
        val next = () => M.flatMap(m()) { _.reduceStream(state, f) }

        buf.result() match {
          case Nil => next()
          case cs  => M.pure(Page(cs, Some(next)))
        }
    }
  }

  def select[B](f: B => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): Page[M, Iterable[B]] =
    this map { _ filter f }

  def reject[B](f: B => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): Page[M, Iterable[B]] =
    this map { _ filterNot f }

  def collectM[B, C](f: B => M[Option[C]])(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[C]]] =
    this mapM { a =>
      val res = ev(a) map f
      M.accumulate(res, Vector.empty[C]) { (s, c) => c match { case Some(c) => s :+ c; case None => s } }
    }

  def selectM[B](f: B => M[Boolean])(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[B]]] =
    this mapM { a =>
      val res = ev(a) map { b => M.map(f(b)) { if (_) Some(b) else None } }
      M.accumulate(res, Vector.empty[B]) { (s, b) => b match { case Some(b) => s :+ b; case None => s } }
    }

  def rejectM[B](f: B => M[Boolean])(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[B]]] =
    this mapM { a =>
      M.accumulate(ev(a) map { b => M.map(f(b)) { if (_) None else Some(b) } }, Vector.empty[B]) { _ ++ _ }
    }

  def alignBy[B](equiv: (B, B) => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[B]]] = {
    def alignBy0(prevGrp: Iterable[B], page: Page[M, A]): M[Page[M, Iterable[B]]] =
      page match {
        case Page(value, Some(next)) if value.isEmpty => M.flatMap(next()) { alignBy0(prevGrp, _) }

        case Page(value, Some(next)) =>
          val b = Vector.newBuilder[B]
          val group = Buffer.concat(prevGrp)

          var prev = if (group.isEmpty) value.head else group.last
          val iter = value.iterator

          while (iter.hasNext) {
            val n = iter.next()

            if (!equiv(prev, n)) {
              b ++= group
              group.clear()
            }

            group += n
            prev = n
          }

          b.result() match {
            case rv if rv.isEmpty => M.flatMap(next()) { alignBy0(group, _) }
            case rv => M.pure(Page(rv, Some(() => M.flatMap(next()) { alignBy0(group, _) })))
          }

        case p @ Page(_, None) if prevGrp.isEmpty =>
          M.pure(p.asInstanceOf[Page[M, Iterable[B]]])

        case Page(value, None) =>
          val b = Vector.newBuilder[B]
          b ++= prevGrp
          b ++= value
          M.pure(Page(b.result()))
      }

    if (next.isEmpty) {
      M.pure(this.asInstanceOf[Page[M, Iterable[B]]])
    } else {
      alignBy0(Nil, this)
    }
  }

  def spans[B](equiv: (B, B) => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[B]]] = {
    def spans0(elems: Iterable[B]): M[Page[M, Iterable[B]]] =
      if (elems.isEmpty) M.pure(Page(Nil, None)) else {
        var prev = elems.head
        elems span { i =>
          val p = prev
          prev = i
          equiv(p, i)
        } match {
          case (l, r) if r.isEmpty => M.pure(Page(l, None))
          case (l, r)              => M.pure(Page(l, Some(() => spans0(r))))
        }
      }

    this flatMap { elems => spans0(ev(elems)) }
  }

  /**
    * Splits this page stream into pages of at most +count+ elements per page.
    */
  def split[B](count: Int)(implicit
    ev: A <:< Iterable[B],
    M: Monad[M]): Page[M, Iterable[B]] = {
    val elems = ev(value)
    val splitNext = next map { n => () => M.map(n()) { _.split(count) } }
    if (elems.isEmpty) {
      Page(Nil, splitNext)
    } else if (elems.size <= count) {
      Page(elems, splitNext)
    } else {
      val groupedElemsReverseIter = elems.iterator.grouped(count).toArray.reverseIterator
      val lastGroup = groupedElemsReverseIter.next()
      val tailPage = Page(lastGroup, splitNext)
      groupedElemsReverseIter.foldLeft(tailPage) {
        case (acc, cur) =>
          Page(cur, M.pure(acc))
      }
    }
  }

  def take[B](count: Int)(implicit ev: A <:< Iterable[B], M: Monad[M]): Page[M, Iterable[B]] =
    if (count <= 0) Page(Nil, None) else {
      var left = count

      val taken = ev(value) takeWhile { _ =>
        if (left == 0) false else {
          left -= 1
          true
        }
      }

      val n = next match {
        case Some(n) if left > 0 => Some(() => M.map(n()) { _ take left })
        case _                   => None
      }

      Page(taken, n)
    }

  def takeWhile[B](f: B => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): Page[M, Iterable[B]] = {
    var taking = true
    val taken = ev(value) takeWhile { e =>
      taking = f(e)
      taking
    }

    val n = next match {
      case Some(n) if taking => Some(() => M.map(n()) { _ takeWhile f })
      case _                 => None
    }

    Page(taken, n)
  }

  def drop[B](count: Int)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[B]]] =
    if (count <= 0) M.pure(this.asInstanceOf[Page[M, Iterable[B]]]) else {
      var left = count

      val dropped = ev(value) dropWhile { _ =>
        if (left == 0) false else {
          left -= 1
          true
        }
      }

      if (dropped.nonEmpty) M.pure(Page(dropped, next)).asInstanceOf[M[Page[M, Iterable[B]]]] else {
        next match {
          case None    => M.pure(Page(Nil, None))
          case Some(n) => M.flatMap(n()) { _ drop left }
        }
      }
    }

  def dropWhile[B](f: B => Boolean)(implicit ev: A <:< Iterable[B], M: Monad[M]): M[Page[M, Iterable[B]]] = {
    val dropped = ev(value) dropWhile f

    if (dropped.nonEmpty) M.pure(Page(dropped, next)).asInstanceOf[M[Page[M, Iterable[B]]]] else {
      next match {
        case None    => M.pure(Page(Nil, None))
        case Some(n) => M.flatMap(n()) { _ dropWhile f }
      }
    }
  }

  def initValues[B](implicit ev: A <:< Iterable[B], M: Monad[M]): M[Iterable[B]] =
    (value, next) match {
      case (vs, Some(next)) if vs.isEmpty => M.flatMap(next()) { _ initValues }
      case (vs, _)                        => M.pure(vs)
    }

  def headValue[B](implicit ev: A <:< Iterable[B], M: Monad[M]): M[Option[B]] =
    (value.headOption, next) match {
      case (s @ Some(_), _)   => M.pure(s)
      case (None, Some(next)) => M.flatMap(next()) { _ headValue }
      case (None, None)       => M.pure(None)
    }

  def isEmpty[B](implicit ev: A <:< Iterable[B], M: Monad[M]): M[Boolean] =
    (ev(value).isEmpty, next) match {
      case (false, _)         => M.pure(false)
      case (true, Some(next)) => M.flatMap(next()) { _ isEmpty }
      case (true, None)       => M.pure(true)
    }

  def nonEmpty[B](implicit ev: A <:< Iterable[B], M: Monad[M]): M[Boolean] =
    (ev(value).nonEmpty, next) match {
      case (true, _)           => M.pure(true)
      case (false, Some(next)) => M.flatMap(next()) { _ nonEmpty }
      case (false, None)       => M.pure(false)
    }

  def count(implicit ev: A <:< Iterable[_], M: Monad[M]): M[Int] =
    foldLeft(0) { _ + _.size }
}
