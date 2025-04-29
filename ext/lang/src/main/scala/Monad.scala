package fauna.lang

import java.util.concurrent.atomic.AtomicInteger
import language.implicitConversions
import scala.collection.immutable.Queue
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

/** In-house definition of the Monad typeclass.
  *
  * Provides the expected functions `pure`, `map`, and `flatMap` as well as
  * `sequence` and shapeless-style `par` functions for flatMapping over
  * tuples in "parallel" in that the `par` functions only make one call to
  * `flatMap`.
  *
  * This class was written before the proliferation of packages such as
  * `cats`, `scalaz`, and `shapeless`. Packages like `cats` and `scalaz`
  * provide a monad typeclass, and `shapeless` provides the tuple-like type
  * `HList`.
  *
  * @tparam M implemented monad instance
  */
trait Monad[M[+_]] {
  def pure[A](a: A): M[A]
  def map[A, B](m: M[A])(f: A => B): M[B]
  def flatMap[A, B](m: M[A])(f: A => M[B]): M[B]

  /** Applies a function to an ordered collection similarly to `foldLeft` and
    * `foldRight`. Implementation of this method determines the order in which
    * the elements are processed. Implementations may backtrack and evaluate
    * the accumulation function multiple times for the same result. It is recommended
    * to use a persistent (immutable) datastructure.
    * (In particular this is true for the Query monad.)
    *
    * inefficient example implementation for forward processing: {{{
    * ms.foldRight(M.pure(seed)) { (ma, mb) => ma.flatMap { a => mb.map { b => f(b, a) } } }
    * }}}
    *
    * @param ms ordered collection of effects
    * @param seed seed for processing with f
    * @param f function to be applied in an order to ms
    * @tparam A innermost type of ms
    * @tparam B innermost type of output
    * @return the accumulation of f on all elements of ms
    */
  def accumulate[A, B](ms: Iterable[M[A]], seed: B)(f: (B, A) => B): M[B]

  def sequence[A](ms: Iterable[M[A]]): M[Seq[A]] =
    accumulate(ms, Queue.empty[A]) { _ :+ _ }

  /** Joins effects of M. Order of effects is determined by the implementation
    * of `accumulate`.
    *
    * @param ms ordered collection of effects
    * @tparam A innermost type
    * @return effects without values
    */
  def join[A](ms: Iterable[M[A]]): M[Unit] =
    accumulate(ms, ()) { (_, _) => () }

  /** The methods `par2` through `par7` provide shapeless-style par` functions
    * for flatMapping over tuples in "parallel" in that the `par` functions only
    * make one call to `flatMap`.
    *
    * This was written before the proliferation of packages such as `shapeless`
    * which provide tuple-like types which has similar capabilities.
    *
    * @param ms arbitrary number of effects
    * @tparam A innermost type of effectful value
    * @return the sequence of ms
    */
  private def parM(ms: M[Any]*): M[List[Any]] =
    accumulate(ms, Nil: List[Any]) { (l, a) => a :: l }

  def par2[A, B, C](ma: M[A], mb: M[B])(fn: (A, B) => M[C]): M[C] =
    flatMap(parM(mb, ma)) {
      case a :: b :: Nil => fn.tupled((a, b).asInstanceOf[(A, B)])
      case l =>
        throw new IllegalArgumentException(s"expected list of size 2 but got $l")
    }

  def par3[A, B, C, D](ma: M[A], mb: M[B], mc: M[C])(fn: (A, B, C) => M[D]): M[D] =
    flatMap(parM(mc, mb, ma)) {
      case a :: b :: c :: Nil => fn.tupled((a, b, c).asInstanceOf[(A, B, C)])
      case l =>
        throw new IllegalArgumentException(s"expected list of size 3 but got $l")
    }

  def par4[A, B, C, D, E](ma: M[A], mb: M[B], mc: M[C], md: M[D])(
    fn: (A, B, C, D) => M[E]): M[E] =
    flatMap(parM(md, mc, mb, ma)) {
      case a :: b :: c :: d :: Nil =>
        fn.tupled((a, b, c, d).asInstanceOf[(A, B, C, D)])
      case l =>
        throw new IllegalArgumentException(s"expected list of size 4 but got $l")
    }

  def par5[A, B, C, D, E, F](ma: M[A], mb: M[B], mc: M[C], md: M[D], me: M[E])(
    fn: (A, B, C, D, E) => M[F]): M[F] =
    flatMap(parM(me, md, mc, mb, ma)) {
      case a :: b :: c :: d :: e :: Nil =>
        fn.tupled((a, b, c, d, e).asInstanceOf[(A, B, C, D, E)])
      case l =>
        throw new IllegalArgumentException(s"expected list of size 5 but got $l")
    }

  def par6[A, B, C, D, E, F, G](
    ma: M[A],
    mb: M[B],
    mc: M[C],
    md: M[D],
    me: M[E],
    mf: M[F])(fn: (A, B, C, D, E, F) => M[G]): M[G] =
    flatMap(parM(mf, me, md, mc, mb, ma)) {
      case a :: b :: c :: d :: e :: f :: Nil =>
        fn.tupled((a, b, c, d, e, f).asInstanceOf[(A, B, C, D, E, F)])
      case l =>
        throw new IllegalArgumentException(s"expected list of size 6 but got $l")
    }

  def par7[A, B, C, D, E, F, G, H](
    ma: M[A],
    mb: M[B],
    mc: M[C],
    md: M[D],
    me: M[E],
    mf: M[F],
    mg: M[G])(fn: (A, B, C, D, E, F, G) => M[H]): M[H] =
    flatMap(parM(mg, mf, me, md, mc, mb, ma)) {
      case a :: b :: c :: d :: e :: f :: g :: Nil =>
        fn.tupled((a, b, c, d, e, f, g).asInstanceOf[(A, B, C, D, E, F, G)])
      case l =>
        throw new IllegalArgumentException(s"expected list of size 7 but got $l")
    }
}

trait MonadAlt[M[+_]] extends Monad[M] {
  def orElse[A, A1 >: A](m: M[A], alt: => M[A1]): M[A1]
}

trait MonadEmpty[M[+_]] extends MonadAlt[M] {
  def empty[A]: M[A]
  def select[A](m: M[A])(f: A => Boolean): M[A]

  def collect[A, B](m: M[A])(f: PartialFunction[A, M[B]]) = flatMap(m) {
    f.applyOrElse(_, Function.const(empty[B]))
  }
}

/** Provides evidence via `distribMap` that this monad can be composed with another arbitrary
  * monad of type `OM`. Some monads cannot satisfy this property.
  *
  * @tparam M monad that satisfies the type of distribMap
  */
trait MonadDistribute[M[+_]] extends Monad[M] {
  def distribMap[A, B, OM[+_]](m: M[A])(f: A => OM[M[B]])(
    implicit OM: Monad[OM]): OM[M[B]]
}

trait MonadAltDistribute[M[+_]] extends MonadDistribute[M] with MonadAlt[M] {
  def distribOrElse[A, A1 >: A, OM[+_]](m: M[A], alt: => OM[M[A1]])(
    implicit OM: Monad[OM]): OM[M[A1]]
  def distribOrAlt[A, A1 >: A, OM[+_]](m: M[A], alt: => A1)(
    implicit OM: Monad[OM]): OM[A1]
}

trait MonadEmptyDistribute[M[+_]] extends MonadAltDistribute[M] with MonadEmpty[M] {}

trait MonadException[M[+_]] extends Monad[M] {
  def fail[A](e: Throwable): M[A]
  def recover[A, A1 >: A](m: M[A], pf: PartialFunction[Throwable, A1]): M[A1]
  def recoverWith[A, A1 >: A](m: M[A], pf: PartialFunction[Throwable, M[A1]]): M[A1]
}

object MonadException {
  def apply[MA](implicit M: Unpack[MonadException, MA]): MonadException[M.M] = M.TC
}

object Monad {

  def apply[MA](implicit M: Unpack[Monad, MA]): Monad[M.M] = M.TC

  // Specific instances

  /** Typeclass instance of `MonadEmptyDistribute[Option]`.
    *
    * Used to compose arbitrary Monads with `Option` via `MonadT`. MonadT[OM, Option]
    * is Comparable to `OptionT` from both `cats` and `scalaz`.
    */
  implicit val optionInstance = new MonadEmptyDistribute[Option] {
    def pure[A](a: A): Option[A] = Some(a)
    def empty[A]: Option[A] = None
    def orElse[A, A1 >: A](m: Option[A], alt: => Option[A1]) = m orElse alt
    def map[A, B](m: Option[A])(f: A => B) = m map f

    def flatMap[A, B](m: Option[A])(f: A => Option[B]) = m flatMap f

    def accumulate[A, B](ms: Iterable[Option[A]], seed: B)(
      f: (B, A) => B): Option[B] = {
      var state: Option[B] = Some(seed)
      val iter = ms.iterator

      while (iter.hasNext && state.isDefined) {
        state = iter.next() map { a => f(state.get, a) }
      }

      state
    }

    def distribMap[A, B, OM[+_]](m: Option[A])(f: A => OM[Option[B]])(
      implicit OM: Monad[OM]) = m match {
      case Some(x) => f(x)
      case None    => OM.pure(None)
    }

    def select[A](m: Option[A])(f: A => Boolean) = m filter f

    def distribOrElse[A, A1 >: A, OM[+_]](m: Option[A], alt: => OM[Option[A1]])(
      implicit OM: Monad[OM]) =
      if (m.isEmpty) alt else OM.pure(m)

    def distribOrAlt[A, A1 >: A, OM[+_]](m: Option[A], alt: => A1)(
      implicit OM: Monad[OM]) =
      OM.pure(m getOrElse alt)
  }

  implicit final def eitherInstance[L] =
    _eitherInstance
      .asInstanceOf[MonadAltDistribute[({ type ap[+x] = Either[L, x] })#ap]]

  private val _eitherInstance =
    new MonadAltDistribute[({ type ap[+x] = Either[Any, x] })#ap] {
      def pure[A](a: A): Either[Any, A] = Right(a)
      def orElse[A, A1 >: A](m: Either[Any, A], alt: => Either[Any, A1]) =
        if (m.isLeft) alt else m
      def map[A, B](m: Either[Any, A])(f: A => B) = m map f
      def flatMap[A, B](m: Either[Any, A])(f: A => Either[Any, B]) = m flatMap f

      def accumulate[A, B](ms: Iterable[Either[Any, A]], seed: B)(f: (B, A) => B) = {
        var state: Either[Any, B] = Right(seed)
        val iter = ms.iterator

        while (iter.hasNext && state.isRight) {
          state = iter.next() map { a =>
            f(state.getOrElse(null.asInstanceOf[B]), a)
          }
        }

        state
      }

      def distribMap[A, B, OM[+_]](m: Either[Any, A])(f: A => OM[Either[Any, B]])(
        implicit OM: Monad[OM]) = m match {
        case Right(x) => f(x)
        case Left(x)  => OM.pure(Left(x))
      }

      def distribOrElse[A, A1 >: A, OM[+_]](
        m: Either[Any, A],
        alt: => OM[Either[Any, A1]])(implicit OM: Monad[OM]) =
        if (m.isLeft) alt else OM.pure(m)

      def distribOrAlt[A, A1 >: A, OM[+_]](m: Either[Any, A], alt: => A1)(
        implicit OM: Monad[OM]) =
        m match {
          case Left(_)  => OM.pure(alt)
          case Right(r) => OM.pure(r)
        }
    }

  /** Typeclass instance of `MonadEmptyDistribute[Iterable]`.
    *
    * Legacy type - new usage is discouraged.
    */
  implicit val iterableInstance = new MonadEmptyDistribute[Iterable] {
    def pure[A](a: A): Iterable[A] = Iterable(a)
    def empty[A]: Iterable[A] = Nil
    def orElse[A, A1 >: A](m: Iterable[A], alt: => Iterable[A1]) =
      if (m.isEmpty) alt else m
    def map[A, B](m: Iterable[A])(f: A => B) = m map f
    def flatMap[A, B](m: Iterable[A])(f: A => Iterable[B]) = m flatMap f

    override def sequence[A](ms: Iterable[Iterable[A]]): Iterable[Seq[A]] =
      accumulate(ms, Nil: List[A]) { (b, a) => a :: b }

    def accumulate[A, B](ms: Iterable[Iterable[A]], seed: B)(
      f: (B, A) => B): Iterable[B] = {
      def accum0(state: Iterable[B], ms: Iterable[Iterable[A]]): Iterable[B] =
        if (ms.isEmpty) state
        else {
          val head = ms.head
          val tail = ms.tail

          head flatMap { a =>
            accum0(state, tail) map { b =>
              f(b, a)
            }
          }
        }

      accum0(Seq(seed), ms)
    }

    def distribMap[A, B, OM[+_]](m: Iterable[A])(f: A => OM[Iterable[B]])(
      implicit OM: Monad[OM]) = {
      def loop(iter: LazyList[A]): OM[Iterable[B]] = {
        val a = iter.head
        val rest = iter.tail

        OM.flatMap(f(a)) { bs =>
          if (rest.isEmpty) OM.pure(bs) else OM.map(loop(rest)) { bs ++ _ }
        }
      }

      if (m.isEmpty) {
        OM.pure(Nil)
      } else {
        loop(m.to(LazyList))
      }
    }

    def select[A](m: Iterable[A])(f: A => Boolean) = m filter f

    def distribOrElse[A, A1 >: A, OM[+_]](m: Iterable[A], alt: => OM[Iterable[A1]])(
      implicit OM: Monad[OM]) =
      if (m.isEmpty) alt else OM.pure(m)

    def distribOrAlt[A, A1 >: A, OM[+_]](m: Iterable[A], alt: => A1)(
      implicit OM: Monad[OM]) =
      if (m.isEmpty) OM.pure(alt) else OM.pure(m.head)
  }

  implicit def futureInstance(implicit ec: ExecutionContext) =
    new MonadException[Future] {
      def pure[A](a: A): Future[A] = Future.successful(a)
      def map[A, B](future: Future[A])(f: A => B): Future[B] = future map f
      def flatMap[A, B](future: Future[A])(f: A => Future[B]): Future[B] =
        future flatMap f

      def accumulate[A, B](ms: Iterable[Future[A]], seed: B)(
        f: (B, A) => B): Future[B] =
        Future.sequence(ms) map { _.foldLeft(seed)(f) }

      override def sequence[A](ms: Iterable[Future[A]]) = Future.sequence(ms.toSeq)

      override def join[A](ms: Iterable[Future[A]]) =
        if (ms.isEmpty) {
          Future.unit
        } else {
          val retP = Promise[Unit]()
          val count = new AtomicInteger(ms.size)
          ms foreach {
            _.onComplete {
              case Success(_) =>
                if (count.decrementAndGet() == 0) retP.trySuccess(())
              case f @ Failure(_) => retP.tryComplete(f.asInstanceOf[Try[Unit]])
            }(ExecutionContext.parasitic) // XXX: can't access IEC
          }
          retP.future
        }

      def fail[A](e: Throwable): Future[A] = Future.failed(e)

      def recover[A, A1 >: A](
        m: Future[A],
        pf: PartialFunction[Throwable, A1]): Future[A1] = m.recover(pf)

      def recoverWith[A, A1 >: A](
        m: Future[A],
        pf: PartialFunction[Throwable, Future[A1]]): Future[A1] = m.recoverWith(pf)
    }
}

trait MonadSyntax_0 {
  implicit class UnpackAccumulateOp[MA](ms: Iterable[MA])(
    implicit val M: IsMonad[MA]) {
    def accumulate[B](seed: B)(f: (B, M.A) => B): M.M[B] =
      M.TC.accumulate(ms.asInstanceOf[Iterable[M.M[M.A]]], seed)(f)

    def sequence = M.TC.sequence(ms.asInstanceOf[Iterable[M.M[M.A]]])

    def join = M.TC.join(ms.asInstanceOf[Iterable[M.M[M.A]]])
  }
}

trait MonadSyntax_1 extends MonadSyntax_0 {
  implicit class AccumulateOp[M[+_], A](ms: Iterable[M[A]])(implicit M: Monad[M]) {
    def accumulate[B](seed: B)(f: (B, A) => B): M[B] = M.accumulate(ms, seed)(f)

    def sequence = M.sequence(ms)

    def join = M.join(ms)
  }

  implicit class Par2Op[M[+_], A, B](t: (M[A], M[B]))(implicit M: Monad[M]) {
    def par[RV](fn: (A, B) => M[RV]) = M.par2(t._1, t._2)(fn)
  }

  implicit class Par3Op[M[+_], A, B, C](t: (M[A], M[B], M[C]))(
    implicit M: Monad[M]) {
    def par[RV](fn: (A, B, C) => M[RV]) = M.par3(t._1, t._2, t._3)(fn)
  }

  implicit class Par4Op[M[+_], A, B, C, D](t: (M[A], M[B], M[C], M[D]))(
    implicit M: Monad[M]) {
    def par[RV](fn: (A, B, C, D) => M[RV]) = M.par4(t._1, t._2, t._3, t._4)(fn)
  }

  implicit class Par5Op[M[+_], A, B, C, D, E](t: (M[A], M[B], M[C], M[D], M[E]))(
    implicit M: Monad[M]) {
    def par[RV](fn: (A, B, C, D, E) => M[RV]) =
      M.par5(t._1, t._2, t._3, t._4, t._5)(fn)
  }

  implicit class Par6Op[M[+_], A, B, C, D, E, F](
    t: (M[A], M[B], M[C], M[D], M[E], M[F]))(implicit M: Monad[M]) {
    def par[RV](fn: (A, B, C, D, E, F) => M[RV]) =
      M.par6(t._1, t._2, t._3, t._4, t._5, t._6)(fn)
  }

  implicit class Par7Op[M[+_], A, B, C, D, E, F, G](
    t: (M[A], M[B], M[C], M[D], M[E], M[F], M[G]))(implicit M: Monad[M]) {
    def par[RV](fn: (A, B, C, D, E, F, G) => M[RV]) =
      M.par7(t._1, t._2, t._3, t._4, t._5, t._6, t._7)(fn)
  }

}

trait MonadSyntax extends MonadSyntax_1 {
  implicit def optionToAccumulateOp[A](ms: Iterable[Option[A]]) = new AccumulateOp(
    ms)

  implicit def optionToPar2Op[A, B](t: (Option[A], Option[B])) = new Par2Op(t)

  implicit def optionToPar3Op[A, B, C](t: (Option[A], Option[B], Option[C])) =
    new Par3Op(t)

  implicit def optionToPar4Op[A, B, C, D](
    t: (Option[A], Option[B], Option[C], Option[D])) = new Par4Op(t)

  implicit def optionToPar5Op[A, B, C, D, E](
    t: (Option[A], Option[B], Option[C], Option[D], Option[E])) = new Par5Op(t)

  implicit def optionToPar6Op[A, B, C, D, E, F](
    t: (Option[A], Option[B], Option[C], Option[D], Option[E], Option[F])) =
    new Par6Op(t)

  implicit def eitherToAccumulateOp[L, R](ms: Iterable[Either[L, R]]) =
    new AccumulateOp[({ type ap[+x] = Either[L, x] })#ap, R](ms)

  implicit def eitherToPar2Op[L, A, B](t: (Either[L, A], Either[L, B])) =
    new Par2Op[({ type ap[+x] = Either[L, x] })#ap, A, B](t)

  implicit def eitherToPar3Op[L, A, B, C](
    t: (Either[L, A], Either[L, B], Either[L, C])) =
    new Par3Op[({ type ap[+x] = Either[L, x] })#ap, A, B, C](t)

  implicit def eitherToPar4Op[L, A, B, C, D](
    t: (Either[L, A], Either[L, B], Either[L, C], Either[L, D])) =
    new Par4Op[({ type ap[+x] = Either[L, x] })#ap, A, B, C, D](t)

  implicit def eitherToPar5Op[L, A, B, C, D, E](
    t: (Either[L, A], Either[L, B], Either[L, C], Either[L, D], Either[L, E])) =
    new Par5Op[({ type ap[+x] = Either[L, x] })#ap, A, B, C, D, E](t)

  implicit def eitherToPar6Op[L, A, B, C, D, E, F](
    t: (
      Either[L, A],
      Either[L, B],
      Either[L, C],
      Either[L, D],
      Either[L, E],
      Either[L, F])) =
    new Par6Op[({ type ap[+x] = Either[L, x] })#ap, A, B, C, D, E, F](t)

  implicit class AccumulateEitherOp[L, R](rs: Seq[Either[L, R]]) {
    def accumulateEither[A, B](seedA: A, seedB: B)(f: (A, L) => A)(
      g: (B, R) => B): Either[A, B] =
      rs.foldLeft(Right(seedB): Either[A, B]) { (acc, r) =>
        (acc, r) match {
          case (Right(b), Right(r)) => Right(g(b, r))
          case (Right(_), Left(l))  => Left(f(seedA, l))
          case (Left(a), Left(l))   => Left(f(a, l))
          case (l, _)               => l
        }
      }

    def sequenceEither: Either[Seq[L], Seq[R]] =
      accumulateEither(Queue.empty[L], Queue.empty[R]) { _ :+ _ } { _ :+ _ }
  }

  implicit class SequenceOptionOp[M[+_], A](oma: Option[M[A]])(
    implicit m: Monad[M]) {
    def sequence: M[Option[A]] = oma match {
      case None     => m.pure(None)
      case Some(ma) => m.map(ma) { Option(_) }
    }
  }
}
