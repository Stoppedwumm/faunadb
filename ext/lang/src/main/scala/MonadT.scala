package fauna.lang

import fauna.lang.syntax.monad._
import language.implicitConversions
import scala.collection.immutable.Queue

/** MonadT is a wrapper for `OM[M[A]]`
  *
  * It can also be said that `MonadT` is a monad transformer between one arbitrary Monad `OM`
  * and another monad with evidence that it is of type `MonadDistribute`. A monad transformer
  * cannot be built between two arbitrary monads, and the function `MonadDistribute.distribMap`
  * makes composing the two monads possible. This can be most easily seen in the implementation
  * of `MonadT.flatMap`.
  *
  * This class was written before the proliferation of packages such as `cats` and `scalaz`
  * which provide specific monad transformer instances such as `OptionT`.
  *
  * @param run wraped value
  * @param OM typeclass evidence that `OM` satisfies `Monad`
  * @param M typeclass evidence that `M` satisfies `MonadDistribute`
  * @tparam OM arbitrary Outer Monad
  * @tparam M Monad with typeclass evidence MonadDistribute`
  * @tparam A innermost type
  */
case class MonadT[OM[+_], M[+_], A](run: OM[M[A]])(
  implicit OM: Monad[OM],
  M: MonadDistribute[M]) {
  def map[B](f: A => B) = OM.map(run) { M.map(_)(f) }

  def flatMap[B](f: A => OM[M[B]]) = OM.flatMap(run) { M.distribMap(_)(f) }

  /** Similar to scala std lib `filter`. Using the Ruby naming convention because select
    * / reject is clearer than filter / filterNot. The package level type alias for
    * `IsMonadEmptyDistribute` uses `Unpack` to expose typeclasses to give access to
    * methods and functions via `E.TC`.
    *
    * @param f function which determines which elements to keep in the result set
    * @param E evidence that M has an empty state, implements distribMap, and `orElse` (alt) methods.
    * @return nested monads with the the filter applied to the underlying value
    */
  def select(f: A => Boolean)(implicit E: IsMonadEmptyDistribute[M[A]]) = {
    val rv: OM[E.M[E.A]] = OM.map(run) { ma =>
      E.TC.select(E(ma)) { ea =>
        f(ea.asInstanceOf[A])
      }
    }

    rv.asInstanceOf[OM[M[A]]]
  }

  /** Similar to scala std lib `filterNot`. Using the Ruby naming convention because
    * select / reject is clearer than filter / filterNot. The package level type alias
    * for `IsMonadEmptyDistribute` uses `Unpack` to expose typeclasses to give access
    * to methods and functions via `E.TC`.
    *
    * @param f function which determines which elements to remove from the result set
    * @param E evidence that M has an empty state, implements distribMap, and `orElse` (alt) methods.
    * @return nested monads with the the filter applied to the underlying value
    */
  def reject(f: A => Boolean)(implicit E: IsMonadEmptyDistribute[M[A]]) =
    select(!f(_))(E)

  def selectM(f: A => OM[Boolean])(implicit E: IsMonadEmptyDistribute[M[A]]) = {
    val rv: OM[E.M[A]] = OM.flatMap(run) { ma: M[A] =>
      E.TC.distribMap(E(ma)) { ea: E.A =>
        val a = ea.asInstanceOf[A]
        OM.flatMap(f(a)) { if (_) OM.pure(E.TC.pure(a)) else OM.pure(E.TC.empty) }
      }
    }

    rv.asInstanceOf[OM[M[A]]]
  }

  def collect[B](f: PartialFunction[A, OM[M[B]]])(
    implicit E: IsMonadEmptyDistribute[M[A]]) = {
    val rv: OM[E.M[B]] = OM.flatMap(run) { ma: M[A] =>
      E.TC.distribMap(E(ma)) { ea: E.A =>
        val omb: OM[M[B]] = f.applyOrElse(
          ea.asInstanceOf[A],
          { _: A =>
            (OM.pure(E.TC.empty): OM[E.M[B]]).asInstanceOf[OM[M[B]]]
          })

        omb.asInstanceOf[OM[E.M[B]]]
      }
    }

    rv.asInstanceOf[OM[M[B]]]
  }
}

trait MonadTSyntax_0 {
  implicit class UnpackOp[OMA](m: OMA)(implicit val S: IsMonadStack[OMA]) {
    def mapT[B](f: S.A => B) = MonadT(S(m))(S.TC1, S.TC2) map f
    def flatMapT[B](f: S.A => S.O[S.M[B]]) = MonadT(S(m))(S.TC1, S.TC2) flatMap f
  }
}

trait MonadTSyntax_1 extends MonadTSyntax_0 {
  implicit class Op[OM[+_], M[+_], A](m: OM[M[A]])(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {
    def mapT[B](f: A => B) = MonadT(m) map f
    def flatMapT[B](f: A => OM[M[B]]) = MonadT(m) flatMap f
  }

  implicit class SelectOp[OM[+_], M[+_], A](m: OM[M[A]])(
    implicit OM: Monad[OM],
    M: MonadEmptyDistribute[M]) {
    def selectT(f: A => Boolean) = MonadT(m) select f
    def rejectT(f: A => Boolean) = MonadT(m) reject f
    def selectMT(f: A => OM[Boolean]) = MonadT(m) selectM f
    def collectT[B](f: PartialFunction[A, OM[M[B]]]) = MonadT(m) collect f
  }

  implicit class AccumulateOp[OM[+_], M[+_], A](ms: Iterable[OM[M[A]]])(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {

    /** Note: It is recommended to use a persistent (immutable) datastructure for accumulation.
      * For more details see Monad.accumulate.
      */
    def accumulateT[B](seed: B)(f: (B, A) => B) = {
      OM.accumulate(ms, M.pure(seed)) { (mb, ma) =>
        M.flatMap(mb) { prev => M.map(ma) { a => f(prev, a) } }
      }
    }

    def sequenceT = (this accumulateT List.empty[A]) { case (acc, a) =>
      a +: acc
    } mapT { _.reverse }

    def joinT = this.accumulateT(()) { (_, _) => () }
  }

  implicit class ParT2Op[OM[+_], M[+_], A, B](t: (OM[M[A]], OM[M[B]]))(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {
    def parT[RV](fn: (A, B) => OM[M[RV]]) =
      OM.par2(t._1, t._2) { (ma, mb) =>
        M.distribMap(ma) { a => M.distribMap(mb) { fn(a, _) } }
      }
  }

  implicit class ParT3Op[OM[+_], M[+_], A, B, C](t: (OM[M[A]], OM[M[B]], OM[M[C]]))(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {
    def parT[RV](fn: (A, B, C) => OM[M[RV]]) =
      OM.par3(t._1, t._2, t._3) { (ma, mb, mc) =>
        M.distribMap(ma) { a =>
          M.distribMap(mb) { b => M.distribMap(mc) { fn(a, b, _) } }
        }
      }
  }

  implicit class ParT4Op[OM[+_], M[+_], A, B, C, D](
    t: (OM[M[A]], OM[M[B]], OM[M[C]], OM[M[D]]))(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {
    def parT[RV](fn: (A, B, C, D) => OM[M[RV]]) =
      OM.par4(t._1, t._2, t._3, t._4) { (ma, mb, mc, md) =>
        M.distribMap(ma) { a =>
          M.distribMap(mb) { b =>
            M.distribMap(mc) { c => M.distribMap(md) { fn(a, b, c, _) } }
          }
        }
      }
  }

  implicit class ParT5Op[OM[+_], M[+_], A, B, C, D, E](
    t: (OM[M[A]], OM[M[B]], OM[M[C]], OM[M[D]], OM[M[E]]))(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {
    def parT[RV](fn: (A, B, C, D, E) => OM[M[RV]]) =
      OM.par5(t._1, t._2, t._3, t._4, t._5) { (ma, mb, mc, md, me) =>
        M.distribMap(ma) { a =>
          M.distribMap(mb) { b =>
            M.distribMap(mc) { c =>
              M.distribMap(md) { d => M.distribMap(me) { fn(a, b, c, d, _) } }
            }
          }
        }
      }
  }

  implicit class ParT6Op[OM[+_], M[+_], A, B, C, D, E, F](
    t: (OM[M[A]], OM[M[B]], OM[M[C]], OM[M[D]], OM[M[E]], OM[M[F]]))(
    implicit OM: Monad[OM],
    M: MonadDistribute[M]) {
    def parT[RV](fn: (A, B, C, D, E, F) => OM[M[RV]]) =
      OM.par6(t._1, t._2, t._3, t._4, t._5, t._6) { (ma, mb, mc, md, me, mf) =>
        M.distribMap(ma) { a =>
          M.distribMap(mb) { b =>
            M.distribMap(mc) { c =>
              M.distribMap(md) { d =>
                M.distribMap(me) { e => M.distribMap(mf) { fn(a, b, c, d, e, _) } }
              }
            }
          }
        }
      }
  }
}

trait MonadTSyntax extends MonadTSyntax_1 {
  implicit class OptionTOp[OM[+_], A](m: OM[Option[A]])(implicit OM: Monad[OM]) {
    def getOrElseT[A1 >: A](alt: => A1): OM[A1] = OM.map(m) {
      case Some(a) => a
      case None    => alt
    }

    def orElseT[A1 >: A](alt: => OM[Option[A1]]): OM[Option[A1]] = OM.flatMap(m) {
      case Some(a) => OM.pure(Some(a))
      case None    => alt
    }

    def foreachT(f: A => OM[Unit]): OM[Unit] = OM.flatMap(m) {
      case None    => OM.pure(())
      case Some(t) => f(t)
    }

    def containsT[A1 >: A](value: A1): OM[Boolean] = OM.map(m) { _.contains(value) }

    def existsT[A1 >: A](fn: A => Boolean): OM[Boolean] = OM.map(m) { _.exists(fn) }

    def isDefinedT: OM[Boolean] = OM.map(m) { _.isDefined }
  }

  implicit class EitherTOp[OM[+_], L, A](m: OM[Either[L, A]])(
    implicit OM: Monad[OM]) {

    def mapLeftT[B](f: L => B): OM[Either[B, A]] =
      OM.map(m) {
        case Left(l)      => Left(f(l))
        case r @ Right(_) => r.asInstanceOf[Either[B, A]]
      }
  }

  implicit class AccumulateEitherTOp[OM[+_], L, R](ms: Iterable[OM[Either[L, R]]])(
    implicit OM: Monad[OM]) {

    /** Note: It is recommended to use a persistent (immutable) datastructure for accumulation.
      * For more details see Monad.accumulate.
      */
    def accumulateEitherT[A, B](seedA: A, seedB: B)(f: (A, L) => A)(
      g: (B, R) => B): OM[Either[A, B]] =
      OM.map(OM.sequence(ms)) { _.accumulateEither(seedA, seedB)(f)(g) }

    def sequenceEitherT: OM[Either[Seq[L], Seq[R]]] =
      accumulateEitherT(Queue.empty[L], Queue.empty[R]) { _ :+ _ } { _ :+ _ }
  }

  implicit def optionToOp[OM[+_], A](m: OM[Option[A]])(implicit OM: Monad[OM]) =
    new Op(m)

  implicit def optionToSelectOp[OM[+_], A](m: OM[Option[A]])(
    implicit OM: Monad[OM]) =
    new SelectOp(m)

  implicit def optionToAccumulateOp[OM[+_], A](ms: Iterable[OM[Option[A]]])(
    implicit OM: Monad[OM]) =
    new AccumulateOp(ms)

  implicit def optionToParT2Op[OM[+_], A, B](t: (OM[Option[A]], OM[Option[B]]))(
    implicit OM: Monad[OM]) = new ParT2Op[OM, Option, A, B](t)

  implicit def optionToParT3Op[OM[+_], A, B, C](
    t: (OM[Option[A]], OM[Option[B]], OM[Option[C]]))(implicit OM: Monad[OM]) =
    new ParT3Op[OM, Option, A, B, C](t)

  implicit def optionToParT4Op[OM[+_], A, B, C, D](
    t: (OM[Option[A]], OM[Option[B]], OM[Option[C]], OM[Option[D]]))(
    implicit OM: Monad[OM]) = new ParT4Op[OM, Option, A, B, C, D](t)

  implicit def optionToParT5Op[OM[+_], A, B, C, D, E](
    t: (OM[Option[A]], OM[Option[B]], OM[Option[C]], OM[Option[D]], OM[Option[E]]))(
    implicit OM: Monad[OM]) = new ParT5Op[OM, Option, A, B, C, D, E](t)

  implicit def optionToParT6Op[OM[+_], A, B, C, D, E, F](
    t: (
      OM[Option[A]],
      OM[Option[B]],
      OM[Option[C]],
      OM[Option[D]],
      OM[Option[E]],
      OM[Option[F]]))(implicit OM: Monad[OM]) =
    new ParT6Op[OM, Option, A, B, C, D, E, F](t)

  implicit def eitherToOp[OM[+_], L, R](m: OM[Either[L, R]])(
    implicit OM: Monad[OM]) =
    new Op[OM, ({ type ap[+x] = Either[L, x] })#ap, R](m)

  implicit def eitherToAccumulateOp[OM[+_], L, R](ms: Iterable[OM[Either[L, R]]])(
    implicit OM: Monad[OM]) =
    new AccumulateOp[OM, ({ type ap[+x] = Either[L, x] })#ap, R](ms)

  implicit def eitherToParT2Op[OM[+_], L, A, B](
    t: (OM[Either[L, A]], OM[Either[L, B]]))(implicit OM: Monad[OM]) =
    new ParT2Op[OM, ({ type ap[+x] = Either[L, x] })#ap, A, B](t)

  implicit def eitherToParT3Op[OM[+_], L, A, B, C](
    t: (OM[Either[L, A]], OM[Either[L, B]], OM[Either[L, C]]))(
    implicit OM: Monad[OM]) =
    new ParT3Op[OM, ({ type ap[+x] = Either[L, x] })#ap, A, B, C](t)

  implicit def eitherToParT4Op[OM[+_], L, A, B, C, D](
    t: (OM[Either[L, A]], OM[Either[L, B]], OM[Either[L, C]], OM[Either[L, D]]))(
    implicit OM: Monad[OM]) =
    new ParT4Op[OM, ({ type ap[+x] = Either[L, x] })#ap, A, B, C, D](t)

  implicit def eitherToParT5Op[OM[+_], L, A, B, C, D, E](
    t: (
      OM[Either[L, A]],
      OM[Either[L, B]],
      OM[Either[L, C]],
      OM[Either[L, D]],
      OM[Either[L, E]]))(implicit OM: Monad[OM]) =
    new ParT5Op[OM, ({ type ap[+x] = Either[L, x] })#ap, A, B, C, D, E](t)

  implicit def eitherToParT6Op[OM[+_], L, A, B, C, D, E, F](
    t: (
      OM[Either[L, A]],
      OM[Either[L, B]],
      OM[Either[L, C]],
      OM[Either[L, D]],
      OM[Either[L, E]],
      OM[Either[L, F]]))(implicit OM: Monad[OM]) =
    new ParT6Op[OM, ({ type ap[+x] = Either[L, x] })#ap, A, B, C, D, E, F](t)

  implicit def iterableToOp[OM[+_], A](m: OM[Iterable[A]])(implicit OM: Monad[OM]) =
    new Op(m)

  implicit def iterableToSelectOp[OM[+_], A](m: OM[Iterable[A]])(
    implicit OM: Monad[OM]) =
    new SelectOp(m)

  implicit def iterableToAccumulateOp[OM[+_], A](ms: Iterable[OM[Iterable[A]]])(
    implicit OM: Monad[OM]) =
    new AccumulateOp(ms)
}
