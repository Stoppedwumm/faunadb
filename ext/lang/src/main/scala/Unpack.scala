package fauna.lang

// The implicit conversions in the companion object provide a means to obtain type class
// instances for partially applied type constructors, in lieu of direct compiler support
// as described in https://issues.scala-lang.org/browse/SI-2712 SI-2712
//
// See also https://github.com/scalaz/scalaz/blob/scalaz-seven/core/src/main/scala/scalaz/Unapply.scala

// @implicitNotFound(
// """Unable to unpack type `${MA}` into a type constructor `M[_]` that has an instance of
// type class `${TC}`, parameterized on a type `A`.
// 1. Check that the instance is defined by compiling `implicitly[${TC}[<type constructor>]]`.
// 2. Review the implicits in Unpack.scala, which only cover some common type shapes.""")
trait Unpack[TC[+_[+_]], MA] {
  type A
  type M[+_]

  implicit val TC: TC[M]

  def apply(m: MA): M[A]
}

object Unpack {
  protected class UnpackM1[TC[+_[+_]], M0[+_], A0](implicit i: TC[M0]) extends Unpack[TC, M0[A0]] {
    type A = A0
    type M[+X] = M0[X]

    implicit val TC = i

    @inline final def apply(m: M0[A0]) = m
  }

  implicit final def unpackM1[TC[+_[+_]], M0[+_], A0](implicit i: TC[M0]) = new UnpackM1[TC, M0, A0]

  protected class UnpackM2[TC[+_[+_]], M0[+_, +_], L, A0](implicit i: TC[({ type ap[+x] = M0[L, x] })#ap]) extends Unpack[TC, M0[L, A0]] {
    type A = A0
    type M[+X] = M0[L, X]

    implicit val TC = i

    @inline final def apply(m: M0[L, A0]) = m
  }

  implicit final def unpackM2[TC[+_[+_]], M0[+_, +_], L, A0](implicit i: TC[({ type ap[+x] = M0[L, x] })#ap]) =
    new UnpackM2[TC, M0, L, A0]
}

// @implicitNotFound(
// """Unable to unpack type `${OMA}` into an outer type constructor `OM[_]` that has an instance of
// type class `${TC1}`, parameterized on an inner type constructor `M[_]` that has an instance of
// type class `${TC2}`, parameterized on a type `A`.
// 1. Check that the instance is defined by compiling `implicitly[${TC1}[<outer type constructor>]]`
//    and `implicitly[${TC2}[<inner type constructor>]]`,
// 2. Review the implicits in Unpack.scala, which only cover some common type shapes.""")
trait Unpack2[TC1[+_[+_]], TC2[+_[+_]], OMA] {
  type A
  type O[+_]
  type M[+_]

  implicit val TC1: TC1[O]
  implicit val TC2: TC2[M]

  def apply(m: OMA): O[M[A]]
}

object Unpack2 {
  protected class Unpack2O1M1[TC1[+_[+_]], TC2[+_[+_]], O0[+_], M0[+_], A0](implicit i1: TC1[O0], i2: TC2[M0]) extends Unpack2[TC1, TC2, O0[M0[A0]]] {
    type A = A0
    type O[+X] = O0[X]
    type M[+X] = M0[X]

    implicit val TC1 = i1
    implicit val TC2 = i2

    @inline final def apply(m: O0[M0[A0]]) = m
  }

  implicit final def unpack2O1M1[TC1[+_[+_]], TC2[+_[+_]], O0[+_], M0[+_], A0](implicit i1: TC1[O0], i2: TC2[M0]) =
    new Unpack2O1M1[TC1, TC2, O0, M0, A0]

  protected class Unpack2O2M1[TC1[+_[+_]], TC2[+_[+_]], O0[+_, +_], L, M0[+_], A0](implicit i1: TC1[({ type ap[+x] = O0[L, x] })#ap], i2: TC2[M0]) extends Unpack2[TC1, TC2, O0[L, M0[A0]]] {
    type A = A0
    type O[+X] = O0[L, X]
    type M[+X] = M0[X]

    implicit val TC1 = i1
    implicit val TC2 = i2

    @inline final def apply(m: O0[L, M0[A0]]) = m
  }

  implicit final def unpack2O2M1[TC1[+_[+_]], TC2[+_[+_]], O0[+_, +_], L, M0[+_], A0](implicit i1: TC1[({ type ap[+x] = O0[L, x] })#ap], i2: TC2[M0]) =
    new Unpack2O2M1[TC1, TC2, O0, L, M0, A0]

  protected class Unpack2O1M2[TC1[+_[+_]], TC2[+_[+_]], O0[+_], M0[+_, +_], L, A0](implicit i1: TC1[O0], i2: TC2[({ type ap[+x] = M0[L, x] })#ap]) extends Unpack2[TC1, TC2, O0[M0[L, A0]]] {
    type A = A0
    type O[+X] = O0[X]
    type M[+X] = M0[L, X]

    implicit val TC1 = i1
    implicit val TC2 = i2

    @inline final def apply(m: O0[M0[L, A0]]) = m
  }

  implicit final def unpack2O1M2[TC1[+_[+_]], TC2[+_[+_]], O0[+_], M0[+_, +_], L, A0](implicit i1: TC1[O0], i2: TC2[({ type ap[+x] = M0[L, x] })#ap]) =
    new Unpack2O1M2[TC1, TC2, O0, M0, L, A0]

  protected class Unpack2O2M2[TC1[+_[+_]], TC2[+_[+_]], O0[+_, +_], L1, M0[+_, +_], L2, A0](implicit i1: TC1[({ type ap[+x] = O0[L1, x] })#ap], i2: TC2[({ type ap[+x] = M0[L2, x] })#ap]) extends Unpack2[TC1, TC2, O0[L1, M0[L2, A0]]] {
    type A = A0
    type O[+X] = O0[L1, X]
    type M[+X] = M0[L2, X]

    implicit val TC1 = i1
    implicit val TC2 = i2

    @inline final def apply(m: O0[L1, M0[L2, A0]]) = m
  }

  implicit final def unpack2O2M2[TC1[+_[+_]], TC2[+_[+_]], O0[+_, +_], L1, M0[+_, +_], L2, A0](implicit i1: TC1[({ type ap[+x] = O0[L1, x] })#ap], i2: TC2[({ type ap[+x] = M0[L2, x] })#ap]) =
    new Unpack2O2M2[TC1, TC2, O0, L1, M0, L2, A0]
}
