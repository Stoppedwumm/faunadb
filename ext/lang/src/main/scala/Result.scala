package fauna.lang

import scala.collection.Factory
import scala.language.experimental.macros
import scala.reflect.macros._
import scala.util.control._

/** Generic implementation of a Result, which is an Either-like type which can
  * hold a concrete error value or a polymorphic success value.
  *
  * The use of control exceptions is inspired by Rust's `Result` type, which
  * uses early return for failure.
  */
object ResultModule {
  trait Result[E, +A, Res[+A] <: Result[E, A, Res]] { self: Res[A] =>

    private type R[+A] = Res[A]

    def isOk: Boolean
    protected def unsafeValue: Any
    protected def companion: Companion {
      type Error = E
      type Res[+A] = R[A]
    }

    protected[ResultModule] def unsafeOk: A = unsafeValue.asInstanceOf[A]
    protected[ResultModule] def unsafeErr: E = unsafeValue.asInstanceOf[E]

    def isErr = !isOk

    def toOption: Option[A] = Option.when(isOk)(unsafeOk)

    def errOption: Option[E] = Option.when(isErr)(unsafeErr)

    def fold[B](f1: E => B, f2: A => B): B =
      if (isErr) f1(unsafeErr) else f2(unsafeOk)

    def getOr[A1 >: A](withErr: E => A1): A1 = fold(withErr, identity)

    def errOr[E1 >: E](withOk: A => E1): E1 = fold(identity, withOk)

    def getOrElse[A1 >: A](alt: => A1): A1 = fold(_ => alt, identity)

    def errOrElse[E1 >: E](alt: => E1): E1 = fold(identity, _ => alt)

    def map[B](f: A => B): Res[B] =
      if (isOk) companion.Ok(f(unsafeOk)) else this.asInstanceOf[Res[B]]

    def mapErr(f: E => E): Res[A] =
      if (isOk) this else companion.Err(f(unsafeErr))

    def flatMap[B](f: A => Res[B]): Res[B] =
      if (isOk) f(unsafeOk) else this.asInstanceOf[Res[B]]

    /** Short-circuit accessor to be called within Result.guard or guardM */
    @annotation.compileTimeOnly(
      "Must be used within Result.guard or Result.guardM block")
    def getOrFail: A = sys.error("compile time only!")
  }

  trait Companion {
    type Error
    type Res[+A] <: Result[Error, A, Res]

    trait OkCompanion { def apply[A](a: A): Res[A] }
    val Ok: OkCompanion
    trait OkType { self: Res[_] =>
      protected def value: Any
      def isOk = true
      protected def unsafeValue = value
    }

    trait ErrCompanion { def apply(err: Error): Res[Nothing] }
    val Err: ErrCompanion
    trait ErrType { self: Res[Nothing] =>
      def err: Error
      def isOk = false
      protected def unsafeValue = err
    }

    // FIXME: extend ControlThrowable, but check NonFatal usage in monads.
    sealed case class ThrownFailure private (err: Error) extends NoStackTrace {
      override def getMessage = err.toString
    }

    object ThrownFailure {
      private def apply(err: Error): ThrownFailure = new ThrownFailure(err)
      // This name is chosen in order to clearly discourage manual use.
      def _unsafeApply(err: Error): ThrownFailure = apply(err)
    }

    def guard[A](body: => Res[A]): Res[A] = macro ResultModule.MacroImpl.guard

    def guardM[M[+_], A](body: => M[Res[A]])(
      implicit ME: MonadException[M]): M[Res[A]] =
      macro ResultModule.MacroImpl.guardM

    /** Short-circuit failed return to be called within Result.guard or guardM */
    @annotation.compileTimeOnly(
      "Must be used within Result.guard or Result.guardM block")
    def fail(err: Error): Nothing =
      sys.error("compile time only!")

    /** Short-circuit failed return. This _must_ be called within a guard block, but
      * is not checked like fail().
      */
    def unsafeFail(err: Error): Nothing =
      throw ThrownFailure._unsafeApply(err)

    /** Collect all errors rather than stopping at the first one like `.sequenceT */
    def collectFailures[E1, A, CC[E] <: Iterable[E]](rs: CC[Res[A]])(
      implicit ev: Error <:< Iterable[E1],
      errSeq: Factory[E1, Error],
      okSeq: Factory[A, CC[A]]
    ): Res[CC[A]] = {
      val (errs, oks) = rs.view.partitionMap { _.fold(Left(_), Right(_)) }
      if (errs.isEmpty) Ok(oks.to(okSeq)) else Err(errs.flatten.to(errSeq))
    }

    implicit val monadInstance = new MonadDistribute[Res] {
      def pure[A](a: A): Res[A] = Ok(a)
      def map[A, B](r: Res[A])(f: A => B): Res[B] = r map f
      def flatMap[A, B](r: Res[A])(f: A => Res[B]): Res[B] = r flatMap f
      def accumulate[A, B](ms: Iterable[Res[A]], seed: B)(f: (B, A) => B): Res[B] = {
        var state: Res[B] = Ok(seed)
        val iter = ms.iterator
        while (iter.hasNext && state.isOk) {
          state = iter.next() map { f(state.getOrElse(null.asInstanceOf[B]), _) }
        }
        state
      }
      def distribMap[A, B, OM[+_]](m: Res[A])(f: A => OM[Res[B]])(
        implicit OM: Monad[OM]): OM[Res[B]] =
        if (m.isOk) f(m.unsafeOk) else OM.pure(m.asInstanceOf[Res[B]])
    }
  }

  class MacroImpl(val c: blackbox.Context) {
    import c.universe._

    val R = c.prefix.tree
    val RFail = q"$R.fail"

    lazy val transformer = new Transformer {
      override def transform(tree: c.Tree) =
        tree match {
          // result.getOrFail form
          case q"$t.getOrFail" =>
            c.typecheck(
              q"${transform(t)}.getOr { e => $R.unsafeFail(e) }",
              c.TERMmode)

          // Result.fail(err) form
          case q"$ap($err)" if ap equalsStructure RFail =>
            c.typecheck(q"$R.unsafeFail(${transform(err)})", c.TERMmode)

          case t =>
            super.transform(t)
        }
    }

    def guard(body: c.Tree): c.Tree =
      q"""try {
        ${transformer.transform(body)}
      } catch {
        case $R.ThrownFailure(err) => $R.Err(err)
      }"""

    def guardM(body: c.Tree)(ME: c.Tree): c.Tree =
      q"""{
        val ME = $ME
        try {
          val m = ${transformer.transform(body)}
          ME.recover(m, {
            case $R.ThrownFailure(err) => $R.Err(err)
          })
        } catch {
          case $R.ThrownFailure(err) => ME.pure($R.Err(err))
        }
      }"""
  }
}
