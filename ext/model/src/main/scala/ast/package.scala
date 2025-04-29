package fauna

import fauna.repo.query.Query
import fauna.lang.{ Monad, MonadDistribute }
import scala.collection.immutable.HashMap

package object ast {
  type R[+T] = Either[List[EvalError], T]
  type Bindings = List[(String, Expression)]
}

package ast {

  object FreeVars {
    val empty = FreeVars(HashMap.empty)

    def apply(name: String, pos: Position): FreeVars = FreeVars(HashMap((name, Set(pos))))
  }

  final case class FreeVars(vars: HashMap[String, Set[Position]]) extends AnyVal {
    def isEmpty = vars.isEmpty
    def nonEmpty = vars.nonEmpty

    def | (o: FreeVars) =
      FreeVars((vars merged o.vars) { case ((k, v1), (_, v2)) => (k, v1 | v2) })

    def - (n: String) = FreeVars(vars - n)

    def -- (ns: Set[String]) = FreeVars(vars -- ns)

    def names = vars.keys

    def toErrors =
      vars flatMap { case (k, ps) => ps map { UnboundVariable(k, _) } } toList
  }

  sealed trait PResult[+A] {
    def map[B](fn: A => B): PResult[B] =
      this match {
        case PResult.Success(a, free) => PResult.Success(fn(a), free)
        case f @ PResult.Failure(_)   => f
      }

    def flatMap[B](fn: A => PResult[B]): PResult[B] =
      this match {
        case PResult.Success(a, free1) =>
          fn(a) match {
            case PResult.Success(b, free2) => PResult.Success(b, free1 | free2)
            case f @ PResult.Failure(_)    => f
          }
        case f @ PResult.Failure(_) => f
      }

    def toEither: Either[List[ParseError], A] =
      this match {
        case PResult.Success(a, free) => if (free.isEmpty) Right(a) else Left(free.toErrors)
        case PResult.Failure(errs)    => Left(errs)
      }

    def toOption: Option[A] = toEither.toOption

    def getErrors: List[ParseError] =
      this match {
        case PResult.Success(_, _) => Nil
        case PResult.Failure(errs) => errs
      }
  }

  object PResult {
    final case class Success[+A](result: A, free: FreeVars) extends PResult[A]

    final case class Failure(errors: List[ParseError]) extends PResult[Nothing]

    final object Success {
      def apply[A](a: A): Success[A] = Success(a, FreeVars.empty)
    }

    final object Failure {
      def apply(err: ParseError): Failure = Failure(List(err))
    }

    def successfulQ[A](a: A): Query[PResult[A]] = Query.value(Success(a))
    def successfulQ[A](a: A, free: FreeVars): Query[PResult[A]] = Query.value(Success(a, free))

    def failedQ[A](errs: List[ParseError]): Query[PResult[A]] = Query.value(Failure(errs))
    def failedQ[A](err: ParseError): Query[PResult[A]] = failedQ((List(err)))

    final implicit val monadInstance = new MonadDistribute[PResult] {
      def pure[A](a: A): PResult[A] = Success(a)

      def map[A, B](m: PResult[A])(f: A => B): PResult[B] = m map f

      def flatMap[A, B](m: PResult[A])(f: A => PResult[B]): PResult[B] = m flatMap f

      def accumulate[A, B](ms: Iterable[PResult[A]], seed: B)(f: (B, A) => B): PResult[B] = {
        var state: PResult[B] = Success(seed, FreeVars.empty)

        ms foreach {
          case Success(a, free2) =>
            state match {
              case Success(b, free1) =>
                state = Success(f(b, a), free1 | free2)
              case Failure(_) => ()
                // state is failed, drop success
            }

          case f @ Failure(errs2) =>
            state match {
              case Success(_, _) =>
                // replace state with error
                state = f
              case Failure(errs1) =>
                // merge errors
                state = Failure(errs1 ++ errs2)
            }
        }

        state
      }

      def distribMap[A, B, OM[+_]](m: PResult[A])(fn: A => OM[PResult[B]])(implicit OM: Monad[OM]): OM[PResult[B]] =
        m match {
          case Success(a, free1) =>
            OM.map(fn(a)) {
              case Success(b, free2) => Success(b, free1 | free2)
              case f @ Failure(_)    => f
            }
          case f @ Failure(_) => OM.pure(f)
        }
    }
  }
}
