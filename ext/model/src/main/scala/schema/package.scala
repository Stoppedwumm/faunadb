package fauna.model

import fauna.lang.{ Monad, ResultModule }
import fauna.model.runtime.fql2.{ Result => FQL2Result }
import fauna.repo.query.Query
import fql.{ Result => FQLResult }
import fql.parser.Parser
import scala.language.implicitConversions

package object schema {

  /** A field path is a string which is a limited v10 expression consisting of field
    * dot-access or []-access with no complex expressions, with an optional initial
    * dot.
    *
    * FIXME: Replace with `fql.ast.Path`.
    */
  type FieldPath = List[Either[Long, String]]

  object FieldPath {
    def apply(path: String): FQLResult[FieldPath] = parse(path).map(_.toList)
    def parse(path: String): FQLResult[fql.ast.Path] =
      Parser.path(if (path.startsWith(".")) path else s".$path")

    def unapply(path: String): Option[FieldPath] =
      FieldPath(path).toOption
  }
}

package schema {

  sealed trait Result[+A] extends ResultModule.Result[Result.Error, A, Result] {
    protected def companion = Result
    def toQuery: Query[Result[A]] = Query.value(this)
  }

  object Result extends ResultModule.Companion {
    import SchemaError._

    type Error = List[SchemaError]
    type Res[+A] = Result[A]

    object Ok extends OkCompanion
    final case class Ok[+A](value: A) extends Result[A] with OkType

    final case class Err(err: Error) extends Result[Nothing] with ErrType
    object Err extends ErrCompanion {
      def apply(err: SchemaError) = new Err(err :: Nil)
    }

    def apply[A](a: A): Result[A] = Ok(a)

    implicit def adapt[A](res: FQLResult[A]): Result[A] =
      res match {
        case FQLResult.Ok(value) => Ok(value)
        case FQLResult.Err(errs) => Err(errs map { FQLError(_) })
      }

    implicit def adapt[A](res: FQL2Result[A]): Result[A] =
      res match {
        case FQL2Result.Ok(value) => Ok(value)
        case FQL2Result.Err(err)  => Err(SchemaError.QueryFailure(err))
      }

    def adaptT[M[+_], A](m: M[FQL2Result[A]])(implicit M: Monad[M]): M[Result[A]] =
      M.map(m) { adapt(_) }
  }
}
