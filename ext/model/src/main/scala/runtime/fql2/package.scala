package fauna.model.runtime

import fauna.lang.ResultModule
import fauna.repo.query.Query
import fauna.repo.values._
import fql.{ Result => FQLResult }
import scala.language.implicitConversions

package fql2 {
  sealed trait Result[+A] extends ResultModule.Result[QueryFailure, A, Result] {
    protected def companion = Result

    def toQuery: Query[Result[A]] = Query.value(this)
  }

  object Result extends ResultModule.Companion {
    type Error = QueryFailure
    type Res[+A] = Result[A]

    object Ok extends OkCompanion
    final case class Ok[+A](value: A) extends Result[A] with OkType

    object Err extends ErrCompanion
    final case class Err(err: QueryFailure) extends Result[Nothing] with ErrType

    implicit def fromFQLResult[A](res: FQLResult[A]): Result[A] =
      res match {
        case FQLResult.Ok(v)     => Result.Ok(v)
        case FQLResult.Err(errs) => Result.Err(QueryCheckFailure(errs))
      }
  }
}

package object fql2 {
  implicit class ValueOps[V <: Value](val value: V) extends AnyVal {
    def toResult: Result[V] = Result.Ok(value)
    def toQuery: Query[Result[V]] = toResult.toQuery
  }

  implicit def setToVSImpl(vs: Value.Set): ValueSet =
    vs.asInstanceOf[ValueSet]

  implicit def singletonObjToSOImpl(so: Value.SingletonObject): SingletonObject =
    so.asInstanceOf[SingletonObject]

  implicit def nativeFuncToNativeFuncImpl(fn: Value.NativeFunc): NativeFunction =
    fn.asInstanceOf[NativeFunction]
}
