package fauna.repo.schema

import fauna.lang.ResultModule
import fauna.repo.query.Query

sealed trait SchemaResult[+A]
    extends ResultModule.Result[Seq[ConstraintFailure], A, SchemaResult] {
  protected def companion = SchemaResult

  def toQuery: Query[SchemaResult[A]] = Query.value(this)
}

object SchemaResult extends ResultModule.Companion {
  type Error = Seq[ConstraintFailure]
  type Res[+A] = SchemaResult[A]

  object Ok extends OkCompanion
  final case class Ok[+A](value: A) extends SchemaResult[A] with OkType

  object Err extends ErrCompanion
  final case class Err(err: Seq[ConstraintFailure])
      extends SchemaResult[Nothing]
      with ErrType
}
