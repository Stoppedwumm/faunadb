package fauna.ast

import fauna.atoms._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.unused

sealed abstract case class Is(fn: PartialFunction[Literal, Unit]) extends QFunction {
  def effect = Effect.Pure

  def apply(
    literal: Literal,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[BoolL]] =
    Query.value(Right(BoolL(fn.isDefinedAt(literal))))
}

object IsNumberFunction extends Is({ case _: NumericL => () })

object IsDoubleFunction extends Is({ case _: DoubleL => () })

object IsIntegerFunction extends Is({ case _: LongL => () })

object IsBooleanFunction extends Is({ case _: BoolL => () })

object IsNullFunction extends Is({ case NullL => () })

object IsBytesFunction extends Is({ case _: BytesL => () })

object IsTimestampFunction extends Is({ case _: AbstractTimeL => () })

object IsDateFunction extends Is({ case _: DateL => () })

object IsUUIDFunction extends Is({ case _: UUIDL => () })

object IsStringFunction
    extends Is({
      case _: StringL => ()
      case _: ActionL => ()
    })

object IsArrayFunction extends Is({ case _: ArrayL => () })

object IsObjectFunction
    extends Is({
      case _: ObjectL  => ()
      case _: PageL    => ()
      case _: VersionL => ()
      case _: EventL   => ()
      case _: CursorL  => ()
    })

object IsRefFunction
    extends Is({
      case _: RefL           => ()
      case _: UnresolvedRefL => ()
    })

object IsSet {
  def unapply(arg: Literal): Option[SetL] =
    Casts.Identifier(arg, RootPosition) match {
      case Right(s @ SetL(_)) => Some(s)
      case _                  => None
    }
}

object IsSetFunction extends Is({ case IsSet(_) => () })

object IsDocFunction
    extends Is({
      case _: RefL     => ()
      case _: VersionL => ()
    })

object IsLambdaFunction extends Is({ case _: LambdaL => () })

private[this] object SchemaCollection {
  def apply(collID: CollectionID): PartialFunction[Literal, Unit] = {
    case RefL(_, DocID(_, `collID`))                => ()
    case VersionL(v, _) if v.docID.collID == collID => ()
  }
}

object IsCollectionFunction extends Is(SchemaCollection(CollectionID.collID))
object IsDatabaseFunction extends Is(SchemaCollection(DatabaseID.collID))
object IsIndexFunction extends Is(SchemaCollection(IndexID.collID))
object IsFunctionFunction extends Is(SchemaCollection(UserFunctionID.collID))
object IsKeyFunction extends Is(SchemaCollection(KeyID.collID))
object IsTokenFunction extends Is(SchemaCollection(TokenID.collID))
object IsCredentialsFunction extends Is(SchemaCollection(CredentialsID.collID))
object IsRoleFunction extends Is(SchemaCollection(RoleID.collID))
