package fauna.storage

import fauna.codex.cbor._
import fauna.lang.syntax.option._
import fauna.storage.cassandra.comparators._

sealed abstract class Action(
  private[Action] val ord: Byte,
  val isCreate: Boolean,
  override val toString: String)
    extends Ordered[Action] {
  def isDelete = !isCreate

  def compare(other: Action) = other.ord compare ord

  def toSetAction: SetAction =
    if (isCreate) {
      Add
    } else {
      Remove
    }

  def toDocAction: DocAction =
    this match {
      case a: DocAction => a
      case Add          => Create
      case Remove       => Delete
    }
}

sealed abstract class DocAction(ord: Byte, isCreate: Boolean, toString: String)
    extends Action(ord, isCreate, toString)

object DocAction {
  val Delete = fauna.storage.Delete
  val Update = fauna.storage.Update
  val Create = fauna.storage.Create

  val MinValue = Create
  val MaxValue = Delete

  implicit val cassandraCodec = CassandraCodec.Alias[DocAction, Boolean](
    {
      case true  => Create
      case false => Delete
    },
    {
      case Create => SomeTrue
      case Update => SomeTrue
      case Delete => SomeFalse
    })

  implicit val cborCodec = CBOR.SumCodec[DocAction](
    CBOR.SingletonCodec(Delete),
    CBOR.SingletonCodec(Update),
    CBOR.SingletonCodec(Create))
}

sealed abstract class SetAction(ord: Byte, isCreate: Boolean, toString: String)
    extends Action(ord, isCreate, toString)

object SetAction {
  val Remove = fauna.storage.Remove
  val Add = fauna.storage.Add

  val MinValue = Add
  val MaxValue = Remove

  implicit val ordering: Ordering[SetAction] = (a, b) => a compare b

  implicit val cassandraCodec = CassandraCodec.Alias[SetAction, Boolean](
    {
      case true  => Add
      case false => Remove
    },
    {
      case Add    => SomeTrue
      case Remove => SomeFalse
    })

  implicit val cborCodec =
    CBOR.SumCodec[SetAction](CBOR.SingletonCodec(Remove), CBOR.SingletonCodec(Add))
}

case object Remove extends SetAction(0, false, "remove")
case object Delete extends DocAction(1, false, "delete")
case object Update extends DocAction(2, true, "update")
case object Create extends DocAction(3, true, "create")
case object Add extends SetAction(4, true, "add")
