package fauna.atoms

import fauna.codex.cbor._
import java.lang.{ Long => JLong }

object GlobalID {
  implicit val CBORCodec = CBOR.SumCodec[GlobalID](
    GlobalKeyID.CBORCodec,
    ScopeID.CBORCodec,
    GlobalDatabaseID.CBORCodec
  )
}

sealed trait GlobalID extends Any {
  def toLong: Long

  def compare(b: GlobalID): Int = JLong.compare(toLong, b.toLong)
}

object GlobalKeyID {
  val MaxValue = GlobalKeyID(Long.MaxValue)
  val MinValue = GlobalKeyID(0)

  implicit val CBORCodec = CBOR.TupleCodec[GlobalKeyID]
}

case class GlobalKeyID(toLong: Long)
    extends AnyVal
    with ID[GlobalKeyID]
    with GlobalID

object ScopeID {
  val MaxValue = ScopeID(Long.MaxValue)
  val MinValue = ScopeID(0)

  val RootID = ScopeID(0L)
  val Invalid = ScopeID(-1)

  implicit val CBORCodec = CBOR.TupleCodec[ScopeID]
}

case class ScopeID(toLong: Long) extends AnyVal with ID[ScopeID] with GlobalID

object GlobalDatabaseID {
  val MaxValue = GlobalDatabaseID(Long.MaxValue)
  val MinValue = GlobalDatabaseID(0)

  val Invalid = ScopeID(-1)

  implicit val CBORCodec = CBOR.TupleCodec[GlobalDatabaseID]
}

case class GlobalDatabaseID(toLong: Long)
    extends AnyVal
    with ID[GlobalDatabaseID]
    with GlobalID
