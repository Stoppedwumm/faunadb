package fauna.atoms

import fauna.codex.cbor._

object AccountID {
  implicit val codec = CBOR.AliasCodec[AccountID, Long](apply, _.toLong)

  val Root = AccountID(0)
}

final case class AccountID(toLong: Long) extends AnyVal
