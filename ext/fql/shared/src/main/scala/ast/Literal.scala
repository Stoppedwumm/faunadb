package fql.ast

sealed trait Literal

object Literal {
  case object Null extends Literal
  case object True extends Literal
  case object False extends Literal
  final case class Int(num: BigInt) extends Literal
  final case class Float(num: BigDecimal) extends Literal
  final case class Str(str: String) extends Literal

  object Int {
    def apply(str: String): Int = Int(BigInt(str))
    def apply(long: Long): Int = Int(BigInt(long))
  }

  object Float {
    def apply(str: String): Float = Float(BigDecimal(str))
  }
}
