package fauna.repo.values

import fauna.codex.cbor.CBOR
import fql.ast._

object ExprCBORCodecs {
  import Source._

  implicit lazy val exprs = Exprs.expr
  implicit lazy val typeExprs = TypeExprs.typeExpr
  implicit lazy val patExprs = PatExprs.patExpr

  // only used for testing
  lazy private[repo] val mcc = Exprs.mcc
  lazy private[repo] val stmt = Exprs.stmt

  private implicit def seq[T: CBOR.Codec]: CBOR.Codec[Seq[T]] = {
    def from(v: Vector[T]): Seq[T] = v
    def to(v: Seq[T]) = v.toVector
    CBOR.AliasCodec(from, to)(CBOR.VectorDecoder, CBOR.VectorEncoder)
  }

  // public to repo for testing
  private[repo] object Source {
    // Srcs serialize to _just_ their ID form
    implicit val src = CBOR.AliasCodec[Src, String](Src.Id.apply, _.name)
    implicit val span = CBOR.TupleCodec[Span]
    implicit val name = CBOR.TupleCodec[Name]

    private implicit val bigInt = {
      def from(a: Array[Byte]) = BigInt(a)
      def to(b: BigInt) = b.toByteArray
      CBOR.AliasCodec[BigInt, Array[Byte]](from, to)
    }

    private implicit val bigDec = {
      def from(t: (BigInt, Int)) = BigDecimal(t._1, t._2)
      def to(b: BigDecimal) = (BigInt(b.underlying.unscaledValue), b.scale)
      CBOR.AliasCodec[BigDecimal, (BigInt, Int)](from, to)
    }

    implicit val literal = {
      import Literal._
      CBOR.SumCodec[Literal](
        CBOR.SingletonCodec(Null),
        CBOR.SingletonCodec(True),
        CBOR.SingletonCodec(False),
        CBOR.AliasCodec[Int, BigInt](Int.apply, _.num),
        CBOR.AliasCodec[Float, BigDecimal](Float.apply, _.num),
        CBOR.AliasCodec[Str, String](Str.apply, _.str)
      )
    }
  }

  private object Exprs {
    import Expr._

    implicit lazy val stmt = CBOR.SumCodec[Stmt](
      CBOR.TupleCodec[Stmt.Let],
      CBOR.AliasCodec[Stmt.Expr, Expr](Stmt.Expr.apply, _.expr)
    )

    implicit lazy val mcc: CBOR.Codec[Expr.MethodChain.Component] =
      CBOR.SumCodec[Expr.MethodChain.Component](
        CBOR.TupleCodec[Expr.MethodChain.Apply],
        CBOR.TupleCodec[Expr.MethodChain.Access],
        CBOR.TupleCodec[Expr.MethodChain.Bang],
        CBOR.TupleCodec[Expr.MethodChain.Select],
        CBOR.TupleCodec[Expr.MethodChain.MethodCall]
      )

    val expr: CBOR.Codec[Expr] = CBOR.SumCodec[Expr](
      CBOR.TupleCodec[Id],
      CBOR.TupleCodec[Lit],
      CBOR.TupleCodec[StrTemplate],
      CBOR.TupleCodec[If],
      CBOR.TupleCodec[IfElse],
      CBOR.TupleCodec[Match],
      CBOR.TupleCodec[LongLambda],
      CBOR.TupleCodec[ShortLambda],
      CBOR.TupleCodec[OperatorCall],
      CBOR.TupleCodec[Project],
      CBOR.TupleCodec[ProjectAll],
      CBOR.TupleCodec[Object],
      CBOR.TupleCodec[Tuple],
      CBOR.TupleCodec[Array],
      CBOR.TupleCodec[Block],
      CBOR.TupleCodec[MethodChain],
      CBOR.TupleCodec[At]
    )
  }

  private object TypeExprs {
    import TypeExpr._

    val typeExpr: CBOR.Codec[TypeExpr] = CBOR.SumCodec[TypeExpr](
      CBOR.TupleCodec[Hole],
      CBOR.TupleCodec[Any],
      CBOR.TupleCodec[Never],
      CBOR.TupleCodec[Singleton],
      CBOR.TupleCodec[Id],
      CBOR.TupleCodec[Cons],
      CBOR.TupleCodec[Object],
      CBOR.TupleCodec[Interface],
      CBOR.TupleCodec[Projection],
      CBOR.TupleCodec[Tuple],
      CBOR.TupleCodec[Lambda],
      CBOR.TupleCodec[Union],
      CBOR.TupleCodec[Intersect],
      CBOR.TupleCodec[Difference],
      CBOR.TupleCodec[Recursive],
      CBOR.TupleCodec[Nullable]
    )
  }

  private object PatExprs {
    import PatExpr._

    val patExpr: CBOR.Codec[PatExpr] = CBOR.SumCodec[PatExpr](
      CBOR.TupleCodec[Hole],
      CBOR.TupleCodec[Bind],
      CBOR.TupleCodec[Type],
      CBOR.TupleCodec[Lit],
      CBOR.TupleCodec[Object],
      CBOR.TupleCodec[Tuple],
      CBOR.TupleCodec[Array]
    )
  }
}
