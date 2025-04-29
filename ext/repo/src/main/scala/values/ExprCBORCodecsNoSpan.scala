package fauna.repo.values

import fauna.codex.cbor.CBOR
import fql.ast._

object ExprCBORCodecsNoSpan {
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
    implicit val name = CBOR.AliasCodec[Name, String](
      str => Name(str, Span.DecodedSet),
      name => name.str
    )

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
      CBOR.AliasCodec[Stmt.Let, (Name, Option[TypeExpr], Expr)](
        { case (name, tpe, value) => Stmt.Let(name, tpe, value, Span.DecodedSet) },
        let => (let.name, let.tpe, let.value)
      ),
      CBOR.AliasCodec[Stmt.Expr, Expr](Stmt.Expr.apply, _.expr)
    )

    implicit lazy val mcc: CBOR.Codec[Expr.MethodChain.Component] =
      CBOR.SumCodec[Expr.MethodChain.Component](
        CBOR.AliasCodec[Expr.MethodChain.Apply, (Seq[Expr], Option[Boolean])](
          { case (args, optional) =>
            Expr.MethodChain
              .Apply(args, optional.map(_ => Span.DecodedSet), Span.DecodedSet)
          },
          app => (app.args, app.optional.map(_ => true))
        ),
        CBOR.AliasCodec[Expr.MethodChain.Access, (Seq[Expr], Option[Boolean])](
          { case (args, optional) =>
            Expr.MethodChain
              .Access(args, optional.map(_ => Span.DecodedSet), Span.DecodedSet)
          },
          access => (access.args, access.optional.map(_ => true))
        ),
        CBOR.AliasCodec[Expr.MethodChain.Bang, Boolean](
          _ => Expr.MethodChain.Bang(Span.DecodedSet),
          _ => true
        ),
        CBOR.AliasCodec[Expr.MethodChain.Select, (Name, Boolean)](
          { case (field, optional) =>
            Expr.MethodChain.Select(Span.DecodedSet, field, optional)
          },
          sel => (sel.field, sel.optional)
        ),
        CBOR.AliasCodec[
          Expr.MethodChain.MethodCall,
          (Name, Seq[Expr], Boolean, Option[Boolean])](
          { case (field, args, selectOptional, applyOptional) =>
            Expr.MethodChain.MethodCall(
              Span.DecodedSet,
              field,
              args,
              selectOptional,
              applyOptional.map(_ => Span.DecodedSet),
              Span.DecodedSet)
          },
          mc =>
            (mc.field, mc.args, mc.selectOptional, mc.applyOptional.map(_ => true))
        )
      )

    val expr: CBOR.Codec[Expr] = CBOR.SumCodec[Expr](
      CBOR.AliasCodec[Id, String](
        idStr => Id(idStr, Span.DecodedSet),
        id => id.str
      ),
      CBOR.AliasCodec[Lit, Literal](
        literal => Lit(literal, Span.DecodedSet),
        lit => lit.value
      ),
      CBOR.AliasCodec[StrTemplate, Seq[Either[String, Expr]]](
        parts => StrTemplate(parts, Span.DecodedSet),
        strTemplate => strTemplate.parts
      ),
      CBOR.AliasCodec[If, (Expr, Expr)](
        { case (pred, thn) => If(pred, thn, Span.DecodedSet) },
        ifExpr => (ifExpr.pred, ifExpr.`then`)
      ),
      CBOR.AliasCodec[IfElse, (Expr, Expr, Expr)](
        { case (pred, thn, els) => IfElse(pred, thn, els, Span.DecodedSet) },
        ifElse => (ifElse.pred, ifElse.`then`, ifElse.`else`)
      ),
      CBOR.AliasCodec[Match, (Expr, Seq[(PatExpr, Expr)])](
        { case (expr, branches) => Match(expr, branches, Span.DecodedSet) },
        matchExpr => (matchExpr.e, matchExpr.branches)
      ),
      CBOR.AliasCodec[LongLambda, (Seq[Option[Name]], Option[Option[Name]], Expr)](
        { case (params, vari, body) =>
          LongLambda(params, vari, body, Span.DecodedSet)
        },
        lambda => (lambda.params, lambda.vari, lambda.body)
      ),
      CBOR.TupleCodec[ShortLambda],
      CBOR.AliasCodec[OperatorCall, (Expr, Name, Option[Expr])](
        { case (e, field, args) => OperatorCall(e, field, args, Span.DecodedSet) },
        opCall => (opCall.e, opCall.field, opCall.args)
      ),
      CBOR.AliasCodec[Project, (Expr, Seq[(Name, Expr)])](
        { case (e, bindings) => Project(e, bindings, Span.DecodedSet) },
        project => (project.e, project.bindings)
      ),
      CBOR.AliasCodec[ProjectAll, Expr](
        e => ProjectAll(e, Span.DecodedSet),
        projectAll => projectAll.e
      ),
      CBOR.AliasCodec[Object, Seq[(Name, Expr)]](
        fields => Object(fields, Span.DecodedSet),
        obj => obj.fields
      ),
      CBOR.AliasCodec[Tuple, Seq[Expr]](
        elems => Tuple(elems, Span.DecodedSet),
        tuple => tuple.elems
      ),
      CBOR.AliasCodec[Array, Seq[Expr]](
        elems => Array(elems, Span.DecodedSet),
        arr => arr.elems
      ),
      CBOR.AliasCodec[Block, Seq[Stmt]](
        stmts => Block(stmts, Span.DecodedSet),
        block => block.body
      ),
      CBOR.AliasCodec[MethodChain, (Expr, Seq[MethodChain.Component])](
        { case (e, chain) => MethodChain(e, chain, Span.DecodedSet) },
        mchain => (mchain.e, mchain.chain)
      ),
      CBOR.AliasCodec[At, (Expr, Expr)](
        { case (ts, body) => At(ts, body, Span.DecodedSet) },
        at => (at.ts, at.body)
      )
    )
  }

  private object TypeExprs {
    import TypeExpr._

    val typeExpr: CBOR.Codec[TypeExpr] = CBOR.SumCodec[TypeExpr](
      CBOR.AliasCodec[Hole, Boolean](
        _ => Hole(Span.DecodedSet),
        _ => true
      ),
      CBOR.AliasCodec[Any, Boolean](
        _ => Any(Span.DecodedSet),
        _ => true
      ),
      CBOR.AliasCodec[Never, Boolean](
        _ => Never(Span.DecodedSet),
        _ => true
      ),
      CBOR.AliasCodec[Singleton, Literal](
        lit => Singleton(lit, Span.DecodedSet),
        singleton => singleton.value
      ),
      CBOR.AliasCodec[Id, String](
        str => Id(str, Span.DecodedSet),
        id => id.str
      ),
      CBOR.AliasCodec[Cons, (Name, Seq[TypeExpr])](
        { case (name, targs) => Cons(name, targs, Span.DecodedSet) },
        cons => (cons.name, cons.targs)
      ),
      CBOR.AliasCodec[Object, (Seq[(Name, TypeExpr)], Option[TypeExpr])](
        { case (fields, wildcard) => Object(fields, wildcard, Span.DecodedSet) },
        obj => (obj.fields, obj.wildcard)
      ),
      CBOR.AliasCodec[Interface, Seq[(Name, TypeExpr)]](
        fields => Interface(fields, Span.DecodedSet),
        interface => interface.fields
      ),
      CBOR.AliasCodec[Projection, (TypeExpr, TypeExpr)](
        { case (proj, ret) => Projection(proj, ret, Span.DecodedSet) },
        proj => (proj.proj, proj.ret)
      ),
      CBOR.AliasCodec[Tuple, Seq[TypeExpr]](
        elems => Tuple(elems, Span.DecodedSet),
        tuple => tuple.elems
      ),
      CBOR.AliasCodec[
        Lambda,
        (Seq[(Option[Name], TypeExpr)], Option[(Option[Name], TypeExpr)], TypeExpr)](
        { case (params, variadic, ret) =>
          Lambda(params, variadic, ret, Span.DecodedSet)
        },
        lambda => (lambda.params, lambda.variadic, lambda.ret)
      ),
      CBOR.AliasCodec[Union, Seq[TypeExpr]](
        members => Union(members, Span.DecodedSet),
        union => union.members
      ),
      CBOR.AliasCodec[Intersect, Seq[TypeExpr]](
        members => Intersect(members, Span.DecodedSet),
        intersect => intersect.members
      ),
      CBOR.AliasCodec[Difference, (TypeExpr, TypeExpr)](
        { case (elem, sub) => Difference(elem, sub, Span.DecodedSet) },
        difference => (difference.elem, difference.sub)
      ),
      CBOR.AliasCodec[Recursive, (Name, TypeExpr)](
        { case (name, in) => Recursive(name, in, Span.DecodedSet) },
        recursive => (recursive.name, recursive.in)
      ),
      CBOR.AliasCodec[Nullable, TypeExpr](
        { case te => Nullable(te, Span.DecodedSet, Span.DecodedSet) },
        nullable => nullable.base
      )
    )
  }

  private object PatExprs {
    import PatExpr._

    val patExpr: CBOR.Codec[PatExpr] = CBOR.SumCodec[PatExpr](
      CBOR.AliasCodec[Hole, Option[PatExpr]](
        inner => Hole(inner, Span.DecodedSet),
        hole => hole.inner
      ),
      CBOR.AliasCodec[Bind, (Name, Option[PatExpr])](
        { case (name, inner) => Bind(name, inner, Span.DecodedSet) },
        bind => (bind.name, bind.inner)
      ),
      CBOR.AliasCodec[Type, TypeExpr](
        tpe => Type(tpe, Span.DecodedSet),
        tpe => tpe.tpe
      ),
      CBOR.AliasCodec[Lit, Literal](
        value => Lit(value, Span.DecodedSet),
        lit => lit.value
      ),
      CBOR.AliasCodec[Object, Seq[(Name, PatExpr)]](
        fields => Object(fields, Span.DecodedSet),
        obj => obj.fields
      ),
      CBOR.AliasCodec[Tuple, Seq[PatExpr]](
        elems => Tuple(elems, Span.DecodedSet),
        tuple => tuple.elems
      ),
      CBOR.AliasCodec[Array, (Seq[PatExpr], Option[PatExpr])](
        { case (elems, rest) => Array(elems, rest, Span.DecodedSet) },
        arr => (arr.elems, arr.rest)
      )
    )
  }
}
