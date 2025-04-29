package fauna.repo.test

import fauna.codex.cbor.CBOR
import fauna.repo.values.ExprCBORCodecs.{
  exprs,
  mcc,
  patExprs,
  stmt => stmts,
  typeExprs
}
import fauna.repo.values.ExprCBORCodecs.Source.literal
import fql.ast.{ Expr, Literal, Name, PatExpr, Span, TypeExpr }

class ExprCBORCodecsSpec extends Spec {
  "Expr CBOR Codec" - {
    "Expressions" in {
      testExpressionEncodeDecode(Expr.Lit(Literal.True, Span.Null))
      testExpressionEncodeDecode(
        Expr.Id("id", Span.Null)
      )
      testExpressionEncodeDecode(
        Expr.StrTemplate(Seq(Left[String, Expr]("str")), Span.Null)
      )
      testExpressionEncodeDecode(
        Expr.If(
          Expr.Lit(Literal.True, Span.Null),
          Expr.Lit(Literal.False, Span.Null),
          Span.Null)
      )
      testExpressionEncodeDecode(
        Expr.IfElse(
          Expr.Lit(Literal.True, Span.Null),
          Expr.Lit(Literal.False, Span.Null),
          Expr.Lit(Literal.False, Span.Null),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.At(
          Expr.Lit(Literal.Int(0), Span.Null),
          Expr.Lit(Literal.Int(0), Span.Null),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.Match(
          Expr.Lit(Literal.True, Span.Null),
          Seq(
            (PatExpr.Lit(Literal.True, Span.Null), Expr.Lit(Literal.True, Span.Null))
          ),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.LongLambda(
          Seq(Some(Name("id", Span.Null))),
          None,
          Expr.Lit(Literal.True, Span.Null),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.LongLambda(
          Seq(Some(Name("id", Span.Null))),
          Some(Some(Name("vari", Span.Null))),
          Expr.Lit(Literal.True, Span.Null),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.ShortLambda(
          Expr.Lit(Literal.True, Span.Null)
        )
      )
      testExpressionEncodeDecode(
        Expr.OperatorCall(
          Expr.Lit(Literal.Int(1), Span.Null),
          Name("+", Span.Null),
          Some(Expr.Lit(Literal.Int(1), Span.Null)),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.MethodChain(
          Expr.Lit(Literal.True, Span.Null),
          Seq(
            Expr.MethodChain.Select(Span.Null, Name("id", Span.Null), true)
          ),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.Project(
          Expr.Lit(Literal.True, Span.Null),
          Seq(
            (Name("id", Span.Null), Expr.Lit(Literal.True, Span.Null))
          ),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.ProjectAll(
          Expr.Lit(Literal.True, Span.Null),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.Object(
          Seq(
            (Name("id", Span.Null), Expr.Lit(Literal.True, Span.Null))
          ),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.Tuple(
          Seq(
            Expr.Lit(Literal.True, Span.Null)
          ),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.Array(
          Seq(
            Expr.Lit(Literal.True, Span.Null)
          ),
          Span.Null
        )
      )
      testExpressionEncodeDecode(
        Expr.Block(
          Seq(
            Expr.Stmt.Expr(Expr.Lit(Literal.True, Span.Null)),
            Expr.Stmt.Expr(Expr.Lit(Literal.True, Span.Null))
          ),
          Span.Null
        )
      )
    }
  }
  "Statements" in {
    testStmtEncodeDecode(
      Expr.Stmt.Let(
        Name("foo", Span.Null),
        Some(TypeExpr.Id("bar", Span.Null)),
        Expr.Id("baz", Span.Null),
        Span.Null))
    testStmtEncodeDecode(Expr.Stmt.Expr(Expr.Id("baz", Span.Null)))
  }
  "Literals" in {
    testLiteralEncodeDecode(Literal.Null)
    testLiteralEncodeDecode(Literal.True)
    testLiteralEncodeDecode(Literal.False)
    testLiteralEncodeDecode(Literal.Int(2))
    testLiteralEncodeDecode(Literal.Int(BigInt(2)))
    testLiteralEncodeDecode(Literal.Float(2.0))
    testLiteralEncodeDecode(Literal.Float(BigDecimal(2.0)))
    testLiteralEncodeDecode(Literal.Str("hello"))
  }
  "Method chains" in {
    testMethodChainEncodeDecode(
      Expr.MethodChain.Select(Span.Null, Name("foo", Span.Null), false))
    testMethodChainEncodeDecode(Expr.MethodChain.Bang(Span.Null))
    testMethodChainEncodeDecode(
      Expr.MethodChain
        .Apply(Seq(Expr.Id("id", Span.Null)), Some(Span.Null), Span.Null))
    testMethodChainEncodeDecode(
      Expr.MethodChain
        .Access(Seq(Expr.Id("id", Span.Null)), Some(Span.Null), Span.Null))
    testMethodChainEncodeDecode(
      Expr.MethodChain
        .MethodCall(
          Span.Null,
          Name("foo", Span.Null),
          Seq(Expr.Id("myArg", Span.Null)),
          false,
          Some(Span.Null),
          Span.Null))
  }
  "Type Expressions" in {
    testTypeExpressionEncodeDecode(TypeExpr.Hole(Span.Null))
    testTypeExpressionEncodeDecode(TypeExpr.Any(Span.Null))
    testTypeExpressionEncodeDecode(TypeExpr.Never(Span.Null))
    testTypeExpressionEncodeDecode(TypeExpr.Singleton(Literal.True, Span.Null))
    testTypeExpressionEncodeDecode(
      TypeExpr.Id("id", Span.Null)
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Cons(
        Name("id", Span.Null),
        Seq(
          TypeExpr.Hole(Span.Null)
        ),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Object(
        Seq(
          (Name("id", Span.Null), TypeExpr.Hole(Span.Null))
        ),
        None,
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Interface(
        Seq(
          (Name("id", Span.Null), TypeExpr.Hole(Span.Null))
        ),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Projection(
        TypeExpr.Any(Span.Null),
        TypeExpr.Any(Span.Null),
        Span.Null)
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Tuple(
        Seq(TypeExpr.Hole(Span.Null)),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Lambda(
        Seq(Some(Name("foo", Span.Null)) -> TypeExpr.Any(Span.Null)),
        Some(None -> TypeExpr.Any(Span.Null)),
        TypeExpr.Any(Span.Null),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Union(
        Seq(TypeExpr.Any(Span.Null)),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Intersect(
        Seq(TypeExpr.Any(Span.Null)),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Difference(
        TypeExpr.Any(Span.Null),
        TypeExpr.Singleton(Literal.Null, Span.Null),
        Span.Null
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Recursive(
        Name("id", Span.Null),
        TypeExpr.Any(Span.Null),
        Span.Null
      )
    )
  }
  "Pattern Expressions" in {
    testPatternExpressionEncodeDecode(
      PatExpr.Hole(None, Span.Null)
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Bind(
        Name("id", Span.Null),
        None,
        Span.Null
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Type(
        TypeExpr.Hole(Span.Null),
        Span.Null
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Lit(
        Literal.True,
        Span.Null
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Object(
        Seq(
          (Name("id", Span.Null), PatExpr.Hole(None, Span.Null))
        ),
        Span.Null
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Tuple(
        Seq(PatExpr.Hole(None, Span.Null)),
        Span.Null
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Array(
        Seq(PatExpr.Hole(None, Span.Null)),
        None,
        Span.Null
      )
    )
  }
  private def testStmtEncodeDecode(stmt: Expr.Stmt) = {

    /** This makes sure we don't forget to add an encoder for a new Expr.Stmt
      */
    val toEnc = stmt match {
      case v: Expr.Stmt.Let  => v
      case v: Expr.Stmt.Expr => v
    }
    val enc = CBOR.encode(toEnc)(stmts)
    val dec = CBOR.decode(enc)(stmts)
    dec shouldEqual toEnc
  }
  private def testLiteralEncodeDecode(lit: Literal) = {

    /** This makes sure we don't forget to add an encoder for a new Literal
      */
    val toEnc = lit match {
      case Literal.Null     => lit
      case Literal.True     => lit
      case Literal.False    => lit
      case v: Literal.Int   => v
      case v: Literal.Float => v
      case v: Literal.Str   => v
    }
    val enc = CBOR.encode(toEnc)(literal)
    val dec = CBOR.decode(enc)(literal)
    dec shouldEqual toEnc
  }
  private def testExpressionEncodeDecode(expr: Expr) = {

    /** This match statement is used to ensure that we don't forget to add new Expressions to the list ExprCBORCodes.exprs
      */
    val toEnc = expr match {
      case v: Expr.Id           => v
      case v: Expr.Lit          => v
      case v: Expr.StrTemplate  => v
      case v: Expr.If           => v
      case v: Expr.IfElse       => v
      case v: Expr.At           => v
      case v: Expr.Match        => v
      case v: Expr.LongLambda   => v
      case v: Expr.ShortLambda  => v
      case v: Expr.OperatorCall => v
      case v: Expr.MethodChain  => v
      case v: Expr.Project      => v
      case v: Expr.ProjectAll   => v
      case v: Expr.Object       => v
      case v: Expr.Tuple        => v
      case v: Expr.Array        => v
      case v: Expr.Block        => v
    }
    val enc = CBOR.encode(toEnc)(exprs)
    val dec = CBOR.decode(enc)(exprs)
    dec shouldEqual toEnc
  }
  private def testMethodChainEncodeDecode(mc: Expr.MethodChain.Component) = {

    /** This match statement is used to ensure that we don't forget to add new MethodChain.Component to the list ExprCBORCodes.mcc
      */
    val toEnc = mc match {
      case v: Expr.MethodChain.Apply      => v
      case v: Expr.MethodChain.Access     => v
      case v: Expr.MethodChain.Bang       => v
      case v: Expr.MethodChain.Select     => v
      case v: Expr.MethodChain.MethodCall => v
    }
    val enc = CBOR.encode(toEnc)(mcc)
    val dec = CBOR.decode(enc)(mcc)
    dec shouldEqual toEnc
  }
  private def testTypeExpressionEncodeDecode(typeExpr: TypeExpr) = {

    /** This match statement is used to ensure that we don't forget to add new Expressions to the list ExprCBORCodes.typeExprs
      */
    val toEnc = typeExpr match {
      case v: TypeExpr.Hole       => v
      case v: TypeExpr.Any        => v
      case v: TypeExpr.Never      => v
      case v: TypeExpr.Singleton  => v
      case v: TypeExpr.Id         => v
      case v: TypeExpr.Cons       => v
      case v: TypeExpr.Object     => v
      case v: TypeExpr.Interface  => v
      case v: TypeExpr.Projection => v
      case v: TypeExpr.Tuple      => v
      case v: TypeExpr.Lambda     => v
      case v: TypeExpr.Union      => v
      case v: TypeExpr.Intersect  => v
      case v: TypeExpr.Recursive  => v
      case v: TypeExpr.Difference => v
      case v: TypeExpr.Nullable   => v
    }
    val enc = CBOR.encode(toEnc)(typeExprs)
    val dec = CBOR.decode(enc)(typeExprs)
    dec shouldEqual toEnc
  }
  private def testPatternExpressionEncodeDecode(patExpr: PatExpr) = {

    /** This match statement is used to ensure that we don't forget to add new Expressions to the list ExprCBORCodes.patExprs
      */
    val toEnc = patExpr match {
      case v: PatExpr.Hole   => v
      case v: PatExpr.Bind   => v
      case v: PatExpr.Type   => v
      case v: PatExpr.Lit    => v
      case v: PatExpr.Object => v
      case v: PatExpr.Tuple  => v
      case v: PatExpr.Array  => v
    }
    val enc = CBOR.encode(toEnc)(patExprs)
    val dec = CBOR.decode(enc)(patExprs)
    dec shouldEqual toEnc
  }
}
