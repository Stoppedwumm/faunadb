package fauna.repo.test

import fauna.codex.cbor.CBOR
import fauna.repo.values.ExprCBORCodecsNoSpan.{
  exprs,
  mcc,
  patExprs,
  stmt => stmts,
  typeExprs
}
import fauna.repo.values.ExprCBORCodecsNoSpan.Source.literal
import fql.ast.{ Expr, Literal, Name, PatExpr, Span, Src, TypeExpr }

class ExprCBORCodecsNoSpanSpec extends Spec {
  def span: Span = Span(1, 10, Src.Id("query"))

  "Expr CBOR Codec" - {
    "Expressions" in {
      testExpressionEncodeDecode(
        Expr.Lit(Literal.True, span),
        Expr.Lit(Literal.True, Span.DecodedSet))
      testExpressionEncodeDecode(
        Expr.Id("id", span),
        Expr.Id("id", Span.DecodedSet)
      )
      testExpressionEncodeDecode(
        Expr.StrTemplate(Seq(Left[String, Expr]("str")), span),
        Expr.StrTemplate(Seq(Left[String, Expr]("str")), Span.DecodedSet)
      )
      testExpressionEncodeDecode(
        Expr.If(Expr.Lit(Literal.True, span), Expr.Lit(Literal.False, span), span),
        Expr.If(
          Expr.Lit(Literal.True, Span.DecodedSet),
          Expr.Lit(Literal.False, Span.DecodedSet),
          Span.DecodedSet)
      )
      testExpressionEncodeDecode(
        Expr.IfElse(
          Expr.Lit(Literal.True, span),
          Expr.Lit(Literal.False, span),
          Expr.Lit(Literal.False, span),
          span
        ),
        Expr.IfElse(
          Expr.Lit(Literal.True, Span.DecodedSet),
          Expr.Lit(Literal.False, Span.DecodedSet),
          Expr.Lit(Literal.False, Span.DecodedSet),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.At(
          Expr.Lit(Literal.Int(0), span),
          Expr.Lit(Literal.Int(0), span),
          span
        ),
        Expr.At(
          Expr.Lit(Literal.Int(0), Span.DecodedSet),
          Expr.Lit(Literal.Int(0), Span.DecodedSet),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.Match(
          Expr.Lit(Literal.True, span),
          Seq(
            (PatExpr.Lit(Literal.True, span), Expr.Lit(Literal.True, span))
          ),
          span
        ),
        Expr.Match(
          Expr.Lit(Literal.True, Span.DecodedSet),
          Seq(
            (
              PatExpr.Lit(Literal.True, Span.DecodedSet),
              Expr.Lit(Literal.True, Span.DecodedSet))
          ),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.LongLambda(
          Seq(Some(Name("id", span))),
          None,
          Expr.Lit(Literal.True, span),
          span
        ),
        Expr.LongLambda(
          Seq(Some(Name("id", Span.DecodedSet))),
          None,
          Expr.Lit(Literal.True, Span.DecodedSet),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.LongLambda(
          Seq(Some(Name("id", span))),
          Some(Some(Name("vari", span))),
          Expr.Lit(Literal.True, span),
          span
        ),
        Expr.LongLambda(
          Seq(Some(Name("id", Span.DecodedSet))),
          Some(Some(Name("vari", Span.DecodedSet))),
          Expr.Lit(Literal.True, Span.DecodedSet),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.ShortLambda(
          Expr.Lit(Literal.True, span)
        ),
        Expr.ShortLambda(
          Expr.Lit(Literal.True, Span.DecodedSet)
        )
      )
      testExpressionEncodeDecode(
        Expr.OperatorCall(
          Expr.Lit(Literal.Int(1), span),
          Name("+", span),
          Some(Expr.Lit(Literal.Int(1), span)),
          span
        ),
        Expr.OperatorCall(
          Expr.Lit(Literal.Int(1), Span.DecodedSet),
          Name("+", Span.DecodedSet),
          Some(Expr.Lit(Literal.Int(1), Span.DecodedSet)),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.MethodChain(
          Expr.Lit(Literal.True, span),
          Seq(
            Expr.MethodChain.Select(span, Name("id", span), true)
          ),
          span
        ),
        Expr.MethodChain(
          Expr.Lit(Literal.True, Span.DecodedSet),
          Seq(
            Expr.MethodChain
              .Select(Span.DecodedSet, Name("id", Span.DecodedSet), true)
          ),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.Project(
          Expr.Lit(Literal.True, span),
          Seq(
            (Name("id", span), Expr.Lit(Literal.True, span))
          ),
          span
        ),
        Expr.Project(
          Expr.Lit(Literal.True, Span.DecodedSet),
          Seq(
            (Name("id", Span.DecodedSet), Expr.Lit(Literal.True, Span.DecodedSet))
          ),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.ProjectAll(
          Expr.Lit(Literal.True, span),
          span
        ),
        Expr.ProjectAll(
          Expr.Lit(Literal.True, Span.DecodedSet),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.Object(
          Seq(
            (Name("id", span), Expr.Lit(Literal.True, span))
          ),
          span
        ),
        Expr.Object(
          Seq(
            (Name("id", Span.DecodedSet), Expr.Lit(Literal.True, Span.DecodedSet))
          ),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.Tuple(
          Seq(
            Expr.Lit(Literal.True, span)
          ),
          span
        ),
        Expr.Tuple(
          Seq(
            Expr.Lit(Literal.True, Span.DecodedSet)
          ),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.Array(
          Seq(
            Expr.Lit(Literal.True, span)
          ),
          span
        ),
        Expr.Array(
          Seq(
            Expr.Lit(Literal.True, Span.DecodedSet)
          ),
          Span.DecodedSet
        )
      )
      testExpressionEncodeDecode(
        Expr.Block(
          Seq(
            Expr.Stmt.Expr(Expr.Lit(Literal.True, span)),
            Expr.Stmt.Expr(Expr.Lit(Literal.True, span))
          ),
          span
        ),
        Expr.Block(
          Seq(
            Expr.Stmt.Expr(Expr.Lit(Literal.True, Span.DecodedSet)),
            Expr.Stmt.Expr(Expr.Lit(Literal.True, Span.DecodedSet))
          ),
          Span.DecodedSet
        )
      )
    }
  }
  "Statements" in {
    testStmtEncodeDecode(
      Expr.Stmt.Let(
        Name("foo", Span.DecodedSet),
        Some(TypeExpr.Id("bar", Span.DecodedSet)),
        Expr.Id("baz", Span.DecodedSet),
        Span.DecodedSet))
    testStmtEncodeDecode(Expr.Stmt.Expr(Expr.Id("baz", Span.DecodedSet)))
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
      Expr.MethodChain.Select(span, Name("foo", span), false),
      Expr.MethodChain.Select(Span.DecodedSet, Name("foo", Span.DecodedSet), false))
    testMethodChainEncodeDecode(
      Expr.MethodChain.Bang(span),
      Expr.MethodChain.Bang(Span.DecodedSet))
    testMethodChainEncodeDecode(
      Expr.MethodChain
        .Apply(Seq(Expr.Id("id", span)), Some(span), span),
      Expr.MethodChain
        .Apply(
          Seq(Expr.Id("id", Span.DecodedSet)),
          Some(Span.DecodedSet),
          Span.DecodedSet)
    )
    testMethodChainEncodeDecode(
      Expr.MethodChain
        .Access(Seq(Expr.Id("id", span)), Some(span), span),
      Expr.MethodChain
        .Access(
          Seq(Expr.Id("id", Span.DecodedSet)),
          Some(Span.DecodedSet),
          Span.DecodedSet)
    )
    testMethodChainEncodeDecode(
      Expr.MethodChain
        .MethodCall(
          span,
          Name("foo", span),
          Seq(Expr.Id("myArg", span)),
          false,
          Some(span),
          span),
      Expr.MethodChain
        .MethodCall(
          Span.DecodedSet,
          Name("foo", Span.DecodedSet),
          Seq(Expr.Id("myArg", Span.DecodedSet)),
          false,
          Some(Span.DecodedSet),
          Span.DecodedSet)
    )
  }
  "Type Expressions" in {
    testTypeExpressionEncodeDecode(TypeExpr.Hole(Span.DecodedSet))
    testTypeExpressionEncodeDecode(TypeExpr.Any(Span.DecodedSet))
    testTypeExpressionEncodeDecode(TypeExpr.Never(Span.DecodedSet))
    testTypeExpressionEncodeDecode(TypeExpr.Singleton(Literal.True, Span.DecodedSet))
    testTypeExpressionEncodeDecode(
      TypeExpr.Id("id", Span.DecodedSet)
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Cons(
        Name("id", Span.DecodedSet),
        Seq(
          TypeExpr.Hole(Span.DecodedSet)
        ),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Object(
        Seq(
          (Name("id", Span.DecodedSet), TypeExpr.Hole(Span.DecodedSet))
        ),
        None,
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Interface(
        Seq(
          (Name("id", Span.DecodedSet), TypeExpr.Hole(Span.DecodedSet))
        ),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Projection(
        TypeExpr.Any(Span.DecodedSet),
        TypeExpr.Any(Span.DecodedSet),
        Span.DecodedSet)
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Tuple(
        Seq(TypeExpr.Hole(Span.DecodedSet)),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Lambda(
        Seq(Some(Name("foo", Span.DecodedSet)) -> TypeExpr.Any(Span.DecodedSet)),
        Some(None -> TypeExpr.Any(Span.DecodedSet)),
        TypeExpr.Any(Span.DecodedSet),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Union(
        Seq(TypeExpr.Any(Span.DecodedSet)),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Intersect(
        Seq(TypeExpr.Any(Span.DecodedSet)),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Difference(
        TypeExpr.Any(Span.DecodedSet),
        TypeExpr.Singleton(Literal.Null, Span.DecodedSet),
        Span.DecodedSet
      )
    )
    testTypeExpressionEncodeDecode(
      TypeExpr.Recursive(
        Name("id", Span.DecodedSet),
        TypeExpr.Any(Span.DecodedSet),
        Span.DecodedSet
      )
    )
  }
  "Pattern Expressions" in {
    testPatternExpressionEncodeDecode(
      PatExpr.Hole(None, Span.DecodedSet)
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Bind(
        Name("id", Span.DecodedSet),
        None,
        Span.DecodedSet
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Type(
        TypeExpr.Hole(Span.DecodedSet),
        Span.DecodedSet
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Lit(
        Literal.True,
        Span.DecodedSet
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Object(
        Seq(
          (Name("id", Span.DecodedSet), PatExpr.Hole(None, Span.DecodedSet))
        ),
        Span.DecodedSet
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Tuple(
        Seq(PatExpr.Hole(None, Span.DecodedSet)),
        Span.DecodedSet
      )
    )
    testPatternExpressionEncodeDecode(
      PatExpr.Array(
        Seq(PatExpr.Hole(None, Span.DecodedSet)),
        None,
        Span.DecodedSet
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

  /** This takes an expected Expr because all spans should be stripped out on encode. This means that
    * a decode/encode should take a span and turn it into a null span.  This allows us to pass this method
    * a expression with a span and ensure that its spans are nulled out as we decode it.
    */
  private def testExpressionEncodeDecode(expr: Expr, expectedExpr: Expr) = {

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
    dec shouldEqual expectedExpr
  }

  /** This takes an expected Expr because all spans should be stripped out on encode. This means that
    * a decode/encode should take a span and turn it into a null span.  This allows us to pass this method
    * a expression with a span and ensure that its spans are nulled out as we decode it.
    */
  private def testMethodChainEncodeDecode(
    mc: Expr.MethodChain.Component,
    expectedMc: Expr.MethodChain.Component) = {

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
    dec shouldEqual expectedMc
  }
  private def testTypeExpressionEncodeDecode(typeExpr: TypeExpr) = {

    /** This match statement is used to ensure that we don't forget to add new Expressions to the list ExprCBORCodecs.typeExprs
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
