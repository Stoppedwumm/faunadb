package fauna.repo.test

import fauna.atoms.{ DocID, SubID, UserCollectionID }
import fauna.lang.Timestamp
import fauna.repo.query.ReadCache
import fauna.repo.values.{ ExprCBORCodecs, Value, ValueReification }
import fql.ast.{ Expr, Literal, Name, Span }
import java.time.LocalDate
import java.util.UUID
import scala.collection.immutable.{ ArraySeq, SeqMap }

class ValueReificationSpec extends Spec {

  object FakeSet extends Value.Set {
    def `Only fauna.model.runtime.fql2.ValueSet may extend Value.Set`() = ()
    def reify(ctx: ValueReification.ReifyCtx) = Expr.Id("FakeSet", Span.Null)
  }

  "ExprCBORSpec" - {
    "can construct CBOR codecs" in {
      ExprCBORCodecs.exprs
      ExprCBORCodecs.typeExprs
      ExprCBORCodecs.patExprs
    }
  }

  "ValueReification" - {
    "reifies IDs as string values, since byId accepts a string. (FIXME...)" in {
      val id = Value.ID(123)
      val idStr = Value.Str("123")
      ValueReification
        .reify(id) shouldEqual ((Expr.Id(s"v0", Span.Null), Seq(idStr)))
    }

    "reifies an ID as-is" in {
      pendingUntilFixed {
        val id = Value.ID(123)
        // We allow comparing ID and Str, so we need this check to make sure it is
        // reified as an ID.
        ValueReification.reify(id) should matchPattern {
          case ((Expr.Id(s"v0", Span.Null), Seq(reifiedId)))
              if reifiedId.isInstanceOf[Value.ID] && id == reifiedId =>
        }
      }
    }

    "reifies SetCursors as-is" in {
      val cur =
        Value.SetCursor(
          Expr.Id(s"v0", Span.Null),
          Vector.empty,
          Some(Vector.empty),
          None,
          0)

      ValueReification.reify(cur) shouldEqual ((Expr.Id(s"v0", Span.Null), Seq(cur)))
    }

    "reifies partials as-is" in {
      val par =
        Value.Struct.Partial(
          Value.Doc(DocID(SubID(1), UserCollectionID.MinValue)),
          ReadCache.Prefix.empty,
          ReadCache.Path.empty,
          null.asInstanceOf[ReadCache.Fragment.Struct], // irrelevant for this test
          Span.Null
        )

      ValueReification.reify(par) shouldEqual ((Expr.Id(s"v0", Span.Null), Seq(par)))
    }

    "can reify a scalar into an expression and seq of persistable values" in {
      // scalars which are saved into context
      val scalars1 = Seq(
        Value.Int(1),
        Value.Long(1),
        Value.Double(1.0),
        Value.Bytes(ArraySeq(1)),
        Value.Time(Timestamp.Epoch),
        Value.TransactionTime,
        Value.Date(LocalDate.EPOCH),
        Value.UUID(UUID.randomUUID),
        Value.Null(Span.Null),
        Value.Doc(DocID(SubID(1), UserCollectionID.MinValue))
      )

      scalars1 foreach { v =>
        ValueReification.reify(v) shouldEqual ((Expr.Id(s"v0", Span.Null), Seq(v)))
      }

      // scalars which are saved as expressions
      ValueReification
        .reify(Value.True) shouldEqual ((Expr.Lit(Literal.True, Span.Null), Nil))
      ValueReification
        .reify(Value.False) shouldEqual ((Expr.Lit(Literal.False, Span.Null), Nil))
      ValueReification.reify(Value.Str("foo")) shouldEqual ((
        Expr.Lit(Literal.Str("foo"), Span.Null),
        Nil))
    }

    "reifies a persistable container as-is" in {
      val arr = Value.Array(Value.Int(1), Value.True)
      ValueReification.reify(arr) shouldEqual ((Expr.Id(s"v0", Span.Null), Seq(arr)))

      val struct = Value.Struct("a" -> Value.Int(1), "b" -> Value.Int(2))
      ValueReification
        .reify(struct) shouldEqual ((Expr.Id(s"v0", Span.Null), Seq(struct)))
    }

    "reifies a non-persistable container as an expr" in {
      val body = Expr.Id("x", Span.Null)
      val lam = Value.Lambda(ArraySeq(Some("x")), None, body, Map.empty)
      val lamE =
        Expr.LongLambda(Seq(Some(Name("x", Span.Null))), None, body, Span.Null)

      val arr = Value.Array(Value.Int(1), lam)
      ValueReification.reify(arr) shouldEqual ((
        Expr.Array(Seq(Expr.Id("v0", Span.Null), lamE), Span.Null),
        Seq(Value.Int(1))))

      val struct = Value.Struct("a" -> Value.Int(1), "b" -> lam)
      ValueReification
        .reify(struct) shouldEqual ((
        Expr.Object(
          Seq(
            (Name("a", Span.Null), Expr.Id(s"v0", Span.Null)),
            (Name("b", Span.Null), lamE)),
          Span.Null),
        Seq(Value.Int(1))))
    }

    "reifies a lambda (non closure)" in {
      val body = Expr.Id("x", Span.Null)
      val lam = Value.Lambda(ArraySeq(Some("x")), None, body, Map.empty)
      val lamE =
        Expr.LongLambda(Seq(Some(Name("x", Span.Null))), None, body, Span.Null)

      ValueReification.reify(lam) shouldEqual ((lamE, Nil))
    }

    "reifies a variadic lambda" in {
      val body = Expr.Id("x", Span.Null)
      val lam = Value.Lambda(ArraySeq(Some("x")), Some(Some("y")), body, Map.empty)
      val lamE = Expr.LongLambda(
        Seq(Some(Name("x", Span.Null))),
        Some(Some(Name("y", Span.Null))),
        body,
        Span.Null)

      ValueReification.reify(lam) shouldEqual ((lamE, Nil))
    }

    "reifies a lambda (with closure)" in {
      val closure = SeqMap("pred" -> Value.True, "y" -> Value.Int(1))
      val body = Expr.IfElse(
        Expr.Id("pred", Span.Null),
        Expr.Id("x", Span.Null),
        Expr.Id("y", Span.Null),
        Span.Null)
      val lam = Value.Lambda(ArraySeq(Some("x")), None, body, closure)

      val exprs = Seq(
        Expr.Stmt.Let(
          Name("pred", Span.Null),
          None,
          Expr.Lit(Literal.True, Span.Null),
          Span.Null),
        Expr.Stmt
          .Let(Name("y", Span.Null), None, Expr.Id("v0", Span.Null), Span.Null),
        Expr.Stmt.Expr(
          Expr.LongLambda(Seq(Some(Name("x", Span.Null))), None, body, Span.Null))
      )
      val expr = Expr.Block(exprs, Span.Null)

      ValueReification.reify(lam) shouldEqual ((expr, Seq(Value.Int(1))))
    }
  }
}
