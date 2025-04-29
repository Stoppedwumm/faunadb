package fql.test

import fql.ast.{ Name, Span, TypeExpr }
import fql.ast.Expr._
import fql.ast.Literal._
import fql.parser.Parser
import fql.Result

class WithNullSpanSpec extends Spec {
  def parseOk(source: String) = Parser.expr(source) match {
    case Result.Ok(v)  => v
    case Result.Err(e) => fail(s"failed to parse: $e")
  }

  it should "nullify ids, lits, str templates, operator calls" in {
    parseOk(
      "{ foo + 1 + 'hi' + \"hi\" + \"hello #{2 + 3}\" }").withNullSpan shouldBe Block(
      List(
        Stmt.Expr(
          OperatorCall(
            OperatorCall(
              OperatorCall(
                OperatorCall(
                  Id("foo", Span.Null),
                  Name("+", Span.Null),
                  Some(Lit(Int(1), Span.Null)),
                  Span.Null),
                Name("+", Span.Null),
                Some(Lit(Str("hi"), Span.Null)),
                Span.Null
              ),
              Name("+", Span.Null),
              Some(Lit(Str("hi"), Span.Null)),
              Span.Null
            ),
            Name("+", Span.Null),
            Some(
              StrTemplate(
                List(
                  Left("hello "),
                  Right(
                    OperatorCall(
                      Lit(Int(2), Span.Null),
                      Name("+", Span.Null),
                      Some(Lit(Int(3), Span.Null)),
                      Span.Null))),
                Span.Null)),
            Span.Null
          ))),
      Span.Null
    )
  }

  it should "nullify blocks, if, at, if/else" in {
    parseOk("{ if (true) 1 else 2; if (true) 2; at (3) 4 }").withNullSpan shouldBe (
      Block(
        List(
          Stmt.Expr(
            IfElse(
              Lit(True, Span.Null),
              Lit(Int(1), Span.Null),
              Lit(Int(2), Span.Null),
              Span.Null)),
          Stmt.Expr(If(Lit(True, Span.Null), Lit(Int(2), Span.Null), Span.Null)),
          Stmt.Expr(At(Lit(Int(3), Span.Null), Lit(Int(4), Span.Null), Span.Null))
        ),
        Span.Null
      )
    )
  }

  it should "nullify lambdas" in {
    parseOk("{ (x) => x + 2; (.x) }").withNullSpan shouldBe (
      Block(
        List(
          Stmt.Expr(
            LongLambda(
              List(Some(Name("x", Span.Null))),
              None,
              OperatorCall(
                Id("x", Span.Null),
                Name("+", Span.Null),
                Some(Lit(Int(2), Span.Null)),
                Span.Null),
              Span.Null)),
          Stmt.Expr(
            Tuple(
              List(
                ShortLambda(
                  MethodChain(
                    Id(s"$$this", Span.Null),
                    List(MethodChain.Select(Span.Null, Name("x", Span.Null), false)),
                    Span.Null))),
              Span.Null))
        ),
        Span.Null
      )
    )
  }

  it should "nullify objects and projections" in {
    parseOk(
      "{ { a: 2, c: 3 } { a, b: .c, foo { bar } } }").withNullSpan shouldBe Block(
      List(
        Stmt.Expr(
          Project(
            Object(
              List(
                (Name("a", Span.Null), Lit(Int(2), Span.Null)),
                (Name("c", Span.Null), Lit(Int(3), Span.Null))),
              Span.Null),
            List(
              (
                Name("a", Span.Null),
                MethodChain(
                  Id(s"$$this", Span.Null),
                  List(MethodChain.Select(Span.Null, Name("a", Span.Null), false)),
                  Span.Null)),
              (
                Name("b", Span.Null),
                MethodChain(
                  Id(s"$$this", Span.Null),
                  List(MethodChain.Select(Span.Null, Name("c", Span.Null), false)),
                  Span.Null)),
              (
                Name("foo", Span.Null),
                Project(
                  MethodChain(
                    Id(s"$$this", Span.Null),
                    List(
                      MethodChain.Select(Span.Null, Name("foo", Span.Null), false)),
                    Span.Null),
                  List(
                    (
                      Name("bar", Span.Null),
                      MethodChain(
                        Id(s"$$this", Span.Null),
                        List(MethodChain
                          .Select(Span.Null, Name("bar", Span.Null), false)),
                        Span.Null))),
                  Span.Null
                ))
            ),
            Span.Null
          ))),
      Span.Null
    )
  }

  it should "nullify tuples and arrays" in {
    parseOk("{ (1, 2); [1, 2, 3]; (); (1) }").withNullSpan shouldBe Block(
      List(
        Stmt.Expr(
          Tuple(List(Lit(Int(1), Span.Null), Lit(Int(2), Span.Null)), Span.Null)),
        Stmt.Expr(
          Array(
            List(
              Lit(Int(1), Span.Null),
              Lit(Int(2), Span.Null),
              Lit(Int(3), Span.Null)),
            Span.Null)),
        Stmt.Expr(Tuple(List(), Span.Null)),
        Stmt.Expr(Tuple(List(Lit(Int(1), Span.Null)), Span.Null))
      ),
      Span.Null
    )
  }

  it should "nullify method chains" in {
    // This expr has one method chain with every combination of optionals.
    parseOk(
      "{ foo()?.().bar?.baz!.foo().bar?.()?.baz()?.baz?.() }").withNullSpan shouldBe Block(
      List(
        Stmt.Expr(
          MethodChain(
            Id("foo", Span.Null),
            List(
              MethodChain.Apply(List.empty, None, Span.Null),
              MethodChain.Apply(List.empty, Some(Span.Null), Span.Null),
              MethodChain.Select(Span.Null, Name("bar", Span.Null), false),
              MethodChain.Select(Span.Null, Name("baz", Span.Null), true),
              MethodChain.Bang(Span.Null),
              MethodChain.MethodCall(
                Span.Null,
                Name("foo", Span.Null),
                List(),
                false,
                None,
                Span.Null),
              MethodChain.MethodCall(
                Span.Null,
                Name("bar", Span.Null),
                List(),
                false,
                Some(Span.Null),
                Span.Null),
              MethodChain.MethodCall(
                Span.Null,
                Name("baz", Span.Null),
                List(),
                true,
                None,
                Span.Null),
              MethodChain.MethodCall(
                Span.Null,
                Name("baz", Span.Null),
                List(),
                true,
                Some(Span.Null),
                Span.Null)
            ),
            Span.Null
          ))
      ),
      Span.Null
    )
  }

  it should "nullify type expr hole, any, never, singleton, id, and cons" in {
    parseOk(
      "{ let foo: _ & Any & Never & 2 & Foo & Array<3> = bar; foo }").withNullSpan shouldBe Block(
      List(
        Stmt.Let(
          Name("foo", Span.Null),
          Some(
            TypeExpr.Intersect(
              Vector(
                TypeExpr.Id("_", Span.Null),
                TypeExpr.Any(Span.Null),
                TypeExpr.Never(Span.Null),
                TypeExpr.Singleton(Int(2), Span.Null),
                TypeExpr.Id("Foo", Span.Null),
                TypeExpr.Cons(
                  Name("Array", Span.Null),
                  List(TypeExpr.Singleton(Int(3), Span.Null)),
                  Span.Null)
              ),
              Span.Null
            )),
          Id("bar", Span.Null),
          Span.Null
        ),
        Stmt.Expr(Id("foo", Span.Null))
      ),
      Span.Null
    )
  }

  it should "nullify type expr interface, tuple, and lambda" in {
    parseOk(
      "{ let foo: { foo: 3, ... } & [1, 2] & (3 => a) & ((_: 3) => a) & ((a: 3) => a) & ((a, ...b) => a) = bar; foo }").withNullSpan shouldBe Block(
      List(
        Stmt.Let(
          Name("foo", Span.Null),
          Some(
            TypeExpr.Intersect(
              Vector(
                TypeExpr.Interface(
                  List(
                    (Name("foo", Span.Null), TypeExpr.Singleton(Int(3), Span.Null))),
                  Span.Null),
                TypeExpr.Tuple(
                  List(
                    TypeExpr.Singleton(Int(1), Span.Null),
                    TypeExpr.Singleton(Int(2), Span.Null)),
                  Span.Null),
                TypeExpr.Lambda(
                  List((None, TypeExpr.Singleton(Int(3), Span.Null))),
                  None,
                  TypeExpr.Id("a", Span.Null),
                  Span.Null),
                TypeExpr.Lambda(
                  List((
                    Some(Name("_", Span.Null)),
                    TypeExpr.Singleton(Int(3), Span.Null))),
                  None,
                  TypeExpr.Id("a", Span.Null),
                  Span.Null),
                TypeExpr.Lambda(
                  List((
                    Some(Name("a", Span.Null)),
                    TypeExpr.Singleton(Int(3), Span.Null))),
                  None,
                  TypeExpr.Id("a", Span.Null),
                  Span.Null),
                TypeExpr.Lambda(
                  List((None, TypeExpr.Id("a", Span.Null))),
                  Some((None, TypeExpr.Id("b", Span.Null))),
                  TypeExpr.Id("a", Span.Null),
                  Span.Null)
              ),
              Span.Null
            )),
          Id("bar", Span.Null),
          Span.Null
        ),
        Stmt.Expr(Id("foo", Span.Null))
      ),
      Span.Null
    )
  }

  it should "nullify type union and intersect" in {
    parseOk("{ let foo: 2 | 3 & 4 = bar; foo }").withNullSpan shouldBe Block(
      List(
        Stmt.Let(
          Name("foo", Span.Null),
          Some(
            TypeExpr.Union(
              Vector(
                TypeExpr.Singleton(Int(2), Span.Null),
                TypeExpr.Intersect(
                  Vector(
                    TypeExpr.Singleton(Int(3), Span.Null),
                    TypeExpr.Singleton(Int(4), Span.Null)),
                  Span.Null)
              ),
              Span.Null
            )),
          Id("bar", Span.Null),
          Span.Null
        ),
        Stmt.Expr(Id("foo", Span.Null))
      ),
      Span.Null
    )
  }
}
