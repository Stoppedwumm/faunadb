package fql.test

import fql.ast.{ Name, Span, Src }
import fql.error.{ TypeError, Warning }
import fql.parser.Parser
import fql.typer.Type
import org.scalactic.source.Position
import scala.collection.immutable.ArraySeq

class TyperExprSpec extends TypeSpec {

  def span(s: Int, e: Int) = Span(s, e, Src.Query(""))

  def parseTScheme(src: String) = {
    val parsed = parse(Parser.typeSchemeExpr(_), src)
    TestTyper().typeTSchemeUnchecked(parsed)
  }

  def checkTExpr(
    src: String,
    expected: Type,
    allowGenerics: Boolean = true
  )(implicit pos: Position) = {
    val typer = TestTyper()

    val actual =
      typer
        .typeTExprType(parseTExpr(src), allowGenerics)
        .getOrElse(fail("failed to parse"))

    if (actual != expected) {
      fail(s"not equal:\n${p(expected)}\n${p(actual)}")
    }
  }

  def checkTExprErr(
    src: String,
    expected: Seq[TypeError],
    allowGenerics: Boolean = true)(implicit pos: Position) = {
    val typer = TestTyper()

    val actual =
      typer
        .typeTExprType(parseTExpr(src), allowGenerics)
        .errOrElse(Nil)

    if (actual != expected) {
      fail(s"not equal:\n$expected}\n$actual")
    }
  }

  def checkTExprWarn(
    src: String,
    expected: Seq[Warning],
    allowGenerics: Boolean = true)(implicit pos: Position) = {
    val typer = TestTyper()

    typer
      .typeTExprType(parseTExpr(src), allowGenerics)
      .getOrElse(fail("parsing failed"))

    val actual = typer.warnings.toList
    if (actual != expected) {
      fail(s"not equal:\n$expected\n$actual")
    }
  }

  // Stringifies this type verbosely, including spans.
  def p(ty: Type): String = ty match {
    case v: Type.Var => v.toString
    case s: Type.Skolem =>
      val prefix = if (s.isInvalid) "InvalidSkolem" else "Skolem"
      s"$prefix(${p(s.name)}${"'".repeat(s.level.toInt)})"
    case t: Type.UnleveledType => s"${t.toString}<${t.span}>"

    case t: Type.Named if t.args.isEmpty => s"Named(${p(t.name)})<${t.span}>"
    case t: Type.Named =>
      s"Named(${p(t.name)}<${t.args.map(p).mkString(", ")}>)<${t.span}>"

    case Type.Function(params, None, ret, span) =>
      s"((${params
          .map {
            case (None, v)       => p(v)
            case (Some(name), v) => s"${p(name)}: ${p(v)}"
          }
          .mkString(", ")}) => ${p(ret)})<$span>"
    case Type.Function(params, Some(variadic), ret, span) =>
      s"((${params
          .map {
            case (None, v)       => p(v)
            case (Some(name), v) => s"${p(name)}: ${p(v)}"
          }
          .mkString(", ")}), ...${p(variadic._2)} => ${p(ret)})<$span>"

    case Type.Interface(field, ret, span) =>
      s"(.$field: ${p(ret)}<$span>"

    case Type.Tuple(elems, span) =>
      s"${elems.map(p).mkString("[", ",", "]")}<$span>"
    case Type.Record(fields, None, span) =>
      s"${fields.map { case (k, v) => s"$k: ${p(v)}" }.mkString("{", ", ", "}")}<$span>"
    case Type.Record(fields, Some(wildcard), span) =>
      s"${fields.map { case (k, v) => s"$k: ${p(v)}" }.mkString("{", ", ", s", ...${p(wildcard)} }")}<$span>"

    case Type.Intersect(elems, span) =>
      s"(${elems.map(p).mkString(" & ")})<$span>"
    case Type.Union(elems, span) =>
      s"(${elems.map(p).mkString(" | ")})<$span>"
  }

  def p(name: Name): String = s"${name.str}<${name.span}>"

  "Typer.typeTExpr" should "work" in {
    checkTExpr(
      "Int => String",
      Type.Function(
        ArraySeq(None -> Type.Int(span(0, 3))),
        None,
        Type.Str(span(7, 13)),
        span(0, 13)))
  }

  it should "skolemize unknown types" in {
    checkTExpr(
      "A => A",
      Type.Function(
        ArraySeq(None -> Type.Skolem(Name("A", span(0, 1)), Type.Level.Zero)),
        None,
        Type.Skolem(Name("A", span(5, 6)), Type.Level.Zero),
        span(0, 6))
    )
  }

  it should "disallow generics when flag is set" in {
    checkTExpr(
      "Int => String",
      Type.Function(
        ArraySeq(None -> Type.Int(span(0, 3))),
        None,
        Type.Str(span(7, 13)),
        span(0, 13)),
      allowGenerics = false)

    checkTExprErr(
      "Int => A",
      Seq(TypeError.UnboundTypeVariable(span(7, 8), "A")),
      allowGenerics = false)
  }

  it should "disallow unbound generics" in {
    checkTExprErr("A => A", Seq.empty)
    checkTExprErr("Int => A", Seq(TypeError("Unknown type `A`", span(7, 8))))
    checkTExprErr("A => Int", Seq(TypeError("Unknown type `A`", span(0, 1))))

    checkTExprErr("(A => Int) => A", Seq(TypeError("Unknown type `A`", span(1, 2))))
    checkTExprErr("A => (A => Int)", Seq(TypeError("Unknown type `A`", span(0, 1))))

    checkTExprErr("(Int => A) => A", Seq.empty)
  }

  it should "warn on lowercase TS type typos" in {
    checkTExprWarn(
      "any",
      Seq(Warning("Unknown type `any`. Did you mean `Any`?", span(0, 3))))
    checkTExprWarn(
      "null",
      Seq(Warning("Unknown type `null`. Did you mean `Null`?", span(0, 4))))
    checkTExprWarn(
      "number",
      Seq(Warning("Unknown type `number`. Did you mean `Number`?", span(0, 6))))
    checkTExprWarn(
      "string",
      Seq(Warning("Unknown type `string`. Did you mean `String`?", span(0, 6))))
    checkTExprWarn(
      "boolean",
      Seq(Warning("Unknown type `boolean`. Did you mean `Boolean`?", span(0, 7))))
  }

  it should "report freevars correctly" in {
    parseTScheme("A | Int").raw.freeTypeVars shouldBe Set("A", "Int")
    parseTScheme("<A> A | Int").raw.freeTypeVars shouldBe Set("Int")

    parseTScheme(
      "2 | { a: A, b: B } | [C, D] | E & F | G"
    ).raw.freeTypeVars shouldBe Set("A", "B", "C", "D", "E", "F", "G")

    parseTScheme(
      "(A, B, ...C) => D"
    ).raw.freeTypeVars shouldBe Set("A", "B", "C", "D")

    parseTScheme(
      "{ foo: A, ... }"
    ).raw.freeTypeVars shouldBe Set("A")
  }

  // This also tests the parsing ¯\_(ツ)_/¯.
  it should "resolve nullable exprs to | Null types" in {
    // Basics.
    checkTExpr(
      "Int?",
      Type.Union(ArraySeq(Type.Int(span(0, 3)), Type.Null(span(3, 4))), span(0, 4))
    )
    checkTExpr(
      "Int ?",
      Type.Union(ArraySeq(Type.Int(span(0, 3)), Type.Null(span(4, 5))), span(0, 5))
    )
    checkTExpr(
      "Int??",
      Type.Union(
        ArraySeq(
          Type.Union(
            ArraySeq(
              Type.Int(span(0, 3)),
              Type.Null(span(3, 4))
            ),
            span(0, 4)),
          Type.Null(span(4, 5))
        ),
        span(0, 5))
    )

    // Unions and intersections.
    checkTExpr(
      "Int? | String",
      Type.Union(
        ArraySeq(
          Type.Union(
            ArraySeq(
              Type.Int(span(0, 3)),
              Type.Null(span(3, 4))
            ),
            span(0, 4)),
          Type.Str(span(7, 13))
        ),
        span(0, 13))
    )
    checkTExpr(
      "Int & String?",
      Type.Intersect(
        ArraySeq(
          Type.Int(span(0, 3)),
          Type.Union(
            ArraySeq(
              Type.Str(span(6, 12)),
              Type.Null(span(12, 13))
            ),
            span(6, 13))),
        span(0, 13))
    )

    // Functions.
    checkTExpr(
      "((String, Int) => Int)?",
      Type.Union(
        ArraySeq(
          Type.Function(
            ArraySeq(None -> Type.Str(span(2, 8)), None -> Type.Int(span(10, 13))),
            None,
            Type.Int(span(18, 21)),
            span(1, 21)
          ),
          Type.Null(span(22, 23))
        ),
        span(1, 23)
      )
    )
    checkTExpr(
      "(String, Int) => Int?",
      Type.Function(
        ArraySeq(None -> Type.Str(span(1, 7)), None -> Type.Int(span(9, 12))),
        None,
        Type.Union(
          ArraySeq(
            Type.Int(span(17, 20)),
            Type.Null(span(20, 21))
          ),
          span(17, 21)),
        span(0, 21)
      )
    )
    checkTExpr(
      "((String?, Int) => Int?)?",
      Type.Union(
        ArraySeq(
          Type.Function(
            ArraySeq(
              None -> Type.Union(
                ArraySeq(Type.Str(span(2, 8)), Type.Null(span(8, 9))),
                span(2, 9)),
              None -> Type.Int(span(11, 14))
            ),
            None,
            Type.Union(
              ArraySeq(Type.Int(span(19, 22)), Type.Null(span(22, 23))),
              span(19, 23)),
            span(1, 23)
          ),
          Type.Null(span(24, 25))
        ),
        span(1, 25)
      )
    )
  }
}
