package fql.test

import fql.{ Result, TextUtil }
import fql.ast.{ Literal, Name, SchemaTypeExpr, Span, Src, TypeExpr }
import fql.ast.display._
import fql.env.{ CollectionTypeInfo, TypeEnv }
import fql.parser.Parser
import fql.typer.{
  CoalescedType,
  Constraint,
  Type,
  TypeScheme,
  TypeShape,
  Typer,
  TyperDebug
}
import org.scalactic.source.Position
import org.scalatest.{ BeforeAndAfter, Inside, OptionValues }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.immutable.{ ArraySeq, SeqMap }

trait Spec
    extends AnyFlatSpec
    with Matchers
    with Inside
    with OptionValues
    with BeforeAndAfter

trait TypeSpec extends Spec {
  import Result.{ Err, Ok }
  import TypeSpec.{ stdlibGlobals, stdlibShapes, testGlobals, testShapes }

  class TestTyper(
    override val globals: Typer.VarCtx,
    override val typeShapes: Typer.ShapeCtx)
      extends Typer(globals, typeShapes) {
    // everything is `int` in fql/ tests, so we override `+`.
    override val hardcodedOps = Map(
      "==" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
      "!=" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
      ">" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
      "<" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
      ">=" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
      "<=" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
      "+" -> Type
        .Intersect(
          Type.Function((Type.Int, Type.Int) -> Type.Int),
          Type.Function((Type.Str, Type.Str) -> Type.Str))
        .typescheme
    )
  }

  def StdlibEnv() = new TypeEnv(stdlibGlobals, stdlibShapes)
  def TestTyper() = new TestTyper(testGlobals, testShapes)

  private def typecheck0(
    query: String): Either[String, (Constraint.Value, CoalescedType, TypeExpr)] = {
    val sourceContext = Map.empty[Src.Id, String]

    val expr = Parser.expr(query, Src.Query(query)) match {
      case Ok(e) => e
      case Err(errs) =>
        return Left(errs.map(_.renderWithSource(sourceContext)).mkString("\n\n"))
    }

    // FIXME: use a builder or something.
    val typer = TestTyper()
    typer.typeExpr(expr) match {
      case Ok(ty) =>
        val ct = typer.simplifyValue(ty)
        val te = ct.toValueExpr(typer.typeShapes)
        Right((ty, ct, te))
      case Err(errs) =>
        Left(errs.map(_.renderWithSource(sourceContext)).mkString("\n\n"))
    }
  }

  def parse[E](parser: String => Result[E], expr: String)(
    implicit pos: Position): E =
    parser(expr) match {
      case Ok(e) => e
      case Err(errs) =>
        val ctx = Map(Src.Null -> expr)
        val msg = errs.map(_.renderWithSource(ctx)).mkString("\n\n")
        fail(s"Parse unexpectedly failed with\n$msg")
    }

  def parseExpr(expr: String, src: Src = Src.Query(""))(implicit pos: Position) =
    parse(Parser.expr(_, src), expr)
  def parseTExpr(texpr: String, src: Src = Src.Query(""))(implicit pos: Position) =
    parse(Parser.typeExpr(_, src), texpr)

  def typecheck(query: String): Unit =
    Typer.withDebugLogging {
      typecheck0(query) match {
        case Right((ty, ct, te)) =>
          val bounds = TyperDebug.getBounds(ty)
          println(s"TYPE: $ty")
          bounds.foreach { case (v, (values, uses)) =>
            val vs =
              if (values.isEmpty) ""
              else {
                values
                  .map { case (v, a) => s"$v$a" }
                  .mkString(" :> ", " | ", "")
              }
            val us =
              if (uses.isEmpty) ""
              else {
                uses
                  .map { case (u, a) => s"$u$a" }
                  .mkString(" <: ", " & ", "")
              }
            println(s"  $v$vs$us")
          }
          println(s"Coalesced: $ct")
          println(s"TYPE: ${te.display}")
        case Left(err) =>
          println(err)
      }
    }

  def typecheck(query: String, expected: String)(implicit pos: Position): Unit = {
    typecheck0(query) match {
      case Right((_, _, te)) =>
        if (expected == "ERROR") {
          val sb = new StringBuilder
          sb.append("Unexpected success (- is test, + is output):\n")
          TextUtil.printDiff(te.display, expected, sb)
        } else {
          te.display shouldEqual expected
        }
      case Left(err) =>
        if (expected == "ERROR") {
          ()
        } else {
          assertStringsEqual(err, expected)
        }
    }
    ()
  }

  def assertStringsEqual(
    actual: String,
    expected: String)(implicit pos: Position) = {
    if (actual != expected) {
      val sb = new StringBuilder
      sb.append("Unexpected result (- is test, + is output):\n")
      TextUtil.printDiff(actual, expected, sb)
      fail(sb.result())
    }
  }
}

object TypeSpec {
  import Type._

  val userColl = CollectionTypeInfo.Checked(
    name = Name("User", Span.Null),
    alias = None,
    hasDocTTL = false,
    defined = SeqMap(
      "name" -> (SchemaTypeExpr.Simple(TypeExpr.Id("String", Span.Null)), false)),
    computed = SeqMap.empty,
    wildcard = None,
    indexes = SeqMap.empty,
    typeHint = TypeShape.TypeHint.UserCollection
  )

  val stdlibGlobals = Map(
    "abort" -> Function(Any -> Never)
  ).view.mapValues(_.typescheme).toMap

  // TODO: port this over to TypeExprs once we have proper env support.
  val testGlobals = stdlibGlobals ++ Map(
    userColl.name.str -> userColl.collType,
    "Math" -> Type.Named("MathModule"),
    "anyval" -> Any,
    "intobj" -> Record.Wild(Int),
    "boolobj" -> Record.Wild(Boolean),
    "anyobj" -> Record.Wild(Any),
    "foobarwild" -> Record.Wild(Boolean, "foo" -> Str, "bar" -> Int),
    "add" -> Function((Int, Int) -> Int),
    "succ" -> Function(Int -> Int),
    "not" -> Function(Boolean -> Boolean),
    "reverse" -> Function(Str -> Str),
    "aFoo" -> Named("Foo"),
    "aMaybeInt" -> Named("MaybeInt"),
    "aliasIntAndBool" -> Named("IntAndBool"),
    "aliasIntAndStr" -> Named("IntAndStr"),
    "aliasStrAndBool" -> Named("StrAndBool"),
    "intIntBoolBoolCurried" -> {
      Intersect(
        Function(Int -> Function(Int -> Int)),
        Function(Boolean -> Function(Boolean -> Boolean))
      )
    },
    "intIntBoolBool" -> {
      Intersect(Function((Int, Int) -> Int), Function((Boolean, Boolean) -> Boolean))
    },
    "intIntBoolAny" -> {
      Intersect(Function((Int, Int) -> Int), Function((Boolean -> Any) -> Boolean))
    },
    "intBool" -> {
      Intersect(Function(Int -> Int), Function(Boolean -> Boolean))
    },
    "intStr" -> {
      Intersect(Function(Int -> Int), Function(Str -> Str))
    },
    "strBool" -> {
      Intersect(Function(Boolean -> Boolean), Function(Str -> Str))
    },
    "boolNull" -> {
      Intersect(Function(Boolean -> Boolean), Function(Null -> Null))
    },
    "intBoolStr" -> {
      Intersect(
        Function(Int -> Int),
        Function(Boolean -> Boolean),
        Function(Str -> Str))
    },
    "intBoolHOF" -> {
      Intersect(
        Function(
          Function(Int -> Int) ->
            Function(Int -> Int)),
        Function(
          Function(Boolean -> Boolean) ->
            Function(Boolean -> Boolean))
      )
    },
    "intBoolStrHOF" -> {
      Intersect(
        Function(
          Function(Int -> Int) ->
            Function(Int -> Int)),
        Function(
          Function(Boolean -> Boolean) ->
            Function(Boolean -> Boolean)),
        Function(
          Function(Str -> Str) ->
            Function(Str -> Str))
      )
    },
    "intBoolObj" -> {
      def rec(t: Type) = Record("a" -> t)
      Intersect(Function(rec(Int) -> Int), Function(rec(Boolean) -> Boolean))
    },
    "intOrIntObj" -> {
      def rec(t: Type) = Record("a" -> t)
      Intersect(Function(Int -> Int), Function(rec(Int) -> rec(Int)))
    },
    "intOrIntBoolObj" -> {
      def rec(t: Type) = Record("a" -> t)
      Intersect(
        Function(Int -> Int),
        Function(rec(Int) -> Int),
        Function(rec(Boolean) -> Boolean)
      )
    },
    "enumABC" -> {
      def lit(l: String) = Singleton(Literal.Str(l))
      val en = Union(lit("a"), lit("b"), lit("c"))
      Function(en -> Int)
    },
    "enumABCObj" -> {
      def lit(l: String) = Singleton(Literal.Str(l))
      def rec(l: String) = Record("x" -> lit(l))
      val en = Union(rec("a"), rec("b"), rec("c"))
      Function(en -> Int)
    },
    "intintint" -> {
      Function((Int, Int, Int) -> Int)
    },
    "gener" -> {
      Function((Param.A, Function(Param.A -> Param.B)) -> Param.B)
    },
    "intStrVarArg" -> {
      Function(ArraySeq(None -> Int), Some(None -> Str), Int, Span.Null)
    },
    "MySet" -> Set(userColl.docType),
    "takeaunion" -> {
      Function(Union(Int, Str) -> Any)
    },
    "objectFromEntries" -> {
      Function(
        Array(Tuple(Str, Param.A)) ->
          Record.Wild(Param.A))
    },
    "gimmeAandB" -> {
      Function(Record("a" -> Int, "b" -> Str) -> Int)
    },
    "gimmeAandwildstr" -> {
      Function(Record.Wild(Str, "a" -> Int) -> Int)
    },
    "gimmewildint" -> {
      Function(Record.Wild(Int) -> Int)
    },
    "gimmeoptA" -> {
      Function(Record.Wild(Int, "a" -> Optional(Int)) -> Int)
    },
    "gimmebody" -> {
      Function(Record("body" -> Str) -> Int)
    },
    "createwildint" -> {
      Function(() -> Record.Wild(Int))
    },
    "createAandwildstr" -> {
      Function(() -> Record.Wild(Str, "a" -> Int))
    },
    "createAandwildint" -> {
      Function(() -> Record.Wild(Int, "a" -> Int))
    },
    "createAandwildany" -> {
      Function(() -> Record.Wild(Any, "a" -> Int))
    }
  ).view.mapValues(_.typescheme).toMap

  val stdlibShapes = Map(
    "Null" -> TypeShape(self = Null.typescheme, isPersistable = true),
    "Int" -> TypeShape(
      self = Int.typescheme,
      ops = Map(
        "+" -> Function(Int -> Int).typescheme,
        "-" -> Function(Int -> Int).typescheme,
        "*" -> Function(Int -> Int).typescheme
      ),
      isPersistable = true
    ),
    "Boolean" -> TypeShape(self = Boolean.typescheme, isPersistable = true),
    "String" -> TypeShape(
      self = Str.typescheme,
      ops = Map("+" -> Function(Any -> Str).typescheme),
      access = Some(Function(Int -> Str).typescheme),
      fields = Map(
        "length" -> Int.typescheme,
        "concat" -> Function(Str -> Str).typescheme
      ),
      isPersistable = true
    ),
    "Set" -> TypeShape(
      self = Set(Param.A).typescheme,
      fields = Map(
        "toStream" -> Function(() -> EventSource(Param.A)).typescheme,
        "count" -> Function(() -> Type.Int).typescheme
      )
    ),
    "MathModule" -> TypeShape(
      self = Type.Named("MathModule").typescheme,
      fields = Map(
        "sum" -> Intersect(
          Function(Array(Type.Int) -> Type.Int),
          Function(Set(Type.Int) -> Type.Int)).typescheme
      )
    ),
    "Array" -> TypeShape(
      self = Array(Param.A).typescheme,
      access = Some(Function(Int -> Param.A).typescheme),
      fields = Map(
        "length" -> Type.Int.typescheme,
        "map" -> {
          Function(Function(Param.A -> Param.B) -> Array(Param.B)).typescheme
        },
        // This is a simpler version of concat, which requires the same type. on
        // the left and right arrays.
        "concat" -> {
          Function(Array(Param.B) -> Array(Union(Param.A, Param.B))).typescheme
        },
        "fold" -> {
          Function(
            (Param.B, Function((Param.B, Param.A) -> Param.B)) -> Param.B).typescheme
        },
        "reduce" -> {
          Function(
            Function((Param.A, Param.A) -> Param.A) -> Optional(Param.A)).typescheme
        },
        "flatMap" -> {
          Function(Function(Param.A -> Array(Param.B)) -> Array(Param.B)).typescheme
        },
        "order" -> {
          val accessorType = Function(Param.A -> Any)
          val ascType = Record("asc" -> accessorType)
          val descType = Record("desc" -> accessorType)
          val orderingType = Union(ascType, descType, accessorType)

          Function(orderingType -> Array(Param.A)).typescheme
        }
      ),
      isPersistable = true
    ),
    "Record" -> TypeShape(
      self = Record.Wild(Any).typescheme,
      ops = Map("[]" -> Function(Str -> Any).typescheme),
      isPersistable = true
    ),
    // Ref types
    Type.Ref.name -> TypeShape(
      self = Type.Ref(Param.A).typescheme,
      isPersistable = true,
      alias = Some(Type.Union(Param.A, Type.EmptyRef(Param.A)).typescheme)
    ),
    Type.EmptyRef.name -> TypeShape(
      self = Type.EmptyRef(Param.A).typescheme,
      fields = Map(
        "id" -> Type.ID.typescheme,
        "coll" -> Type.Any.typescheme,
        "exists" -> Function(() -> Singleton(Literal.False)).typescheme
      ),
      isPersistable = false,
      alias = Some(Type.Null.typescheme)
    )
  )

  val testShapes = stdlibShapes ++ Map(
    "IntAndBool" -> TypeShape(
      self = Named("IntAndBool").typescheme,
      alias = Some(Intersect(Int, Boolean).typescheme)
    ),
    "IntAndStr" -> TypeShape(
      self = Named("IntAndStr").typescheme,
      alias = Some(Intersect(Int, Str).typescheme)
    ),
    "StrAndBool" -> TypeShape(
      self = Named("StrAndBool").typescheme,
      alias = Some(Intersect(Str, Boolean).typescheme)
    ),
    "IntOrBool" -> TypeShape(
      self = Named("IntOrBool").typescheme,
      alias = Some(Union(Int, Boolean).typescheme)
    ),
    "IntOrStr" -> TypeShape(
      self = Named("IntOrStr").typescheme,
      alias = Some(Union(Int, Str).typescheme)
    ),
    "StrOrBool" -> TypeShape(
      self = Named("StrOrBool").typescheme,
      alias = Some(Union(Str, Boolean).typescheme)
    ),

    // An alias
    "Foo" -> TypeShape(
      self = Named("Foo").typescheme,
      alias = Some(TypeScheme.Simple(Str))),
    // nullish alias
    "MaybeInt" -> TypeShape(
      self = Named("MaybeInt").typescheme,
      alias = Some(TypeScheme.Simple(Optional(Int)))),

    // A collection
    userColl.collName.str -> userColl.collShape,

    // A document. Attempt to mirror the current ref chaos
    userColl.docName.str -> userColl.docShape,

    // A null User
    userColl.nullDocName.str -> userColl.nullDocShape
  )
}
