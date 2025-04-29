package fql.typer

import fql.ast.{ Expr, Literal, Span, TypeExpr }
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{ Map => MMap }

trait TyperCompletions { self: Typer =>

  var recordingSpans = false
  val typeSpans = MMap.empty[Span, Constraint.Value]

  protected def visit(span: Span, v: Constraint.Value) = {
    if (recordingSpans) {
      typeSpans.put(span, v)
    }
  }

  protected def visitExpr(expr: Expr, tc: Typecheck[Constraint.Value]) = {
    visit(expr.span, tc.value)
  }

  protected def visitMethodChain(
    mc: Expr.MethodChain.Component,
    tc: Typecheck[Constraint.Value]) = {
    visit(mc.span, tc.value)
  }

  def isSubtype(a: Constraint.Value, b: Type): Boolean =
    canConstrain(a, b)(rootAlt)

  def paramTypesOf(v: Constraint.Value): Option[Seq[TypeExpr]] =
    paramTypesOf(valueToTypeExpr(v))

  def paramTypesOf(te: TypeExpr): Option[Seq[TypeExpr]] = {
    te match {
      case TypeExpr.Lambda(args, _, _, _) => Some(args.map(_._2))
      case TypeExpr.Intersect(vs, span)   =>
        // this transforms an intersection around. for example:
        // (A, B) => C & (A, D) => C & (E) => C & "hello"
        // this flatMap returns
        // Seq(Seq(A, B), Seq(A, D), Seq(E))
        val variants = vs.flatMap(paramTypesOf(_))
        // this flips it around to
        // Seq(Seq(A, A, E), Seq(B, D))
        val args = (0 until variants.map(_.length).max).map { i =>
          val argVariants = variants.flatMap { _.lift(i) }.distinct
          if (argVariants.length == 1) {
            argVariants.head
          } else {
            TypeExpr.Union(argVariants, span)
          }
        }
        Some(args)
      case _ => None
    }
  }

  def valueToTypeExpr(v: Constraint.Value): TypeExpr =
    simplifyValue(v).toValueExpr(typeShapes)

  def typeAt(span: Span): Option[Constraint.Value] = typeSpans.get(span)

  def fieldsOfValue(v: Constraint.Value): Map[String, Constraint.Value] =
    fieldsOfTypeExpr(valueToTypeExpr(v))

  def projectionFieldsOfValue(v: Constraint.Value): Map[String, Constraint.Value] =
    projectionFieldsOfTypeExpr(valueToTypeExpr(v))

  private def typeExprToType(te: TypeExpr): Type = {
    val (ty, _) =
      typeTExpr0(te, checked = false, allowGenerics = true, allowVariables = true)(
        Map.empty,
        rootAlt,
        Type.Level.Zero,
        pol = true)
    ty
  }

  def fieldsOfTypeExpr(te: TypeExpr): Map[String, Constraint.Value] = te match {
    case TypeExpr.Singleton(Literal.Null, _)   => Map.empty
    case TypeExpr.Singleton(Literal.True, _)   => fieldsOfName(Type.Boolean.name.str)
    case TypeExpr.Singleton(Literal.False, _)  => fieldsOfName(Type.Boolean.name.str)
    case TypeExpr.Singleton(Literal.Int(_), _) => fieldsOfName(Type.Number.name.str)
    case TypeExpr.Singleton(Literal.Float(_), _) =>
      fieldsOfName(Type.Number.name.str)
    case TypeExpr.Singleton(Literal.Str(_), _) =>
      fieldsOfName(Type.Str.name.str)

    case TypeExpr.Id(name, _) => fieldsOfName(name)
    case TypeExpr.Cons(name, targs, _) =>
      fieldsOfName(name.str, targs.map(typeExprToType))

    case TypeExpr.Object(fields, _, _) =>
      fields.toMap.map { case (k, v) => k.str -> typeExprToType(v) }
    case TypeExpr.Interface(fields, _) =>
      fields.toMap.map { case (k, v) => k.str -> typeExprToType(v) }
    case TypeExpr.Projection(_, ret, _) =>
      fieldsOfTypeExpr(ret)

    case TypeExpr.Tuple(fields, sp) =>
      fieldsOfName(
        Type.Array.name,
        Seq(Type.Union(fields.map(typeExprToType).to(ArraySeq), sp)))

    case TypeExpr.Intersect(types, _) => types.flatMap(fieldsOfTypeExpr).toMap
    case TypeExpr.Union(types, _) =>
      val allFields = types.map(fieldsOfTypeExpr)
      // get every field name on every variant
      val allFieldNames = allFields.flatMap(_.map(_._1)).toSet

      allFieldNames.map { field =>
        // find the type of this field on each type
        val fieldTypes = allFields.flatMap(_.find(_._1 == field).map(_._2))
        // the field is defined on all variants of the union, so now build a union
        // of all those fields.
        val v = freshVar(Span.Null)(Type.Level.Zero)
        fieldTypes.foreach { ty => v.addValue(ty, rootAlt) }
        field -> v
      }.toMap

    case TypeExpr.Difference(elem, sub, _) =>
      fieldsOfTypeExpr(elem) -- fieldsOfTypeExpr(sub).keys

    case _ => Map.empty
  }

  def projectionFieldsOfTypeExpr(te: TypeExpr): Map[String, Constraint.Value] =
    te match {
      case TypeExpr.Object(fields, _, _) =>
        fields.toMap.map { case (k, v) => k.str -> typeExprToType(v) }

      case _ => Map.empty
    }

  private def fieldsOfName(
    name: String,
    args: Seq[Constraint.Value] = Nil): Map[String, Constraint.Value] =
    typeShapes.get(name) match {
      case Some(shape) =>
        shape.fields.map { case (k, v) =>
          k -> instantiateValue(v, shape.tparams.zip(args))(Type.Level.Zero)
        } ++ shape.alias
          .map { alias => fieldsOfValue(alias.raw) }
          .getOrElse(Map.empty)
      case None => Map.empty
    }
}
