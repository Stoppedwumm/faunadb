package fql.typer

import fql.ast.{ Expr, Name, Span, TypeExpr }
import fql.ast.display._
import fql.error.{ Hint, TypeError, Warning }
import fql.Result
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{ LinkedHashSet, ListBuffer, Map => MMap }
import scala.concurrent.duration._
import scala.util.control.NonFatal

object Typer extends TyperEnvCompanion {

  type VarCtx = Map[String, TypeScheme]
  type TVarCtx = Map[String, Type]
  type ShapeCtx = Map[String, TypeShape]

  val HardcodedOps = Map(
    "==" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
    "!=" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
    ">" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
    "<" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
    ">=" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
    "<=" -> Type.Function((Type.Any, Type.Any) -> Type.Boolean).typescheme,
    "+" -> Type
      .Intersect(
        Type.Function((Type.Number, Type.Number) -> Type.Number),
        Type.Function((Type.Str, Type.Str) -> Type.Str))
      .typescheme
  )

  @volatile private[typer] var debugLogging = false
  def enableDebugLogging(): Unit = debugLogging = true
  def disableDebugLogging(): Unit = debugLogging = false
  def withDebugLogging[T](f: => T): T = {
    debugLogging = true
    try f
    finally { debugLogging = false }
  }

  def apply(globals: VarCtx = Map.empty, shapes: ShapeCtx = Map.empty) =
    new Typer(globals, Type.DefaultCoreShapes ++ shapes)

  def typeTSchemeUnchecked(sch: TypeExpr.Scheme): TypeScheme =
    Typer().typeTSchemeUnchecked(sch)

  def typeTSchemeUncheckedType(t: TypeExpr.Scheme): Type =
    Typer().typeTSchemeUncheckedType(t)
}

class Typer protected (val globals: Typer.VarCtx, val typeShapes: Typer.ShapeCtx)
    extends TyperAlts
    with TyperConstrain
    with TyperCompletions
    with TyperDebug
    with TyperEnv
    with TyperExprs
    with TyperExtrude
    with TyperFreshen
    with TyperSimplifier
    with TyperTypeExprs
    with TyperVars {

  override def toString = s"Typer($globals, $typeShapes)"

  val warnings = ListBuffer.empty[Warning]
  def emit(w: Warning): Unit = warnings += w

  var logException: Throwable => Unit = _ => ()

  // FIXME: clean up test override and remove this
  protected def hardcodedOps = Typer.HardcodedOps

  lazy val ops = {
    val m = MMap.empty[String, LinkedHashSet[Type]]
    typeShapes.foreach { case (_, shape) =>
      val self = shape.self match {
        case TypeScheme.Simple(ty)     => ty.asInstanceOf[Type]
        case TypeScheme.Skolemized(ty) => ty
        case ts                        => sys.error(s"$ts")
      }

      shape.ops.filter { case (op, _) => !Typer.HardcodedOps.contains(op) }.foreach {
        case (op, ts) =>
          val ty = ts match {
            case TypeScheme.Simple(ty)     => ty.asInstanceOf[Type]
            case TypeScheme.Skolemized(ty) => ty
            case ts                        => sys.error(s"$ts")
          }

          val tys = ty match {
            case Type.Intersect(vs, _) => vs
            case t                     => Seq(t)
          }

          tys.foreach { ty =>
            val fn = ty match {
              case Type.Function(ps, variadic, ret, span) =>
                Type.Function((None -> self) +: ps, variadic, ret, span)
              case ret => Type.Function(ArraySeq(None -> self), None, ret, ty.span)
            }

            m.getOrElseUpdate(op, LinkedHashSet.empty).add(fn)
          }
      }
    }

    m.view.mapValues { s =>
      if (s.sizeIs == 1) {
        s.head.typescheme
      } else {
        Type.Intersect(s.to(ArraySeq), Span.Null).typescheme
      }
    }.toMap ++ hardcodedOps
  }

  def withGlobals(vctx: Typer.VarCtx) = {
    val t = new Typer(globals.toMap ++ vctx, typeShapes)
    t.setVarStateFrom(this)
    t
  }

  def withShapes(shapes: Typer.ShapeCtx) = {
    val t = new Typer(globals, typeShapes.toMap ++ shapes)
    t.setVarStateFrom(this)
    t
  }

  protected def typeShape(name: Name) =
    typeShapes.getOrElse(name.str, TypeShape.empty)

  def withErrHandling[T](span: Span)(thnk: => Result[T]): Result[T] =
    try {
      thnk
    } catch {
      case _: StackOverflowError =>
        Result.Err(TypeError.StackOverflow(span))
      case NonFatal(e) =>
        logException(e)
        Result.Err(TypeError.UnknownInternalError(span))
    }

  def typeExpr(
    expr: Expr,
    closure: Typer.VarCtx = Map.empty,
    deadline: Deadline = Int.MaxValue.seconds.fromNow
  ): Result[Constraint.Value] =
    withErrHandling(expr.span) {
      val Typecheck(ty, fails) =
        typeExpr0(expr)(closure, rootAlt, Type.Level.Zero, deadline)

      if (dbgEnabled) {
        if (fails.nonEmpty) {
          dbg("FAILURES")
          fails.foreach(f => dbg(s"  $f"))
        }
      }

      if (fails.nonEmpty) {
        Result.Err(fails)
      } else {
        dbg("SUCCESS")
        val free = freeAlts(closure)(rootAlt)
        Result.Ok(annealValue(ty, rootAlt, Type.Level.Zero, free))
      }
    }

  // re-expose sig checking
  def typeAnnotatedExpr(e: Expr, sig: Type)(
    implicit deadline: Deadline): Typecheck[Unit] = {
    implicit val vctx: Typer.VarCtx = Map.empty
    implicit val containerAlt = rootAlt
    implicit val lvl = Type.Level.Zero
    for {
      e_ty <- typeExpr0(e)
      _    <- constrain(e_ty, sig, e.span)
    } yield ()
  }

  def typeTExpr(expr: TypeExpr): Result[TypeScheme] =
    typeTExprType(expr).map(_.typescheme)

  def typeTExprType(
    expr: TypeExpr,
    allowGenerics: Boolean = true,
    allowVariables: Boolean = true): Result[Type] =
    withErrHandling(expr.span) {
      val (ty, fails) =
        typeTExpr0(expr, checked = true, allowGenerics, allowVariables)(
          Map.empty,
          rootAlt,
          Type.Level.Zero,
          pol = true)

      if (fails.nonEmpty) {
        Result.Err(fails)
      } else {
        Result.Ok(ty)
      }
    }

  def typeTSchemeUnchecked(sch: TypeExpr.Scheme): TypeScheme =
    typeTSchemeUncheckedType(sch).typescheme

  def typeTSchemeUncheckedType(sch: TypeExpr.Scheme): Type = {
    val tctx =
      sch.params
        .map(n => n -> Type.Skolem(Name(n, Span.Null), Type.Level.Zero))
        .toMap
    val (ty, errs) = typeTExpr0(
      sch.expr,
      checked = false,
      allowGenerics = false,
      allowVariables = false
    )(tctx, rootAlt, Type.Level.Zero, pol = true)

    if (errs.nonEmpty) {
      throw new IllegalStateException(
        s"typeTSchemeUnchecked unexpectedly failed $errs")
    }

    ty
  }

  protected def instantiateValue(
    scheme: TypeScheme,
    args: Iterable[(String, Constraint)] = Nil)(
    implicit lvl: Type.Level): Constraint.Value =
    scheme match {
      case TypeScheme.Simple(v) => v

      case TypeScheme.Skolemized(t) =>
        // FIXME: in what cases do we not have the root alt here?
        deskolemize(t, args)(rootAlt, lvl)

      case TypeScheme.Polymorphic(v, level) =>
        require(args.isEmpty)

        // FIXME: in what cases do we not have the root alt here?
        freshenV(level, v)(rootAlt, lvl, MMap.empty, MMap.empty)
    }

  protected def instantiateUse(
    scheme: TypeScheme,
    args: Iterable[(String, Constraint)])(implicit lvl: Type.Level): Constraint.Use =
    scheme match {
      case TypeScheme.Simple(v: Type) => v
      case TypeScheme.Skolemized(t)   =>
        // FIXME: in what cases do we not have the root alt here?
        deskolemize(t, args)(rootAlt, lvl)
      case _ =>
        throw new IllegalStateException(
          "Cannot instantiate non-type Constraints for use positions")
    }

  protected def deskolemize(ty: Type, args: Iterable[(String, Constraint)])(
    implicit containerAlt: Alt,
    lvl: Type.Level): Type = {

    val tcache = MMap.empty[String, Type]

    args.foreach {
      case (name, ty: Type) => tcache(name) = ty
      case (name, ty) =>
        val v = freshVar(ty.span)
        // we match twice since `cons` may be both a value and use type.
        ty match {
          case ty: Constraint.Value => v.addValue(ty, containerAlt)
          case _                    =>
        }
        ty match {
          case ty: Constraint.Use => v.addUse(ty, containerAlt)
          case _                  =>
        }
        tcache(name) = v
    }

    def deskol0(ty: Type): Type =
      ty match {
        case Type.Skolem(name, _) =>
          tcache.getOrElseUpdate(name.str, freshVar(name.span))

        case t @ (_: Type.Var | _: Type.Any | _: Type.Top | _: Type.Never |
            _: Type.Singleton) =>
          t

        case c: Type.Named => c.copy(args = c.args.map(deskol0))
        case f: Type.Function =>
          f.copy(
            params = f.params.map(p => p._1 -> deskol0(p._2)),
            variadic = f.variadic.map(v => v._1 -> deskol0(v._2)),
            ret = deskol0(f.ret))
        case u: Type.Union     => u.copy(variants = u.variants.map(deskol0))
        case i: Type.Intersect => i.copy(variants = i.variants.map(deskol0))

        case i: Type.Interface => i.copy(ret = deskol0(i.ret))
        case r: Type.Record =>
          r.copy(
            fields = r.fields.map { case (n, t) => (n, deskol0(t)) },
            wildcard = r.wildcard.map(deskol0))

        case t: Type.Tuple => t.copy(elems = t.elems.map(deskol0))
      }

    deskol0(ty)
  }

  private def isTypeShapePersistable(name: String): Boolean =
    typeShapes.get(name).fold(false) { _.isPersistable }

  private def useRefError(ty: Type, col: String) = {
    TypeError(
      s"Type $ty is not persistable",
      ty.span,
      Nil,
      Seq(
        Hint(
          s"Use Ref<$col> as the type of a reference to a $col document",
          ty.span,
          Some(s"Ref<$col>")))
    )
  }

  def checkPersistability(ty: Type): Seq[TypeError] = {
    lazy val tyErr = TypeError(s"Type ${ty.display} is not persistable", ty.span)
    val errs = Seq.newBuilder[TypeError]
    ty match {
      case _: Type.Any | _: Type.Singleton => ()
      case _: Type.Function | _: Type.Interface | _: Type.Top | _: Type.Never |
          _: Type.Var | _: Type.Skolem =>
        errs += tyErr
      case Type.Intersect(variants, _) =>
        val varErrs = variants map checkPersistability
        if (varErrs forall { _.nonEmpty }) {
          errs ++= varErrs.flatten
        }
      case Type.Named(Name(Type.Ref.name, _), _, _) =>
      // Refs are persistable even though the parameter type is not.
      // It's kind of the point.
      case Type.Named(name, _, args) =>
        if (!isTypeShapePersistable(name.str)) {
          // In case this is a reference type, try to produce a better error.
          typeShapes.get(name.str) match {
            case Some(ts) if ts.docType != TypeShape.DocType.None =>
              errs += useRefError(ty, ts.docType.col)
            case _ =>
              errs += tyErr
          }
        } else {
          errs ++= args flatMap checkPersistability
        }
      case Type.Record(fields, wildcard, _) =>
        errs ++= (fields.values ++ wildcard) flatMap checkPersistability
      case Type.Tuple(elems, _) =>
        errs ++= elems flatMap checkPersistability
      case Type.Union(variants, _) =>
        // Unions handle doc types specially. Doc and nulldocs types are not
        // persistable, but the type (Doc | NullDoc) is, as is
        // Doc | EmptyRef<Doc>.
        val (docVariants, otherVariants) = variants.partition {
          case Type.Named(Name(Type.EmptyRef.name, _), _, _) =>
            true
          case Type.Named(Name(str, _), _, _) =>
            typeShapes.get(str).fold(false) { ts =>
              ts.docType != TypeShape.DocType.None
            }
          case _ => false
        }
        errs ++= otherVariants flatMap checkPersistability

        // Attempt to pair Doc with NullDoc or EmptyRef<Doc> in the union.
        // Everyone needs (at least one) partner.
        val shapes = docVariants.collect {
          case tt @ Type.Named(Name(str, _), _, _) =>
            typeShapes.get(str) map { (tt, _) }
        }.flatten
        val docShapes = shapes.collect {
          case p @ (_, ts) if ts.docType.isInstanceOf[TypeShape.DocType.Doc] => p
        }
        val nullDocShapes = shapes.collect {
          case p @ (_, ts) if ts.docType.isInstanceOf[TypeShape.DocType.NullDoc] => p
        }
        val docCols = docShapes.map { case (tt, ts) =>
          ts.docType.col -> tt
        }.toMap
        val nullDocCols = nullDocShapes.map { case (tt, ts) =>
          ts.docType.col -> tt
        }
        val eRefCols = docVariants.flatMap {
          case tt @ Type.EmptyRef(Type.Named(Name(col, _), _, _), _) =>
            Option.when {
              typeShapes.get(col).exists {
                _.docType.isInstanceOf[TypeShape.DocType.Doc]
              }
            }(col -> tt)
          case _ => None
        }
        val nullCols = (nullDocCols ++ eRefCols).toMap
        (docCols -- nullCols.keySet) foreach { case (col, tt) =>
          errs += useRefError(tt, col)
        }
        (nullCols -- docCols.keySet) foreach { case (col, tt) =>
          errs += useRefError(tt, col)
        }

        // Docs and NullDocs don't have parameters, but let's check anyway.
        // EmptyRef is special and we don't care that its doc parameter is not
        // persistable.
        errs ++= docVariants flatMap {
          case Type.Named(Name(Type.EmptyRef.name, _), _, _) => Seq.empty
          case Type.Named(_, _, args) => args flatMap checkPersistability
          case _                      => Seq.empty // There aren't any of these.
        }
    }

    errs.result()
  }
}
