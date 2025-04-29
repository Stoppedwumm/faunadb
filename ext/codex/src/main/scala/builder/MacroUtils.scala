package fauna.codex.builder

import scala.reflect.macros._

object MacroUtils {
  val TupleClasses = 2 to 22 flatMap { i => List(s"Tuple$i", s"scala.this.Tuple$i", s"scala.Tuple$i") } toSet

  def isTupleClass(T: Universe#Type) = TupleClasses contains T.typeConstructor.toString

  def isCaseClass(T: Universe#Type) = T.typeSymbol.asClass.isCaseClass
}

import MacroUtils._

trait MacroUtils {
  val c: whitebox.Context

  import c.universe._

  protected def eval[T](expr: Tree) = c.eval(c.Expr[T](c.untypecheck(expr.duplicate)))
  protected def typed(expr: Tree) = c.typecheck(expr, c.TYPEmode, silent = false).tpe

  protected def inferImplicit(tpe: Type) =
    try {
      c.inferImplicitValue(tpe, silent = false)
    } catch {
      case _: TypecheckException =>
        c.abort(c.enclosingPosition, s"could not find implicit value of type $tpe")
    }

  // Module basics

  protected def M = c.prefix.tree

  case class FieldInfo(tpe: Type, name: String, default: Option[Symbol]) {
    def getter = TermName(name)
    def hasDefault = default.isDefined
    def encoder = inferImplicit(typed(tq"$M.Encoder[$tpe]"))
    def decoder = inferImplicit(typed(tq"$M.Decoder[$tpe]"))
    def codec = inferImplicit(typed(tq"$M.Codec[$tpe]"))
  }

  object TypeInfo {
    def apply(T: Type) =
      if (isTupleClass(T)) {
        TupleInfo(T.dealias)
      } else if (isCaseClass(T)) {
        CaseClassInfo(T.dealias)
      } else {
        c.abort(c.enclosingPosition, s"Not a tuple or case class type: $T")
      }
  }

  abstract class TypeInfo {
    val T: Type
    def fields: List[FieldInfo]
    def construct(args: List[Tree]): Tree
    def hasDefault: Boolean = fields exists { _.hasDefault }

    lazy val arity = fields.size
    lazy val minArity = fields filterNot { _.hasDefault } size
  }

  case class TupleInfo(T: Type) extends TypeInfo {
    if (!isTupleClass(T)) {
      c.abort(c.enclosingPosition, s"Not a tuple type: $T")
    }

    def construct(args: List[Tree]) = q"(..$args)"

    val fields = {
      val ts = T.typeArgs
      ts zip (1 to ts.size) map { case (t, i) => FieldInfo(t, s"_$i", None) }
    }
  }

  case class CaseClassInfo(T: Type) extends TypeInfo {
    if (!isCaseClass(T)) {
      c.abort(c.enclosingPosition, s"Not a case class: $T")
    }

    def construct(args: List[Tree]) = q"new $T(..$args)"

    val fields = {
      val typeParams = T.typeConstructor.typeParams
      val typeArgs = T.typeArgs

      val ctor = T.decl(termNames.CONSTRUCTOR).asInstanceOf[MethodSymbol]

      if (ctor.paramLists.size != 1) {
        c.abort(c.enclosingPosition,
          s"Unsupported: Constructor for case class $T has more than one param list.")
      }

      val params = ctor.paramLists.head

      params zip (1 to params.size) map {
        case (p, i) =>
          val default = if (p.asTerm.isParamWithDefault) {
            Some(T.companion.decl(TermName("$lessinit$greater$default$" + i)))
          } else {
            None
          }

          val typeSig = p.typeSignature.substituteTypes(typeParams, typeArgs)
          FieldInfo(typeSig, p.name.toString, default)
      }
    }
  }
}
