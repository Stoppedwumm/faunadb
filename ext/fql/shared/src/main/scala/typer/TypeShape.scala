package fql.typer

import scala.collection.immutable.ArraySeq

/** Stores the definition structure of a type, including fields and apply
  * signatures, what the type aliases to, and constructor arity.
  *
  * FIXME: add constructor arity when we add explicit type annotation support.
  * FIXME: add constructor param co/contra-variance once that matters.
  */
case class TypeShape(
  self: TypeScheme,
  fields: Map[String, TypeScheme] = Map.empty,
  ops: Map[String, TypeScheme] = Map.empty,
  apply: Option[TypeScheme] = None,
  access: Option[TypeScheme] = None,
  alias: Option[TypeScheme] = None,
  isPersistable: Boolean = false,
  docType: TypeShape.DocType = TypeShape.DocType.None,
  typeHint: Option[TypeShape.TypeHint] = None
) {
  lazy val tparams: ArraySeq[String] =
    self match {
      case TypeScheme.Skolemized(Type.Named(_, _, args))
          if args.forall(_.isInstanceOf[Type.Skolem]) =>
        args.collect { case Type.Skolem(n, _) => n.str }
      case _ => ArraySeq.empty
    }

  // FIXME: prelim support for aliases of overloads
  def isUnionAlias = alias.fold(false)(_.raw.isInstanceOf[Type.Union])
  def isIntersectAlias = alias.fold(false)(_.raw.isInstanceOf[Type.Intersect])
}

object TypeShape {
  val empty =
    TypeShape(Type.Named("<empty>").typescheme, Map.empty, Map.empty, None, None)

  /** DocType distinguishes docs and nulldocs for special handling.
    * The collection name makes it possible to match doc and
    * null doc type shapes from the same collection.
    */
  sealed trait DocType {
    def col: String

    def isDoc: Boolean = this match {
      case DocType.Doc(_)                    => true
      case DocType.NullDoc(_) | DocType.None => false
    }
  }
  object DocType {
    object None extends DocType {
      def col = ""
    }
    case class Doc(col: String) extends DocType
    case class NullDoc(col: String) extends DocType
  }

  /** These hints allow us to provide contextual information to the user
    * while working on their queries. The main use case to start will be as
    * part of the intellisense/autocomplete support we provide.
    */
  sealed trait TypeHint
  object TypeHint {
    object ServerCollection extends TypeHint
    object UserCollection extends TypeHint
    object Module extends TypeHint
  }
}
