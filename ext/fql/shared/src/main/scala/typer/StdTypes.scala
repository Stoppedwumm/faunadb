package fql.typer

import fql.ast.{ Name, Span }
import scala.collection.immutable.{ ArraySeq, SeqMap }

/** Module containing helpers for core types required for typer functionality.
  */
trait CoreTypes {
  import Type.{ Level, Named, Skolem }

  // These are used by the typer as a fallback for required types which are not
  // passed in as part of the typer env.
  lazy val DefaultCoreShapes = {
    val scalars = Seq(Null, Boolean, Number, Int, Long, Double, Str)
    val containers = Seq(
      Array(Param.A),
      Set(Param.A),
      EventSource(Param.A),
      Ref(Param.A),
      EmptyRef(Param.A),
      NamedRef(Param.A),
      EmptyNamedRef(Param.A))

    (scalars ++ containers)
      .map(t => t.name.str -> TypeShape(self = t.typescheme))
      .toMap
  }

  val Null: Type.Named = Null(Span.Null)
  def Null(span: Span) = Named(Name("Null", span), span)

  val Boolean: Type.Named = Boolean(Span.Null)
  def Boolean(span: Span) = Named(Name("Boolean", span), span)

  val Number: Type.Named = Number(Span.Null)
  def Number(span: Span) = Named(Name("Number", span), span)

  val Int: Type.Named = Int(Span.Null)
  def Int(span: Span) = Named(Name("Int", span), span)

  val Long: Type.Named = Long(Span.Null)
  def Long(span: Span) = Named(Name("Long", span), span)

  val Double: Type.Named = Double(Span.Null)
  def Double(span: Span) = Named(Name("Double", span), span)

  val Float: Type.Named = Float(Span.Null)
  def Float(span: Span) = Named(Name("Float", span), span)

  val Str: Type.Named = Str(Span.Null)
  def Str(span: Span) = Named(Name("String", span), span)

  object Array {
    val name = "Array"
    def apply(elems: Type): Type.Named = Array(elems, Span.Null)
    def apply(elems: Type, span: Span) =
      Named(Name(name, span), span, ArraySeq(elems))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }
  object Set {
    val name = "Set"
    def apply(elems: Type): Type.Named = Set(elems, Span.Null)
    def apply(elems: Type, span: Span = Span.Null) =
      Named(Name(name, span), span, ArraySeq(elems))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }
  object EventSource {
    val name = "EventSource"
    def apply(elems: Type): Type.Named = EventSource(elems, Span.Null)
    def apply(elems: Type, span: Span) =
      Named(Name(name, span), span, ArraySeq(elems))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }

  // A dangling reference to a non-existing document of type `docType`.
  object EmptyRef {
    val name = "EmptyRef"
    def apply(docType: Type): Type.Named = EmptyRef(docType, Span.Null)
    def apply(docType: Type, span: Span) =
      Named(Name(name, span), span, ArraySeq(docType))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }

  // A reference to a document of type `docType`. By type shape aliasing,
  // Ref<D> is equivalent to D | EmptyRef<D>.
  object Ref {
    val name = "Ref"
    def apply(docType: Type): Type.Named = Ref(docType, Span.Null)
    def apply(docType: Type, span: Span) =
      Named(Name(name, span), span, ArraySeq(docType))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }

  // A dangling reference to a non-existing document of type `docType`,
  // for named docs.
  object EmptyNamedRef {
    val name = "EmptyNamedRef"
    def apply(docType: Type): Type.Named = EmptyNamedRef(docType, Span.Null)
    def apply(docType: Type, span: Span) =
      Named(Name(name, span), span, ArraySeq(docType))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }

  // A reference to a document of type `docType`, for named docs.
  object NamedRef {
    val name = "NamedRef"
    def apply(docType: Type): Type.Named = NamedRef(docType, Span.Null)
    def apply(docType: Type, span: Span) =
      Named(Name(name, span), span, ArraySeq(docType))
    def unapply(ty: Type): Option[(Type, Span)] = Some(ty) collect {
      case t: Named if (t.name.str == name) => (t.args.head, t.span)
    }
  }

  /** Params for internally defined type sigs.
    *
    * By convention Type.Param members are used as so: Constructor params start
    * with A and method params continue where they left off.
    *
    * For example, array map() has the signature (A => B) => B[]. `A` will be
    * instantiated with the provided arg, and `B` will be instantiated with a
    * fresh variable.
    */
  object Param {
    val A = Skolem(Name("A", Span.Null), Level.Zero)
    val B = Skolem(Name("B", Span.Null), Level.Zero)
    val C = Skolem(Name("C", Span.Null), Level.Zero)
    val D = Skolem(Name("D", Span.Null), Level.Zero)
    val E = Skolem(Name("E", Span.Null), Level.Zero)
    val F = Skolem(Name("F", Span.Null), Level.Zero)
    val G = Skolem(Name("G", Span.Null), Level.Zero)
  }
}

/** Module containing helpers for standard types.
  *
  * FIXME: clean this up and move all helpers which are not strictly
  * necessary for typer internals to ext/model.
  */
trait StdTypes {
  import Type.{ Named, Null, Union }

  def Optional(ty: Type): Union = Union(ty, Null)

  // other primitive scalars
  val ID = Named("ID")
  val Time = Named("Time")
  val Bytes = Named("Bytes")
  val TransactionTime = Named("TransactionTime")
  val Date = Named("Date")
  val UUID = Named("UUID")
  val SetCursor = Named("SetCursor")

  val AnyRecord: Type.Record = AnyRecord(Span.Null)
  def AnyRecord(span: Span) =
    Type.Record(SeqMap.empty[String, Type], Some(Type.Any(span)), span)

  // TODO: Needs to actually be anydoc
  val AnyDoc: Type.Record = AnyRecord(Span.Null)
  val AnyNullDoc = Null(Span.Null)

  val AnyLambda =
    Type.Function(ArraySeq.empty, Some((None, Type.Any)), Type.Any, Span.Null)

  def WildRecord(wildcard: Type): Type.Record = WildRecord(wildcard, Span.Null)
  def WildRecord(wildcard: Type, span: Span): Type.Record =
    Type.Record(SeqMap.empty[String, Type], Some(wildcard), span)

}
