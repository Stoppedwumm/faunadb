package fauna.model.runtime.fql2

import fauna.repo.values._
import fql.ast.{ Literal, Name, Span }
import fql.typer.Type
import scala.collection.immutable.ArraySeq
import scala.language.implicitConversions

/** A `TypeTag` is the glue that ties together:
  *   1. Static types in `fql.typer.Type`
  *   2. ValueTypes in `repo.values.ValueType`, and the corresponding subtypes
  *      `V <: Value` (i.e. `Value.Int`, `Value.Array`, etc.).
  *   3. FQL2 function implementations in `model.runtime.fql2`
  * `TypeTag` provides the facility that casts a `Value` to the appropriate
  * subtype when used as a function argument.
  */
sealed abstract class TypeTag[V <: Value](
  val staticType: Type,
  val valueType: ValueType { type Repr >: V }) {
  def cast(v: Value): Option[V]
  def displayString: String = valueType.displayString
  def name: String = staticType match {
    case Type.Named(name, _, _) => name.str

    // FIXME: This shouldn't show up in the environment.
    case Type.Record(_, _, _) => "object"

    case _ => throw new IllegalStateException("unnamed runtime type")
  }
}

object TypeTag {
  // When constructing any subtype of `TypeTag`, a `TypeTag` may be used
  // in place of a `fql.typer.Type`.
  implicit def runtimeTypeToStaticType(rt: TypeTag[_]): Type = rt.staticType

  case class Erased(t: Type) extends TypeTag[Value](t, ValueType.AnyType) {
    def cast(v: Value): Option[Value] = Some(v)
  }

  def Named(name: String) = Erased(Type.Named(name))

  val Any = Erased(Type.Any)
  val Never = Erased(Type.Never)

  // type params
  val A = Erased(Type.Param.A)
  val B = Erased(Type.Param.B)
  val C = Erased(Type.Param.C)
  val D = Erased(Type.Param.D)
  val E = Erased(Type.Param.E)
  val F = Erased(Type.Param.F)
  val G = Erased(Type.Param.G)

  // TODO: implement these:
  def Union(variants: Type*) = Erased(Type.Union(variants: _*))
  def Intersect(variants: Type*) = Erased(Type.Intersect(variants: _*))

  def Optional(t: Type) = t match {
    case Type.Union(vs, span) => Erased(Type.Union(vs :+ Type.Null(span), span))
    case v => Erased(Type.Union(ArraySeq(v, Type.Null(v.span)), v.span))
  }
  def Predicate(arg: Type) = Function(Seq(arg), Optional(Boolean))

  case object Null extends TypeTag[Value.Null](Type.Null, ValueType.NullType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Null => v }
  }
  case object ID extends TypeTag[Value.ID](Type.ID, ValueType.IDType) {
    def cast(v: Value): Option[Value.ID] = v match {
      case Value.Str(v)  => v.toLongOption.map(Value.ID(_))
      case Value.Long(v) => Some(Value.ID(v))
      case Value.Int(v)  => Some(Value.ID(v))
      case v: Value.ID   => Some(v)
      case _             => None
    }
  }
  case object Boolean
      extends TypeTag[Value.Boolean](Type.Boolean, ValueType.BooleanType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Boolean => v }
  }
  case object True
      extends TypeTag[Value.Boolean](
        Type.Singleton(Literal.True),
        ValueType.BooleanType) {
    def cast(v: Value) = Some(v) collect { case v @ Value.Boolean(true) => v }
  }
  case object False
      extends TypeTag[Value.Boolean](
        Type.Singleton(Literal.False),
        ValueType.BooleanType) {
    def cast(v: Value) = Some(v) collect { case v @ Value.Boolean(false) => v }
  }
  case object Number
      extends TypeTag[Value.Number](Type.Number, ValueType.NumberType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Number => v }
  }
  case object Int extends TypeTag[Value.Int](Type.Number, ValueType.IntType) {
    def cast(v: Value) = Some(v) collect {
      case i: Value.Int                    => i
      case Value.Long(l) if l.isValidInt   => Value.Int(l.intValue)
      case Value.Double(d) if d.isValidInt => Value.Int(d.intValue)
    }
    // special case until we figure out number hierarchy
    override def name = Type.Int.name.str
  }
  case object Long extends TypeTag[Value.Long](Type.Number, ValueType.LongType) {
    def cast(v: Value) = Some(v) collect {
      case Value.Int(i)                 => Value.Long(i)
      case l: Value.Long                => l
      case Value.Double(d) if d.isWhole => Value.Long(d.longValue)
    }
    // special case until we figure out number hierarchy
    override def name = Type.Long.name.str
  }
  case object Double
      extends TypeTag[Value.Double](Type.Number, ValueType.DoubleType) {
    def cast(v: Value) = Some(v) collect {
      case Value.Int(i)    => Value.Double(i)
      case Value.Long(l)   => Value.Double(l.toDouble)
      case d: Value.Double => d
    }
    // special case until we figure out number hierarchy
    override def name = Type.Double.name.str
  }
  case object Float
      extends TypeTag[Value.Double](Type.Number, ValueType.DoubleType) {
    def cast(v: Value) = Some(v) collect {
      case Value.Int(i)    => Value.Double(i)
      case Value.Long(l)   => Value.Double(l.toDouble)
      case d: Value.Double => d
    }
    // special case until we figure out number hierarchy
    override def name = Type.Float.name.str
  }

  case object Str extends TypeTag[Value.Str](Type.Str, ValueType.StringType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Str => v }
  }
  case class StrLit(s: String)
      extends TypeTag[Value.Str](
        Type.Singleton(Literal.Str(s), Span.Null),
        ValueType.StringType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Str => v }
  }
  case object Bytes extends TypeTag[Value.Bytes](Type.Bytes, ValueType.BytesType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Bytes => v }
  }
  case object Time extends TypeTag[Value.Time](Type.Time, ValueType.TimeType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Time => v }
  }
  case object TransactionTime
      extends TypeTag[Value.TransactionTime.type](
        Type.TransactionTime,
        ValueType.TransactionTimeType) {
    def cast(v: Value) = Some(v) collect { case Value.TransactionTime =>
      Value.TransactionTime
    }
  }
  case object Date extends TypeTag[Value.Date](Type.Date, ValueType.DateType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Date => v }
  }
  case object UUID extends TypeTag[Value.UUID](Type.UUID, ValueType.UUIDType) {
    def cast(v: Value) = Some(v) collect { case v: Value.UUID => v }
  }

  case object AnyObject
      extends TypeTag[Value.Object](Type.Any, ValueType.AnyObjectType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Object => v }
  }
  case class WildObject(wildcard: Type)
      extends TypeTag[Value.Object](
        Type.Record().copy(wildcard = Some(wildcard)),
        ValueType.AnyObjectType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Object => v }
  }

  case class Struct(ty: Type)
      extends TypeTag[Value.Struct](ty, ValueType.AnyStructType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Struct => v }
  }
  object Struct {
    // Scala won't use the implicit for overloaded applies, so use TypeTag here!
    def apply(fields: (String, TypeTag[_])*) = new Struct(Type.Record(fields.map {
      case (k, v) => k -> v.staticType
    }: _*))
  }
  object WildStruct {
    def apply(wildcard: Type) =
      new Struct(Type.Record().copy(wildcard = Some(wildcard)))
  }

  val AnyStruct = WildStruct(Type.Any)

  case class NamedDoc(collName: String)
      extends TypeTag[Value.Doc](Type.Named(collName), ValueType.AnyDocType) {
    def cast(v: Value) = AnyDoc.cast(v)
    override def name = collName
  }
  case class NullDoc(collName: String)
      extends TypeTag[Value.Doc](
        Type.Named("Null" + collName),
        ValueType.AnyNullDocType) {
    def cast(v: Value) = None
    override def name = "Null" + collName
  }
  case class EmptyRef(docType: Type)
      extends TypeTag[Value.Doc](
        Type.EmptyRef(docType, docType.span),
        ValueType.AnyNullDocType) {
    def cast(v: Value) = None
  }
  case class Ref(docType: Type)
      extends TypeTag[Value.Doc](
        Type.Ref(docType, docType.span),
        ValueType.AnyDocType) {
    def cast(v: Value) = AnyDoc.cast(v)
  }
  case class EmptyNamedRef(docType: Type)
      extends TypeTag[Value.Doc](
        Type.EmptyNamedRef(docType, docType.span),
        ValueType.AnyNullDocType) {
    def cast(v: Value) = None
  }
  case class NamedRef(docType: Type)
      extends TypeTag[Value.Doc](
        Type.NamedRef(docType, docType.span),
        ValueType.AnyDocType) {
    def cast(v: Value) = AnyDoc.cast(v)
  }

  // FIXME: should be named "Document" type which is an alias for the minimal
  // fields
  case object AnyDoc
      extends TypeTag[Value.Doc](Type.AnyRecord, ValueType.AnyDocType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Doc => v }
  }
  case object AnyNullDoc
      extends TypeTag[Value.Doc](Type.Null, ValueType.AnyNullDocType) {
    def cast(v: Value) = None
  }

  case class Array(elems: Type)
      extends TypeTag[Value.Array](
        Type.Array(elems, elems.span),
        ValueType.AnyArrayType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Array => v }
  }
  val AnyArray = Array(Type.Any)

  case class Tuple(elems: Type*)
      extends TypeTag[Value.Array](Type.Tuple(elems: _*), ValueType.AnyArrayType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Array => v }
  }

  case class Set(elems: Type)
      extends TypeTag[Value.Set](Type.Set(elems), ValueType.AnySetType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Set => v }
  }
  val AnySet = Set(Type.Any)

  case class EventSource(elems: Type)
      extends TypeTag[Value.EventSource](
        Type.EventSource(elems),
        ValueType.AnyEventSource) {
    def cast(v: Value) = Some(v) collect { case v: Value.EventSource => v }
  }
  val AnyEventSource = EventSource(Type.Any)

  case object SetCursor
      extends TypeTag[Value.SetCursor](Type.SetCursor, ValueType.SetCursorType) {
    def cast(v: Value) = v match {
      case c: Value.SetCursor => Some(c)
      case Value.Str(v)       => Value.SetCursor.fromBase64(v)
      case _                  => None
    }
  }

  case class Function(params: Seq[Type], ret: Type)
      extends TypeTag[Value.Func](
        Type.Function(params.view.map((None, _)).to(ArraySeq), None, ret, Span.Null),
        ValueType.AnyFunctionType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Func => v }
  }
  case object AnyFunction
      extends TypeTag[Value.Func](Type.Any, ValueType.AnyFunctionType) {
    def cast(v: Value) = Some(v) collect { case v: Value.Func => v }
  }
  case class NamedUserFunction(sig: Type)
      extends TypeTag[UserFunction](
        Type.Named(Name("UserFunction", Span.Null), Span.Null, ArraySeq(sig)),
        ValueType.AnyFunctionType) {
    def cast(v: Value) = Some(v) collect { case v: UserFunction => v }
  }
}
