package fauna.repo.values

import fauna.atoms.{ CollectionID, DocID, SubID }
import fauna.codex.cbor.CBOR
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo.doc.Version
import fauna.repo.query.ReadCache
import fauna.repo.schema.Path
import fauna.storage.ir.IRValue
import fauna.util.Base64
import fql.ast.{ Expr, Literal, Name, Span }
import io.netty.util.AsciiString
import java.lang.{ Double => JDouble, Integer => JInteger, Long => JLong }
import java.time.LocalDate
import java.util.Objects
import scala.collection.immutable.{ ArraySeq, BitSet, SeqMap }
import scala.collection.mutable.Builder
import scala.collection.Factory
import scala.math.{ BigDecimal, BigInt }
import scala.util.control.NonFatal
import scala.util.Try

// TODO: The FQL1 equiv of this is the Literal type. Need to unify these.
// TODO: There are kludges to allow Value to be extended downstream. See note.

/** Error thrown when attempt to access a field on an unexpected type.
  */
class UnexpectedValue(msg: String) extends Exception(msg)

/** Runtime representation of all possible values in Fauna. This type only
  * describes shape. Functionality is provided in higher level models through a
  * typeclass pattern.
  *
  * There are some kludges below which are meant to allow the type hierarchy to
  * be extended in a constrained way downstream (see Set and NativeFunction). I
  * couldn't figure out how to make these fully type safe without unreasonably
  * complicating the types, so went with the hack. Hopefully we can fix this in
  * the future, probably by reorganizing the model components to include
  * everything FQL/data model related that lives in the coordinator.
  */
sealed trait Value extends Ordered[Value] {

  /** Returns the erased, dynamic type of the value. */
  def dynamicType: ValueType.DynamicType

  /** Returns true if the value can be persisted as document data. In other words,
    * whether or not the value is directly representable as an IRValue value.
    *
    * Whether or not a runtime value can be persisted really only matters for
    * document IO. So a dynamic check seems reasonable. (IRValue continues to
    * exist for dedicated persisted data repr.)
    */
  def isPersistable: Boolean

  def totalOrderingPosition: Int = {
    this match {
      // normal data
      case _: Value.Number       => 0
      case _: Value.Bytes        => 1
      case _: Value.Str          => 2
      case _: Value.Array        => 3
      case _: Value.Struct       => 4
      case _: Value.ID           => 5
      case _: Value.Doc          => 6
      case _: Value.Time         => 7
      case Value.TransactionTime => 8
      case _: Value.Date         => 9
      case _: Value.UUID         => 10
      case _: Value.Boolean      => 11

      // special types
      case _: Value.SingletonObject => 12
      case _: Value.Set             => 13
      case _: Value.SetCursor       => 14
      case _: Value.Lambda          => 15
      case _: Value.NativeFunc      => 16
      case _: Value.EventSource     => 17

      // null goes last
      case _: Value.Null => 18
    }
  }

  def compare(other: Value): Int = {
    def listCmp[A](
      a: Iterable[A],
      b: Iterable[A])(implicit ord: Ordering[A]): Int = {
      val iterA = a.iterator
      val iterB = b.iterator

      while (iterA.hasNext && iterB.hasNext) {
        val elemA = iterA.next()
        val elemB = iterB.next()
        val cmp = ord.compare(elemA, elemB)
        if (cmp != 0) return cmp
      }

      if (iterA.hasNext) 1
      else if (iterB.hasNext) -1
      else 0
    }

    @inline def chainCmp(cmp: Int)(next: => Int): Int =
      if (cmp == 0) next else cmp

    (this, other) match {
      case (Value.ID(v1), Value.ID(v2))           => v1.compare(v2)
      case (v1: Value.Number, v2: Value.Number)   => v1.compareNumbers(v2)
      case (Value.Boolean(v1), Value.Boolean(v2)) => v1.compare(v2)
      case (Value.Str(v1), Value.Str(v2))         => v1.compare(v2)
      case (Value.Bytes(v1), Value.Bytes(v2)) =>
        val buf1 = v1.unsafeByteBuf
        val buf2 = v2.unsafeByteBuf
        buf1.compareTo(buf2)

      case (Value.Time(v1), Value.Time(v2)) => v1.compare(v2)
      case (Value.Date(v1), Value.Date(v2)) => v1.compareTo(v2)
      case (Value.UUID(v1), Value.UUID(v2)) => v1.compareTo(v2)

      case (Value.Doc(id1, name1, _, _, _), Value.Doc(id2, name2, _, _, _)) =>
        chainCmp(id1.collID compare id2.collID) {
          (name1, name2) match {
            case (Some(n1), Some(n2)) => n1.compare(n2)
            case _                    => id1.compare(id2)
          }
        }

      case (Value.Struct.Full(v1, _, _, _), Value.Struct.Full(v2, _, _, _)) =>
        listCmp(v1.toSeq.sortBy(_._1), v2.toSeq.sortBy(_._1))

      case (Value.Array(v1), Value.Array(v2)) =>
        listCmp(v1, v2)

      case (v1: Value.SingletonObject, v2: Value.SingletonObject) =>
        v1.name.compare(v2.name)

      case (_: Value.Struct.Partial, _) | (_, _: Value.Struct.Partial) =>
        sys.error("Cannot compare partial structs values.")

      case (v1, v2) =>
        JInteger.compare(v1.totalOrderingPosition, v2.totalOrderingPosition)

      // TODO: add ordering for remaining types
      //  case (v1: Value.Set, v2: Value.Set)
      //  case (v1: Value.SetCursor, v2: Value.SetCursor)
      //  case (v1: Value.Lambda, v2: Value.Lambda)
      //  case (v1: Value.NativeFunc, v2: Value.NativeFunc)
    }
  }

  override def equals(other: Any): Boolean = {
    (this, other) match {
      case (_: Value.Null, _: Value.Null) => true
      case (_: Value.Null, _)             => false

      case (Value.ID(v), Value.ID(v2))           => v == v2
      case (Value.ID(v), Value.Str(v2))          => v2.toLongOption.exists(_ == v)
      case (Value.ID(_), _)                      => false
      case (Value.Int(v), Value.Int(v2))         => v == v2
      case (Value.Int(v), Value.Long(v2))        => v == v2
      case (Value.Int(v), Value.Double(v2))      => v == v2
      case (Value.Int(_), _)                     => false
      case (Value.Long(v), Value.Int(v2))        => v == v2
      case (Value.Long(v), Value.Long(v2))       => v == v2
      case (Value.Long(v), Value.Double(v2))     => v == v2
      case (Value.Long(_), _)                    => false
      case (Value.Double(v), Value.Int(v2))      => v == v2
      case (Value.Double(v), Value.Long(v2))     => v == v2
      case (Value.Double(v), Value.Double(v2))   => v == v2
      case (Value.Double(_), _)                  => false
      case (Value.Str(v), Value.Str(v2))         => v == v2
      case (Value.Str(v), Value.ID(v2))          => v.toLongOption.exists(_ == v2)
      case (Value.Str(_), _)                     => false
      case (Value.Boolean(v), Value.Boolean(v2)) => v == v2
      case (Value.Boolean(_), _)                 => false
      case (Value.Time(v), Value.Time(v2))       => v == v2
      case (Value.Time(_), _)                    => false
      case (Value.Date(v), Value.Date(v2))       => v == v2
      case (Value.Date(_), _)                    => false
      case (Value.Bytes(v), Value.Bytes(v2))     => v == v2
      case (Value.Bytes(_), _)                   => false
      case (Value.UUID(v), Value.UUID(v2))       => v == v2
      case (Value.UUID(_), _)                    => false

      case (Value.Doc(id1, name1, _, _, _), Value.Doc(id2, name2, _, _, _)) =>
        (id1.collID == id2.collID) && ((name1, name2) match {
          case (Some(n1), Some(n2)) => n1 == n2
          case _                    => id1.subID == id2.subID
        })
      case (Value.Doc(_, _, _, _, _), _) => false

      case (Value.Array(v), Value.Array(v2)) => v == v2
      case (Value.Array(_), _)               => false

      case (Value.Struct.Full(v, _, _, _), Value.Struct.Full(v2, _, _, _)) => v == v2
      case (Value.Struct.Full(_, _, _, _), _)                              => false

      // NOTE: partials are special values. This equality definition is meant to
      // allow for them to be part of hash tables / sets. API level value comparison
      // is overriden by the FQL interpreter.
      case (a: Value.Struct.Partial, b: Value.Struct.Partial) =>
        a.doc == b.doc && a.prefix == b.prefix && a.path == b.path
      case (_: Value.Struct.Partial, _) => false

      case (v: Value.Lambda, v2: Value.Lambda) =>
        v.closure == v2.closure && v.params == v2.params && v.expr == v2.expr
      case (_: Value.Lambda, _) => false
      case (v: Value.NativeFunc, v2: Value.NativeFunc) =>
        v.name == v2.name && v.callee == v2.callee
      case (_: Value.NativeFunc, _) => false

      case (v: Value.SingletonObject, v2: Value.SingletonObject) => v.name == v2.name
      case (_: Value.SingletonObject, _)                         => false
      case (_: Value.TransactionTime.type, _: Value.TransactionTime.type) => true
      case (_: Value.TransactionTime.type, _)                             => false

      case (v: Value.SetCursor, v2: Value.SetCursor) =>
        v.pageSize == v2.pageSize &&
        v.systemValidTS == v2.systemValidTS &&
        v.ords == v2.ords &&
        v.set == v2.set &&
        v.values == v2.values
      case (_: Value.SetCursor, _) => false

      case (v: Value.EventSource, v2: Value.EventSource) =>
        v.set == v2.set &&
        v.cursor == v2.cursor &&
        v.watchedFields == v2.watchedFields
      case (_: Value.EventSource, _) => false

      case (_: Value.Set, _) => sys.error("overridden")
    }
  }

  override def hashCode(): Int = {
    this match {
      case Value.Null(_) => Value.Null.hashCode()

      case Value.ID(v)      => v.hashCode()
      case Value.Int(v)     => v.hashCode()
      case Value.Long(v)    => v.hashCode()
      case Value.Double(v)  => v.hashCode()
      case Value.Str(v)     => v.hashCode()
      case Value.Boolean(v) => v.hashCode()
      case Value.Time(v)    => v.hashCode()
      case Value.Date(v)    => v.hashCode()
      case Value.Bytes(v)   => v.hashCode()
      case Value.UUID(v)    => v.hashCode()

      case Value.Doc(v1, v2, _, _, _)    => Objects.hash(v1, v2)
      case Value.Array(v)                => v.hashCode()
      case Value.Struct.Full(v, _, _, _) => v.hashCode()

      // NOTE: partials are special values. This hash code definition is meant to
      // allow for them to be part of hash tables / sets. API level value comparison
      // is overriden by the FQL interpreter.
      case Value.Struct.Partial(doc, prefix, path, _, _) =>
        Objects.hash(doc, prefix, path)

      case v: Value.Lambda          => v.hashCode()
      case v: Value.NativeFunc      => Objects.hash(v.name, v.callee)
      case v: Value.SingletonObject => Objects.hash(Value.SingletonObject, v.name)
      case _: Value.TransactionTime.type =>
        System.identityHashCode(Value.TransactionTime)

      case v: Value.SetCursor =>
        Objects.hash(v.set, v.values, v.ords, v.systemValidTS, v.pageSize)

      case Value.EventSource(set, path, start) =>
        Objects.hash(Value.EventSource, set, path, start)

      case _: Value.Set => sys.error("overridden")
    }
  }

  def /(field: String): Value = this match {
    case Value.Struct.Full(fields, _, _, _) =>
      fields.getOrElse(field, Value.Null.missingField(this, Name(field, Span.Null)))

    case v => throw new UnexpectedValue(s"Unexpected value $v")
  }

  def /(index: Int): Value = this match {
    case Value.Array(elems) if index >= 0 && elems.lengthIs > index =>
      elems(index)

    case Value.Array(_) => throw new UnexpectedValue("Index out of bound")

    case v => throw new UnexpectedValue(s"Unexpected value $v")
  }

  def as[T](implicit ev: ValueDecoder[T]): T = ev(this)

  def asOpt[T](implicit ev: ValueDecoder[T]): Option[T] = try {
    Some(ev(this))
  } catch {
    case InvalidCast => None
  }

  def isNull: Boolean = this match {
    case _: Value.Null => true
    case _             => false
  }
}

object Value extends IRValueConversion {
  sealed trait Persistable extends Value {
    def isPersistable = true
  }

  // Scalars

  object Scalar { def unapply(v: Scalar) = Some(v) }
  sealed trait Scalar extends Persistable

  final case class ID(value: scala.Long) extends Scalar {
    // FIXME: this type should be persistible, but IRValue does not support it
    override def isPersistable = false
    def dynamicType = ValueType.IDType
  }

  object Number {
    def apply(bd: BigDecimal): Number = Double(bd.doubleValue)
    def apply(d: scala.Double): Number = Double(d)
    def apply(bi: BigInt): Number =
      if (bi.isValidInt) Int(bi.intValue)
      else if (bi.isValidLong) Long(bi.longValue)
      else Double(bi.doubleValue)
    def apply(l: scala.Long): Number =
      if (l.isValidInt) Int(l.intValue)
      else Long(l)
    def apply(i: scala.Int): Number = Int(i)
  }
  sealed trait Number extends Scalar {
    def toDouble: scala.Double
    def compareNumbers(other: Number): Integer
  }
  final case class Int(value: scala.Int) extends Number {
    def dynamicType = ValueType.IntType
    def toDouble = value.toDouble
    def compareNumbers(other: Number) = other match {
      case Int(o)    => JInteger.compare(value, o)
      case Long(o)   => JLong.compare(value.toLong, o)
      case Double(o) => JDouble.compare(value.toDouble, o)
    }
  }
  final case class Long(value: scala.Long) extends Number {
    def dynamicType = ValueType.LongType
    def toDouble = value.toDouble
    def compareNumbers(other: Number) = other match {
      case Int(o)    => JLong.compare(value, o)
      case Long(o)   => JLong.compare(value, o)
      case Double(o) => JDouble.compare(value.toDouble, o)
    }
  }
  final case class Double(value: scala.Double) extends Number {
    def dynamicType = ValueType.DoubleType
    def toDouble = value
    def compareNumbers(other: Number) = other match {
      case Int(o)    => JDouble.compare(value, o)
      case Long(o)   => JDouble.compare(value, o.toDouble)
      case Double(o) => JDouble.compare(value, o)
    }
  }

  object Boolean {
    def apply(value: scala.Boolean) = if (value) True else False
    def unapply(bool: Boolean) = Some(bool.value)
  }
  sealed trait Boolean extends Scalar {
    def value: scala.Boolean
    def dynamicType = ValueType.BooleanType

    def unary_! = Boolean(!value)
  }
  case object True extends Boolean { val value = true }
  case object False extends Boolean { val value = false }

  final case class Str(value: String) extends Scalar {
    def dynamicType = ValueType.StringType
  }
  object Bytes {
    def fromBase64(encoded: String) =
      Bytes(ArraySeq.unsafeWrapArray(Base64.decodeStandard(encoded)))
  }
  final case class Bytes(value: ArraySeq[Byte]) extends Scalar {
    def dynamicType = ValueType.BytesType

    def toBase64: AsciiString =
      Base64.encodeStandardAscii(value.unsafeByteArray)
  }
  final case class Time(value: Timestamp) extends Scalar {
    def dynamicType = ValueType.TimeType
  }
  // TODO: TransactionTime should be able to participate in most/all places
  // Time values can, however this requires being able to defer computation
  // until after the transaction time is known. This certainly a complex
  // requirement, so for the time being the types are separate.
  case object TransactionTime extends Scalar {
    val dynamicType = ValueType.TransactionTimeType
  }
  final case class Date(value: LocalDate) extends Scalar {
    def dynamicType = ValueType.DateType
  }
  final case class UUID(value: java.util.UUID) extends Scalar {
    def dynamicType = ValueType.UUIDType
  }

  object Null {

    def apply(span: Span): Null = Null(Cause.Lit(span))

    def invalidNumber(value: String, span: Span) =
      Null(Cause.InvalidNumber(value, span))

    def missingField(value: Value, field: Name) =
      Null(Cause.MissingField(value, field))

    def noSuchElement(reason: String, span: Span) =
      Null(Cause.NoSuchElement(reason, span))

    def docNotFound(doc: Value.Doc, collName: String, span: Span) =
      Null(Cause.DocNotFound(doc, collName, span))

    def collectionDeleted(doc: Value.Doc, span: Span) =
      Null(Cause.CollectionDeleted(doc, span))

    def readPermissionDenied(doc: DocID, span: Span) =
      Null(Cause.ReadPermissionDenied(doc, span))

    sealed trait Cause {
      def span: Span
    }
    object Cause {
      case class Lit(span: Span) extends Cause
      case class InvalidNumber(value: String, span: Span) extends Cause
      // field name includes a span
      case class MissingField(value: Value, field: Name) extends Cause {
        def span = field.span
      }
      case class NoSuchElement(reason: String, span: Span) extends Cause
      case class DocNotFound(doc: Value.Doc, collName: String, span: Span)
          extends Cause
      case class DocDeleted(doc: Value.Doc, collName: String, span: Span)
          extends Cause
      case class CollectionDeleted(doc: Value.Doc, span: Span) extends Cause
      case class ReadPermissionDenied(doc: DocID, span: Span) extends Cause
    }
  }
  final case class Null(cause: Null.Cause) extends Scalar {
    def dynamicType = ValueType.NullType
  }

  // Set Cursor

  /** We want to be able to send set cursors as values. They must be a special
    * value since we need to render them with the transaction time on the way
    * out. We don't make them persistable, either, since they are short-lived
    * objects.
    */
  object SetCursor {

    type SetCursorCodec = CBOR.Codec[(
      Expr,
      Vector[IRValue],
      Option[Vector[IRValue]],
      Option[Timestamp],
      scala.Int)]

    private val codec = {
      import ExprCBORCodecs.exprs
      CBOR
        .TupleCodec[(
          Expr,
          Vector[IRValue],
          Option[Vector[IRValue]],
          Option[Timestamp],
          scala.Int)]
    }

    private val spanlessCodec = {
      import ExprCBORCodecsNoSpan.exprs
      CBOR
        .TupleCodec[(
          Expr,
          Vector[IRValue],
          Option[Vector[IRValue]],
          Option[Timestamp],
          scala.Int)]
    }

    def fromBase64(str: String): Option[SetCursor] =
      fromBase64(str, spanlessCodec, Some(codec))

    private def fromBase64(
      str: String,
      codec: SetCursorCodec,
      fallbackCodec: Option[SetCursorCodec]): Option[SetCursor] = {
      try {
        val bytes = Base64.decodeStandard(str)
        val (set, vctx, ords, vt, psize) = CBOR.parse(bytes)(codec)
        Some(SetCursor(set, vctx.map(Value.fromIR(_, Span.Null)), ords, vt, psize))
      } catch {
        case NonFatal(_) if fallbackCodec.isDefined =>
          fromBase64(str, fallbackCodec.get, None)
        case NonFatal(_) => None
      }
    }

    def toBase64(cur: SetCursor, txnTime: Option[Timestamp]) = {
      val SetCursor(set, values, ords, stOpt, psize) = cur
      val st = stOpt.orElse(txnTime)
      val vctx = values.map(Value.toIR(_).toOption.get)
      val buf = CBOR.encode((set, vctx, ords, st, psize))(spanlessCodec)
      Base64.encodeStandardAscii(buf.nioBuffer)
    }
  }
  case class SetCursor(
    set: Expr,
    values: Vector[Value],
    ords: Option[Vector[IRValue]],
    systemValidTS: Option[Timestamp],
    pageSize: scala.Int)
      extends Value {
    def dynamicType = ValueType.SetCursorType
    def isPersistable = false
  }

  // Collections

  sealed trait Iterable extends Value

  trait Set extends Iterable {
    def dynamicType = ValueType.AnySetType
    def isPersistable = false

    /** We want sets to participate in the Value hierarchy, but the implementation
      * depends on ext/model. This abstract method is meant to act as a
      * compile-time speedbump for anyone attempting to use the trait here.
      */
    protected def `Only fauna.model.runtime.fql2.ValueSet may extend Value.Set`()
      : Unit

    /** Return an expression using the ReifyCtx, which when evaluated, reproduces an
      * equivalent set.
      */
    def reify(ctx: ValueReification.ReifyCtx): Expr
  }

  object Array extends Factory[Value, Value.Array] {
    def apply(elems: Value*): Array =
      Array.fromSpecific(elems)

    val empty = Array()

    def fromSpecific(it: IterableOnce[Value]) =
      Value.Array(ArraySeq.from(it))

    def newBuilder: Builder[Value, Value.Array] =
      new Builder[Value, Value.Array] {
        val b = ArraySeq.newBuilder[Value]
        def clear() = b.clear()
        def result() = Value.Array(b.result())
        def addOne(e: Value) = {
          b += e
          this
        }
      }
  }
  final case class Array(elems: ArraySeq[Value]) extends Iterable {
    def dynamicType = ValueType.AnyArrayType
    lazy val isPersistable = elems.forall(_.isPersistable)
  }

  object EventSource {

    /** Event cursors are composed of the txn start time and the ordinal from the event
      * in the given txn time.
      */
    final case class Cursor(ts: Timestamp, ord: scala.Int = 0)
        extends Ordered[Cursor] {

      def next(eventTS: Timestamp): Cursor =
        if (eventTS == ts) Cursor(eventTS, ord + 1)
        else Cursor(eventTS, 0)

      @inline def toBase64 =
        Cursor.toBase64(this)

      def compare(that: Cursor): scala.Int =
        (ts, ord) compare (that.ts, that.ord)
    }

    object Cursor {
      implicit val Codec = CBOR.TupleCodec[Cursor]

      val MinValue = Cursor(Timestamp.Min, 0)
      val MaxOrdinal = scala.Int.MaxValue

      def toBase64(cursor: Cursor): AsciiString =
        Base64.encodeStandardAscii(
          CBOR.encode(cursor).nioBuffer
        )

      def fromBase64(str: String): Option[Cursor] =
        Try(
          CBOR.parse[Cursor](
            Base64.decodeStandard(str)
          )).toOption
    }

    /** Analogous to `SetCursor`, this intermediate representation carries the reified
      * expression that produces an event source along with its saved value context.
      * The event source's start time from `cursor` gets assigned to the transaction
      * time when rendered.
      */
    final case class IR(source: Expr, values: Vector[Value], cursor: Cursor) {
      @inline def start: Timestamp = cursor.ts
    }

    object IR {
      implicit private val ValueCodec =
        CBOR.AliasCodec[Value, IRValue](
          Value.fromIR(_, Span.Null),
          Value.toIR(_).toOption.get
        )

      val codec = {
        import ExprCBORCodecs.exprs
        CBOR.TupleCodec[IR]
      }

      val spanlessCodec = {
        import ExprCBORCodecsNoSpan.exprs
        CBOR.TupleCodec[IR]
      }
    }

    def toBase64(source: EventSource, txnTime: Timestamp): AsciiString = {
      val (expr, closure) = ValueReification.reify(source)
      val cursor = source.cursor.getOrElse(Cursor(txnTime))
      val buf = CBOR.encode(IR(expr, closure, cursor))(IR.spanlessCodec)
      Base64.encodeStandardAscii(buf.nioBuffer)
    }

    def fromBase64(str: String): Option[IR] =
      fromBase64(str, IR.spanlessCodec, Some(IR.codec))

    def fromBase64(
      str: String,
      codec: CBOR.Codec[IR],
      fallbackCodec: Option[CBOR.Codec[IR]]): Option[IR] =
      try {
        val bytes = Base64.decodeStandard(str)
        Some(CBOR.parse[IR](bytes)(codec))
      } catch {
        case NonFatal(_) if fallbackCodec.isDefined =>
          fromBase64(str, fallbackCodec.get, None)
        case NonFatal(_) => None
      }
  }

  final case class EventSource(
    set: Set,
    cursor: Option[EventSource.Cursor],
    watchedFields: Seq[Path] = Seq.empty)
      extends Iterable {
    def dynamicType = ValueType.AnyEventSource
    def isPersistable = false

    def reify(ctx: ValueReification.ReifyCtx): Expr =
      Expr.MethodChain(
        ctx.save(set),
        Seq(
          Expr.MethodChain.MethodCall(
            Span.Null,
            Name(
              if (watchedFields.isEmpty) "eventSource" else "eventsOn",
              Span.Null
            ),
            watchedFields map { path =>
              Expr.ShortLambda(
                Expr.MethodChain(
                  Expr.This(Span.Null),
                  path.elements map {
                    case Right(field) =>
                      Expr.MethodChain.Select(
                        Span.Null,
                        Name(field, Span.Null),
                        optional = false
                      )
                    case Left(idx) =>
                      Expr.MethodChain.Access(
                        Seq(Expr.Lit(Literal.Int(idx), Span.Null)),
                        optional = None,
                        Span.Null
                      )
                  },
                  Span.Null
                ))
            },
            selectOptional = false,
            applyOptional = None,
            Span.Null
          )
        ),
        Span.Null
      )
  }

  sealed trait Object extends Value

  /** `id` is the document's ID, except for one edge case. Possible states of
    * `id` and `name` are:
    *
    * - Documents by ID will have the `id` set, and `name` is `None`.
    * - Documents by name that exist will have `id` and `name` set to `Some`.
    * - Documents by name from `byName`, that don't exist, will have `id` set to
    *   `SentinelSubID`, and name set to `Some`.
    * - Documents by name from storage will have `id` set and `name` set to `None`.
    *
    * In order to tell if a document is by ID or by name, we must use the collection
    * ID. The `ReadBroker` in model handles this logic.
    *
    * `readTS` is the validTime for this document. Any field lookups must use `readTS`
    * when fetching the version for this document. Generally, this is only needed when
    * the document is being read at a time other than the time the query is being
    * executed at, such as when a doc is returned from an at expression.
    *
    * versionOverride is currently only to be used to evaluate role predicates,
    * in all other cases you DO NOT want to use this parameter as the desired behavior
    * is to utilize the read cache to pull the most up to date version for the
    * document.
    * versionOverride will also be used in the future when we implement an FQLX
    * indexer.
    *
    * `versionOverride` will be set to a deleted version if the doc came from a `delete()`
    * call.
    */
  final case class Doc(
    id: DocID,
    name: Option[String] = None,
    readTS: Option[Timestamp] = None,
    versionOverride: Option[Version] = None,
    srcHint: Option[ReadCache.CachedDoc.SetSrcHint] = None)
      extends Object
      with Persistable {
    def dynamicType = ValueType.AnyDocType
  }

  object Doc {

    /** Sentinel subID for a named doc that doesn't exist.
      */
    val SentinelSubID = SubID(-1)

    /** Creates a named document with a `SentinelSubID`.
      */
    def createNamed(
      collID: CollectionID,
      name: String,
      ts: Option[Timestamp] = None,
      versionOverride: Option[Version] = None) =
      Doc(DocID(SentinelSubID, collID), Some(name), ts, versionOverride)
  }

  sealed trait Struct extends Object {
    def dynamicType = ValueType.AnyStructType
  }

  object Struct extends Factory[(String, Value), Value.Struct] {
    import ReadCache.CachedDoc.SrcHints

    /** A fully materialized struct. */
    final case class Full(
      fields: SeqMap[String, Value],
      srcHints: SrcHints = SrcHints.Empty,
      /** represents path from a document that this was pulled from if it came off of a document. Used to provide
        * query performance hints when reading unindexed fields from a document that has previously been read into the
        * cache. This will happen on the first unindexed field access for a document and we want to be able to provide
        * hints on subsequent unindexed field access.
        */
      path: ReadCache.Path = Nil,
      /** Used to emit query performance hints when this is materialized if it was obtained from
        * non covered field access.
        */
      accessSpan: Span = Span.Null
    ) extends Struct {
      lazy val isPersistable = fields.forall(_._2.isPersistable)
      def ++(other: Full): Full = Full(this.fields ++ other.fields)
    }

    /** A partially known struct a given path of a document snapshot.
      *
      * Note that similarly to Doc, Partial must be materialized during query
      * interpretation in order to work as a persistible value.
      */
    final case class Partial(
      doc: Doc,
      prefix: ReadCache.Prefix,
      path: ReadCache.Path,
      fragment: ReadCache.Fragment.Struct,
      /** Used to emit query performance hints when this is materialized requiring a document read. */
      accessSpan: Span
    ) extends Struct {
      var srcHints: SrcHints = SrcHints.Empty
      def isPersistable = false
    }

    val empty: Full = Struct()

    def apply(map: SeqMap[String, Value]): Full =
      Full(map)

    def apply(fields: (String, Value)*): Full =
      Struct.fromSpecific(fields)

    def fromSpecific(it: IterableOnce[(String, Value)]): Full =
      Full(SeqMap.from(it))

    def newBuilder: Builder[(String, Value), Full] =
      new Builder[(String, Value), Full] {
        val b = SeqMap.newBuilder[String, Value]
        def clear() = b.clear()
        def result() = Full(b.result())
        def addOne(e: (String, Value)) = {
          b += e
          this
        }
      }
  }

  trait SingletonObject extends Object {
    val name: String
    val parent: Option[Value]

    def dynamicType: ValueType.DynamicType = ValueType.SingletonObjectType(name)
    def isPersistable = false

    /** We want singleton objects to participate in the Value hierarchy, but the
      * implementation depends on ext/model. This abstract method is meant to
      * act as a compile-time speedbump for anyone attempting to use the trait
      * here.
      */
    protected def `Only fauna.model.runtime.fql2.SingletonObject may extend Value.SingletonObject`()
      : Unit

    /** Return an expression using the ReifyCtx, which when evaluated, reproduces an
      * equivalent singleton object.
      */
    def reify(ctx: ValueReification.ReifyCtx): Expr =
      parent match {
        // This singleton object is in the top-level global context.
        case None => Expr.Id(name, Span.Null)
        // This singleton object is a member of some parent value.
        case Some(value) =>
          Expr.MethodChain(
            ctx.save(value),
            Seq(
              Expr.MethodChain.Select(
                Span.Null,
                Name(name, Span.Null),
                false
              )
            ),
            Span.Null
          )
      }
  }
  object SingletonObject {
    def unapply(o: SingletonObject) = Some((o.name, o.parent))
  }

  // Functions

  sealed trait Func extends Value {
    def arity: Func.Arity

    /** Return an expression using the ReifyCtx, which when evaluated, reproduces an
      * equivalent function value.
      */
    def reify(ctx: ValueReification.ReifyCtx): Expr
  }

  object Func {
    sealed abstract trait Arity {
      def accepts(numArgs: scala.Int): scala.Boolean
      def numRequiredArgs: scala.Int
      def displayString(argWord: String = "argument"): String
    }

    object Arity {
      final case class Fixed(allowed: BitSet) extends Arity {
        def accepts(numArgs: scala.Int) = allowed(numArgs)
        def numRequiredArgs = allowed.min
        def displayString(argWord: String = "argument") =
          allowed.toList.sorted[scala.Int] match {
            case List(0) => s"no ${argWord}s"
            case List(1) => s"exactly 1 ${argWord}"
            case List(n) => s"exactly $n ${argWord}s"
            case l       => l.init.mkString(", ") + s" or ${l.last} ${argWord}s"
          }
      }
      final case class Variable(required: scala.Int) extends Arity {
        def accepts(numArgs: scala.Int) = numArgs >= required
        def numRequiredArgs = required
        def displayString(argWord: String = "argument") = required match {
          case 0 => s"any number of ${argWord}s"
          case 1 => s"at least 1 ${argWord}"
          case n => s"at least $n ${argWord}s"
        }
      }

      def apply(numArgs: scala.Int*): Arity = Fixed(BitSet(numArgs: _*))
    }
  }

  final case class Lambda(
    params: ArraySeq[Option[String]],
    vari: Option[Option[String]],
    expr: Expr,
    closure: Map[String, Value])
      extends Func {
    def arity = vari.fold(Func.Arity(params.size)) { _ =>
      Func.Arity.Variable(params.size)
    }
    def dynamicType = ValueType.AnyLambdaType

    // This type is only be persistible _only in schema contexts_. If we allow
    // normal documents to hold lambdas, it will be impossible to typecheck
    // them, or rename types etc.
    def isPersistable = false

    // Caching the hash code prevents exponential work in some thorny cases
    // where the closure contains values which reference other values which
    // reference this lambda again. For example,
    //   [0, 0, 0, 0, 0].fold((acc, param) =>
    //     acc.concat(Foo.byX(param.x).where(.y != null))
    //   )
    lazy val _hashCode = Objects.hash(closure, params, vari, expr)
    override def hashCode(): scala.Int = _hashCode

    def reify(ctx: ValueReification.ReifyCtx): Expr = {
      val free = expr.freeVars
      val lets = closure.iterator
        .filter { case (name, _) => free.contains(name) }
        .map { case (name, v) =>
          Expr.Stmt.Let(Name(name, Span.Null), None, ctx.save(v), Span.Null)
        }
        .toSeq

      val lambda =
        Expr.LongLambda(
          params.map(_.map(Name(_, Span.Null))),
          vari.mapT(Name(_, Span.Null)),
          expr,
          Span.Null)

      if (lets.isEmpty) {
        lambda
      } else {
        Expr.Block(lets :+ Expr.Stmt.Expr(lambda), Span.Null)
      }
    }
  }

  trait NativeFunc extends Func {
    def dynamicType: ValueType.DynamicType = ValueType.AnyFunctionType
    def isPersistable = false

    def callee: Value
    def name: String

    /** We want native functions to participate in the Value hierarchy, but the
      * implementation depends on ext/model. This abstract method is meant to
      * act as a compile-time speedbump for anyone attempting to use the trait
      * here.
      */
    protected def `Only fauna.model.runtime.fql2.NativeFunction may extend Value.NativeFunc`()
      : Unit

    def reify(ctx: ValueReification.ReifyCtx): Expr =
      callee match {
        // This function is in the top-level global context
        case _: Value.Null => Expr.Id(name, Span.Null)
        case callee =>
          Expr.MethodChain(
            ctx.save(callee),
            Seq(
              Expr.MethodChain.Select(
                Span.Null,
                Name(name, Span.Null),
                false
              )
            ),
            Span.Null
          )
      }
  }
}
