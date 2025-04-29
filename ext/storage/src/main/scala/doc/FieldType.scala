package fauna.storage.doc

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.lang._
import fauna.lang.syntax._
import fauna.storage.macros._
import fauna.scheduler.Priority
import fauna.storage.ir._
import language.experimental.macros
import scala.collection.immutable.Queue
import scala.util.Try

object FieldType {

  def RecordCodec[T]: FieldType[T] = macro RecordMacros.genCodec[T]
  def RecordOverrideCodec[T](overrides: (String, String)*): FieldType[T] =
    macro RecordMacros.genOverrideCodec[T]

  /**
    * Given a field definition and a set of sum type variants and
    * their tag values, yields a FieldType which can encode/decode
    * each variant.
    *
    * Each variant must encode to a MapV, and each field value must be
    * unique.
    *
    * Example:
    *   sealed abstract class State
    *   case object Pending extends State
    *   case object Complete extends State
    *
    *   val f = SumCodec[Boolean, State](Field[Boolean]("complete"),
    *     false -> Empty(Pending),
    *     true  -> Empty(Complete))
    */
  def SumCodec[T, U](field: Field[T], variants: (T, FieldType[_ <: U])*): FieldType[U] =
    macro SumMacros.genCodec[U]

  implicit val LongT = new BasicFieldType[Long](LongV.Type, LongV(_), { case LongV(v) => v })
  implicit val IntT = new BasicFieldType[Int](LongV.Type, LongV(_), { case LongV(v) => v.toInt })
  implicit val DoubleT = new BasicFieldType[Double](DoubleV.Type, DoubleV(_), { case LongV(v) => v.toDouble; case DoubleV(v) => v })
  implicit val BooleanT = new BasicFieldType[Boolean](BooleanV.Type, BooleanV(_), { case BooleanV(v) => v })
  implicit val StringT = new BasicFieldType[String](StringV.Type, StringV(_), { case StringV(v) => v })
  implicit val DocIDT = new BasicFieldType[DocID](DocIDV.Type, DocIDV(_), { case DocIDV(v) => v })
  implicit val DataT = new BasicFieldType[Data](MapV.Type, { _.fields }, { case v: MapV => Data(v) })
  implicit val QueryT = new BasicFieldType[QueryV](QueryV.Type, identity[QueryV], { case v: QueryV => v })
  implicit val ScalarT =
    new NullableFieldType[ScalarV](ScalarV.Type, identity, { case v: ScalarV => v })
  implicit val AnyT =
    new NullableFieldType[IRValue](IRType.Custom("Any"), identity, v => v)

  def apply[T](vtype: String)(encode: T => IRValue)(decode: PartialFunction[IRValue, T]) =
    new BasicFieldType[T](IRType.Custom(vtype), encode, decode)

  def validating[T](vtype: String)(encode: T => IRValue)(decode: PartialFunction[IRValue, Either[List[ValidationException], T]]) =
    new ValidatingFieldType[T](IRType.Custom(vtype), encode, decode)

  def Lazy[T](tpe: => FieldType[T]): FieldType[T] =
    new FieldType[T] {
      def vtype = tpe.vtype
      def decode(value: Option[IRValue], path: Queue[String]) = tpe.decode(value, path)
      def encode(t: T) = tpe.encode(t)
    }

  def Empty[T](ctor: T) =
    new FieldType[T] {
      val vtype = IRType.Custom("Empty")
      private val decoded = Right(ctor)
      def decode(v: Option[IRValue], path: Queue[String]) = decoded
      def encode(t: T): Option[IRValue] = None
    }

  /*
   * Given conversion functions between two types, T and U, create a
   * FieldType[T] based on an existing FieldType[U].
   */
  def Alias[T, U](to: T => U, from: U => T)(implicit f: FieldType[U]): FieldType[T] =
    new FieldType[T] {
      val vtype = f.vtype

      def decode(value: Option[IRValue], path: Queue[String]) =
        f.decode(value, path) map from

      def encode(t: T): Option[IRValue] = f.encode(to(t))
    }

  implicit def Tuple2T[S, T](implicit s: FieldType[S], t: FieldType[T]) =
    new FieldType[Tuple2[S, T]] {
      val vtype = IRType.Custom("Tuple2")

      def decode(value: Option[IRValue], path: Queue[String]) =
        value match {
          case Some(ArrayV(vs)) if vs.size == 2 =>
            s.decode(vs.headOption, path :+ "0") flatMap { one =>
              t.decode(vs.lastOption, path :+ "1") map { two =>
                (one, two)
              }
            }

          case Some(v) => Left(List(InvalidType(path.toList, vtype, v.vtype)))
          case None    => Left(List(ValueRequired(path.toList)))
        }

      def encode(tup: Tuple2[S, T]): Option[IRValue] =
        s.encode(tup._1) flatMap { one =>
          t.encode(tup._2) map { two =>
            ArrayV(Vector(one, two))
          }
        }
    }

  implicit def ArrayT[E](implicit e: FieldType[E]) =
    new FieldType[Vector[E]] {
      val vtype = IRType.Array(e.vtype)

      def decode(value: Option[IRValue], path: Queue[String]) = value match {
        case Some(ArrayV(vs)) =>
          (vs map { v => e.decode(Some(v), path) }).sequence map { _.toVector }
        case Some(v)          => Left(List(InvalidType(path.toList, vtype, v.vtype)))
        case None             => Left(List(ValueRequired(path.toList)))
      }

      def encode(vs: Vector[E]): Option[IRValue] = {
        val b = Vector.newBuilder[IRValue]
        vs foreach { v =>
          e.encode(v) foreach { b += _ }
        }
        Some(ArrayV(b.result()))
      }
    }

  implicit def MapT[E](implicit e: FieldType[E]) =
    new FieldType[List[(String, E)]] {
      val vtype = IRType.Map(e.vtype)

      def decode(value: Option[IRValue], path: Queue[String]) = value match {
        case Some(MapV(vs)) =>
          (vs map { case (k, v) => e.decode(Some(v), path :+ k) map { k -> _ } }).sequence map { _.toList }
        case Some(v)        => Left(List(InvalidType(path.toList, vtype, v.vtype)))
        case None           => Left(List(ValueRequired(path.toList)))
      }

      def encode(vs: List[(String, E)]) = {
        val es = vs flatMap { case (k, v) =>
          val enc = e.encode(v)
          if (enc.isDefined) {
            Some(k -> enc.get)
          } else {
            None
          }
        }
        Some(MapV(es))
      }
    }

  /**
    * A field type which may be either undefined (missing/null) or of
    * some type E.
    */
  implicit def OptionT[E](implicit e: FieldType[E]) =
    new FieldType[Option[E]] {
      val vtype = e.vtype
      override val isRequired = false

      def decode(value: Option[IRValue], path: Queue[String]) =
        value match {
          case None        => Right(None)
          case Some(NullV) => Right(None)
          case _           => e.decode(value, path) match {
            case Left(errs) =>
              val typeErr = errs exists {
                case _: InvalidType => true
                case _ => false
              }

              if (typeErr) {
                Left(errs)
              } else {
                Right(None)
              }
            case Right(v) => Right(Some(v))
          }
        }

      def encode(v: Option[E]): Option[IRValue] =
        v flatMap { e.encode(_) }
    }

  /** A field type which may be either type E or type F. */
  implicit def EitherT[E, F](implicit e: FieldType[E], f: FieldType[F]) =
    new FieldType[Either[E, F]] {
      val vtype = IRType.Custom("Either")

      def decode(value: Option[IRValue], path: Queue[String]) =
        f.decode(value, path) match {
          case Left(err) =>
            e.decode(value, path) match {
              case Right(v) => Right(Left(v))
              case _        => Left(err)
            }
          case Right(v) => Right(Right(v))
        }

      def encode(v: Either[E, F]): Option[IRValue] =
        v match {
          case Left(ev)  => e.encode(ev)
          case Right(fv) => f.encode(fv)
        }
    }

  /*
   * Given a type E, allows a field value to consist of either E or
   * List[E], but not undefined.
   */
  def OneOrMoreT[E](t: FieldType[E], l: FieldType[Vector[E]]) =
    new FieldType[Vector[E]] {
      val vtype = l.vtype

      def decode(value: Option[IRValue], path: Queue[String]) = t.decode(value, path) match {
        case Left(err) =>
          // don't throw away the original error if we don't have an array
          value match {
            case Some(ArrayV(_)) => l.decode(value, path)
            case _               => Left(err)
          }
        case Right(v) => Right(Vector(v))
      }

      def encode(v: Vector[E]) = l.encode(v)
    }

  /*
   * As OneOrMoreT, but allows undefined.
   */
  def ZeroOrMoreT[E](t: FieldType[E], l: FieldType[Vector[E]]) =
    new FieldType[Vector[E]] {
      private[this] val oom = OneOrMoreT(t, l)

      val vtype = l.vtype
      override val isRequired = false

      def decode(value: Option[IRValue], path: Queue[String]) =
        oom.decode(value, path) match {
          case Right(v)                     => Right(v)
          case Left(List(ValueRequired(_))) => Right(Vector.empty)
          case Left(errs)                   => Left(errs)
        }

      def encode(v: Vector[E]) = l.encode(v)
    }

  def CBORBytes[T](implicit codec: CBOR.Codec[T]) =
    new FieldType[T] {
      val vtype = IRType.Custom("CBOR bytes")

      def decode(value: Option[IRValue], path: Queue[String]): Either[List[ValidationException], T] =
        value match {
          case Some(b: BytesV) =>
            Try(CBOR.decode(b.value)).toEither.left map { _ =>
              List(ValueRequired(path.toList))
            }
          case Some(v) =>
            Left(List(InvalidType(path.toList, vtype, v.vtype)))
          case None =>
            Left(List(ValueRequired(path.toList)))
        }

      def encode(t: T): Option[IRValue] =
        Some(BytesV(CBOR.encode(t)))
    }

  implicit val SegmentT = FieldType[Segment]("Segment") { rng =>
    ArrayV(StringV(rng.left.toString), StringV(rng.right.toString))
  } {
    case ArrayV(Vector(StringV(start), StringV(end))) =>
      new Segment(Location.fromString(start), Location.fromString(end))
  }

  implicit val TimestampT = FieldType[Timestamp]("Timestamp")(TimeV(_)) { case TimeV(ts) => ts }

  implicit val HostIDT = FieldType[HostID]("HostID") { id => StringV(id.toString) } {
    case StringV(str) => HostID(str)
  }

  implicit val PriorityT = FieldType[Priority]("Priority") {
    case Priority(p) if p >= Priority.MinValue && p <= Priority.MaxValue => LongV(p)
    case p => throw new IllegalStateException(s"Invalid priority $p.")
  } {
    case LongV(p)
        if p >= Priority.MinValue &&
          p <= Priority.MaxValue =>
      Priority(p.toInt)
  }

  implicit val GlobalKeyIDT = FieldType[GlobalKeyID]("GlobalKey") { id =>
    LongV(id.toLong)
  } {
    case LongV(id)
        if id >= GlobalKeyID.MinValue.toLong &&
          id <= GlobalKeyID.MaxValue.toLong =>
      GlobalKeyID(id)
  }

  implicit val ScopeIDT = FieldType[ScopeID]("Scope") { id =>
    LongV(id.toLong)
  } {
    case LongV(id)
        if id >= ScopeID.MinValue.toLong &&
          id <= ScopeID.MaxValue.toLong =>
      ScopeID(id)
  }

  implicit val GlobalDatabaseIDT = FieldType[GlobalDatabaseID]("GlobalDatabase") {
    id => LongV(id.toLong)
  } {
    case LongV(id)
        if id >= GlobalDatabaseID.MinValue.toLong &&
          id <= GlobalDatabaseID.MaxValue.toLong =>
      GlobalDatabaseID(id)
  }

  implicit val CollectionIDT = FieldType[CollectionID]("Class Ref") { id =>
    DocIDV(DocID(SubID(id.toLong), CollectionID.collID))
  } {
    case DocIDV(id) if id.collID == CollectionID.collID => CollectionID(id.subID.toLong)
  }

  implicit val DatabaseIDT = FieldType[DatabaseID]("Database Ref") { id =>
    DocIDV(DocID(SubID(id.toLong), DatabaseID.collID))
  } {
    case DocIDV(id) if id.collID == DatabaseID.collID => DatabaseID(id.subID.toLong)
  }

  implicit val IndexIDT = FieldType[IndexID]("Index Ref") { id =>
    DocIDV(DocID(SubID(id.toLong), IndexID.collID))
  } {
    case DocIDV(id) if id.collID == IndexID.collID => IndexID(id.subID.toLong)
  }

  implicit val TaskIDT = FieldType[TaskID]("Task Ref") { id =>
    DocIDV(DocID(SubID(id.toLong), TaskID.collID))
  } {
    case DocIDV(id) if id.collID == TaskID.collID => TaskID(id.subID.toLong)
  }

  implicit val JournalEntryIDT = FieldType[JournalEntryID]("Journal Entry Ref") { id =>
    DocIDV(DocID(SubID(id.toLong), JournalEntryID.collID))
  } {
    case DocIDV(id) if id.collID == JournalEntryID.collID => JournalEntryID(id.subID.toLong)
  }

  implicit val RoleIDT = FieldType[RoleID]("Role Ref") { id =>
    DocIDV(DocID(SubID(id.toLong), RoleID.collID))
  } {
    case DocIDV(id) if id.collID == RoleID.collID => RoleID(id.subID.toLong)
  }

  implicit val AccountIDT = FieldType[AccountID]("Account") { id =>
    LongV(id.toLong)
  } {
    case LongV(id) => AccountID(id)
  }
}

/**
  * A FieldType[T] is a codec from IRValues to/from a Scala type T
  * (defined via Field[T]). They validate both the type and presence
  * of the field within a Data/Diff object.
  */
trait FieldType[T] {
  def vtype: IRType
  val isRequired = true
  def decode(value: Option[IRValue], path: Queue[String]): Either[List[ValidationException], T]
  def encode(t: T): Option[IRValue]
}

/**
  * The most basic type of field is required to be present and
  * non-null. The decode partial function should only be defined over
  * the types of IRValue which are valid for this Field[T]. For
  * example, a string might be defined as follows:
  *
  * Field[String]("name") { str => StringV(str) } { case StringV(str) => str }
  *
  * The field type of this field is BasicFieldType[T], and only Scala
  * Strings and StringV can represent values of this field type.
  */
class BasicFieldType[T](val vtype: IRType, enc: T => IRValue, dec: PartialFunction[IRValue, T]) extends FieldType[T] {
  def decode(value: Option[IRValue], path: Queue[String]) =
    value match {
      case Some(NullV) => Left(List(ValueRequired(path.toList)))
      case Some(v)     =>
        dec.lift(v) toRight List(InvalidType(path.toList, vtype, v.vtype))
      case None => Left(List(ValueRequired(path.toList)))
    }
  def encode(t: T): Option[IRValue] = Some(enc(t))
}

/**
  * Similar to BasicFieldType, but allows a field to be null (but it
  * must still be present).
  */
class NullableFieldType[T](val vtype: IRType, enc: T => IRValue, dec: PartialFunction[IRValue, T]) extends FieldType[T] {
  def decode(value: Option[IRValue], path: Queue[String]) =
    value match {
      case Some(v)     =>
        dec.lift(v) toRight List(InvalidType(path.toList, vtype, v.vtype))
      case None => Left(List(ValueRequired(path.toList)))
    }
  def encode(t: T): Option[IRValue] = Some(enc(t))
}

/**
  * Similar to NullableFieldType, but accepts a decoder which yields
  * either ValidationExceptions, or a T. This is convenient when a
  * custom field type needs to return a more specific validation
  * exception than InvalidType.
  */
case class ValidatingFieldType[T](vtype: IRType, enc: T => IRValue, dec: PartialFunction[IRValue, Either[List[ValidationException], T]]) extends FieldType[T] {
  def decode(value: Option[IRValue], path: Queue[String]) =
    value match {
      case Some(v) =>
        dec.lift(v) getOrElse Left(List(InvalidType(path.toList, vtype, v.vtype)))
      case None => Left(List(ValueRequired(path.toList)))
    }
  def encode(t: T): Option[IRValue] = Some(enc(t))
}
