package fauna.repo.schema.migration

import fauna.atoms._
import fauna.repo.schema.{ FieldSchema, ScalarType, SchemaType }
import fauna.storage.doc.{ ConcretePath, Field, FieldType, ValidationException }
import fauna.storage.ir._
import fql.ast.Literal
import scala.collection.immutable.Queue

object MigrationCodec {
  import SchemaTypeCodec._

  implicit val schemaTypeCodec = new FieldType[SchemaType] {
    override def vtype: IRType = StringV.Type

    override def decode(
      value: Option[IRValue],
      path: Queue[String]): Either[List[ValidationException], SchemaType] =
      value match {
        case Some(v) => Right(SchemaTypeCodec.decode(v))
        case v =>
          throw new IllegalStateException(
            s"expected string for internal migration type, got $v")
      }

    override def encode(t: SchemaType): Option[IRValue] = Some(t.encode)
  }

  private implicit val pathCodec = new FieldType[ConcretePath] {
    override def vtype: IRType = ArrayV.Type
    override def decode(
      value: Option[IRValue],
      path: Queue[String]): Either[List[ValidationException], ConcretePath] =
      value match {
        case Some(v: ArrayV) => Right(ConcretePath.decode(v))
        case v =>
          throw new IllegalStateException(
            s"expected array for internal migration path, got $v")
      }

    override def encode(t: ConcretePath): Option[IRValue] = Some(t.encode)
  }
  implicit class MigrationOps(m: Migration) {
    def encode: MapV = MigrationCodec.encode(m)
  }

  private val TypeField = Field[String]("type")
  private val PathField = Field[ConcretePath]("path")
  private val IntoField = Field[ConcretePath]("into")
  private val MoveToField = Field[ConcretePath]("move_to")
  private val ValueField = Field[Option[IRValue]]("value")
  private val ReplaceField = Field[Option[IRValue]]("replace")
  private val BackfillField = Field[Option[IRValue]]("backfill")
  private val ExpectedField = Field[SchemaType]("expected")
  private val DiscriminatorField = Field[SchemaType]("discriminator")
  private val KeepField = Field[Vector[String]]("keep")

  private val AddFieldType = "add"
  private val DropFieldType = "drop"
  private val MoveFieldType = "move"
  private val MoveConflictingFieldsType = "move_conflicting"
  private val SplitFieldType = "split"
  private val MoveWildcardType = "move_wildcard"

  def decode(v: MapV) = {
    TypeField(v) match {
      case AddFieldType =>
        Migration.AddField(
          PathField(v),
          DiscriminatorField(v),
          ValueField(v).getOrElse(NullV))
      case DropFieldType => Migration.DropField(PathField(v))
      case MoveFieldType => Migration.MoveField(PathField(v), MoveToField(v))
      case MoveConflictingFieldsType =>
        Migration.MoveConflictingFields(IntoField(v))
      case SplitFieldType =>
        Migration.SplitField(
          PathField(v),
          MoveToField(v),
          ExpectedField(v),
          ReplaceField(v).getOrElse(NullV),
          BackfillField(v).getOrElse(NullV))
      case MoveWildcardType =>
        Migration.MoveWildcard(PathField(v), IntoField(v), KeepField(v).toSet)

      case ty => throw new IllegalStateException(s"invalid migration type $ty")
    }
  }

  def encode(m: Migration) = m match {
    case Migration.AddField(path, discriminator, value) =>
      MapV(
        TypeField.path.head -> AddFieldType,
        PathField.path.head -> path.encode,
        DiscriminatorField.path.head -> discriminator.encode,
        ValueField.path.head -> value
      )

    case Migration.DropField(path) =>
      MapV(
        TypeField.path.head -> DropFieldType,
        PathField.path.head -> path.encode
      )

    case Migration.MoveField(from, to) =>
      MapV(
        TypeField.path.head -> MoveFieldType,
        PathField.path.head -> from.encode,
        MoveToField.path.head -> to.encode
      )

    case Migration.MoveConflictingFields(into) =>
      MapV(
        TypeField.path.head -> MoveConflictingFieldsType,
        IntoField.path.head -> into.encode
      )

    case Migration.SplitField(from, to, expected, replace, backfill) =>
      MapV(
        TypeField.path.head -> SplitFieldType,
        PathField.path.head -> from.encode,
        MoveToField.path.head -> to.encode,
        ExpectedField.path.head -> expected.encode,
        ReplaceField.path.head -> replace,
        BackfillField.path.head -> backfill
      )

    case Migration.MoveWildcard(field, into, keep) =>
      MapV(
        TypeField.path.head -> MoveWildcardType,
        PathField.path.head -> field.encode,
        IntoField.path.head -> into.encode,
        KeepField.path.head -> ArrayV(keep.view.map(StringV(_)).toSeq: _*)
      )

    case _ => MapV()
  }

  implicit val migrationCodec = new FieldType[Migration] {
    def vtype = MapV.Type
    def decode(
      value: Option[IRValue],
      path: Queue[String]): Either[List[ValidationException], Migration] = {
      value match {
        case Some(v: MapV) => Right(MigrationCodec.decode(v))
        case v =>
          throw new IllegalStateException(
            s"expected map for internal migration, got $v")
      }
    }

    def encode(t: Migration): Option[IRValue] = Some(t.encode)
  }

  implicit val schemaVersionCodec = new FieldType[SchemaVersion] {
    def vtype = LongV.Type
    def decode(
      value: Option[IRValue],
      path: Queue[String]): Either[List[ValidationException], SchemaVersion] = {
      value match {
        case Some(TimeV(v))            => Right(SchemaVersion(v))
        case Some(TransactionTimeV(_)) => Right(SchemaVersion.Pending)
        case v =>
          throw new IllegalStateException(
            s"expected long for internal migration version, got $v")
      }
    }

    def encode(t: SchemaVersion): Option[IRValue] = Some(TimeV(t.ts))
  }

  val MigrationField = Field[Migration]("migration")
  val SchemaVersionField = Field[SchemaVersion]("version")

  implicit val migrationListCodec = new FieldType[MigrationList] {
    def vtype = ArrayV.Type
    def decode(
      value: Option[IRValue],
      path: Queue[String]): Either[List[ValidationException], MigrationList] = {
      value match {
        case Some(ArrayV(elems)) =>
          Right(MigrationList(elems.view.map {
            case m: MapV => SchemaVersionField(m) -> MigrationField(m)
            case v       => throw new IllegalStateException(s"Invalid migration $v")
          }))

        case Some(v) => throw new IllegalStateException(s"Invalid migration list $v")
        case None    => Right(MigrationList.empty)
      }
    }

    def encode(t: MigrationList): Option[IRValue] = Some(t.encode0 { (ver, m) =>
      MapV(
        SchemaVersionField.path.head -> TimeV(ver.ts),
        MigrationField.path.head -> m.encode)
    })
  }
}

// The basic format for schema types is a string for scalars, and a map with a
// `type` field for everything else. It's not the prettiest thing in the world,
// but it encodes and decodes nicely to documents, and is expandable when we
// want more types.
//
// For example:
// ```
// String | Null
//
// {
//   "type": "Union",
//   "elems": ["Str", "Null"]
// }
// ```
//
// ```
// {
//   foo: Int | String,
//   bar: Array<Int>,
// }
//
// {
//   "type": "Struct",
//   "fields": {
//     "foo": { "type": "Union", "elems": ["Int", "Str"] },
//     "bar": { "type": "Array", "elem": "Int" }
//   }
// }
// ```
object SchemaTypeCodec {
  import IRValue._
  import MigrationCodec._

  implicit class SchemaTypeOps(s: SchemaType) {
    def encode: IRValue = SchemaTypeCodec.encode(s)
  }

  def decode(v: IRValue): SchemaType = v match {
    case m: MapV    => decodeMapType(m)
    case StringV(s) => decodeStringType(s)
    case _ =>
      throw new IllegalStateException(
        s"expected string or map for internal migration type, got $v")
  }

  private val TypeField = Field[String]("type")
  private val CollField = Field[CollectionID]("coll")
  private val ElemField = Field[SchemaType]("elem")
  private val ElemsField = Field[Vector[SchemaType]]("elems")
  private val WildcardField = Field[Option[SchemaType]]("wildcard")

  private def decodeMapType(m: MapV): SchemaType = {
    val ty = TypeField(m)
    ty match {
      case Name.Singleton =>
        m.get(List("value")) match {
          case Some(StringV(v)) => ScalarType.Singleton(Literal.Str(v))
          case Some(DoubleV(v)) => ScalarType.Singleton(Literal.Float(v))
          case Some(LongV(v))   => ScalarType.Singleton(Literal.Int(v))
          case Some(NullV)      => ScalarType.Singleton(Literal.Null)
          case Some(TrueV)      => ScalarType.Singleton(Literal.True)
          case Some(FalseV)     => ScalarType.Singleton(Literal.False)
          case _ => throw new IllegalStateException(s"invalid singleton value $ty")
        }

      // FIXME: remove the collection name from this field.
      case Name.Doc     => ScalarType.DocType(CollField(m), "Document")
      case Name.NullDoc => ScalarType.NullDocType(CollField(m), "Document")

      // Aggregate types

      // Note that custom validators, immutability, and internal fields are not
      // encode/decoded.
      case Name.Struct =>
        val fields = m.get(List("fields")) match {
          case Some(MapV(fields)) =>
            fields.map { case (k, v) => k -> FieldSchema(decode(v)) }.toMap

          case _ => throw new IllegalStateException(s"invalid object type $ty")
        }

        val wildcard = WildcardField(m)
        SchemaType.ObjectType(SchemaType.StructSchema(fields, wildcard))

      case Name.Array => SchemaType.Array(ElemField(m))
      case Name.Tuple => SchemaType.Tuple(ElemsField(m))
      case Name.Union => SchemaType.Union(ElemsField(m))

      case _ => throw new IllegalStateException(s"invalid type $ty")
    }
  }

  private def decodeStringType(ty: String) = ty match {
    case Name.Any     => ScalarType.Any
    case Name.Null    => ScalarType.Null
    case Name.Int     => ScalarType.Int
    case Name.Long    => ScalarType.Long
    case Name.Double  => ScalarType.Double
    case Name.Number  => ScalarType.Number
    case Name.Boolean => ScalarType.Boolean
    case Name.Str     => ScalarType.Str
    case Name.Time    => ScalarType.Time
    case Name.Date    => ScalarType.Date
    case Name.Bytes   => ScalarType.Bytes
    case Name.UUID    => ScalarType.UUID
    case Name.V4Query => ScalarType.V4Query
    case Name.AnyDoc  => ScalarType.AnyDoc

    case _ => throw new IllegalStateException(s"invalid scalar type $ty")
  }

  private def encode(ty: SchemaType): IRValue = ty match {
    case SchemaType.Scalar(s) => encodeScalarType(s)

    case SchemaType.ObjectType(s) =>
      val fields: List[(String, IRValue)] = List(
        TypeField.path.head -> Name.Struct,
        "fields" -> MapV(s.fields.view.map { case (k, v) =>
          k -> encode(v.expected)
        }.toList)
      )

      MapV(fields ++ s.wildcard.map(v => WildcardField.path.head -> encode(v)))

    case SchemaType.Array(elem) =>
      MapV(
        TypeField.path.head -> Name.Array,
        ElemField.path.head -> encode(elem)
      )
    case SchemaType.Tuple(elems) =>
      MapV(
        TypeField.path.head -> Name.Tuple,
        ElemsField.path.head -> ArrayV(elems.map(encode))
      )
    case SchemaType.Union(elems) =>
      MapV(
        TypeField.path.head -> Name.Union,
        ElemsField.path.head -> ArrayV(elems.map(encode))
      )
  }

  private def encodeScalarType(ty: ScalarType): IRValue = ty match {
    case ScalarType.Any     => Name.Any
    case ScalarType.Null    => Name.Null
    case ScalarType.Int     => Name.Int
    case ScalarType.Long    => Name.Long
    case ScalarType.Double  => Name.Double
    case ScalarType.Number  => Name.Number
    case ScalarType.Boolean => Name.Boolean
    case ScalarType.Str     => Name.Str
    case ScalarType.Time    => Name.Time
    case ScalarType.Date    => Name.Date
    case ScalarType.Bytes   => Name.Bytes
    case ScalarType.UUID    => Name.UUID
    case ScalarType.V4Query => Name.V4Query
    case ScalarType.AnyDoc  => Name.AnyDoc

    case ScalarType.ID =>
      throw new IllegalStateException("Cannot use ID type in a migration")

    case ScalarType.DocType(coll, _) =>
      MapV(TypeField.path.head -> Name.Doc, CollField.path.head -> coll)

    case ScalarType.NullDocType(coll, _) =>
      MapV(TypeField.path.head -> Name.NullDoc, CollField.path.head -> coll)

    case ScalarType.Singleton(v) =>
      val value = v match {
        case Literal.Str(v)   => StringV(v)
        case Literal.Float(v) => DoubleV(v.toDouble)
        case Literal.Int(v)   => LongV(v.toLong)
        case Literal.Null     => NullV
        case Literal.True     => TrueV
        case Literal.False    => FalseV
      }

      MapV(
        TypeField.path.head -> Name.Singleton,
        "value" -> value
      )
  }

  object Name {
    // Scalar types encoded as strings.
    val Any = "Any"
    val Null = "Null"
    val Int = "Int"
    val Long = "Long"
    val Double = "Double"
    val Number = "Number"
    val Boolean = "Boolean"
    val Str = "Str"
    val Time = "Time"
    val Date = "Date"
    val Bytes = "Bytes"
    val UUID = "UUID"
    val V4Query = "V4Query"
    val AnyDoc = "AnyDoc"

    // Scalar types with extra info that get encoded as maps.
    val Doc = "Doc"
    val NullDoc = "NullDoc"
    val Singleton = "AnyDoc"

    // Aggregate types encoded as maps.
    val Struct = "Struct"
    val Array = "Array"
    val Tuple = "Tuple"
    val Union = "Union"
  }
}
