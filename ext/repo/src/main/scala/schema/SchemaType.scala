package fauna.repo.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.repo.query.Query
import fauna.storage.ir._
import fql.ast.{ Literal, Span }
import fql.typer.Type
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.collection.mutable.{ Map => MMap }
import scala.language.implicitConversions

/** This ValueType, while poorly named, is effectively a thin wrapper around
  * `fql.typer.Type`. It is used at runtime to validate values against a schema.
  * It is required to store which fields of documents are read only, internal,
  * and what validators they have.
  *
  * TODO: At some point, this should be moved to `fql`, so that users can
  * declare read-only fields and such directly in FSL, and then we can remove
  * this duplicate type tree.
  */
sealed trait SchemaType {
  def isNullable: Boolean = false

  def internalType: SchemaType = this

  def toFQL: Type
}

sealed trait ScalarType {
  def toFQL: Type
}

// FIXME: Unify with ValueType.
object ScalarType {
  implicit def scalarToType(s: ScalarType): SchemaType = SchemaType.Scalar(s)
  implicit def scalarToField(s: ScalarType): FieldSchema = FieldSchema(s)

  final case object Any extends ScalarType { def toFQL = Type.Any }
  final case object Null extends ScalarType { def toFQL = Type.Null }
  final case object Int extends ScalarType { def toFQL = Type.Int }
  final case object Long extends ScalarType { def toFQL = Type.Long }
  final case object Double extends ScalarType { def toFQL = Type.Double }
  final case object Number extends ScalarType { def toFQL = Type.Number }
  final case object Boolean extends ScalarType { def toFQL = Type.Boolean }
  final case object Str extends ScalarType { def toFQL = Type.Str }
  final case object Time extends ScalarType { def toFQL = Type.Time }
  final case object Date extends ScalarType { def toFQL = Type.Date }
  final case object Bytes extends ScalarType { def toFQL = Type.Bytes }
  final case object UUID extends ScalarType { def toFQL = Type.UUID }

  // The FQL type for this is kinda dumb, especially considering that it could
  // conflict with the environment. But it renders nicely, and its only used for v4,
  // so its probably fine.
  final case object V4Query extends ScalarType { def toFQL = Type.Named("Query") }

  // This type is normally not persistable, but the `id` field of documents is an
  // ID, so it can be used there when creating documents.
  final case object ID extends ScalarType { def toFQL = Type.ID }

  // This is only used in native collections.
  final case object AnyDoc extends ScalarType { def toFQL = Type.AnyDoc }

  object Singleton {
    def apply(v: Long): Singleton = Singleton(Literal.Int(v))
    def apply(v: String): Singleton = Singleton(Literal.Str(v))
  }

  final case class Singleton(lit: Literal) extends ScalarType {
    def toFQL = Type.Singleton(lit)
  }

  // FIXME: `toFQL` should return `Query[Type]` and lookup the collection name at
  // runtime.
  final case class DocType(coll: CollectionID, collName: String) extends ScalarType {
    def toFQL = Type.Named(collName)
  }
  final case class NullDocType(coll: CollectionID, collName: String)
      extends ScalarType {
    def toFQL = Type.Named("Null" + collName)
  }
}

object SchemaType {
  import ConstraintFailure._

  implicit def typeToField(t: SchemaType): FieldSchema = FieldSchema(t)

  final case class Scalar(scalar: ScalarType) extends SchemaType {
    override def isNullable = scalar == ScalarType.Null

    def toFQL = scalar.toFQL
  }

  object ObjectType {
    def apply(fields: Map[String, FieldSchema]): ObjectType = ObjectType(
      StructSchema(fields))
  }

  final case class ObjectType(schema: StructSchema) extends SchemaType {
    override def internalType: ObjectType = {
      this.copy(
        schema = schema.internalSchema
      )
    }

    def toFQL = Type.Record(
      schema.fields
        .map { case (n, f) =>
          n -> f.expected.toFQL
        }
        .to(SeqMap),
      schema.wildcard.map(_.toFQL),
      Span.Null)
  }

  final case class Array(inner: SchemaType) // is an AnyArrayType
      extends SchemaType {
    override def internalType: Array = this.copy(
      inner = inner.internalType
    )

    def toFQL = Type.Array(inner.toFQL)
  }

  object Tuple {
    def apply(inner: SchemaType*): Tuple = Tuple(inner.toVector)
  }

  final case class Tuple(
    inner: Vector[SchemaType]) // is an ArrayType of the union of all inner types
      extends SchemaType {
    override def internalType: Tuple = this.copy(
      inner = inner.map(_.internalType)
    )

    def toFQL = Type.Tuple(inner.map(_.toFQL).to(ArraySeq), Span.Null)
  }

  final case class Union(members: Vector[SchemaType]) extends SchemaType {
    override def isNullable = members.exists { _.isNullable }

    override def internalType: Union = this.copy(
      members = members.map(_.internalType)
    )

    def toFQL: Type.Union = Type.Union(members.map(_.toFQL).to(ArraySeq), Span.Null)
  }

  object Union {
    def apply(members: SchemaType*): Union = Union(members.toVector)
  }

  val AnyRecord = ObjectType(StructSchema.dynamic)
  def Record(fields: (String, FieldSchema)*): SchemaType =
    SchemaType.ObjectType(SchemaType.StructSchema(fields.toMap))

  def Optional(tpe: SchemaType): SchemaType = tpe match {
    case Union(members) => Union(members :+ Scalar(ScalarType.Null))
    case _              => Union(tpe, ScalarType.Null)
  }

  def ZeroOrMore(tpe: SchemaType): SchemaType =
    Optional(Union(tpe, Array(tpe)))

  def OneOrMore(tpe: SchemaType): SchemaType =
    Union(tpe, Array(tpe))

  final case class StructSchema(
    fields: Map[String, FieldSchema],
    wildcard: Option[SchemaType] = None,
    // Indicates this is a container of user data, so the prefix up to this point
    // should be suppressed from errors, e.g. "missing field foo" instead of
    // "missing field data.foo".
    isUserData: Boolean = false,
    alias: Option[String] = None
  ) {
    lazy val aliases = fields.flatMap { case (name, f) =>
      f.aliasTo.map(_ -> name)
    }

    def internalSchema: StructSchema = this.copy(
      fields = fields.map { case (name, schema) =>
        (name, schema.internalSchema)
      },
      wildcard = wildcard.map(_.internalType)
    )

    // FIXME: `alias` was previously used to make a `Type.Named` here, but there are
    // no longer any named types. Instead, `alias` should be reworked into a
    // collection ID, and this should produce a `DocType`, that then looks up the
    // name for a collection when rendering an error.
    def tpe = ObjectType(this)
    def field(name: String): Option[FieldSchema] =
      fields.get(name).orElse { wildcard.map(FieldSchema(_)) }
  }
  object StructSchema {
    val dynamic = StructSchema(Map.empty[String, FieldSchema], Some(ScalarType.Any))

    def wildcard(tpe: SchemaType) =
      StructSchema(Map.empty[String, FieldSchema], Some(tpe))
  }

  def isInt(v: IRValue) = v match {
    case LongV(v) if v >= scala.Int.MinValue && v <= scala.Int.MaxValue => true
    case _                                                              => false
  }

  def valueBaseType(v: IRValue): SchemaType = v match {
    case NullV                          => ScalarType.Null
    case _: BooleanV                    => ScalarType.Boolean
    case LongV(_) if isInt(v)           => ScalarType.Int
    case LongV(_)                       => ScalarType.Long
    case DoubleV(_)                     => ScalarType.Double
    case StringV(_)                     => ScalarType.Str
    case BytesV(_)                      => ScalarType.Bytes
    case TransactionTimeV(_) | TimeV(_) => ScalarType.Time
    case DateV(_)                       => ScalarType.Date
    case UUIDV(_)                       => ScalarType.UUID
    case DocIDV(id)                     => ScalarType.DocType(id.collID, "Document")
    case QueryV(_, _)                   => ScalarType.V4Query
    case ArrayV(v) => SchemaType.Tuple(v.map(valueBaseType).to(Vector))
    case MapV(f) =>
      SchemaType.ObjectType(f.map { case (k, v) =>
        k -> FieldSchema(valueBaseType(v))
      }.toMap)
  }

  def valuePreciseType(v: IRValue): SchemaType = v match {
    // FIXME: Lookup collection name in `DocType`
    case DocIDV(DocID(_, coll)) => ScalarType.DocType(coll, "Document")

    case ArrayV(elems) =>
      val tpes = elems.view.map { valuePreciseType(_) }.to(Vector)
      SchemaType.Tuple(tpes)

    case MapV(fields) =>
      val fs = fields.view.map { case (n, v) =>
        n -> FieldSchema(valuePreciseType(v))
      }.toMap
      SchemaType.ObjectType(fs)

    case v => valueBaseType(v)
  }

  private def isLitValue(lit: Literal, v: IRValue) = lit match {
    case Literal.Null  => v == NullV
    case Literal.True  => v == TrueV
    case Literal.False => v == FalseV
    case Literal.Int(num) =>
      if (num.isValidLong) v == LongV(num.toLong) else false
    case Literal.Float(num) => v == DoubleV(num.toDouble)
    case Literal.Str(s)     => v == StringV(s)
  }

  private def superType(lit: Literal) = lit match {
    case Literal.Null     => ScalarType.Null
    case Literal.True     => ScalarType.Boolean
    case Literal.False    => ScalarType.Boolean
    case Literal.Int(_)   => ScalarType.Number
    case Literal.Float(_) => ScalarType.Number
    case Literal.Str(_)   => ScalarType.Str
  }

  def validate(
    expected: SchemaType,
    prefix: Path.Prefix,
    value: IRValue,
    updateFrom: => Option[IRValue]
  ): Query[Seq[FieldConstraintFailure]] = {

    val baseType = valueBaseType(value)

    (expected, baseType) match {
      case (ObjectType(struct), _: SchemaType.ObjectType) =>
        validateStruct(struct, prefix, value, updateFrom)

      case (Array(inner), SchemaType.Tuple(_)) =>
        val arr = value.asInstanceOf[ArrayV]
        validateArray(Iterator.continually(inner), prefix, arr, updateFrom)

      case (e @ Tuple(inner), SchemaType.Tuple(_)) =>
        val arr = value.asInstanceOf[ArrayV]

        if (arr.elems.size != inner.size) {
          Query.value(failInvalidArity(prefix.toPath, e, value))
        } else {
          validateArray(inner.iterator, prefix, arr, updateFrom)
        }

      case (e: Union, _) => validateUnion(e, prefix, value, updateFrom)

      // Special case to produce a nicer error.
      case (Scalar(ScalarType.DocType(coll, _)), _) =>
        value match {
          case ref: DocIDV =>
            if (ref.value.collID == coll) {
              Query.value(Seq.empty)
            } else {
              Query.value(failCollectionMismatch(prefix.toPath, expected.toFQL, ref))
            }
          case _ => Query.value(failTypeMismatch(prefix.toPath, expected, value))
        }
      case (Scalar(ScalarType.NullDocType(coll, _)), _) =>
        value match {
          case ref: DocIDV =>
            if (ref.value.collID == coll) {
              Query.value(Seq.empty)
            } else {
              Query.value(failCollectionMismatch(prefix.toPath, expected.toFQL, ref))
            }
          case _ => Query.value(failTypeMismatch(prefix.toPath, expected, value))
        }

      case (Scalar(ScalarType.Singleton(lit)), p) =>
        if (isLitValue(lit, value)) {
          Query.value(Nil)
        } else if (Scalar(superType(lit)) == p) {
          // in the case where the provided value is of the same general type of
          // the singleton, we report the provided type as a singleton type as
          // well in order to make the resulting error message more
          // intelligible.
          //
          // Consider '"foo" expected, string provided' vs '"foo" expected, "bar"
          // provided'
          val actual = value match {
            case NullV      => Literal.Null
            case TrueV      => Literal.True
            case FalseV     => Literal.False
            case LongV(v)   => Literal.Int(v)
            case DoubleV(v) => Literal.Float(v)
            case StringV(s) => Literal.Str(s)
            case _          => sys.error("unreachable")
          }
          Query.value(
            List(
              TypeMismatch(prefix.toPath, expected, ScalarType.Singleton(actual))))
        } else {
          Query.value(failTypeMismatch(prefix.toPath, expected, value))
        }

      // `validatePure` doesn't check for immutability or run any custom validators,
      // so it may only be run after all the above cases have failed.
      case _ =>
        Query.value(if (isValueOfType(expected, value)) {
          Nil
        } else {
          failTypeMismatch(prefix.toPath, expected, value)
        })
    }
  }

  // Validates `value` against the given schema type. This does not check for
  // immutability or run any custom validators.
  //
  // Returns `true` if the value is of the expected type, `false` otherwise.
  def isValueOfType(expected: SchemaType, value: IRValue): Boolean = {
    val baseType = valueBaseType(value)

    (expected, baseType) match {
      // Scalar subtypes.
      case (Scalar(ScalarType.Any), _) => true

      case (
            Scalar(ScalarType.AnyDoc),
            Scalar(ScalarType.DocType(_, _) | ScalarType.NullDocType(_, _))) =>
        true

      case (
            Scalar(ScalarType.DocType(expectedColl, _)),
            Scalar(ScalarType.DocType(providedColl, _)))
          if expectedColl == providedColl =>
        true

      case (
            Scalar(ScalarType.NullDocType(expectedColl, _)),
            Scalar(ScalarType.NullDocType(providedColl, _)))
          if expectedColl == providedColl =>
        true

      case (Scalar(ScalarType.Long), Scalar(ScalarType.Int)) => true
      case (
            Scalar(ScalarType.Number),
            Scalar(ScalarType.Long | ScalarType.Int | ScalarType.Double)) =>
        true

      case (Scalar(ScalarType.Singleton(lit)), _) => isLitValue(lit, value)

      // FIXME: These types (DocType, NullDocType) should validate if the doc exists
      // or not. Because we don't have referential integrity yet, we don't actually
      // need to bother.
      case (Scalar(ScalarType.DocType(coll, _)), _) =>
        value match {
          case ref: DocIDV => ref.value.collID == coll
          case _           => false
        }
      case (Scalar(ScalarType.NullDocType(coll, _)), _) =>
        value match {
          case ref: DocIDV => ref.value.collID == coll
          case _           => false
        }

      // Aggregate types.

      case (ObjectType(struct), _: SchemaType.ObjectType) =>
        isValueOfStruct(struct, value)

      case (Array(inner), SchemaType.Tuple(_)) =>
        val arr = value.asInstanceOf[ArrayV]
        isValueOfArray(Iterator.continually(inner), arr)

      case (Tuple(inner), SchemaType.Tuple(_)) =>
        val arr = value.asInstanceOf[ArrayV]

        if (arr.elems.size != inner.size) {
          false
        } else {
          isValueOfArray(inner.iterator, arr)
        }

      case (e: Union, _) => e.members.exists { tpe => isValueOfType(tpe, value) }

      // NB. Custom validators are considered equal, so this case can only be run
      // after the above cases have failed.
      case (e, p) => e == p
    }
  }

  // Constraint failure helpers

  private def failTypeMismatch(path: Path, expected: SchemaType, value: IRValue) = {
    val provided = valuePreciseType(value)
    List(TypeMismatch(path, expected, provided))
  }

  private def failCollectionMismatch(path: Path, expected: Type, value: DocIDV) = {
    List(CollectionMismatch(path, expected, value.value.collID))
  }

  private def failInvalidField(
    fieldPath: Path,
    expected: SchemaType,
    field: String,
    container: MapV) = {
    val containerType = valuePreciseType(container)
    List(InvalidField(fieldPath, expected, containerType, field))
  }

  private def isFieldUpdate(prev: Option[IRValue], value: IRValue): Boolean = {
    prev match {
      case Some(prev) if prev != value => true
      // When a field is not present in an update, NullV is passed as the value
      case None if value != NullV => true
      case _                      => false
    }
  }

  private def failMissingField(
    fieldPath: Path,
    expected: SchemaType,
    field: String) =
    List(MissingField(fieldPath, expected, field))

  private def failInvalidArity(
    path: Path,
    expected: SchemaType.Tuple,
    value: IRValue) = {
    val provided = valuePreciseType(value)
    assert(provided.isInstanceOf[SchemaType.Tuple])
    List(InvalidTupleArity(path, expected, provided.asInstanceOf[SchemaType.Tuple]))
  }

  // Validation helpers

  def validateDocRef(
    expected: SchemaType,
    expectedID: CollectionID,
    prefix: Path.Prefix,
    value: IRValue
  ): Seq[FieldConstraintFailure] =
    value match {
      case DocIDV(DocID(_, coll)) if expectedID == coll => Nil
      case _ => failTypeMismatch(prefix.toPath, expected, value)
    }

  def validateArray(
    expected: Iterator[SchemaType],
    prefix: Path.Prefix,
    value: ArrayV,
    updateFrom: => Option[IRValue]
  ): Query[Seq[FieldConstraintFailure]] = {

    // memoize call-by-name updateFrom
    lazy val updateFrom0 = updateFrom
    def getUpdateFrom(i: Int) =
      updateFrom0.map {
        case ArrayV(es) => es.lift(i).getOrElse(NullV)
        case _          => NullV
      }

    expected
      .zip(value.elems)
      .zipWithIndex
      .map { case ((e, v), idx) =>
        validate(e, prefix :+ idx, v, getUpdateFrom(idx))
      }
      .to(Iterable)
      .sequence
      .map { _.flatten }
  }

  def isValueOfArray(
    expected: Iterator[SchemaType],
    value: ArrayV
  ): Boolean =
    expected
      .zip(value.elems)
      .forall { case (e, v) => isValueOfType(e, v) }

  private def isScalar(ty: SchemaType) = ty match {
    case SchemaType.Scalar(_) => true
    case _                    => false
  }

  def validateUnion(
    expected: Union,
    prefix: Path.Prefix,
    value: IRValue,
    updateFrom: => Option[IRValue]
  ): Query[Seq[FieldConstraintFailure]] = {
    lazy val updateFrom0 = updateFrom
    expected.members
      .map { tpe => validate(tpe, prefix, value, updateFrom0) }
      .sequence
      .map { checks => checkUnion(checks, expected, prefix, value) }
  }

  def checkUnion(
    checks: Seq[Seq[FieldConstraintFailure]],
    expected: Union,
    prefix: Path.Prefix,
    value: IRValue) = {
    if (checks.exists { _.isEmpty }) {
      Nil
    } else {
      val path = prefix.toPath

      // reject based on invalid field update first (immutable, readonly).
      // This means the type was actually compatible.
      def immUpdate = checks
        .find {
          _.exists { cf =>
            cf.isInstanceOf[ImmutableFieldUpdate] || cf
              .isInstanceOf[ReadOnlyFieldUpdate]
          }
        }

      // Try to narrow reported failures based on heuristics:
      // - if present, collate invalid fields and missing fields and report
      // those.
      // - if exactly one of the variants has structural failures, then report
      // just those.
      // - if exactly one set of structural failures does not involve invalid
      // (extra) fields, or missing fields report those.
      //
      // TODO: Reporting will be more precise if types are
      // normalized, both by flattening out nested unions and rewriting
      // unions of similar types to push shape differences as far out to
      // leaves as possible. For example, the types { "a": string | boolean }
      // and { "a": string } | { "a": boolean } are equivalent, but the
      // former will result in a failure within the "a" field as opposed to
      // the top-level union. We will rely on consumers of this module to do
      // type normalization.
      def structFailure = {
        lazy val structChecks = checks.filter { _.forall { _.path != path } }
        lazy val nonInvalid = structChecks.filterNot {
          _.exists { v =>
            v.isInstanceOf[InvalidField] || v.isInstanceOf[MissingField]
          }
        }

        // Compare invalid (extra) fields between variants. If all variants
        // shun the field, keep the failure. Otherwise, drop it.
        // TODO: Sometimes this causes useful invalid field errors to be
        //            dropped from check failures against optional fields.
        //            See the unique constraint definition test case.
        val invalid = structChecks.flatten
          .filter(_.isInstanceOf[InvalidField])
          .groupBy(_.path)
          .filter({ case (_, extra) => extra.sizeIs == expected.members.size })
          .map(_._2.head)

        // Gather missing fields within each variant into a union-specific
        // failure.
        // If some member has no missing fields, drop the errors.
        val missing = structChecks
          .map(_.collect { case e: MissingField => e })
          .distinctBy(_.map { m => (m.path, m.field) })
        val tail = if (missing.nonEmpty) {
          if (missing.sizeIs == 1 && missing.head.sizeIs == 1) {
            // Actually, it all reduces to a single missing field.
            Seq(missing.head.head)
          } else {
            Seq(UnionMissingFields(path, missing))
          }
        } else {
          Seq.empty
        }
        val invalidOrMissing =
          if (missing.isEmpty || missing.exists { _.isEmpty }) { invalid }
          else { invalid ++ tail }

        if (invalidOrMissing.nonEmpty) {
          Some(invalidOrMissing.toSeq)
        } else if (structChecks.sizeIs == 1) {
          Some(structChecks.head)
        } else if (nonInvalid.sizeIs == 1) {
          Some(nonInvalid.head)
        } else {
          None
        }
      }

      // fall back to reporting a mismatch against the union
      // TODO: report complex failure details for multiple sets of
      // structural failures.
      def fallbackFailure = {
        // If an inner mismatch reports a provided singleton type, we want
        // to use that as the provided type. See comment on singleton type
        // checking above. Otherwise if we've already calculated the value's
        // precise type we can reuse it.
        var prov = Option.empty[SchemaType]
        var done = false
        val iter = checks.iterator.flatten
        if (iter.hasNext && !done) {
          iter.next() match {
            case TypeMismatch(path0, _, p) if path0 == path =>
              prov = Some(p)
              if (isScalar(p)) done = true
            case _ =>
          }
        }

        prov match {
          case Some(p) => List(TypeMismatch(path, expected, p))
          case None    => failTypeMismatch(path, expected, value)
        }
      }

      immUpdate orElse structFailure getOrElse fallbackFailure
    }
  }

  def validateStruct(
    struct: StructSchema,
    prefix: Path.Prefix,
    value: IRValue,
    updateFrom: => Option[IRValue]
  ): Query[Seq[FieldConstraintFailure]] = {

    val (container, values) = value match {
      case c @ MapV(fs) => (c, fs)
      case _ =>
        return Query.value(failTypeMismatch(prefix.toPath, struct.tpe, value))
    }

    // memoize call-by-name updateFrom
    lazy val updateFrom0 = updateFrom
    def getUpdateFrom(f: String) =
      updateFrom0.flatMap {
        case MapV(fs) => fs.find { _._1 == f }.map { _._2 }
        case _        => None
      }

    // This MMap is only updated in the `map` call below, before entering a `Query`,
    // so it is safe.
    val missing: MMap[String, FieldSchema] = struct.fields.to(MMap)

    val validateQs = values.map { case (name0, value) =>
      val name = struct.aliases.getOrElse(name0, name0)
      val field = struct.field(name)
      if (field.isDefined) missing -= name

      // For-non meta fields of user collections, reset the prefix so that errors
      // don't have an extra `data` element at the front.
      // This is the ideal code format. You may not like it, but this is what peak
      // scalafmt performance looks like.
      val p =
        if (
          struct.isUserData
          && prefix.elems == List(Right("data"))
          && !CollectionSchema.DisplayedMetaFields.contains(name)
        ) {
          Path.RootPrefix
        } else {
          prefix
        }

      field match {
        case Some(field) =>
          validateField(
            struct,
            p,
            name,
            field,
            container,
            value,
            getUpdateFrom(name))
        case None =>
          Query.value(
            failInvalidField((p :+ name).toPath, struct.tpe, name, container))
      }
    }

    // Missing values in input are equivalent to null values.
    val missingValueQs = missing.iterator
      .map { case (name, field) =>
        // For-non meta fields of user collections, reset the prefix so that errors
        // don't have an extra `data` element at the front.
        val p =
          if (
            struct.isUserData
            && prefix.elems == List(Right("data"))
            && !CollectionSchema.DisplayedMetaFields.contains(name)
          ) {
            Path.RootPrefix
          } else {
            prefix
          }

        validateField(struct, p, name, field, container, NullV, getUpdateFrom(name))
      }
      .to(Iterable)

    for {
      fails1 <- validateQs.sequence
      fails2 <- missingValueQs.sequence
    } yield {
      (fails1 ++ fails2).flatten
    }
  }

  def isValueOfStruct(struct: StructSchema, value: IRValue): Boolean = {
    val values = value match {
      case MapV(fs) => fs
      case _        => return false
    }

    val missing: MMap[String, FieldSchema] = struct.fields.to(MMap)

    val validateCheck = values.forall { case (name0, value) =>
      val name = struct.aliases.getOrElse(name0, name0)
      val field = struct.field(name)
      if (field.isDefined) missing -= name

      field match {
        case Some(field) => isValueOfType(field.expected, value)
        case None        => false
      }
    }

    // Missing values in input are equivalent to null values.
    val missingValuesCheck = missing.forall { case (_, field) =>
      isValueOfType(field.expected, NullV)
    }

    validateCheck && missingValuesCheck
  }

  def validateField(
    struct: StructSchema,
    prefix: Path.Prefix,
    name: String,
    schema: FieldSchema,
    container: MapV,
    value: IRValue,
    updateFrom: => Option[IRValue]
  ): Query[Seq[FieldConstraintFailure]] = {
    val fieldPrefix = prefix :+ name

    // internal fields should never be present on the update. However, when we
    // validate on a patched update,
    // they currently come through but are unchanged in that case.
    // Because of this, we allow them to be present as long as they are unchanged.
    // We would need to do validation on purely the incoming data to be able to
    // outright prevent them.
    // This does mean that if a customer passes an internal field in with the
    // exact same value that is already present, we will accept it.
    if (schema.internal && (value != NullV && isFieldUpdate(updateFrom, value))) {
      return Query.value(
        failInvalidField(fieldPrefix.toPath, struct.tpe, name, container))
    }

    // for read only fields it is ok if it is not present, because we will make sure
    // those are maintained internally.  It is also ok if it is present but the same
    // as the current value.
    if (schema.readOnly && (value != NullV && isFieldUpdate(updateFrom, value))) {
      return Query.value(
        List(ReadOnlyFieldUpdate(fieldPrefix.toPath, struct.tpe, name)))
    }

    if (schema.immutable) {
      updateFrom match {
        case Some(prev) if prev != value =>
          return Query.value(
            List(ImmutableFieldUpdate(fieldPrefix.toPath, struct.tpe, name)))
        case _ => ()
      }
    }

    validate(schema.expected, fieldPrefix, value, updateFrom) flatMap {
      // Handle missing/optional field
      case Seq(TypeMismatch(fieldPath, _, Scalar(ScalarType.Null)))
          if value == NullV && fieldPath == fieldPrefix.toPath =>
        if (schema.optional) {
          Query.value(Nil)
        } else {
          Query.value(failMissingField(fieldPath, schema.expected, name))
        }

      // If no failures found, run the custom validator too.
      case Seq() => schema.validator(fieldPrefix, value, updateFrom)

      case errs => Query.value(errs)
    }
  }
}
