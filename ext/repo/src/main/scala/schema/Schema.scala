package fauna.repo.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo.doc._
import fauna.repo.query.Query
import fauna.repo.schema.migration.MigrationList
import fauna.repo.schema.ConstraintFailure.FieldConstraintFailure
import fauna.repo.store.IDStore
import fauna.repo.values.Value
import fauna.repo.IndexConfig
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fql.parser.Tokens
import scala.concurrent.duration.Duration

sealed trait DocIDSource {
  val min: Long
  val max: Long
}

object DocIDSource {
  case object Snowflake extends DocIDSource {
    val min = Long.MinValue
    val max = Long.MaxValue
  }
  final case class Sequential(min: Long, max: Long) extends DocIDSource
}

// A DataMode controls how a schema processes user-provided fields
// into data stored by the system.
sealed trait DataMode

object DataMode {

  // Meta fields like "ttl" are interpreted as meta fields and
  // reserved field names like "update" are disallowed.
  case object Default extends DataMode

  // All fields are interpreted as non-meta fields and reserved field
  // names are allowed.
  case object PlainData extends DataMode
}

sealed trait SchemaOp {
  def mode: DataMode
  def fields: MapV
}

object SchemaOp {
  case class Create(schema: CollectionSchema, mode: DataMode, fields: MapV)
      extends SchemaOp

  case class Update(
    schema: CollectionSchema,
    mode: DataMode,
    fields: MapV,
    prev: Data)
      extends SchemaOp

  case class Replace(
    schema: CollectionSchema,
    mode: DataMode,
    fields: MapV,
    prev: Data)
      extends SchemaOp
}

object CollectionSchema {
  import SchemaType._

  val TSField = "ts"
  val TTLField = "ttl"
  val DataField = "data"
  val DisplayedMetaFields = Seq("id", TSField, TTLField)
  val MetaFields = DisplayedMetaFields.appended(DataField)

  def UserCollectionSchema(
    scope: ScopeID,
    id: CollectionID,
    name: String,
    migrations: MigrationList,
    fields: Map[String, FieldSchema],
    reservedReadFields: List[String] = Nil,
    wildcard: Option[SchemaType] = None,
    indexes: List[IndexConfig] = Nil,
    idSource: DocIDSource = DocIDSource.Snowflake,
    writeHooks: List[WriteHook] = Nil): CollectionSchema = {

    // FIXME: This currently just shifts all under defined schema fields underneath
    // data
    // we may want to take a different approach when we actually release user
    // collection schemas
    val dataFields = Map(
      DataField ->
        FieldSchema(SchemaType.ObjectType(
          SchemaType.StructSchema(fields = fields, wildcard = wildcard))))

    CollectionSchema(
      scope = scope,
      id = id,
      name = name,
      migrations = migrations,
      fields = dataFields,
      indexes = indexes,
      idSource = idSource,
      writeHooks = writeHooks,
      reservedReadFields = reservedReadFields
    )
  }

  def apply(
    scope: ScopeID,
    id: CollectionID,
    name: String,
    migrations: MigrationList,
    fields: Map[String, FieldSchema],
    reservedReadFields: List[String],
    wildcard: Option[SchemaType] = None,
    indexes: List[IndexConfig] = Nil,
    idSource: DocIDSource = DocIDSource.Snowflake,
    writeHooks: List[WriteHook] = Nil,
    defaultTTLDuration: Duration = Duration.Inf
  ): CollectionSchema = {

    val struct = StructSchema(fields, wildcard, alias = Some(name))

    CollectionSchema(
      scope,
      id,
      name,
      migrations,
      struct,
      indexes,
      idSource,
      writeHooks,
      defaultTTLDuration,
      reservedReadFields
    )
  }

  def empty(scope: ScopeID, coll: CollectionID): CollectionSchema =
    CollectionSchema(
      scope,
      coll,
      "",
      MigrationList.empty,
      StructSchema(
        Map(
          "data" -> SchemaType
            .ObjectType(SchemaType
              .StructSchema(fields = Map.empty, wildcard = Some(ScalarType.Any)))
        )),
      Nil,
      DocIDSource.Snowflake,
      Nil,
      Duration.Inf,
      Nil
    )
}

final case class CollectionSchema(
  scope: ScopeID,
  collID: CollectionID,
  name: String,
  migrations: MigrationList,
  struct: SchemaType.StructSchema,
  indexes: List[IndexConfig],
  idSource: DocIDSource,
  writeHooks: List[WriteHook],
  defaultTTLDuration: Duration,
  // These fields are bumped up to `data` on rendering. This is used for computed
  // fields that overlap with defined fields.
  reservedReadFields: List[String]
) {
  import CollectionSchema._

  val schemaVersion: SchemaVersion = migrations.latestVersion

  def isUserCollection: Boolean =
    collID match {
      case UserCollectionID(_) =>
        true
      case _ =>
        false
    }

  def reservedFields: Set[String] =
    if (isUserCollection) {
      Tokens.ReservedFieldNames
    } else {
      Tokens.ReservedFieldNames - DataField
    }

  /** Used by our code that provides users with index field hints to turn fully specified paths that include the .data
    * field to the more common non .data prefixed path.
    */
  def toDisplayPath(path: List[String]): List[String] =
    path match {
      case head :: tail if tail.nonEmpty =>
        if (
          head == CollectionSchema.DataField && !reservedFields.contains(tail.head)
        ) {
          tail
        } else {
          path
        }
      case _ => path
    }

  /** The internal schema is used when Fauna is performing internal updates on a
    * collection. This allows modifications of internal fields, an update that
    * would fail when coming from a customer.
    */
  lazy val internalSchema: CollectionSchema = this.copy(
    struct = struct.internalSchema,
    writeHooks = writeHooks.filter(_.applyInternal)
  )

  // FIXME: This could be a single value on some scope-level schema object
  val documentsIndex = IndexConfig.DocumentsByCollection(scope)
  val documentsIndexTerm = Vector(IndexTerm(DocIDV(collID.toDocID)))

  val indexer = AggregateIndexer(indexes map { _.indexer })

  def nextID: Query[Option[DocID]] =
    idSource match {
      case DocIDSource.Snowflake =>
        IDStore.nextSnowflakeID(scope, collID) map { Some(_) }
      case DocIDSource.Sequential(min, max) =>
        IDStore.nextSequentialID(scope, collID, min, max)
    }

  def isValidID(id: DocID) =
    id.collID == collID && id.subID.toLong >= idSource.min && id.subID.toLong <= idSource.max

  def toExternalData(data: Data): Data = {
    if (isUserCollection) {
      data.fields.get(List(DataField)) map {
        case dataFields: MapV =>
          val specialDataFields =
            (Tokens.SpecialFieldNames ++ reservedReadFields).foldLeft(MapV.empty) {
              case (acc, name) =>
                val field = dataFields.get(List(name))
                if (field.nonEmpty) {
                  acc.update(List(name), field.get)
                } else {
                  acc
                }
            }

          // Non-reserved data fields get moved to the top level,
          // and reserved fields remain in data.
          val ud = data.fields
            .remove(List(DataField))
            .merge(dataFields.subtract(specialDataFields))
          if (specialDataFields.isEmpty) {
            Data(ud)
          } else {
            Data(ud.update(List(DataField), specialDataFields))
          }
        case _ =>
          throw new IllegalStateException(
            s"Found non MapV for data field in user collection data $data")
      } getOrElse data
    } else {
      data
    }
  }

  private def aliasFields(fields: MapV): MapV = {
    val toRename = struct.fields.toList.flatMap {
      case (name, field) if field.aliasTo.isDefined =>
        val aliasTo = field.aliasTo.get

        fields.get(List(name)) match {
          case Some(v) => List(name -> NullV, aliasTo -> v)
          case None    => Nil
        }

      case _ => Nil
    }

    if (toRename.nonEmpty) {
      val diff = Diff(MapV(toRename))
      Data(fields).patch(diff).fields
    } else {
      fields
    }
  }

  private def separateUserAndMetaFields(fields: MapV): MapV = {
    // Build top level meta fields.
    val metaFields = DisplayedMetaFields.foldLeft(MapV.empty) { (mf, field) =>
      fields.get(List(field)).map(mf.update(List(field), _)).getOrElse(mf)
    }
    val dataWithoutMetaFields = DisplayedMetaFields.foldLeft(fields) {
      (dwm, field) =>
        dwm.remove(List(field))
    }
    // Move all non-meta fields underneath data (if there are any).
    val userData = MapV((DataField, dataWithoutMetaFields))

    // Merge user data with top level meta fields.
    userData.merge(metaFields)
  }

  private def applyDefaultTTL(data: MapV, snapTS: Timestamp): MapV =
    if (!defaultTTLDuration.isFinite) {
      data
    } else {
      data.modify(
        TTLField :: Nil,
        {
          case ttl @ Some(_) => ttl
          case None          => Some(TimeV(snapTS + defaultTTLDuration))
        })
    }

  // This method pulls out all of our top level fields we don't want
  // nested within data, and moves all of the other fields below data
  def toInternalData(op: SchemaOp, snapTS: Timestamp): MapV = {
    val data = aliasFields(op.fields)
    if (!isUserCollection) {
      data
    } else {
      val internalData = op.mode match {
        case DataMode.Default   => separateUserAndMetaFields(data)
        case DataMode.PlainData => MapV((DataField, data))
      }
      applyDefaultTTL(internalData, snapTS)
    }
  }

  // Copies internal fields from `prev` over to `current`, and returns an
  // updated version of `current`.
  //
  // Note that this logic only applies to v10. All nested internal fields are not
  // writable in v4, so it doesn't need to worry about this.
  //
  // The specific rules are:
  // - Any common fields in a map are recursively copied.
  // - Arrays are disallowed. Any internal fields in arrays will be skipped.
  // - Unions are copied by copying every variant over, and merging the result.
  //
  // For example:
  //
  // current: {
  //   foo: 3,
  // }
  // prev: {
  //   foo: 3,
  //   my_internal_field: 5
  // }
  // returns: {
  //   foo: 3,
  //   my_internal_field: 5
  // }
  def copyInternalFieldMap(
    current: MapV,
    prev: MapV,
    schema: SchemaType.StructSchema): MapV = {
    MapV(current.elems.map { case (name, curr) =>
      val updated = for {
        prev <- prev.get(List(name))
        // this should be a validation error, but things happen, so we don't want to
        // blow up if the previous data doesn't match schema.
        schemaField <- schema.field(name)
      } yield {
        name -> copyInternalField(curr, prev, schemaField.expected)
      }
      updated.getOrElse(name -> curr)
    } ++ schema.fields.flatMap { case (name, schemaField) =>
      (if (schemaField.internal) {
         // Don't replace a field if it just got handled in the `current.elems.map`
         // above
         current.get(List(name)) match {
           case Some(_) => None
           case None    => prev.get(List(name)).map { name -> _ }
         }
       } else {
         None
       })
    })
  }

  def copyInternalField(
    current: IRValue,
    prev: IRValue,
    schema: SchemaType): IRValue =
    (current, prev, schema) match {
      case (c, p, SchemaType.Union(u)) =>
        // This isn't entirely right, but internal fields are only defined, well,
        // internally. So we can just check internal-ness against all variants of the
        // union.
        //
        // All of our unions fit into two categories:
        // - { foo: T } | Null: `null` has no fields, so this check will work.
        // - { foo: T } | { bar: T }: No internal fields are found in these two
        // objects in practice.
        u.foldLeft(c) { case (c, u) => copyInternalField(c, p, u) }

      case (c: MapV, p: MapV, SchemaType.ObjectType(s: SchemaType.StructSchema)) =>
        copyInternalFieldMap(c, p, s)

      case _ => current
    }

  def onWrite(ev: WriteHook.Event): Query[Seq[ConstraintFailure]] =
    writeHooks
      .flatMap(_.lift((this, ev)))
      .sequence
      .map(_.flatten)

  def validateAndTransform(
    op: SchemaOp,
    snapTS: Timestamp
  ): Query[SchemaResult[Data]] = {

    val (isCreate, prevData) = op match {
      case _: SchemaOp.Create   => (true, None)
      case up: SchemaOp.Update  => (false, Some(up.prev.fields))
      case rp: SchemaOp.Replace => (false, Some(rp.prev.fields))
    }

    val reservedFailures = op.mode match {
      case DataMode.Default   => validateReserved(isCreate, op.fields)
      case DataMode.PlainData => List.empty
    }

    if (reservedFailures.isEmpty) {
      transformAndInsertDefaults(op, snapTS).flatMapT { data =>
        SchemaType
          .validateStruct(struct, Path.RootPrefix, data.fields, prevData)
          .map { failures =>
            if (failures.isEmpty) {
              SchemaResult.Ok(data.elideNulls)
            } else {
              SchemaResult.Err(failures)
            }
          }
      }
    } else {
      SchemaResult.Err(reservedFailures).toQuery
    }
  }

  def transformAndInsertDefaults(
    op: SchemaOp,
    snapTS: Timestamp): Query[SchemaResult[Data]] = {
    val merged = transformToData(op, snapTS)

    op match {
      case _: SchemaOp.Create | _: SchemaOp.Replace =>
        insertDefaultFieldsMap(merged.fields, struct).mapT { fields =>
          Data(fields)
        }

      case _ => Query.value(SchemaResult.Ok(merged))
    }
  }

  def transformToData(op: SchemaOp, snapTS: Timestamp): Data = {
    op match {
      case cr: SchemaOp.Create => Data(toInternalData(cr, snapTS))
      case up: SchemaOp.Update => up.prev.patch(Diff(toInternalData(up, snapTS)))
      case rp: SchemaOp.Replace =>
        val userData = toInternalData(rp, snapTS)
        val dataWithInternalFields =
          copyInternalFieldMap(userData, rp.prev.fields, struct)
        Data(dataWithInternalFields)
    }
  }

  def insertDefaultFieldsMap(
    data: MapV,
    schema: SchemaType.StructSchema): Query[SchemaResult[MapV]] = {
    val existingQ = data.elems.map { case (k, v) =>
      schema.field(k) match {
        case Some(schema) =>
          insertDefaultFields(v, schema.expected).mapT { k -> _ }
        case None =>
          // this is an error that validation will catch later.
          Query.value(SchemaResult.Ok(k -> v))
      }
    }.sequenceT

    val defaultsQ = schema.fields.view
      .map { case (k, schema) =>
        // We filter here so that we can just concat lists later, and maintain
        // ordering of fields.
        if (data.elems.exists(_._1 == k)) {
          Query.value(SchemaResult.Ok(None))
        } else {
          schema.default match {
            case Some(default) =>
              val defaultQ = default(scope)
              defaultQ.mapT { v => Some(k -> v) }

            case None =>
              schema.expected match {
                case SchemaType.ObjectType(s: SchemaType.StructSchema) =>
                  insertDefaultFieldsMap(MapV.empty, s).mapT { v =>
                    Some(k -> v)
                  }

                case _ =>
                  Query.value(SchemaResult.Ok(None))
              }
          }
        }
      }
      .sequenceT
      .mapT(_.flatten)

    existingQ.flatMapT { existing =>
      defaultsQ.mapT { defaults =>
        MapV((existing.view ++ defaults).toList)
      }
    }
  }
  def insertDefaultFields(
    data: IRValue,
    schema: SchemaType): Query[SchemaResult[IRValue]] = {
    (data, schema) match {
      case (m: MapV, SchemaType.ObjectType(s: SchemaType.StructSchema)) =>
        insertDefaultFieldsMap(m, s)
      case (a: ArrayV, SchemaType.Array(s)) =>
        a.elems.map(insertDefaultFields(_, s)).sequenceT.mapT { elems =>
          ArrayV(elems.toVector)
        }

      case _ => Query.value(SchemaResult.Ok(data))
    }
  }

  private def validateReserved(
    isCreate: Boolean,
    fields: MapV
  ): Seq[FieldConstraintFailure] = {
    val reserved = if (isCreate) {
      reservedFields - "id"
    } else {
      reservedFields
    }
    (reserved.filter { f =>
      fields.contains(List(f))
    }) match {
      case Nil => Nil
      case res =>
        val tpe = SchemaType.valuePreciseType(fields)
        res.map { field =>
          val path = (Path.RootPrefix :+ field).toPath
          ConstraintFailure.ReservedField(path, tpe, field)
        }.toSeq
    }
  }
}

object FieldSchema {
  type FieldValidator =
    (Path.Prefix, IRValue, Option[IRValue]) => Query[Seq[FieldConstraintFailure]]
  val DefaultFieldValidator: FieldValidator = { (_, _, _) => Query.value(Nil) }

  // Transforms the field value for reading
  type ReadField = Value => Query[Value]
  val DefaultReadField: ReadField = Query.value
}

final case class FieldSchema(
  expected: SchemaType,
  immutable: Boolean = false,
  readOnly: Boolean = false,
  internal: Boolean = false,
  validator: FieldSchema.FieldValidator = FieldSchema.DefaultFieldValidator,
  readFieldImp: FieldSchema.ReadField = FieldSchema.DefaultReadField,
  aliasTo: Option[String] = None,
  default: Option[ScopeID => Query[SchemaResult[IRValue]]] = None) {

  def readField(value: Value): Query[Value] = {
    val masked = maskInternalField(value, expected)
    readFieldImp(masked)
  }

  // Internal fields are masked on read. This recurses through the entire value being
  // read, `data`, and removes any references to internal fields.
  def maskInternalFieldMap(
    data: Value.Struct.Full,
    schema: SchemaType.StructSchema): Value.Struct.Full = {
    data.copy(fields = data.fields.flatMap { case (name, value) =>
      val schemaField = schema.fields.get(name)
      if (schemaField.exists(_.internal)) {
        None
      } else {
        val expected = schemaField.map(_.expected).orElse(schema.wildcard)
        Some(expected match {
          case Some(expected) => name -> maskInternalField(value, expected)
          case None           => name -> value
        })
      }
    })
  }

  def maskInternalField(data: Value, schema: SchemaType): Value =
    (data, schema) match {
      case (v, SchemaType.Union(u)) =>
        // This isn't entirely right, but internal fields are only defined, well,
        // internally. So we can just check internal-ness against all variants of the
        // union.
        //
        // All of our unions fit into two categories:
        // - { foo: T } | Null: Masking against null does nothing, so this check
        // works.
        // - { foo: T } | { bar: T }: This only comes up for index terms/values,
        // which have no internal fields. So we don't really need to worry about it.
        u.foldLeft(v)(maskInternalField)

      case (
            m: Value.Struct.Full,
            SchemaType.ObjectType(s: SchemaType.StructSchema)) =>
        maskInternalFieldMap(m, s)
      case (a: Value.Array, SchemaType.Array(s)) =>
        Value.Array(a.elems.map(maskInternalField(_, s)))

      case _ => data
    }

  /** When using the internal verison of the schema, we remove the internal and
    * readOnly qualifiers. This is because when doing an internal update, these
    * fields are modifiable.
    */
  def internalSchema = {
    this.copy(
      expected = expected.internalType,
      internal = false,
      readOnly = false
    )
  }

  def optional = expected.isNullable

  override def equals(obj: Any): Boolean = obj match {
    case other: FieldSchema =>
      expected == other.expected && immutable == other.immutable && readOnly == other.readOnly && internal == other.internal && aliasTo == other.aliasTo
    case _ => false
  }
}
