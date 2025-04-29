package fauna.model.schema

import fauna.model.schema.index.CollectionIndex
import fauna.repo.schema._
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType._

object CollectionIndexSchemaTypes {
  val termsFieldName = "terms"
  val valuesFieldName = "values"
  val uniqueFieldName = "unique"
  val queryableFieldName = "queryable"
  val statusFieldName = "status"
  val indexIDFieldName = "indexID"

  private[this] val termType = SchemaType.Record(
    "field" -> FieldSchema(Str, validator = IndexFieldValidator.termValidator),
    "mva" -> FieldSchema(Optional(Boolean))
  )
  private[this] val valueType = SchemaType.Record(
    "field" -> FieldSchema(Str, validator = IndexFieldValidator.valueValidator),
    "order" -> FieldSchema(Optional(Union(Singleton("asc"), Singleton("desc")))),
    "mva" -> FieldSchema(Optional(Boolean))
  )
  private[this] val termsField = FieldSchema(SchemaType.Array(termType))
  private[this] val valuesField = FieldSchema(SchemaType.Array(valueType))
  private[this] val uniqueField = FieldSchema(Optional(Boolean))
  private[this] val statusField = FieldSchema(
    Optional(
      Union(
        Singleton(CollectionIndex.Status.Building.asStr),
        Singleton(CollectionIndex.Status.Complete.asStr),
        Singleton(CollectionIndex.Status.Failed.asStr)
      )),
    readOnly = true
  )
  private[this] val queryableField = FieldSchema(Optional(Boolean))
  private[this] val backingIndexField = FieldSchema(Optional(AnyDoc))

  private[this] val valueless = SchemaType.Record(
    termsFieldName -> termsField,
    statusFieldName -> statusField,
    queryableFieldName -> queryableField
  )

  private[this] val termless = SchemaType.Record(
    valuesFieldName -> valuesField,
    statusFieldName -> statusField,
    queryableFieldName -> queryableField
  )

  private[this] val termsAndValues = SchemaType.Record(
    termsFieldName -> termsField,
    valuesFieldName -> valuesField,
    statusFieldName -> statusField,
    queryableFieldName -> queryableField
  )

  val UserDefinedIndexType = SchemaType.Union(valueless, termless, termsAndValues)
  val BackingIndexType = SchemaType.Record(
    termsFieldName -> termsField,
    valuesFieldName -> valuesField,
    uniqueFieldName -> uniqueField,
    indexIDFieldName -> backingIndexField,
    statusFieldName -> statusField
  )
}
