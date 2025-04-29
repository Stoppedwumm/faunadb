package fauna.model.runtime.fql2.stdlib

import fauna.model.schema.SchemaCollection
import fql.typer.Type

object AccessProviderCompanion
    extends SchemaCollectionCompanion(
      SchemaCollection.AccessProvider,
      NativeDocPrototype(SchemaCollection.AccessProvider.name),
      NamedNullDocPrototype) {

  lazy val apRolePredicateType = tt.Struct(
    "role" -> tt.Str,
    "predicate" -> tt.Str
  )
  lazy val apRoleType = tt.Union(tt.Str, apRolePredicateType)

  override lazy val docImplType = Type
    .Record(
      "name" -> Type.Str,
      "ts" -> Type.Time,
      "coll" -> selfType.staticType,
      "issuer" -> Type.Str,
      "jwks_uri" -> Type.Str,
      "roles" -> Type.Array(apRolePredicateType),
      "audience" -> Type.Str,
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme
  override lazy val docCreateType = tt.Struct(
    "name" -> tt.Str,
    "issuer" -> tt.Str,
    "jwks_uri" -> tt.Str,
    "roles" -> tt.Union(apRoleType, tt.Array(apRoleType), tt.Null),
    "data" -> tt.Optional(tt.AnyStruct)
  )
}
