package fauna.model.runtime.fql2.stdlib

import fauna.model.schema.SchemaCollection
import fql.typer.Type

object RoleCompanion
    extends SchemaCollectionCompanion(
      SchemaCollection.Role,
      NativeDocPrototype(SchemaCollection.Role.name),
      NamedNullDocPrototype) {
  override lazy val docImplType = Type
    .Record(
      "name" -> Type.Str,
      "ts" -> Type.Time,
      "coll" -> selfType.staticType,
      "privileges" -> Type.Any,
      "membership" -> Type.Any,
      "data" -> Type.Optional(Type.AnyRecord)
    )
    .typescheme
  override lazy val docCreateType = tt.Struct(
    "name" -> tt.Str,
    "privileges" -> tt.Optional(tt.Any),
    "membership" -> tt.Optional(tt.Any),
    "data" -> tt.Optional(tt.AnyStruct)
  )
}
