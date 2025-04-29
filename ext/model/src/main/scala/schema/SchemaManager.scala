package fauna.model.schema

import fauna.model.schema.manager._
import fauna.repo.values.Value
import fql.ast.SchemaItem

sealed trait SchemaManager
    extends SchemaValidate
    with SchemaStagedDiff
    with SchemaUpdate
    with SchemaToStruct

final object SchemaManager extends SchemaManager {
  final case class UpdatedItem(item: SchemaItem, doc: Value.Doc)
}
