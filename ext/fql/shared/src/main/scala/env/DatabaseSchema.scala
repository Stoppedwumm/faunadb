package fql.env

import fql.ast._

final case class DatabaseSchema(
  colls: Seq[SchemaItem.Collection],
  funcs: Seq[SchemaItem.Function],
  roles: Seq[SchemaItem.Role],
  aps: Seq[SchemaItem.AccessProvider],
  // A list of v4 functions and v4 roles. Collections aren't version specific, and we
  // don't need v4 access providers.
  v4Funcs: Seq[String],
  v4Roles: Seq[String]
)

object DatabaseSchema {
  def fromItems(
    items: Iterable[SchemaItem],
    v4Funcs: Seq[String],
    v4Roles: Seq[String]): DatabaseSchema = {
    val collsB = Seq.newBuilder[SchemaItem.Collection]
    val funcsB = Seq.newBuilder[SchemaItem.Function]
    val rolesB = Seq.newBuilder[SchemaItem.Role]
    val apsB = Seq.newBuilder[SchemaItem.AccessProvider]

    items.foreach {
      case c: SchemaItem.Collection     => collsB += c
      case f: SchemaItem.Function       => funcsB += f
      case r: SchemaItem.Role           => rolesB += r
      case a: SchemaItem.AccessProvider => apsB += a
      case _                            => ()
    }

    DatabaseSchema(
      collsB.result(),
      funcsB.result(),
      rolesB.result(),
      apsB.result(),
      v4Funcs,
      v4Roles)
  }
}
