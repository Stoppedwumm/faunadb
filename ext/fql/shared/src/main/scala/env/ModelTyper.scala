package fql.env

import fql.typer._

// This object wraps all model-specific functionality of environment typechecking.
// This includes a lot of hardcoded things, like the fields on documents, and how
// to build index methods on collections.
object ModelTyper {

  // These operators need to be manually appended to some type shapes. We use this
  // set to filter out of `typer.hardcodedOps`.
  val ComparisonOps = Set("==", "!=", ">", "<", ">=", "<=")

  /** Builds a typer with empty shapes for all collections in the schema. This
    * should only be used with `typeTExpr`, in order to check names and type
    * persistability.
    */
  def stubTyper(typer: Typer, schema: DatabaseSchema): Typer = {
    def types(name: String) =
      Seq(
        name -> TypeShape(
          self = Type.Never.typescheme,
          docType = CollectionTypeInfo.docTag(name)),
        CollectionTypeInfo.nullDocName(name) -> TypeShape(
          self = Type.Never.typescheme,
          docType = CollectionTypeInfo.nullDocTag(name)),
        CollectionTypeInfo.collName(name) -> TypeShape(self = Type.Never.typescheme)
      )

    val shapes = schema.colls.view.flatMap { c =>
      types(c.name.str) ++ (c.alias match {
        case Some(alias) => types(alias.configValue.str)
        case None        => Seq.empty
      })
    }

    typer.withShapes(shapes.toMap)
  }
}
