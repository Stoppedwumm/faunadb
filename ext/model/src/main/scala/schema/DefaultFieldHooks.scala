package fauna.model.schema

import fauna.atoms.DocID
import fauna.lang.syntax._
import fauna.model.Collection
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, WriteHook }
import fauna.repo.schema.SchemaResult
import fauna.repo.Store
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.ir._

/** Hooks that are called when a collection is created, updated, or deleted.
  * - Manage FQL2 indexes on the collections. These indexes have an associated
  *   underlying index document that allows them to hook into the existing
  *   indexing system.  This object is used to manage the underlying indexes
  *   associated with a collection level index.
  * - If on, enforce protected mode: no deleting backing indexes.
  * - Validate that check constraint names are unique.
  */
object DefaultFieldHooks {

  val writeHook = WriteHook {
    case (schema, WriteHook.OnCreate(id, updated)) =>
      processUpdate(schema, id, updated)
    case (schema, WriteHook.OnUpdate(id, _, updated)) =>
      processUpdate(schema, id, updated)
    case (_, WriteHook.OnDelete(_, _)) =>
      Query.value(Seq.empty)
  }

  def processUpdate(
    collectionSchema: CollectionSchema,
    docID: DocID,
    updatedData: Data) = {
    val _ = (collectionSchema, docID, updatedData)

    val defaults = findDefaults(updatedData)

    defaults
      .map { case (name, expr) =>
        val res: Query[SchemaResult[IRValue]] =
          DefinedField.evalDefault(
            collectionSchema.scope,
            name,
            expr,
            s"*field:$name*")

        res.mapT { v => Some(name -> v) }
      }
      .sequenceT
      .flatMap {
        case SchemaResult.Ok(backfillValues) if backfillValues.nonEmpty =>
          val diff = MapV("fields" -> MapV(backfillValues.flatten.map {
            case (name, value) => name -> MapV("backfill_value" -> value)
          }.toList))

          Store
            .internalUpdate(collectionSchema, docID, Diff(diff))
            .map { _ => Seq.empty }

        case SchemaResult.Ok(_)     => Query.value(Seq.empty)
        case SchemaResult.Err(errs) => Query.value(errs)
      }

  }

  private def findDefaults(data: Data): Seq[(String, String)] = {
    val fields = (data.fields.get(Collection.DefinedFields.path)) match {
      case Some(MapV(fields)) => fields
      case _                  => Seq.empty
    }

    fields.flatMap {
      case (name, m: MapV) =>
        m.get(List("default")).flatMap {
          case StringV(expr) => Some(name -> expr)
          case _             => None
        }

      case _ => None
    }
  }
}
