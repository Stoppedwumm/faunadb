package fauna.model.schema

import fauna.atoms.{ CollectionID, ScopeID }
import fauna.model.runtime.fql2.stdlib.Global
import fauna.model.runtime.fql2.TypeTag
import fauna.model.SchemaNames
import fauna.repo.schema.WriteHook
import fauna.storage.ir.StringV
import fql.ast.Span
import fql.error.{ Error => FQLError, TypeError }
import scala.collection.mutable.{ Map => MMap }

object CollectionDerivedTypeValidator {
  def apply(
    scope: ScopeID,
    collections: Seq[(CollectionID, String)]): CollectionDerivedTypeValidator = {
    // We don't pull these from the typer because we want to see the duplicates
    // for validation.
    val typeNames = collections.flatMap { case (_, collName) =>
      typeNamesForCollection(collName)
    } ++ Global.StaticEnv.typeShapes.keys

    val typeNameCounts = MMap[String, Int]()
    typeNames.foreach { typeName =>
      typeNameCounts(typeName) = typeNameCounts.getOrElse(typeName, 0) + 1
    }

    new CollectionDerivedTypeValidator(
      scope,
      collections,
      typeNameCounts.toMap
    )
  }

  def typeNamesForCollection(collName: String): Set[String] = {
    // NB: Keep in sync with `stdlib/Collection`.
    Set(collName, collName + "Collection", TypeTag.NullDoc(collName).name)
  }
}

class CollectionDerivedTypeValidator(
  val scope: ScopeID,
  val collections: Seq[(CollectionID, String)],
  private val typeNameCounts: Map[String, Int]
) {
  def validateDerivedTypeNameConflicts(
    writeEvent: WriteHook.Event,
    span: Span): Seq[FQLError] = {
    if (writeEvent.id.collID != CollectionID.collID) {
      Nil
    } else {
      val nameTypeConflicts =
        getUpdatedFieldStr(SchemaNames.NameField.path, writeEvent)
          .map { updatedName =>
            checkCollectionDerivedTypeConflicts("name", updatedName, span)
          }
          .getOrElse(Nil)

      val aliasTypeConflicts =
        getUpdatedFieldStr(SchemaNames.AliasField.path, writeEvent)
          .map { updatedAlias =>
            checkCollectionDerivedTypeConflicts("alias", updatedAlias, span)
          }
          .getOrElse(Nil)

      nameTypeConflicts ++ aliasTypeConflicts
    }
  }

  private def getUpdatedFieldStr(
    fieldPath: List[String],
    writeEvent: WriteHook.Event): Option[String] = {
    val prevOpt = writeEvent.prevData.flatMap(_.fields.get(fieldPath).flatMap {
      case StringV(v) => Some(v)
      case _          => None
    })
    val updatedOpt = writeEvent.newData.flatMap(_.fields.get(fieldPath).flatMap {
      case StringV(v) => Some(v)
      case _          => None
    })
    (prevOpt, updatedOpt) match {
      case (Some(prev), Some(updated)) if prev != updated => Some(updated)
      case (None, Some(updated))                          => Some(updated)
      case _                                              => None
    }
  }

  private def checkCollectionDerivedTypeConflicts(
    field: String, // "name" or "alias" - just used for the error message
    updatedNameOrAlias: String,
    span: Span): Seq[FQLError] = {
    collections
      .find(_._2 == updatedNameOrAlias)
      .map { case (_, collName) =>
        val conflictingTypeNames = typeNameCounts.view
          .filterKeys(
            CollectionDerivedTypeValidator.typeNamesForCollection(collName))
          .filter(_._2 > 1)
          .keys
        if (conflictingTypeNames.nonEmpty) {
          Seq(
            TypeError(
              s"""The collection $field `$updatedNameOrAlias` generates types that conflict with the following existing types: ${conflictingTypeNames
                  .mkString(", ")}""",
              span
            )
          )
        } else {
          Nil
        }
      }
      .getOrElse(Nil)
  }
}
