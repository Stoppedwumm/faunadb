package fauna.model.schema

import fauna.atoms.UserCollectionID
import fauna.model.runtime.fql2.stdlib.{
  IDedCollectionCompanion,
  UserCollectionCompanion,
  UserDocPrototype
}
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, Path }
import fauna.repo.schema.FieldSchema.FieldValidator
import fauna.storage.ir.{ MapV, NullV }
import fql.parser.Tokens

object CollectionValidators {

  // TODO: remove ReservedCollectionMethods once it is no longer needed
  // because the methods have actually been added to the collection
  private val ReservedCollectionMethods = Set("createAll")

  /** A dummy user collection and prototype used to gather static field and method
    * names.
    */
  private object StaticResolvers
      extends IDedCollectionCompanion(
        UserCollectionID.MaxValue,
        UserDocPrototype.Any)
      with UserCollectionCompanion.StaticResolvers

  def isNameReservedInDoc(name: String) =
    Tokens.SpecialFieldNames.contains(name) ||
      UserDocPrototype.Any.hasResolver(name)

  def isNameReservedInColl(name: String) =
    ReservedCollectionMethods.contains(name) ||
      StaticResolvers.hasResolver(name)

  def memberValidator(isReserved: String => Boolean): FieldValidator = {
    case (_, NullV, _) => Query.value(Seq.empty)
    case (pathPrefix, MapV(fields), _) =>
      Query.value(fields flatMap { case (name, _) =>
        validateMemberName(pathPrefix.toPath, name, isReserved)
      })
    case (pathPrefix, toValue, fromValue) =>
      throw new IllegalStateException(
        s"Unexpected type for field $pathPrefix, expected MapV, received toValue: $toValue, fromValue: $fromValue")
  }

  private def validateMemberName(
    path: Path,
    name: String,
    isReserved: String => Boolean
  ): Option[ConstraintFailure.FieldConstraintFailure] =
    if (!Tokens.isValidIdent(name)) {
      Some(
        ConstraintFailure.ValidatorFailure(
          path,
          "Invalid identifier."
        ))
    } else if (isReserved(name)) {
      Some(
        ConstraintFailure.ValidatorFailure(
          path,
          s"The name '$name' is reserved."
        ))
    } else {
      None
    }
}
