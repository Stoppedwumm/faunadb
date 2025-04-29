package fauna.model.schema

import fauna.atoms.ScopeID
import fauna.lang.syntax._
import fauna.model.{ Collection, UserFunction }
import fauna.model.runtime.fql2.stdlib.Global
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, Path }
import fauna.repo.schema.ConstraintFailure.FieldConstraintFailure
import fauna.repo.schema.FieldSchema.FieldValidator
import fauna.storage.ir.{ NullV, StringV }
import fql.parser.Tokens

object GlobalNamespaceValidator {

  def globalNameValidator(scopeID: ScopeID): FieldValidator = {
    case (pathPrefix, StringV(toValue), Some(StringV(fromValue))) =>
      if (toValue != fromValue) {
        evalNameConflicts(
          pathPrefix.toPath,
          scopeID,
          toValue
        )
      } else {
        Query.value(Seq.empty)
      }
    case (pathPrefix, StringV(toValue), None) =>
      evalNameConflicts(
        pathPrefix.toPath,
        scopeID,
        toValue
      )
    case (pathPrefix, toValue, fromValue) =>
      throw new IllegalStateException(
        s"Unexpected type for field $pathPrefix, expected StringV, received toValue: $toValue, fromValue: $fromValue")
  }

  def globalAliasValidator(scopeID: ScopeID): FieldValidator = {
    case (_, NullV, _) => Query.value(Seq.empty)
    case (pathPrefix, StringV(alias), Some(StringV(prevAlias))) =>
      if (alias != prevAlias) {
        evalNameConflicts(
          pathPrefix.toPath,
          scopeID,
          alias
        )
      } else {
        Query.value(Seq.empty)
      }
    case (pathPrefix, StringV(alias), _) =>
      evalNameConflicts(
        pathPrefix.toPath,
        scopeID,
        alias
      )
    case (pathPrefix, toValue, _) =>
      throw new IllegalStateException(
        s"Unexpected type for field $pathPrefix, expected StringV, received toValue: $toValue")
  }

  private def evalNameConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Seq[FieldConstraintFailure]] = {
    evalStaticEnvironmentConflicts(
      path,
      name
    ) map { Query.value } getOrElse {
      evalDynamicEnvironmentConflicts(path, scopeID, name)
    }.getOrElseT(Seq.empty)
  }

  private def evalStaticEnvironmentConflicts(
    path: Path,
    name: String
  ): Option[Seq[FieldConstraintFailure]] = {
    if (nameConflictsWithEnvironment(name)) {
      Some(
        Seq(
          ConstraintFailure.ValidatorFailure(
            path,
            s"The identifier `$name` is reserved."
          )
        ))
    } else if (!Validators.isValidName(name)) {
      Some(Seq(ConstraintFailure.ValidatorFailure(path, "Invalid identifier.")))
    } else {
      None
    }
  }

  // Public to use in FQL2SchemaFuzzSpec.
  def nameConflictsWithEnvironment(name: String): Boolean =
    Tokens.invalidVariables.contains(name) ||
      Tokens.reservedTypeNames.contains(name) ||
      Global.StaticEnv.typeShapes.keySet.contains(name) ||
      Global.Default.hasResolver(name)

  private def evalDynamicEnvironmentConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] =
    CollectionConflictManager
      .evalCollectionConflicts(path, scopeID, name)
      .orElseT(FunctionConflictManager.evalFunctionConflicts(path, scopeID, name))
}

object CollectionConflictManager {
  def evalCollectionConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] = {
    evalCollectionNameConflicts(
      path,
      scopeID,
      name
    ).orElseT(evalCollectionAliasConflicts(path, scopeID, name))
  }

  private def evalCollectionNameConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] = {
    Collection.idByNameUncachedStaged(scopeID, name).mapT { _ =>
      Seq(
        ConstraintFailure.ValidatorFailure(
          path,
          s"A Collection already exists with the name `$name`"
        )
      )
    }
  }

  private def evalCollectionAliasConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] = {
    Collection.idByAliasUncachedStaged(scopeID, name).mapT { _ =>
      Seq(
        ConstraintFailure.ValidatorFailure(
          path,
          s"A Collection already exists with the alias `$name`"
        )
      )
    }
  }
}

object FunctionConflictManager {
  def evalFunctionConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] = {
    evalFunctionNameConflicts(
      path,
      scopeID,
      name
    ).orElseT(evalFunctionAliasConflicts(path, scopeID, name))
  }

  private def evalFunctionNameConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] = {
    UserFunction.idByNameUncached(scopeID, name).mapT { _ =>
      Seq(
        ConstraintFailure.ValidatorFailure(
          path,
          s"A Function already exists with the name `$name`"
        )
      )
    }
  }

  private def evalFunctionAliasConflicts(
    path: Path,
    scopeID: ScopeID,
    name: String
  ): Query[Option[Seq[FieldConstraintFailure]]] =
    UserFunction.idByAliasUncached(scopeID, name).mapT { _ =>
      Seq(
        ConstraintFailure.ValidatorFailure(
          path,
          s"A Function already exists with the alias `$name`"
        )
      )
    }

}
