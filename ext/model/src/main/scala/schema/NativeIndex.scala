package fauna.model.schema

import fauna.atoms._
import fauna.model._
import fauna.repo._
import fauna.storage.index._
import scala.annotation.unused

object NativeIndex {

  def index(scope: ScopeID, id: IndexID, source: IDCompanion[_], path: List[String], paths: List[String]*) = {
    val terms = (path +: paths) map { p => (IndexConfig.defaultExtractor(p, false), false) }
    IndexConfig.Native(scope, id, IndexSources(source.collID), terms.toVector, Unconstrained, false)
  }

  def unique(scope: ScopeID, id: IndexID, source: IDCompanion[_], path: List[String], paths: List[String]*) = {
    val terms = (path +: paths) map { p => (IndexConfig.defaultExtractor(p, false), false) }
    IndexConfig.Native(scope, id, IndexSources(source.collID), terms.toVector, UniqueValues, false)
  }

  def serial(scope: ScopeID, id: IndexID, source: IDCompanion[_], path: List[String], paths: List[String]*) = {
    val terms = (path +: paths) map { p => (IndexConfig.defaultExtractor(p, false), false) }
    IndexConfig.Native(scope, id, IndexSources(source.collID), terms.toVector, Unconstrained, true)
  }

  def apply(scope: ScopeID, id: IndexID): Option[IndexConfig.Native] =
    if (id > Index.NativeIndexMaxID) {
      // This id is in the user-defined index range.
      None
    } else {
      id match {
        case NativeIndexID(native) =>
          native match {
            case NativeIndexID.DatabaseByName =>
              Some(DatabaseByName(scope))
            case NativeIndexID.DatabaseByAccountID =>
              Some(DatabaseByAccountID())
            case NativeIndexID.DatabaseByDisabled =>
              Some(DatabaseByDisabled(scope))
            case NativeIndexID.IndexByName =>
              Some(IndexByName(scope))
            case NativeIndexID.CollectionByName =>
              Some(CollectionByName(scope))
            case NativeIndexID.CollectionByAlias =>
              Some(CollectionByAlias(scope))
            case NativeIndexID.JournalEntryByHost =>
              Some(JournalEntryByHost(scope))
            case NativeIndexID.KeyByDatabase =>
              Some(KeyByDatabase(scope))
            case NativeIndexID.TaskByCompletion =>
              Some(TaskByCompletion(scope))
            case NativeIndexID.CredentialsByDocument =>
              Some(CredentialsByDocument(scope))
            case NativeIndexID.TokenByDocument =>
              Some(TokenByDocument(scope))
            case NativeIndexID.UserFunctionByName =>
              Some(UserFunctionByName(scope))
            case NativeIndexID.UserFunctionByAlias =>
              Some(UserFunctionByName(scope))
            case NativeIndexID.JournalEntryByHostAndTag =>
              Some(JournalEntryByHostAndTag(scope))
            case NativeIndexID.RoleByName =>
              Some(RoleByName(scope))
            case NativeIndexID.RolesByResource =>
              Some(RolesByResource(scope))
            case NativeIndexID.JournalEntryByTag =>
              Some(JournalEntryByTag(scope))
            case NativeIndexID.DocumentsByCollection =>
              Some(DocumentsByCollection(scope))
            case NativeIndexID.AccessProviderByName =>
              Some(AccessProviderByName(scope))
            case NativeIndexID.AccessProviderByIssuer =>
              Some(AccessProviderByIssuer(scope))
            case NativeIndexID.ExecutingTasksByAccountAndName =>
              Some(ExecutingTasksByAccountAndName(scope))
            case NativeIndexID.PrioritizedTasksByCreatedAt =>
              Some(PrioritizedTasksByCreatedAt(scope))
            case NativeIndexID.ChangesByCollection =>
              Some(ChangesByCollection(scope))
          }
        case _ => None
      }
    }

  def DocumentsByCollection(scopeID: ScopeID) =
    IndexConfig.DocumentsByCollection(scopeID)

  // NB: This index is not backfilled! It was initially added in January of 2024,
  // where it only coverred user defined documents. Then, it was later updated in
  // August of 2024 to cover native collections as well.
  //
  // All updates written before these times are not filled in this index, as it has
  // not been backfilled!
  def ChangesByCollection(scopeID: ScopeID) =
    IndexConfig.ChangesByCollection(scopeID)

  def DatabaseByName(scope: ScopeID) =
    unique(scope, NativeIndexID.DatabaseByName, DatabaseID, SchemaNames.NameField.path)

  def DatabaseByAccountID(@unused scope: ScopeID = ScopeID.RootID) = {
    val terms = Vector(
      (
        IndexConfig.defaultExtractor(Database.AccountIDField.path, reverse = false),
        false))
    val vals = Vector(
      (
        IndexConfig.defaultExtractor(Database.ScopeField.path, reverse = false),
        false))
    IndexConfig.Native(
      ScopeID.RootID,
      NativeIndexID.DatabaseByAccountID,
      IndexSources(DatabaseID.collID),
      terms,
      UniqueTerms,
      isSerial = false,
      vals)
  }

  def DatabaseByDisabled(scope: ScopeID) = {
    val terms = Vector(
      (IndexConfig.defaultExtractor(Database.DisabledField.path, reverse = false), false))

    IndexConfig.Native(
      scope,
      NativeIndexID.DatabaseByDisabled,
      IndexSources(DatabaseID.collID),
      terms,
      Unconstrained,
      isSerial = false,
      partitions = 8)
  }

  def CollectionByName(scope: ScopeID) = {
    val terms = Vector((IndexConfig.defaultExtractor(SchemaNames.NameField.path, false), false))
    IndexConfig.Native(scope, NativeIndexID.CollectionByName, IndexSources(CollectionID.collID), terms, UniqueValues, false)
  }

  def CollectionByAlias(scope: ScopeID) =
    unique(
      scope,
      NativeIndexID.CollectionByAlias,
      CollectionID,
      SchemaNames.AliasField.path)

  def IndexByName(scope: ScopeID) =
    unique(scope, NativeIndexID.IndexByName, IndexID, SchemaNames.NameField.path)

  def ByName(scope: ScopeID, collectionID: CollectionID) =
    (collectionID: @unchecked) match {
      case DatabaseID.collID       => DatabaseByName(scope)
      case CollectionID.collID     => CollectionByName(scope)
      case IndexID.collID          => IndexByName(scope)
      case UserFunctionID.collID   => UserFunctionByName(scope)
      case RoleID.collID           => RoleByName(scope)
      case AccessProviderID.collID => AccessProviderByName(scope)
    }

  def ByAlias(scope: ScopeID, collectionID: CollectionID) =
    (collectionID: @unchecked) match {
      case CollectionID.collID => CollectionByAlias(scope)
      case UserFunctionID.collID => UserFunctionByAlias(scope)
    }

  def KeyByDatabase(scope: ScopeID) =
    index(scope, NativeIndexID.KeyByDatabase, KeyID, Key.DatabaseField.path)

  def JournalEntryByHost(scope: ScopeID) =
    index(scope, NativeIndexID.JournalEntryByHost, JournalEntryID, JournalEntry.HostField.path)

  def TaskByCompletion(scope: ScopeID) = {
    val terms = Vector((Task.CompletedExtractor, false))
    IndexConfig.Native(
      scope,
      NativeIndexID.TaskByCompletion,
      IndexSources(TaskID.collID),
      terms,
      Unconstrained,
      isSerial = false)
  }

  def CredentialsByDocument(scope: ScopeID) =
    unique(scope, NativeIndexID.CredentialsByDocument, CredentialsID, Credentials.DocumentField.path)

  def TokenByDocument(scope: ScopeID) =
    index(scope, NativeIndexID.TokenByDocument, TokenID, Token.DocumentField.path)

  def UserFunctionByName(scope: ScopeID) =
    unique(scope, NativeIndexID.UserFunctionByName, UserFunctionID, SchemaNames.NameField.path)

  def UserFunctionByAlias(scope: ScopeID) =
    unique(scope, NativeIndexID.UserFunctionByAlias, UserFunctionID, SchemaNames.AliasField.path)

  def RoleByName(scope: ScopeID) =
    unique(scope, NativeIndexID.RoleByName, RoleID, SchemaNames.NameField.path)

  def RolesByResource(scope: ScopeID) =
    index(scope, NativeIndexID.RolesByResource, RoleID, Role.MembershipField.path ++: Membership.ResourceField.path)

  def AccessProviderByName(scope: ScopeID) =
    unique(scope, NativeIndexID.AccessProviderByName, AccessProviderID, SchemaNames.NameField.path)

  def AccessProviderByIssuer(scope: ScopeID) =
    unique(scope, NativeIndexID.AccessProviderByIssuer, AccessProviderID, AccessProvider.IssuerField.path)

  def JournalEntryByHostAndTag(scope: ScopeID) =
    index(scope, NativeIndexID.JournalEntryByHostAndTag, JournalEntryID, JournalEntry.HostField.path, JournalEntry.TagField.path)

  def JournalEntryByTag(scope: ScopeID) =
    index(scope, NativeIndexID.JournalEntryByTag, JournalEntryID, JournalEntry.TagField.path)

  def PrioritizedTasksByCreatedAt(scope: ScopeID) = {
    val terms = Vector(
      (IndexConfig.defaultExtractor(Task.HostField.path, false), false),
      (Task.RunnableExtractor, false))

    // ref doesn't strictly need to be covered, because task execution
    // can get ahold of the internal docID on index tuples, but it
    // seems like the right thing to do in case this index becomes
    // available to operators
    val vals = Vector(
      (IndexConfig.defaultExtractor(Task.PriorityField.path, true), false), // prio desc
      (IndexConfig.defaultExtractor(Task.CreatedAtField.path, false), false), // created_at asc
      (IndexConfig.defaultExtractor(List("ref"), false), false)) // ref
    IndexConfig.Native(
      scope,
      NativeIndexID.PrioritizedTasksByCreatedAt,
      IndexSources(TaskID.collID),
      terms,
      Unconstrained,
      false,
      vals)
  }

  def ExecutingTasksByAccountAndName(scope: ScopeID) = {
    val terms = Vector(
      (IndexConfig.defaultExtractor(Task.AccountField.path, false), false),
      (IndexConfig.defaultExtractor(Task.NameField.path, false), false),
      (Task.ExecutingExtractor, false))

    IndexConfig.Native(
      scope,
      NativeIndexID.ExecutingTasksByAccountAndName,
      IndexSources(TaskID.collID),
      terms,
      Unconstrained,
      isSerial = true)
  }
}
