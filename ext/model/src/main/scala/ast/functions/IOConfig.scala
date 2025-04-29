package fauna.ast

import fauna.auth.JWTToken
import fauna.model._
import fauna.repo.doc.Version
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.storage.DocEvent

sealed abstract class IOConfig {

  protected val internalFields: List[Field[_]] = Nil

  protected val readOnlyFields: List[Field[_]] = Nil

  // These fields can only be written to when the document
  // is created. They are read-only on subsequent updates.
  protected val finalFields: List[Field[_]] = Nil

  protected val hasInternalFields = false

  protected lazy val readMask =
    internalFields.foldLeft(MaskTree.empty) { (m, f) => m.merge(MaskTree(f.path)) }

  private lazy val writeMask =
    (readOnlyFields ++ finalFields).foldLeft(readMask) { (m, f) =>
      m.merge(MaskTree(f.path))
    }

  private lazy val createMask =
    readOnlyFields.foldLeft(readMask) { (m, f) => m.merge(MaskTree(f.path)) }

  def writeableFields(fields: MapV, isCreate: Boolean): MapV = {
    if (isCreate) {
      createMask reject fields
    } else {
      writeMask reject fields
    }
  }

  def writeProtectedFields(fields: MapV): MapV =
    writeMask select fields

  def readableFields(fields: MapV): MapV =
    readMask reject fields

  def allowedInContainer: Boolean = false

  def writeableDiff(diff: Diff, isCreate: Boolean): Diff =
    Diff(writeableFields(diff.fields, isCreate))

  def writeProtectedData(data: Data): Data =
    Data(writeProtectedFields(data.fields))

  def readableData(version: Version): Data =
    if (hasInternalFields) {
      Data(readableFields(version.data.fields))
    } else {
      version.data
    }

  def readableData(event: DocEvent): Data =
    if (hasInternalFields) {
      Data(readableFields(event._diff))
    } else {
      Data(event._diff)
    }

  def readableDataStream(version: Version): DataStream =
    if (hasInternalFields) {
      IRValueDataStream(readableFields(version.data.fields))
    } else {
      version.dataStream
    }

  def readableDiffStream(event: DocEvent): DataStream =
    if (hasInternalFields) {
      IRValueDataStream(readableFields(event.diff.fields))
    } else {
      event.diffStream
    }
}

object MoveDatabaseIOConfig extends IOConfig {
  override def allowedInContainer: Boolean = true
}

abstract class DatabaseIOConfig extends IOConfig {
  override def allowedInContainer: Boolean = true

  override def readableData(version: Version): Data = {
    super.readableData(version.patch(patchGlobalID(version)))
  }

  override def readableDataStream(version: Version): DataStream = {
    super.readableDataStream(version.patch(patchGlobalID(version)))
  }

  private def patchGlobalID(version: Version) =
    version.data.getOpt(Database.GlobalIDField) match {
      case Some(id) =>
        Some(Diff(MapV("global_id" -> Database.encodeGlobalID(id))))
      case None =>
        // Prior to ENG-XXX, global_id was equal to scope.
        val scope = version.data(Database.ScopeField)
        Some(Diff(MapV("global_id" -> Database.encodeGlobalID(scope))))
    }
}

object DatabaseWriteIOConfig extends DatabaseIOConfig {
  override val hasInternalFields = true
  override val internalFields =
    List(
      Database.ScopeField,
      Database.GlobalIDField,
      Database.ActiveSchemaVersField,
      Database.NativeSchemaVersField,
      Database.MVTPinsField,
      Database.MVTPinTSField)
  override val finalFields = List(Database.AccountIDField)
}

object DatabaseReadIOConfig extends DatabaseIOConfig {
  override val hasInternalFields = true
  override val internalFields =
    List(
      Database.ScopeField,
      Database.AccountField,
      Database.DisabledField,
      Database.ActiveSchemaVersField,
      Database.NativeSchemaVersField,
      Database.MVTPinsField,
      Database.MVTPinTSField
    )
}

object CollectionIOConfig extends IOConfig {
  override val hasInternalFields = true

  override lazy val readMask =
    MaskTree(
      Collection.LegacyCollectionIndexField.path(0) -> MaskAll,
      Collection.BackingIndexesField.path(0) -> MaskAll,
      Collection.InternalSignaturesField.path(0) -> MaskAll,
      Collection.InternalMigrationsField.path(0) -> MaskAll,
      Collection.MinValidTimeFloorField.path(0) -> MaskAll,
      Collection.MigrationsField.path(0) -> MaskAll,
      Collection.DefinedFields.path(0) -> MaskTree.Wildcard(
        MaskTree(Collection.BackfillValue.path(0) -> MaskAll)
      )
    )
}

object KeyIOConfig extends IOConfig {
  override val finalFields = List(Key.HashField)
  override def allowedInContainer: Boolean = true
}

object AccessProviderIOConfig extends IOConfig {
  override def allowedInContainer: Boolean = true

  override val finalFields = List(AccessProvider.AudienceField)

  override def readableData(version: Version): Data = {
    super.readableData(version.patch(patchAudience(version)))
  }

  override def readableDataStream(version: Version): DataStream = {
    super.readableDataStream(version.patch(patchAudience(version)))
  }

  private def patchAudience(version: Version) =
    version.data.getOpt(AccessProvider.AudienceField) match {
      case Some(_) => Some(Diff.empty)
      case None    =>
        // Prior to ENG-XXX, global_id was not denormalized into AccessProvider.
        Some(
          Diff(MapV("audience" -> JWTToken.canonicalDBUrl(version.parentScopeID))))
    }
}

object RoleIOConfig extends IOConfig {
  override def allowedInContainer: Boolean = true
}

object IndexIOConfig extends IOConfig {
  override val hasInternalFields = true
  override val internalFields = List(Index.HiddenField)
}

object TokenIOConfig extends IOConfig {
  override val hasInternalFields = true
  override val internalFields = List(Token.GlobalIDField)
}
object CredentialsIOConfig extends IOConfig
object TaskIOConfig extends IOConfig

object UserFunctionIOConfig extends IOConfig {
  override val hasInternalFields = true
  override val internalFields = List(UserFunction.InternalSigField)
}
object DocumentIOConfig extends IOConfig
