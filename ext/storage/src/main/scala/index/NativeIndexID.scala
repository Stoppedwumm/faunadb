package fauna.storage.index

import fauna.atoms.IndexID
import fauna.storage.macros.EnumerationMacros
import scala.language.implicitConversions

object NativeIndexID {
  import EnumerationMacros._

  case class InvalidNativeIndexID(id: IndexID, message: String) extends Exception(message)

  implicit def nativeToIndexID(native: NativeIndexID): IndexID = native.id

  case object DatabaseByName extends NativeIndexID(IndexID(0))
  case object IndexByName extends NativeIndexID(IndexID(1))
  case object CollectionByName extends NativeIndexID(IndexID(2))
  case object JournalEntryByHost extends NativeIndexID(IndexID(6))
  case object KeyByDatabase extends NativeIndexID(IndexID(8))
  case object TaskByCompletion extends NativeIndexID(IndexID(11))
  case object CredentialsByDocument extends NativeIndexID(IndexID(14))
  case object TokenByDocument extends NativeIndexID(IndexID(16))
  case object UserFunctionByName extends NativeIndexID(IndexID(17))
  case object JournalEntryByHostAndTag extends NativeIndexID(IndexID(18))
  case object RoleByName extends NativeIndexID(IndexID(19))
  case object RolesByResource extends NativeIndexID(IndexID(20))
  case object JournalEntryByTag extends NativeIndexID(IndexID(22))
  case object DocumentsByCollection extends NativeIndexID(IndexID(24))
  case object AccessProviderByName extends NativeIndexID(IndexID(25))
  case object AccessProviderByIssuer extends NativeIndexID(IndexID(26))
  case object ExecutingTasksByAccountAndName extends NativeIndexID(IndexID(28))
  case object DatabaseByAccountID extends NativeIndexID(IndexID(29))
  case object PrioritizedTasksByCreatedAt extends NativeIndexID(IndexID(30))
  case object DatabaseByDisabled extends NativeIndexID(IndexID(31))
  case object CollectionByAlias extends NativeIndexID(IndexID(32))
  case object UserFunctionByAlias extends NativeIndexID(IndexID(33))
  case object ChangesByCollection extends NativeIndexID(IndexID(34))

  private[this] val ids = sealedInstancesOf[NativeIndexID]
  private[this] val byIdxMap = ids.iterator.map { id => id.id -> id }.toMap
  val All = byIdxMap.keySet

  def unapply(id: IndexID): Option[NativeIndexID] =
    byIdxMap.get(id)
}

/**
  * IndexIDs for natively-defined (as opposed to user-defined)
  * indexes.
  *
  * Schema for these indexes is kept in fauna.model.NativeIndex.
  */
sealed abstract class NativeIndexID(val id: IndexID) extends Ordered[NativeIndexID] {
  def compare(other: NativeIndexID): Int = id.compare(other.id)
}
