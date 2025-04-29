package fauna.atoms

import fauna.codex.cbor._
import java.lang.{ Long => JLong }

/** Table of hard-coded internal collection IDs. External consumers should use <IDCompanion>.collID
  */
private[atoms] object InternalCollID {
  final val Database = 0
  final val Collection = 1
  final val Key = 2
  final val Token = 3
  final val SampleData = 6
  final val Index = 7
  final val JournalEntry = 8
  final val Task = 9
  final val Credentials = 10
  final val UserFunction = 11
  final val Role = 12
  final val AccessProvider = 13
  final val SchemaSource = 14
}

trait ID[T <: ID[_]] extends Any with Ordered[T] {
  def toLong: Long
  def compare(b: T) = JLong.compare(toLong, b.toLong)
  def toDocID(implicit ev: CollectionIDTag[T]) = DocID(SubID(toLong), ev.collID)
}

object ID {
// marker trait for schema ID types
  sealed abstract class SchemaMarker[T <: ID[T]]
}

sealed abstract class IDCompanion[T <: ID[T]](
  val typeName: String,
  val collID: CollectionID
) {
  def apply(id: Long): T
  def unapply(id: DocID): Option[T] =
    if (id.collID == collID) Some(apply(id.subID.toLong)) else None

  val MaxValue = apply(Long.MaxValue)
  val MinValue = apply(0)

  implicit lazy val collIDTag = CollectionIDTag[T](collID, apply)
  implicit val idCompanion = this
  implicit val CBORCodec = CBOR.AliasCodec[T, Long](apply, _.toLong)
}

object DatabaseID
    extends IDCompanion[DatabaseID](
      "Database",
      new CollectionID(InternalCollID.Database)) {
  val RootID = DatabaseID(0)
  override val MinValue = DatabaseID(1L)
}

case class DatabaseID(toLong: Long) extends AnyVal with ID[DatabaseID]

case class CollectionIDTag[T](collID: CollectionID, toT: Long => T) {
  def apply(long: Long): T = toT(long)

  def fromDocID(docID: DocID) =
    if (docID.collID == collID) Some(apply(docID.subID.toLong)) else None
}

object CollectionID
    extends IDCompanion[CollectionID](
      "Collection",
      new CollectionID(InternalCollID.Collection)) {

  implicit object SchemaMarker extends ID.SchemaMarker[CollectionID]

  override val MinValue = CollectionID(0)
  override val MaxValue = CollectionID(Short.MaxValue) // 2 bytes

  def apply(value: Long): CollectionID = CollectionID(value.shortValue)
}

object UserCollectionID {
  val MinValue = CollectionID(1024)
  val MaxValue = CollectionID.MaxValue

  def unapply(id: CollectionID): Option[CollectionID] =
    if (id >= MinValue) {
      Some(id)
    } else {
      None
    }
}

case class CollectionID(value: Short) extends AnyVal with ID[CollectionID] {
  def toLong = value.longValue
}

object KeyID extends IDCompanion[KeyID]("Key", new CollectionID(InternalCollID.Key))

case class KeyID(toLong: Long) extends AnyVal with ID[KeyID]

object TokenID
    extends IDCompanion[TokenID]("Token", new CollectionID(InternalCollID.Token))

case class TokenID(toLong: Long) extends AnyVal with ID[TokenID]

object SampleDataID
    extends IDCompanion[SampleDataID](
      "SampleData",
      new CollectionID(InternalCollID.SampleData))

case class SampleDataID(toLong: Long) extends AnyVal with ID[SampleDataID] {
  def toSampleDataID = SampleDataID(toLong)
}

object IndexID
    extends IDCompanion[IndexID]("Index", new CollectionID(InternalCollID.Index)) {
  val NativeMinValue = IndexID(0)
  val NativeMaxValue = IndexID(32767)
}

object UserIndexID {
  val MinValue = IndexID(32768)
  val MaxValue = IndexID.MaxValue

  def unapply(id: IndexID): Option[IndexID] =
    if (id >= MinValue) {
      Some(id)
    } else {
      None
    }
}

case class IndexID(toLong: Long) extends AnyVal with ID[IndexID]

object JournalEntryID
    extends IDCompanion[JournalEntryID](
      "JournalEntry",
      new CollectionID(InternalCollID.JournalEntry))

case class JournalEntryID(toLong: Long) extends AnyVal with ID[JournalEntryID]

object TaskID
    extends IDCompanion[TaskID]("Task", new CollectionID(InternalCollID.Task))

case class TaskID(toLong: Long) extends AnyVal with ID[TaskID]

object CredentialsID
    extends IDCompanion[CredentialsID](
      "Credential",
      new CollectionID(InternalCollID.Credentials))

case class CredentialsID(toLong: Long) extends AnyVal with ID[CredentialsID]

object UserFunctionID
    extends IDCompanion[UserFunctionID](
      "Function",
      new CollectionID(InternalCollID.UserFunction)) {
  implicit object SchemaMarker extends ID.SchemaMarker[UserFunctionID]
}

case class UserFunctionID(toLong: Long) extends AnyVal with ID[UserFunctionID]

object RoleID
    extends IDCompanion[RoleID]("Role", new CollectionID(InternalCollID.Role)) {
  implicit object SchemaMarker extends ID.SchemaMarker[RoleID]
}

case class RoleID(toLong: Long) extends AnyVal with ID[RoleID]

object AccessProviderID
    extends IDCompanion[AccessProviderID](
      "AccessProvider",
      new CollectionID(InternalCollID.AccessProvider)) {
  implicit object SchemaMarker extends ID.SchemaMarker[AccessProviderID]
}

case class AccessProviderID(toLong: Long) extends AnyVal with ID[AccessProviderID]

object SchemaSourceID
    extends IDCompanion[SchemaSourceID](
      "SchemaSource",
      new CollectionID(InternalCollID.SchemaSource))

case class SchemaSourceID(toLong: Long) extends AnyVal with ID[SchemaSourceID]
