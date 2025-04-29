package fauna.model

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.{ NativeIndex, PublicCollection }
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.util.{ BCrypt, ClientPasswordValidator, ServerPasswordValidator }

/** Credentials associate instances with login credentials.
  */
case class Credentials(version: Version) extends Document {
  def id = docID.as[CredentialsID]
  def documentID = version.data(Credentials.DocumentField)
  def hashedPassword = version.data(Credentials.HashedPasswordField)

  def matches(password: String) =
    hashedPassword match {
      case Some(hash) => BCrypt.check(password, hash)
      case None       => false
    }
}

object Credentials {

  val DocumentField = Field[DocID]("instance")
  val PasswordField = Field[Option[String]]("password")
  val HashedPasswordField = Field[Option[String]]("hashed_password")

  val ClientValidator =
    ClientPasswordValidator(
      List("hashed_password"),
      List("password"),
      List("current_password"))

  val ServerValidator =
    ServerPasswordValidator(List("hashed_password"), List("password"))

  val VersionValidator =
    Document.VersionValidator + DocumentField.validator + ServerValidator

  def get(scope: ScopeID, id: CredentialsID) =
    PublicCollection.Credential(scope).get(id) mapT { apply(_) }

  def idByDocument(scope: ScopeID, id: DocID): Query[Option[CredentialsID]] = {
    val idx = NativeIndex.CredentialsByDocument(scope)
    val terms = Vector(IndexTerm(DocIDV(id)))
    Store.uniqueIDForKey(idx, terms, Timestamp.MaxMicros).mapT {
      _.as[CredentialsID]
    }
  }

  def getByDocument(scope: ScopeID, id: DocID): Query[Option[Credentials]] =
    idByDocument(scope, id).flatMapT { get(scope, _) }
}
