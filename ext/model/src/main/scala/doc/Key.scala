package fauna.model

import fauna.ast.{ EvalContext, KeyWriteConfig, RootPosition, WriteAdaptor }
import fauna.atoms._
import fauna.auth.{ AdminPermissions, AuthLike, EvalAuth, KeyLike }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.model
import fauna.model.schema.{ NativeIndex, PublicCollection }
import fauna.repo._
import fauna.repo.cache.CacheKey
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.scheduler._
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.util.ReferencesValidator
import scala.annotation.unused

case class Key(
  id: KeyID,
  parentScopeID: ScopeID,
  globalID: GlobalKeyID,
  scopeID: ScopeID,
  role: Key.Role,
  priorityGroup: PriorityGroup,
  hashedSecret: Option[String],
  deletedTS: Option[Timestamp],
  ttl: Option[Timestamp]) {

  if (hashedSecret.isEmpty) {
    throw new IllegalArgumentException("hashedSecret must be defined.")
  }

  def matches(kl: KeyLike) =
    !isDeleted && (hashedSecret match {
      case Some(hashed) => kl.matches(globalID, hashed)
      case _            => false
    })

  def priority = Priority(priorityGroup.treeGroup.weight)

  def isDeleted = deletedTS.isDefined
}

object Key extends ExceptionLogging {

  val PriorityField = Field[Option[Priority]]("priority")

  def apply(
    live: Version,
    latest: Version,
    dbScope: ScopeID,
    dbPG: PriorityGroup): Key = {
    val gid = GlobalKeyID(live.id.as[KeyID].toLong)
    val priority = live.data(PriorityField) getOrElse Priority.Default
    val tree =
      dbPG.treeGroup.descendant((live.parentScopeID, live.id), priority.toInt)
    val group = PriorityGroup(gid, tree)

    Key(
      live.id.as[KeyID],
      live.parentScopeID,
      gid,
      dbScope,
      live.data(RoleField),
      group,
      live.data(HashField),
      if (latest.isDeleted) Some(latest.ts.validTS) else None,
      latest.data(Version.TTLField)
    )
  }

  case class MultipleKeysException(id: GlobalKeyID, keys: Iterable[Key])
      extends Exception(s"GlobalID: $id / Keys: ${keys.map { _.id }}")

  case class MoveKeyException(reason: String) extends Exception(reason)

  sealed trait Role
  object Role {
    sealed abstract class Builtin(val name: String) extends Role
    object Builtin {
      lazy val Names = Set(
        "admin",
        "server",
        "server-readonly",
        "client"
      )
      def unapply(name: String): Option[Role] =
        name match {
          case "admin"           => Some(AdminRole)
          case "server"          => Some(ServerRole)
          case "server-readonly" => Some(ServerReadOnlyRole)
          case "client"          => Some(ClientRole)
          case _                 => None
        }
    }
  }
  case object AdminRole extends Role.Builtin("admin")
  case object ServerRole extends Role.Builtin("server")
  case object ServerReadOnlyRole extends Role.Builtin("server-readonly")
  case object ClientRole extends Role.Builtin("client")

  object UserRoles {
    def apply(id: RoleID) = new UserRoles(Set(Right(id)))
    def apply(name: String) = new UserRoles(Set(Left(name)))
  }
  final case class UserRoles(roles: Set[Either[String, RoleID]]) extends Role {
    def roleIDs(scope: ScopeID): Query[Set[RoleID]] =
      roles.map {
        case Right(id)  => Query.some(id)
        case Left(name) => model.Role.idByNameActive(scope, name)
      }.sequence map {
        _.view.flatten.toSet
      }
  }

  implicit val RoleFieldType = {
    def validRoleIDs(ids: Vector[IRValue]): Boolean =
      ids forall {
        case StringV(Role.Builtin(_))       => false
        case DocIDV(RoleID(_)) | StringV(_) => true
        case _                              => false
      }

    FieldType[Role]("Role") {
      case role: Role.Builtin => StringV(role.name)
      case UserRoles(roles) =>
        ArrayV(roles.view.map {
          case Right(id)  => DocIDV(id.toDocID)
          case Left(name) => StringV(name)
        }.toVector)
    } {
      case StringV(Role.Builtin(role)) => role
      case StringV(name)               => UserRoles(name)
      case DocIDV(RoleID(id))          => UserRoles(id)
      case ArrayV(ids) if validRoleIDs(ids) =>
        UserRoles(ids.view.map { id =>
          (id: @unchecked) match {
            case DocIDV(RoleID(id)) => Right(id)
            case StringV(name)      => Left(name)
          }
        }.toSet)
    }
  }

  val RoleField = Field[Role]("role")
  val DatabaseField = Field[Option[DatabaseID]]("database")
  val SecretField = Field[Option[String]]("secret")
  val HashField = Field[Option[String]]("hashed_secret")

  val VersionValidator =
    Document.DataValidator +
      RoleField.validator +
      DatabaseField.validator +
      PriorityField.validator +
      Version.TTLField.validator

  def LiveValidator(ec: EvalContext) =
    VersionValidator +
      ReferencesValidator(ec) +
      RoleNameValidator(ec, RoleField.path)

  final case class RoleNameValidator(ec: EvalContext, path: List[String])
      extends Validator[Query] {

    protected def filterMask =
      MaskTree.empty

    override protected def validateData(
      data: Data): Query[List[ValidationException]] =
      data.fields
        .get(path)
        .collect {
          case StringV(Role.Builtin(_)) => Query.value(Nil)
          case StringV(name)            => validateName(name)
          case ArrayV(elems) =>
            elems.collect { case StringV(name) =>
              validateName(name)
            }.sequence map {
              _.view.flatten.toList
            }
        }
        .getOrElse(Query.value(Nil))

    private def validateName(name: String): Query[List[ValidationException]] =
      model.Role.idByNameActive(ec.scopeID, name).isDefinedT map {
        case true => Nil
        case false =>
          List(
            ValidationFailure(
              path,
              s"$Role name `$name` does not exist."
            ))
      }
  }

  case class SecretValidator(scopeID: ScopeID, id: Option[GlobalKeyID])
      extends Validator[Query] {
    protected val filterMask = MaskTree(HashField.path)

    private def validateSecret(secret: String, diff: Diff) =
      AuthLike.fromBase64(secret) match {
        case Some(k: KeyLike) if id.contains(k.id) =>
          getAll(k) findValueT { _.parentScopeID != scopeID } map {
            case Some(_) =>
              Left(List(DuplicateValue(SecretField.path)))
            case None =>
              Right(diff.update(HashField -> Some(k.hashedSecret)))
          }
        case _ =>
          Query(Left(List(InvalidSecret(SecretField.path))))
      }

    private def validateHashedSecret(hashed: String, diff: Diff) =
      id match {
        case None =>
          Query(Right(diff.update(HashField -> Some(hashed))))
        case Some(gID) =>
          getAll(gID) findValueT { _.parentScopeID != scopeID } map {
            case Some(_) =>
              Left(List(DuplicateValue(HashField.path)))
            case None =>
              Right(diff.update(HashField -> Some(hashed)))
          }
      }

    override protected def validatePatch(current: Data, diff: Diff) =
      (SecretField.read(diff.fields), HashField.read(diff.fields)) match {
        case (Left(List(ValueRequired(_))), Left(List(ValueRequired(_)))) =>
          Query.value(Right(diff))

        case (Right(None), Right(None)) =>
          Query.value(Right(diff))

        case (Right(Some(secret)), _) =>
          validateSecret(secret, diff)

        case (_, Right(Some(hashed))) =>
          validateHashedSecret(hashed, diff)

        case (e1, e2) =>
          Query.value(Validator.collectErrors(e1, e2))
      }
  }

  private case class CKey(key: KeyLike) extends CacheKey[Key] {
    override def hashCode(): Int = key.id.hashCode

    override val shouldHaveRegion = false

    // overridden to allow compare with CKeyInvalidator
    override def equals(obj: Any): Boolean = obj match {
      case other: CKey            => key equals other.key
      case other: CKeyInvalidator => key.id == other.id
      case _                      => false
    }

    def query =
      getAll(key).flattenT flatMap {
        case Seq() => Query.none

        case Seq(key) => Query.some(key)

        // Somehow we matched multiple keys. Bad times.
        case keys =>
          squelchAndLogException {
            throw MultipleKeysException(key.id, keys)
          }
          Query.none
      }
  }

  private case class ByGlobalID(id: GlobalKeyID) extends CacheKey[Key] {
    override def hashCode(): Int = id.hashCode

    // overridden to allow compare with CKeyInvalidator
    override def equals(obj: Any): Boolean = obj match {
      case x: ByGlobalID      => id == x.id
      case x: CKeyInvalidator => id == x.id
      case _                  => false
    }

    def query =
      Store.keys(id).headValueT flatMapT { case (scope, key) =>
        Key.getLatest(scope, key)
      }

    override val shouldHaveRegion: Boolean = false
  }

  /** That key cache allows the invalidation of Keys in the cache by only using
    * the GlobalKeyID (the KeyID) without the need of the secret.
    */
  private case class CKeyInvalidator(id: GlobalKeyID) extends CacheKey[Key] {
    override def hashCode(): Int = id.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case x: CKey            => id == x.key.id
      case x: ByGlobalID      => id == x.id
      case x: CKeyInvalidator => id == x.id
      case _                  => false
    }

    def query = Query.none
  }

  /** Uncached get by KeyID */
  def getLatest(scope: ScopeID, id: KeyID): Query[Option[Key]] = {
    val liveQ = PublicCollection.Key(scope).getVersionLiveNoTTL(id)
    val latestQ = PublicCollection.Key(scope).getVersionNoTTL(id)

    (liveQ, latestQ) parT { (live, latest) =>
      val byField = live.data(DatabaseField) map { Database.getUncached(scope, _) }
      val byScope = Database.forScope(live.parentScopeID)
      byField getOrElse byScope mapT { db =>
        Key(live, latest, db.scopeID, db.priorityGroup)
      }
    }
  }

  // We don't guarantee the uniqueness of a Key's GlobalKeyID. Multiple keys are
  // filtered by whether they match the provided KeyLike.
  private def getAll(key: KeyLike): PagedQuery[Iterable[Key]] =
    getAll(key.id) selectT { _ matches key }

  private def getAll(id: GlobalKeyID): PagedQuery[Iterable[Key]] =
    Store.keys(id) flatMapValuesT { case (sID, kID) =>
      getLatest(sID, kID) map {
        _.toSeq
      }
    }

  def forKeyLike(key: KeyLike): Query[Option[Key]] =
    Query.timing("Key.Cached.Get") {
      Query.repo flatMap {
        _.cacheContext.keys.get(CKey(key)) rejectT { _.isDeleted }
      }
    }

  def forGlobalID(id: GlobalKeyID): Query[Option[Key]] =
    Query.timing("Key.Cached.Get") {
      Query.repo flatMap { _.cacheContext.keys.get(ByGlobalID(id)) }
    }

  /** Returns all keys within the parent scope which access the
    * provided database.
    */
  def forDatabase(scope: ScopeID, db: DatabaseID): PagedQuery[Iterable[Key]] =
    idsForDatabase(scope, db).flatMapValuesT(getLatest(scope, _).map(_.toSeq))

  def idsForDatabase(scope: ScopeID, db: DatabaseID): PagedQuery[Iterable[KeyID]] = {
    val idx = NativeIndex.KeyByDatabase(scope)
    val terms = Vector(Scalar(DocIDV(db.toDocID)))
    Store.collection(idx, terms, Timestamp.MaxMicros).mapValuesT(_.docID.as[KeyID])
  }

  /** Move all keys for database "from" into database "to". */
  def moveKeys(from: Database, to: Database): Query[Unit] = {
    Query.snapshotTime flatMap { ts =>
      val keyAdaptor = new WriteAdaptor(KeyWriteConfig.Move)
      val dstEc = EvalContext.write(
        EvalAuth(to.parentScopeID, AdminPermissions),
        ts,
        APIVersion.Default)

      Key.forDatabase(from.parentScopeID, from.id).foldLeftValuesMT(()) {
        case (_, key) =>
          val diff = Diff(
            Key.DatabaseField -> Some(to.id),
            Key.RoleField -> key.role,
            Key.HashField -> key.hashedSecret,
            Version.TTLField -> key.ttl)

          PublicCollection.Key(key.parentScopeID).clearDocument(key.id) flatMap {
            _ =>
              PublicCollection.Key(to.parentScopeID).get(key.id) flatMap {
                case None =>
                  // The key does not exist in the
                  // destination. It may have been deleted since
                  // the snapshot. We're free to insert a new one.
                  keyAdaptor.create(
                    dstEc,
                    Some(key.id.toDocID.subID),
                    diff,
                    RootPosition) flatMap {
                    case Right(_) => Query.unit
                    case Left(_) =>
                      Query.fail(MoveKeyException(
                        "Failed to create key in the destination database."))
                  }

                case Some(_) =>
                  // The key already exist in the destination database, just update
                  // the fields.
                  keyAdaptor.update(
                    dstEc,
                    key.id.toDocID.subID,
                    diff,
                    isPartial = true,
                    RootPosition) flatMap {
                    case Right(_) => Query.unit
                    case Left(_) =>
                      Query.fail(MoveKeyException(
                        "Failed to update the key in destination database."))
                  }
              }
          }
      }
    }
  }

  def invalidateCaches(@unused scope: ScopeID, keyID: KeyID): Query[Unit] =
    Query.repo map { repo =>
      val id = GlobalKeyID(keyID.toLong)
      repo.cacheContext.keys.invalidate(CKeyInvalidator(id))
      repo.cacheContext.keys.invalidate(ByGlobalID(id))
    }
}
