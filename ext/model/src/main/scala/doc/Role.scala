package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.auth.ActionNames
import fauna.lang._
import fauna.lang.syntax._
import fauna.model.schema.{ NativeIndex, SchemaCollection, SchemaItemView }
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query._
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.util._

final case class Membership(
  resource: Either[String, CollectionID],
  predicate: Option[Either[String, QueryV]])

object Membership {
  import PredicateValidator._

  implicit val MembershipFieldType = FieldType.RecordCodec[Membership]

  val ResourceField = Field[Either[String, CollectionID]]("resource")
  val PredicateField = Field[Option[QueryV]]("predicate")

  private[model] def fieldsMask(rootPath: List[String]): MaskTree =
    MaskTree(rootPath, ResourceField.path) merge
      MaskTree(rootPath, PredicateField.path)

  private[model] def validator(rootPath: List[String]): Validator[Query] =
    PredicateValidator(rootPath ::: PredicateField.path, expectedArity = Fixed(1))
}

sealed trait RoleAction
final case class StaticRoleAction(allowed: Boolean) extends RoleAction

final case class DynamicRoleAction(predicate: Either[String, QueryV])
    extends RoleAction

object RoleAction {

  implicit val RoleActionFieldType = FieldType[RoleAction]("RoleAction") {
    case StaticRoleAction(allowed)       => BooleanV(allowed)
    case DynamicRoleAction(Left(query))  => StringV(query)
    case DynamicRoleAction(Right(query)) => query
  } {
    case BooleanV(allowed) => StaticRoleAction(allowed)
    case StringV(query)    => DynamicRoleAction(Left(query))
    case query: QueryV     => DynamicRoleAction(Right(query))
  }
}

final case class Privilege(
  resource: Either[String, DocID],
  actions: Option[List[(String, RoleAction)]]
)

object Privilege {
  import ActionNames._
  import PredicateValidator._

  val ResourceField = Field[Either[String, DocID]]("resource")

  implicit val PrivilegeFieldType = FieldType.RecordCodec[Privilege]

  private val ActionFieldsConfig: Seq[(Field[Option[RoleAction]], Arity)] =
    Seq(
      (Field("actions", Read), Fixed(1)),
      (Field("actions", Write), Fixed(2)),
      (Field("actions", Create), Fixed(1)),
      (Field("actions", CreateWithId), Fixed(2)),
      (Field("actions", Delete), Fixed(1)),
      (Field("actions", HistoryWrite), Fixed(4)),
      (Field("actions", HistoryRead), Fixed(1)),
      (Field("actions", UnrestrictedRead), Fixed(1)),
      (Field("actions", Call), Fixed(1))
    )

  private[model] def fieldsMask(rootPath: List[String]): MaskTree = {
    ActionFieldsConfig.foldLeft(MaskTree(rootPath, ResourceField.path)) {
      case (mask, (field, _)) =>
        mask merge MaskTree(rootPath, field.path)
    }
  }

  private[model] def validator(rootPath: List[String]): Validator[Query] = {
    val validators: Seq[Validator[Query]] = ActionFieldsConfig map {
      case (field, arity) =>
        PredicateValidator(rootPath ::: field.path, arity)
    }

    validators reduce { _ + _ }
  }
}

/** This validator is used to ensure that all of the data is FQL4 compatible.
  * If there is any FQLX data in the role privileges this validator will fail the update.
  * This means that both the resource and predicate bodies may not be strings.
  */
object PrivilegesFQL4FormatValidator extends Validator[Query] {
  override protected def filterMask: MaskTree = MaskTree.empty

  override protected def validateData(
    data: Data): Query[List[ValidationException]] = {
    Query.value(validatePrivileges(data))
  }

  private def validatePrivileges(data: Data): List[ValidationException] = {
    Role.PrivilegesField.read(data.fields) match {
      // this is handled by a different validator
      case Left(_)           => List.empty
      case Right(privileges) => privileges.flatMap { validatePrivilege(_) }.toList
    }
  }

  private def validatePrivilege(privilege: Privilege): List[ValidationException] = {
    val resourceValidationErrs = privilege.resource match {
      case Left(_) =>
        List(
          InvalidType(
            path = Role.PrivilegesField.path ++ Privilege.ResourceField.path,
            expected = DocIDV.Type,
            actual = StringV.Type
          )
        )
      case _ => List.empty
    }
    resourceValidationErrs ++ (privilege.actions.map { actions =>
      actions.collect { case (field, DynamicRoleAction(Left(_))) =>
        InvalidType(
          path = Role.PrivilegesField.path ++ List("actions", field),
          expected = QueryV.Type,
          actual = StringV.Type
        )
      }
    } getOrElse (List.empty))
  }
}

/** This validator is used to ensure that all of the data is FQL4 compatible.
  * If there is any FQLX data in the role memberships this validator will fail the update.
  * This means that both the resource and predicate bodies may not be strings.
  */
object MembershipsFQL4FormatValidator extends Validator[Query] {
  override protected def filterMask: MaskTree = MaskTree.empty

  override protected def validateData(
    data: Data): Query[List[ValidationException]] = {
    Query.value(validateMemberships(data))
  }

  private def validateMemberships(data: Data): List[ValidationException] = {
    Role.MembershipField.read(data.fields) match {
      // this is handled by another validator
      case Left(_)            => List.empty
      case Right(memberships) => memberships.flatMap { validateMembership(_) }.toList
    }
  }

  private def validateMembership(
    membership: Membership): List[ValidationException] = {
    val resourceValidationErrs =
      membership.resource match {
        case Left(_) =>
          List(
            InvalidType(
              path = Role.MembershipField.path ++ Membership.ResourceField.path,
              expected = DocIDV.Type,
              actual = StringV.Type
            )
          )
        case _ => List.empty
      }
    resourceValidationErrs ++ (membership.predicate match {
      case Some(Left(_)) =>
        List(
          InvalidType(
            path = Role.MembershipField.path ++ Membership.PredicateField.path,
            expected = QueryV.Type,
            actual = StringV.Type
          )
        )
      case _ => List.empty
    })
  }
}

final case class Role(
  id: RoleID,
  scope: ScopeID,
  name: String,
  membership: Vector[Membership],
  privileges: Vector[Privilege]
)

object Role {

  val MaxRolesPerCollection = 64
  val MembershipField = Field.ZeroOrMore[Membership]("membership")
  val PrivilegesField = Field.ZeroOrMore[Privilege]("privileges")

  def apply(vers: Version): Role = {
    Role(
      vers.id.as[RoleID],
      vers.parentScopeID,
      SchemaNames.findName(vers),
      vers.data(MembershipField),
      vers.data(PrivilegesField)
    )
  }

  def isV10Role(data: Data): Boolean = {
    val privCheck = data(Role.PrivilegesField) forall { priv =>
      priv.resource.isLeft && (priv.actions.exists {
        _.forall {
          case (_, StaticRoleAction(_))        => true
          case (_, DynamicRoleAction(Left(_))) => true
          case _                               => false
        }
      })
    }

    val membCheck = data(Role.MembershipField) forall { memb =>
      memb.resource.isLeft && memb.predicate.forall { _.isLeft }
    }

    privCheck && membCheck
  }

  val VersionValidator =
    Document.DataValidator +
      SchemaNames.NameField.validator +
      MembershipField.validator(Membership.fieldsMask(MembershipField.path)) +
      PrivilegesField.validator(Privilege.fieldsMask(PrivilegesField.path)) +
      Privilege.validator(PrivilegesField.path) +
      Membership.validator(MembershipField.path) +
      PrivilegesFQL4FormatValidator +
      MembershipsFQL4FormatValidator

  def LiveValidator(ec: EvalContext): Validator[Query] =
    VersionValidator + ReferencesValidator(ec)

  def idByNameActive(scope: ScopeID, name: String): Query[Option[RoleID]] =
    Cache.roleIDByName(scope, name).map(_.flatMap(_.active))

  def idByNameStaged(scope: ScopeID, name: String): Query[Option[RoleID]] =
    Cache.roleIDByName(scope, name).map(_.flatMap(_.staged))

  def get(scope: ScopeID, id: RoleID): Query[Option[Role]] =
    getItem(scope, id).map(_.flatMap(_.active))

  def getItem(scope: ScopeID, id: RoleID): Query[Option[SchemaItemView[Role]]] =
    Cache.roleByID(scope, id)

  def getItemUncached(
    scope: ScopeID,
    id: RoleID): Query[Option[SchemaItemView[Role]]] =
    SchemaCollection.Role(scope).schemaVersState(id).flatMapT {
      _.flatMapSimplified(vers => Query.value(Role(vers))).map(Some(_))
    }

  def byMembership(
    scope: ScopeID,
    id: Either[String, CollectionID]): PagedQuery[Iterable[RoleID]] = {

    val index = NativeIndex.RolesByResource(scope)
    val t = id.fold(StringV.apply, { c => DocIDV(c.toDocID) })
    val terms = Vector(Scalar(t))

    Store.collection(
      index,
      terms,
      Timestamp.MaxMicros,
      pageSize = MaxRolesPerCollection) mapValuesT {
      _.docID.as[RoleID]
    }
  }

  // This validation requires an index read. There is not enough information
  // in the [[Validator]] API to enable this validation before the write operation.
  def validateMaxRolesPerResource(
    scope: ScopeID,
    data: Data,
    pos: Position = RootPosition
  ): Query[Either[ResourcesExceeded, Unit]] = {

    val countsQ = data(Role.MembershipField) map { membership =>
      Role.byMembership(scope, membership.resource).countT
    }

    countsQ.sequence map { counts =>
      if (counts.exists { _ > Role.MaxRolesPerCollection }) {
        Left(
          ResourcesExceeded(
            pos.toPath,
            "roles per membership class",
            Role.MaxRolesPerCollection))
      } else {
        Right(())
      }
    }
  }
}
