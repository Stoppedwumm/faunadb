package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.model.schema.{ NativeIndex, SchemaCollection, SchemaItemView }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fauna.util.ReferencesValidator
import java.net.URL
import scala.collection.immutable.Queue

case class AccessProviderRole(roles: Option[Vector[AccessProviderRole.Persisted]])
    extends ExceptionLogging {

  def parse(
    scope: ScopeID,
    staged: Boolean): Query[Vector[AccessProviderRole.Parsed]] = {
    roles
      .getOrElse {
        Vector.empty
      }
      .map {
        case AccessProviderRole.Persisted(role, Some(predicate)) =>
          LambdaWrapper.parsePredicate(scope, predicate) flatMap { parsed =>
            getRoleID(scope, role, staged) map { roleID =>
              AccessProviderRole.Parsed(roleID, Some(parsed))
            }
          }

        case AccessProviderRole.Persisted(role, None) =>
          getRoleID(scope, role, staged) map { roleID =>
            AccessProviderRole.Parsed(roleID, None)
          }
      }
      .sequence
      .map { _.toVector }
  }

  private def getRoleID(
    scope: ScopeID,
    role: Either[String, RoleID],
    staged: Boolean): Query[RoleID] = role match {
    case Right(roleID) => Query.value(roleID)
    case Left(role) =>
      val roleQ = if (staged) {
        Role.idByNameStaged(scope, role)
      } else {
        Role.idByNameActive(scope, role)
      }
      roleQ map {
        case Some(roleID) => roleID
        case None =>
          logException(
            new IllegalStateException(s"Role `$role` does not exist in $scope."))
          throw new IllegalStateException(s"Role `$role` does not exist.")
      }
  }
}

object AccessProviderRole {
  case class Persisted(
    role: Either[String, RoleID],
    predicate: Option[LambdaWrapper.Src])
  case class Parsed(role: RoleID, predicate: Option[LambdaWrapper])

  val RoleField = Field[Either[String, RoleID]]("role")
  val PredicateField = Field[LambdaWrapper.Src]("predicate")

  implicit val RoleFieldType =
    FieldType.validating[Persisted]("Role") {
      case Persisted(Right(roleID), None) =>
        DocIDV(roleID.toDocID)

      case Persisted(Left(role), None) =>
        StringV(role)

      case Persisted(roleID, Some(predicate)) =>
        MapV(
          "role" -> roleID.fold(StringV(_), _.toDocID),
          "predicate" -> predicate.fold(StringV(_), identity))
    } {
      case DocIDV(roleID) =>
        Right(Persisted(Right(roleID.as[RoleID]), None))

      case StringV(role) =>
        Right(Persisted(Left(role), None))

      case map: MapV =>
        for {
          role      <- RoleField.read(map)
          predicate <- PredicateField.read(map)
        } yield {
          Persisted(role, Some(predicate))
        }
    }

  implicit val RolesListFieldType = new FieldType[AccessProviderRole] {
    override def vtype: IRType = IRType.Custom("RolesList")

    override def decode(
      value: Option[IRValue],
      path: Queue[String]): Either[List[ValidationException], AccessProviderRole] =
      value match {
        case Some(ArrayV(_)) =>
          val fieldType = implicitly[FieldType[Vector[Persisted]]]

          fieldType.decode(value, path) map { membership =>
            AccessProviderRole(Some(membership))
          }

        case Some(_) =>
          RoleFieldType.decode(value, path) map { membership =>
            AccessProviderRole(Some(Vector(membership)))
          }

        case None =>
          Right(AccessProviderRole(None))
      }

    override def encode(value: AccessProviderRole): Option[IRValue] =
      value.roles flatMap { roles =>
        if (roles.sizeIs == 1) {
          RoleFieldType.encode(roles.head)
        } else {
          val fieldType = implicitly[FieldType[Vector[Persisted]]]
          fieldType.encode(roles)
        }
      }
  }
}

/** This validator is used to ensure that all of the data is FQL4 compatible.
  * If there is any FQLX data in the role privileges this validator will fail the update.
  * This means that both the resource and predicate bodies may not be strings.
  *
  * This was copied from doc/Role.scala.
  */
object RolesFQL4FormatValidator extends Validator[Query] {
  override protected def filterMask: MaskTree = MaskTree.empty

  override protected def validateData(
    data: Data): Query[List[ValidationException]] = {
    Query.value(validateRoles(data))
  }

  private def validateRoles(data: Data): List[ValidationException] = {
    AccessProvider.RolesField.read(data.fields) match {
      case Left(_) => List.empty
      case Right(roles) =>
        roles.roles.getOrElse(Vector.empty).flatMap { validateRole(_) }.toList
    }
  }

  private def validateRole(
    role: AccessProviderRole.Persisted): Option[ValidationException] = {
    role.predicate.flatMap {
      case Left(_) =>
        Some(InvalidType(
          path =
            AccessProvider.RolesField.path ++ AccessProviderRole.PredicateField.path,
          expected = QueryV.Type,
          actual = StringV.Type
        ))
      case Right(_) => None
    }
  }
}

/** Represents an access provider authority that will be used to validate Json Web Tokens (JWT).
  */
final case class AccessProvider(
  id: AccessProviderID,
  scopeID: ScopeID,
  name: String,
  issuer: String,
  jwksUri: URL,
  roles: Vector[AccessProviderRole.Parsed])

object AccessProvider {

  implicit val URLT: FieldType[URL] = FieldType.validating[URL]("URI") { url =>
    StringV(url.toString)
  } { case StringV(str) =>
    try {
      val url = new URL(str)

      if (url.getProtocol.toLowerCase != "https") {
        Left(List(InvalidURI(JwksUriField.path, str)))
      } else {
        Right(url)
      }
    } catch {
      case _: Exception =>
        Left(List(InvalidURI(JwksUriField.path, str)))
    }
  }

  // uniquely identify the authority/user that issued the token, ie:
  // https://dev-xpto.auth0.com/
  val IssuerField = Field[String]("issuer")

  // contains the url with the public key to validate tokens signature
  val JwksUriField = Field[URL]("jwks_uri")

  // a list of roles and predicates
  val RolesField = Field[AccessProviderRole]("roles")

  /** Audience is a denormalized copy of Database.GlobalIDField,
    * prefixed with JWTToken.CanonicalDBUrlPrefix.
    */
  val AudienceField = Field[String]("audience")

  def apply(
    vers: Version.Live,
    parsed: Vector[AccessProviderRole.Parsed]): AccessProvider =
    AccessProvider(
      vers.id.as[AccessProviderID],
      vers.parentScopeID,
      SchemaNames.findName(vers),
      vers.data(IssuerField),
      vers.data(JwksUriField),
      parsed)

  val VersionValidator =
    Document.DataValidator +
      SchemaNames.NameField.validator +
      IssuerField.validator +
      JwksUriField.validator +
      RolesField.validator +
      RolesFQL4FormatValidator

  def LiveValidator(ec: EvalContext, sub: Option[SubID]) =
    VersionValidator +
      IssuerValidator(ec, sub) +
      ReferencesValidator(ec) +
      RolesValidator(ec)

  def idByNameActive(scope: ScopeID, name: String): Query[Option[AccessProviderID]] =
    Cache.accessProviderIDByName(scope, name).map(_.flatMap(_.active))

  def getByIssuer(scope: ScopeID, issuer: String) =
    Cache.accessProviderByIssuer(scope, issuer)

  def getByIssuerUncached(
    scope: ScopeID,
    issuer: String): Query[Option[AccessProvider]] = {
    idByIssuer(scope, issuer) flatMapT { id =>
      getItemUncached(scope, id).map(_.flatMap(_.active))
    }
  }

  def getItemUncached(
    scope: ScopeID,
    id: AccessProviderID): Query[Option[SchemaItemView[AccessProvider]]] =
    SchemaCollection.AccessProvider(scope).schemaVersState(id).flatMapT { view =>
      for {
        // Parse this twice, in case roles got renamed (in which case `view` will be
        // `Unchanged`, but the parsed view should be `Updated`).
        active <- Query.value(view.active).flatMapT { v =>
          v.data(RolesField).parse(v.parentScopeID, staged = false).map { parsed =>
            Some(AccessProvider(v, parsed))
          }
        }
        staged <- Query.value(view.staged).flatMapT { v =>
          v.data(RolesField).parse(v.parentScopeID, staged = true).map { parsed =>
            Some(AccessProvider(v, parsed))
          }
        }
      } yield Some(SchemaItemView(active, staged))
    }

  def idByIssuer(scope: ScopeID, issuer: String): Query[Option[AccessProviderID]] =
    SchemaCollection
      .AccessProvider(scope)
      .uniqueIDForKeyActive(
        NativeIndex.AccessProviderByIssuer,
        Vector(IndexTerm(issuer)))

  private def validateRoles(
    scope: ScopeID,
    role: AccessProviderRole): Query[Either[Seq[ValidationFailure], Unit]] = {
    val roleNames = role.roles match {
      case Some(roles) => roles flatMap { _.role.swap.toOption }
      case None        => Seq.empty
    }

    val builtinRoleNames = roleNames collect { case role @ Key.Role.Builtin(_) =>
      role
    }

    val rolesButBuiltin = roleNames.diff(builtinRoleNames)

    rolesButBuiltin
      .map {
        Role.idByNameActive(scope, _)
      }
      .sequence
      .flatMap { roles =>
        val rolesNotFound = roles zip rolesButBuiltin collect { case (None, role) =>
          ValidationFailure(
            RolesField.path,
            s"Role `$role` does not exist."
          )
        }

        val builtinRoles = builtinRoleNames map { r =>
          ValidationFailure(
            RolesField.path,
            s"Builtin role `$r` not allowed."
          )
        }

        val failures = rolesNotFound ++ builtinRoles

        if (failures.nonEmpty) {
          Query.value(Left(failures))
        } else {
          Query.value(Right(()))
        }
      }
  }

  final case class RolesValidator(ec: EvalContext) extends Validator[Query] {
    override protected def filterMask = MaskTree(RolesField.path)

    override protected def validateData(
      data: Data): Query[List[ValidationException]] = {
      validateRoles(ec.scopeID, data(RolesField)) map {
        case Left(failures) => failures.toList
        case Right(_)       => List.empty
      }
    }
  }

  final case class IssuerValidator(ec: EvalContext, sub: Option[SubID])
      extends Validator[Query] {
    override protected def filterMask = MaskTree(IssuerField.path)

    override protected def validateData(
      data: Data): Query[List[ValidationException]] =
      IssuerField.read(data.fields) match {
        case Left(errs) =>
          Query.value(errs)

        case Right(issuer) =>
          idByIssuer(ec.scopeID, issuer) map {
            case Some(id) if !(sub contains id.toDocID.subID) =>
              List(DuplicateValue(IssuerField.path))

            case _ =>
              List.empty
          }
      }
  }
}
