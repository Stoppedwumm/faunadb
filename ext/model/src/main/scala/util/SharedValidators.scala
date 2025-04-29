package fauna.util

import fauna.ast._
import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.NativeCollectionID
import fauna.repo.query.Query
import fauna.storage.doc._
import fauna.storage.ir._

case class ClientPasswordValidator(
    hashedPasswordPath: List[String],
    passwordPath: List[String],
    currentPasswordPath: List[String]) extends Validator[Query] {

  // only the hashed password is saved.
  protected val filterMask = MaskTree(hashedPasswordPath)

  private val hashedPassword = Field[Option[String]](hashedPasswordPath)
  private val password = Field[Option[String]](passwordPath)
  private val currentPassword = Field[Option[String]](currentPasswordPath)

  // Set the diff's `hashed_password` based on its `password`, as
  // long as `current_password` matches.
  override protected def validatePatch(current: Data, diff: Diff) =
    Query((password.read(diff.fields), currentPassword.read(diff.fields)) match {
      // hashed password cannot be set directly, so clear it.

      case (Right(None), Right(_)) => Right(diff clear hashedPassword)

      // If newPW is present, then we expect a valid oldPW
      case (Right(Some(newPW)), Right(oldPW)) =>
        val oldHash = current(hashedPassword)

        if (!checkOldPW(oldHash, oldPW)) {
          Left(List(InvalidPassword(currentPasswordPath)))
        } else {
          Right(diff.update(hashedPassword -> Some(BCrypt.hash(newPW))))
        }

      // Something went wrong. Collect and return errors.
      case (e1, e2) => Validator.collectErrors(e1, e2)
    })

  private def checkOldPW(oldHash: Option[String], oldPW: Option[String]) =
    (oldHash, oldPW) match {
      // There was no previous password.
      case (None, _) => true
      // The oldPW matches.
      case (Some(oldHash), Some(oldPW)) if BCrypt.check(oldPW, oldHash) => true

      // An oldPW was not provided or did not match.
      case _ => false
    }
}

case class ServerPasswordValidator(
    hashedPasswordPath: List[String],
    passwordPath: List[String]) extends Validator[Query] {

  // only the hashed password is saved.
  protected val filterMask = MaskTree(hashedPasswordPath)

  private val hashedPassword = Field[Option[String]](hashedPasswordPath)
  private val password = Field[Option[String]](passwordPath)

  // Set the diff's `hashed_password` based on its `password`.
  override protected def validatePatch(current: Data, diff: Diff) =
    Query((password.read(diff.fields), hashedPassword.read(diff.fields)) match {
      case (Right(clear), Right(hashed)) =>
        val opt = (clear map BCrypt.hash orElse hashed)
        Right(if (opt.isDefined) diff.update(hashedPassword -> opt) else diff)
      case (Left(List(ValueRequired(_))), _) => Right(diff.update(hashedPassword -> None))
      case (_, Left(List(ValueRequired(_)))) => Right(diff.update(hashedPassword -> None))
      case (e1, e2)                      => Validator.collectErrors(e1, e2)
    })
}

case class ReferencesValidator(ec: EvalContext) extends Validator[Query] {
  protected val filterMask = MaskTree.empty

  override protected def validateData(data: Data) = {
    val checks = data.fields collectValues {
      case (p, DocIDV(id)) => checkReference(ec.scopeID, id, p)
    }

    (checks accumulate List.newBuilder[ValidationException]) { _ ++= _ } map { _.result() }
  }

  private def checkReference(scope: ScopeID, id: DocID, path: List[String]) = {
    val nativeClass = id.asOpt[CollectionID] collect {
      case NativeCollectionID(_) => Query.value(Nil)
    }

    nativeClass getOrElse {
      ReadAdaptor.getInternal(ec, scope, id, Timestamp.MaxMicros) map {
        case Left(errs) =>
          errs collect {
            case PermissionDenied(_, _) => InaccessibleReference(path)
            case InstanceNotFound(_, _) => ReferenceMissing(path)
          }
        case Right(_) =>
          Nil
      }
    }
  }
}

object PredicateValidator {

  sealed trait Arity {

    def compatible(callerExpects: Arity): Boolean = {
      (this, callerExpects) match {
        case (Fixed(callee), Fixed(caller)) if callee == caller => true
        case (Wildcard, _)                                      => true
        case _                                                  => false
      }
    }
  }

  final case object Wildcard extends Arity {
    override def toString: String = "_"
  }

  final case class Fixed(count: Int) extends Arity {
    override def toString: String =
      count.toString
  }
}

final case class PredicateValidator(
  path: List[String],
  expectedArity: PredicateValidator.Arity)
    extends Validator[Query] {

  import PredicateValidator._

  protected def filterMask: MaskTree =
    MaskTree.empty

  override protected def validateData(
    data: Data): Query[List[ValidationException]] = {

    data.fields.get(path) match {
      case Some(q: QueryV) => validateArity(q)
      case _               => Query.value(List.empty)
    }
  }

  private def validateArity(q: QueryV): Query[List[ValidationException]] = {
    val errs = q.expr.get(List("lambda")) flatMap { args =>
      arityOf(args) match {
        case Some(arity) if arity.compatible(expectedArity) => None
        case Some(arity)                                    => fail(arity.toString)
        case None                                           => fail("unknown")
      }
    } toList

    Query.value(errs)
  }

  private def fail(actual: String): Option[ValidationException] =
    Some(InvalidArity(path, expectedArity.toString, actual))

  private def arityOf(args: IRValue): Option[Arity] = {
    args match {
      case ArrayV(elems) => Some(Fixed(elems.size))
      case StringV("_")  => Some(Wildcard)
      case StringV(_)    => Some(Fixed(1))
      case _             => None
    }
  }
}
