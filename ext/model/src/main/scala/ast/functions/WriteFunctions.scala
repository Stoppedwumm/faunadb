package fauna.ast

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.runtime.Effect
import fauna.model.schema.NativeCollectionID
import fauna.model.RefParser.RefScope
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.doc._

object RefValidator {
  def validate(
    reqScope: ScopeID,
    l: Literal,
    pos: Position): List[ValidationError] = {
    val errors = validate0(reqScope, l, pos)

    if (errors.nonEmpty) {
      List(ValidationError(errors, pos))
    } else {
      List.empty
    }
  }

  private def validate0(
    reqScope: ScopeID,
    l: Literal,
    pos: Position): List[ValidationException] = l match {
    case RefL(scope, _) if scope != reqScope =>
      List(InvalidScopedReference(pos.toPath))

    case _: UnresolvedRefL =>
      List(InvalidReference(pos.toPath))

    case ObjectL(elems) =>
      elems flatMap { case (k, r) =>
        validate0(reqScope, r, pos at k)
      }

    case ArrayL(elems) =>
      elems.zipWithIndex flatMap { case (r, i) =>
        validate0(reqScope, r, pos at i)
      }

    case _ =>
      List.empty
  }
}

abstract class AbstractCreateFunction(val functionName: String) extends QFunction {
  val effect = Effect.Write

  def apply(
    refE: Either[StringL, RefL],
    objOpt: Option[ObjectL],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val obj = objOpt getOrElse ObjectL.empty
    val errs = RefValidator.validate(ec.scopeID, obj, pos)

    val createPos = pos at functionName

    if (errs.isEmpty) {
      refE match {
        case Right(ref) if ref.scope == ec.scopeID =>
          write(ec, ref, obj, createPos)
        case Right(_) => Query.value(Left(List(InvalidScopedRef(createPos))))
        case Left(StringL(name)) =>
          Collection.idByNameActive(ec.scopeID, name) flatMap {
            case Some(id) => write(ec, RefL(ec.scopeID, id), obj, createPos)
            case None =>
              val ref = NativeCollectionID.fromName(name) match {
                case None =>
                  RefParser.RefScope.UserCollectionRef(name, None)
                case Some(id) =>
                  RefParser.RefScope.NativeCollectionRef(id, None)
              }

              Query.value(Left(List(UnresolvedRefError(ref, createPos))))
          }
      }
    } else {
      Query.value(Left(errs))
    }
  }

  private def write(ec: EvalContext, ref: RefL, obj: ObjectL, pos: Position) =
    ref match {
      case RefL(_, CollectionID(NativeCollectionID(AccessProviderID))) =>
        Database.forScope(ec.scopeID) flatMap {
          case Some(_) =>
            WriteAdaptor(AccessProviderID.collID).create(ec, None, obj.toDiff, pos)

          case None =>
            Query.value(
              Left(List(FeatureNotAvailable("Create Access Providers", pos))))
        }

      case RefL(_, CollectionID(cls)) =>
        WriteAdaptor(cls).create(ec, None, obj.toDiff, pos)
      case RefL(_, DocID(sub, cls)) =>
        WriteAdaptor(cls).create(ec, Some(sub), obj.toDiff, pos)
    }
}

object CreateFunction extends AbstractCreateFunction("create")

abstract class SchemaCreateFunction(functionName: String, val collID: CollectionID)
    extends AbstractCreateFunction(functionName) {
  def apply(obj: ObjectL, ec: EvalContext, pos: Position): Query[R[Literal]] =
    super.apply(Right(RefL(ec.scopeID, collID.toDocID)), Some(obj), ec, pos)
}

object CreateClassFunction
    extends SchemaCreateFunction("create_class", CollectionID.collID)

object CreateCollectionFunction
    extends SchemaCreateFunction("create_collection", CollectionID.collID)

object CreateDatabaseFunction
    extends SchemaCreateFunction("create_database", DatabaseID.collID)

object CreateIndexFunction
    extends SchemaCreateFunction("create_index", IndexID.collID)

object CreateUserFunctionFunction
    extends SchemaCreateFunction("create_function", UserFunctionID.collID)
object CreateKeyFunction extends SchemaCreateFunction("create_key", KeyID.collID)
object CreateRoleFunction extends SchemaCreateFunction("create_role", RoleID.collID)

/** Creates an Access Provider
  *
  * @since 4
  */
object CreateAccessProviderFunction
    extends SchemaCreateFunction("create_access_provider", AccessProviderID.collID)

object ReplaceFunction extends AbstractUpdateFunction(false)

object UpdateFunction extends AbstractUpdateFunction(true)

sealed abstract class AbstractUpdateFunction(isPartial: Boolean) extends QFunction {
  val effect = Effect.Write
  val name = if (isPartial) "update" else "replace"

  def apply(
    ref: RefL,
    objOpt: Option[ObjectL],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val obj = objOpt getOrElse ObjectL.empty
    val errs =
      (if (ref.scope == ec.scopeID) List.empty
       else List(InvalidScopedRef(pos at name))) ++
        RefValidator.validate(ec.scopeID, obj, pos)

    if (errs.isEmpty) {
      val diff = obj.toDiff
      WriteAdaptor(ref.id.collID).update(ec, ref.id.subID, diff, isPartial, pos)
    } else {
      Query(Left(errs))
    }
  }
}

object DeleteFunction extends QFunction {
  val effect = Effect.Write

  def apply(ref: RefL, ec: EvalContext, pos: Position): Query[R[Literal]] = {
    val errs =
      (if (ref.scope == ec.scopeID) List.empty
       else List(InvalidScopedRef(pos at "delete")))
    if (errs.isEmpty) {
      WriteAdaptor(ref.id.collID).delete(ec, ref.id.subID, pos)
    } else {
      Query(Left(errs))
    }
  }
}

object InsertVersionFunction extends QFunction {
  val effect = Effect.Write

  def apply(
    ref: RefL,
    ts: Timestamp,
    action: DocAction,
    objOpt: Option[ObjectL],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    if (ts > ec.snapshotTime) {
      Query.value(Left(List(InvalidWriteTime(pos))))
    } else {
      val obj = objOpt getOrElse ObjectL.empty
      val errs =
        (if (ref.scope == ec.scopeID) List.empty
         else List(InvalidScopedRef(pos at "insert"))) ++
          RefValidator.validate(ec.scopeID, obj, pos)

      if (errs.isEmpty) {
        val diff = (objOpt getOrElse ObjectL.empty).toDiff
        ref match {
          case RefL(_, DocID(sub, UserCollectionID(cls))) =>
            WriteAdaptor(cls).insertVersion(ec, sub, ts, action, diff, pos)
          case RefL(_, DocID(sub, KeyID.collID)) =>
            WriteAdaptor(KeyID.collID).insertVersion(ec, sub, ts, action, diff, pos)
          case _ =>
            Query(Left(List(InvalidInsertRefArgument(pos))))
        }
      } else {
        Query(Left(errs))
      }
    }
  }
}

object RemoveVersionFunction extends QFunction {
  val effect = Effect.Write

  def apply(
    ref: RefL,
    ts: Timestamp,
    action: DocAction,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val errs =
      (if (ref.scope == ec.scopeID) List.empty
       else List(InvalidScopedRef(pos at "remove")))
    if (errs.isEmpty) {
      ref.id.collID match {
        case UserCollectionID(id) =>
          WriteAdaptor(id).removeVersion(ec, ref.id.subID, ts, action, pos)
        case _ =>
          Query(Left(List(InvalidRemoveRefArgument(pos))))
      }
    } else {
      Query(Left(errs))
    }
  }
}

object MoveDatabaseFunction extends QFunction {
  def effect = Effect.Write

  def apply(
    srcRef: RefL,
    dstRef: RefL,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val srcDBQ = Database.getUncached(srcRef.scope, srcRef.id.as[DatabaseID])
    val dstDBQ = Database.getUncached(dstRef.scope, dstRef.id.as[DatabaseID])

    (srcDBQ, dstDBQ) par {
      case (Some(srcDB), Some(dstDB)) =>
        if ((dstDB.ancestors + dstDB.scopeID).contains(srcDB.scopeID)) {
          val reason = "Cannot move database from ancestor to descendant."
          val err = MoveDatabaseError(srcDB.name, dstDB.name, reason, pos)
          Query.value(Left(List(err)))
        } else {
          ec.auth.checkDeletePermission(ec.scopeID, srcRef.id) flatMap { canDelete =>
            if (!canDelete) {
              Query.value(Left(List(PermissionDenied(Right(srcRef), pos))))
            } else {
              Database.moveDatabase(srcDB, dstDB, restore = false, pos)
            }
          }
        }

      case (srcDB, dstDB) =>
        val errors = List.newBuilder[EvalError]

        srcDB foreach { v =>
          errors += UnresolvedRefError(
            RefScope.DatabaseRef(v.name, None),
            pos at "move_database")
        }
        dstDB foreach { v =>
          errors += UnresolvedRefError(
            RefScope.DatabaseRef(v.name, None),
            pos at "to")
        }

        Query(Left(errors.result()))
    }
  }
}
