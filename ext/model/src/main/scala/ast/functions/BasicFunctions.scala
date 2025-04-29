package fauna.ast

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.Effect
import fauna.model.schema.NamedCollectionID
import fauna.model.RefParser.RefScope._
import fauna.repo.query.Query
import java.lang.{ Double => JDouble, Long => JLong }
import scala.annotation.unused

object AbortFunction extends QFunction {
  val effect = Effect.Pure

  def apply(msg: String, @unused ec: EvalContext, pos: Position): Query[R[Literal]] =
    Query(Left(List(TransactionAbort(msg, pos))))
}

object QueryFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    lambda: LambdaL,
    @unused ec: EvalContext,
    @unused pos: Position): Query[R[LambdaL]] =
    Query.value(Right(lambda))
}

object EqualsFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    elems: List[Literal],
    ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {

    val atLeastV5 = ec.apiVers >= APIVersion.V5

    @annotation.tailrec
    def allEqual(scrutinee: Literal, elems: List[Literal]): Boolean =
      elems match {
        case Nil                                => true
        case r :: tail if compare(scrutinee, r) => allEqual(scrutinee, tail)
        case _                                  => false
      }

    def compare(a: Literal, b: Literal): Boolean = (a, b) match {
      case (LongL(l0), LongL(l1))           => JLong.compare(l0, l1) == 0
      case (DoubleL(d0), DoubleL(d1))       => JDouble.compare(d0, d1) == 0
      case (LongL(l), DoubleL(d))           => JDouble.compare(l.toDouble, d) == 0
      case (DoubleL(d), LongL(l))           => JDouble.compare(d, l.toDouble) == 0
      case (l, a @ ActionL(_)) if atLeastV5 => a.soundEquals(l)
      case (a @ ActionL(_), l) if atLeastV5 => a.soundEquals(l)
      case _                                => a == b
    }

    Query(Right(BoolL(allEqual(elems.head, elems.tail))))
  }
}

/** Deprecated: should call ContainsPath instead
  *
  * @deprecated 3
  */
object ContainsFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    path: List[Either[Long, String]],
    container: Literal,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    ContainsPathFunction(path, container, ec, pos)
}

/** Check if the specified path exists on the container
  *
  * @since 3
  */
object ContainsPathFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    path: List[Either[Long, String]],
    container: Literal,
    ec: EvalContext,
    pos: Position): Query[R[BoolL]] =
    ReadAdaptor.contains(ec, path, container, pos)
}

/** Checks if the specified field exists on the object.
  *
  * @since 3
  */
object ContainsFieldFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    field: String,
    container: Literal,
    ec: EvalContext,
    pos: Position): Query[R[BoolL]] = container match {
    case _: ArrayL =>
      Query.value(
        Left(List(InvalidArgument(List(Type.Object), container.rtype, pos at "in"))))
    case _ =>
      ReadAdaptor.contains(ec, List(Right(field)), container, pos)
  }
}

/** Check if the container has the specified value on its attribute.
  */
object ContainsValueFunction extends QFunction {
  val effect = Effect.Read

  def apply(
    value: Literal,
    container: Literal,
    ec: EvalContext,
    pos: Position): Query[R[BoolL]] = container match {
    case _: ScalarL if !container.isInstanceOf[RefL] =>
      Query.value(
        Left(
          List(
            InvalidArgument(
              List(Type.Object, Type.Array, Type.Page, Type.Set, Type.Ref),
              Type.Scalar,
              pos at "in"))))

    case RefL(scope, id) =>
      val idQ = id.collID match {
        case NamedCollectionID(cls) =>
          SchemaNames.lookupCachedName(scope, cls.collID.toDocID) mapT { StringL(_) }

        case _ =>
          Query.value(Some(StringL(id.subID.toLong.toString)))
      }

      idQ flatMap {
        case Some(idStr) =>
          val containsDbQ = if (scope == ec.scopeID) {
            Query.value(false)
          } else {
            Database.forScope(scope) map {
              case Some(db) =>
                val dbRef = RefL(db.parentScopeID, db.id.toDocID)
                dbRef == value

              case None =>
                false
            }
          }

          containsDbQ map { containsDb =>
            val collRef = RefL(scope, id.collID.toDocID)

            Right(BoolL(idStr == value || collRef == value || containsDb))
          }

        case None =>
          Query.value(Right(FalseL))
      }

    case SetL(set) =>
      ReadAdaptor.containsValue(ec, set, value, pos) mapT { BoolL(_) }

    case VersionL(version, _) =>
      val scopeID = version.parentScopeID
      val ref = RefL(scopeID, version.docID)
      val collRef = RefL(scopeID, version.collID.toDocID)
      val ts = version.ts.validTSOpt map { ts => LongL(ts.micros) } getOrElse {
        TransactionTimeMicrosL
      }
      val data = ReadAdaptor(version.collID).readableData(version)

      val containData = data.fields.elems map { case (_, v) =>
        Literal(scopeID, v)
      } contains value

      Query.value(
        Right(BoolL(ref == value || collRef == value || ts == value || containData)))

    case e: EventL =>
      val event = e.event
      val ts = event.ts.validTSOpt map { ts => LongL(ts.micros) } getOrElse {
        TransactionTimeMicrosL
      }
      val action = ActionL(event.action)
      val ref = RefL(event.scopeID, event.docID)

      val data = e match {
        case DocEventL(e) =>
          val data = ReadAdaptor(e.docID.collID).readableData(e)

          data.fields.elems map { case (_, v) =>
            Literal(e.scopeID, v)
          }

        case SetEventL(e) =>
          e.values map { v => Literal(e.scopeID, v.value) }
      }

      Query.value(
        Right(
          BoolL(
            ts == value || action == value || ref == value || data.contains(value))))

    case PageL(elems, _, before, after) =>
      val containsBefore = before collect { case CursorL(Right(arr)) =>
        arr == value
      } getOrElse false

      val containsAfter = after collect { case CursorL(Right(arr)) =>
        arr == value
      } getOrElse false

      val containsElem = ArrayL(elems) == value

      Query.value(Right(BoolL(containsElem || containsBefore || containsAfter)))

    case ObjectL(elems) =>
      val exists = elems exists { case (_, v) => v == value }
      Query.value(Right(BoolL(exists)))

    case ArrayL(elems) =>
      Query.value(Right(BoolL(elems.contains(value))))

    case _ =>
      Query.value(Right(FalseL))
  }
}

object SelectFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    path: List[Either[Long, String]],
    container: Literal,
    all: Option[Boolean],
    default: Option[Literal],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    resolve(path, container, all, default, ec, pos)

  def resolve(
    path: List[Either[Long, String]],
    container: Literal,
    all: Option[Boolean],
    default: Option[Literal],
    ec: EvalContext,
    pos: Position) = container match {
    case UnresolvedRefL(orig) =>
      Query(Left(List(UnresolvedRefError(orig, pos at "from"))))
    case _ =>
      ReadAdaptor.select(
        ec,
        path,
        all getOrElse false,
        container,
        default,
        pos at "from")
  }
}

// DEPRECATE in favor of `SelectAsIndex`
//TODO: If we implement tree rewriting, having `SelectAll` just emit a `Select` with
//the appropriate field set might be nicer than this
object SelectAllFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    path: List[Either[Long, String]],
    container: Literal,
    default: Option[Literal],
    ec: EvalContext,
    pos: Position) =
    SelectFunction.resolve(path, container, SomeTrue, default, ec, pos)
}

object SelectAsIndexFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    path: List[Either[Long, String]],
    container: Literal,
    default: Option[Literal],
    ec: EvalContext,
    pos: Position) =
    SelectFunction.resolve(path, container, SomeTrue, default, ec, pos)
}

object RefFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    cls: Literal,
    id: Literal,
    scope: Option[Literal],
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val clsE = cls match {
      case RefL(_, CollectionID(NamedCollectionID(c))) =>
        Left(List(InvalidSchemaClassArgument(c.collID, pos at "ref")))
      case RefL(s, CollectionID(c))             => Right(Right((s, c)))
      case UnresolvedRefL(c: UserCollectionRef) => Right(Left(c))
      case r =>
        Left(List(InvalidArgument(List(Type.CollectionRef), r.rtype, pos at "ref")))
    }

    val idE = id match {
      case LongL(id) => Right(SubID(id))
      case StringL(id) =>
        id.toLongOption match {
          case Some(idLong) =>
            Right(SubID(idLong))
          case None if id forall Character.isDigit =>
            Left(List(SubIDArgumentTooLarge(id, pos at "id")))
          case None =>
            Left(List(NonNumericSubIDArgument(id, pos at "id")))
        }
      case r =>
        Left(
          List(
            InvalidArgument(List(Type.String, Type.Number), r.rtype, pos at "id")))
    }

    val scopeE = scope match {
      case None                                                => Right(None)
      case Some(r @ RefL(s, _)) if r.rtype == Type.DatabaseRef => Right(Some(s))
      case Some(r) =>
        Left(List(InvalidArgument(List(Type.DatabaseRef), r.rtype, pos at "scope")))
    }

    Query.value((clsE, idE, scopeE) match {
      case (Right(Right((_, c))), Right(sub), Right(Some(dbScope))) =>
        Right(RefL(dbScope, DocID(sub, c)))
      case (Right(Right((clsScope, c))), Right(sub), Right(None)) =>
        Right(RefL(clsScope, DocID(sub, c)))
      case (Right(Left(c)), Right(sub), _) =>
        Right(UnresolvedRefL(InstanceRef(Right(sub), c)))
      case (e1, e2, e3) => Parser.collectErrors(e1, e2, e3)
    })
  }
}

abstract class RefConstructFunction[I <: ID[I]](implicit ev: CollectionIDTag[I])
    extends QFunction {
  val effect = Effect.Pure

  val ref: (String, Option[DatabaseRef]) => Ref
  val id: (ScopeID, String) => Query[Option[I]]

  def apply(
    str: String,
    scope: Option[RefL],
    ec: EvalContext,
    @unused pos: Position): Query[R[Literal]] = {

    val scopeQ = scope match {
      case None                => Database.forScope(ec.scopeID)
      case Some(RefL(sID, id)) => Database.getUncached(sID, id.as[DatabaseID])
    }

    scopeQ flatMap {
      case None => Query.value(Right(UnresolvedRefL(ref(str, None))))
      case Some(db) =>
        val parentScope = db.scopeID
        id(parentScope, str) map {
          case Some(id) => Right(RefL(parentScope, id.toDocID))
          case None =>
            val scope = Some(DatabaseRef(db.name, None)) filter { _ =>
              parentScope != ec.scopeID
            }
            Right(UnresolvedRefL(ref(str, scope)))
        }
    }
  }
}

object DatabaseRefFunction extends RefConstructFunction[DatabaseID] {
  val ref = DatabaseRef(_, _)
  val id = Database.idByName(_, _)
}

object IndexRefFunction extends RefConstructFunction[IndexID] {
  val ref = IndexRef(_, _)
  val id = Index.idByName(_, _)
}

object ClassRefFunction extends RefConstructFunction[CollectionID] {
  val ref = UserCollectionRef(_, _)
  val id = Collection.idByNameActive(_, _)
}

object UserFunctionRefFunction extends RefConstructFunction[UserFunctionID] {
  val ref = UserFunctionRef(_, _)
  val id = UserFunction.idByNameActive(_, _)
}

object RoleRefFunction extends RefConstructFunction[RoleID] {
  val ref = RoleRef(_, _)
  val id = Role.idByNameActive(_, _)
}

object AccessProviderRefFunction extends RefConstructFunction[AccessProviderID] {
  val ref = AccessProviderRef(_, _)
  val id = AccessProvider.idByNameActive(_, _)
}

object NewIDFunction extends QFunction {
  val effect = Effect.Write

  @annotation.nowarn("cat=unused-params")
  def apply(n: Unit, ec: EvalContext, pos: Position): Query[R[Literal]] =
    Query.nextID map { id =>
      Right(StringL(id.toString))
    }
}

trait NativeCollectionConstructFun extends QFunction {
  val effect = Effect.Pure

  def id: CollectionID

  def apply(
    scope: Option[Literal],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val scopeQ = scope match {
      case None => Query.value(Right(ec.scopeID))
      case Some(r @ RefL(scopeID, DatabaseID(dbID))) =>
        Database.getUncached(scopeID, dbID) map {
          case Some(db) => Right(db.scopeID)
          case None =>
            Left(
              List(InvalidArgument(List(Type.DatabaseRef), r.rtype, pos at "scope")))
        }
      case Some(UnresolvedRefL(original)) =>
        Query.value(Left(List(UnresolvedRefError(original, pos at "scope"))))
      case Some(r) =>
        Query.value(
          Left(
            List(InvalidArgument(List(Type.DatabaseRef), r.rtype, pos at "scope"))))
    }

    scopeQ mapT { scopeID =>
      RefL(scopeID, id)
    }
  }
}

object DatabasesNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = DatabaseClassRef.collectionID
}

object IndexesNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = IndexClassRef.collectionID
}

object CollectionsNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = CollectionCollectionRef.collectionID
}

object KeysNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = KeyClassRef.collectionID
}

object TokensNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = TokenClassRef.collectionID
}

object CredentialsNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = CredentialsClassRef.collectionID
}

object UserFunctionsNativeCollectionConstructFun
    extends NativeCollectionConstructFun {
  def id = UserFunctionClassRef.collectionID
}

object RolesNativeCollectionConstructFun extends NativeCollectionConstructFun {
  def id = RoleClassRef.collectionID
}

object AccessProvidersNativeCollectionConstructFun
    extends NativeCollectionConstructFun {
  def id = AccessProviderClassRef.collectionID
}
