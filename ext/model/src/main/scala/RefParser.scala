package fauna.model

import fauna.ast.{ Parser => FParser, _ }
import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model.schema.NativeCollectionID
import fauna.storage._
import org.parboiled2.{ Position => _, _ }
import scala.util.Success

object RefParser {
  // ref hierarchy

  object RefScope {
    sealed trait Path

    case class EventsRef(ref: Ref) extends Path
    case class EventRef(ref: Ref, versionID: VersionID) extends Path

    sealed trait Ref extends Path {
      val rtype: Type = Type.Ref
      val scope: Option[DatabaseRef] = None
    }

    sealed trait ObjectRef extends Ref

    sealed trait CollectionRef extends ObjectRef {
      override val rtype = Type.CollectionRef
    }
    sealed case class NativeCollectionRef(
      collectionID: CollectionID,
      override val scope: Option[DatabaseRef])
        extends CollectionRef

    val DatabaseClassRef = NativeCollectionRef(DatabaseID.collID, None)
    val CollectionCollectionRef = NativeCollectionRef(CollectionID.collID, None)
    val IndexClassRef = NativeCollectionRef(IndexID.collID, None)
    val KeyClassRef = NativeCollectionRef(KeyID.collID, None)
    val TokenClassRef = NativeCollectionRef(TokenID.collID, None)
    val CredentialsClassRef = NativeCollectionRef(CredentialsID.collID, None)
    val UserFunctionClassRef = NativeCollectionRef(UserFunctionID.collID, None)
    val RoleClassRef = NativeCollectionRef(RoleID.collID, None)
    val AccessProviderClassRef = NativeCollectionRef(AccessProviderID.collID, None)

    case class DatabaseRef(name: String, override val scope: Option[DatabaseRef])
        extends ObjectRef {
      override val rtype = Type.DatabaseRef
      override def toString = s"DatabaseRef($name,$scope)"
    }

    case class UserCollectionRef(
      name: String,
      override val scope: Option[DatabaseRef])
        extends CollectionRef {
      override def toString = s"CollectionRef($name,$scope)"
    }

    case class IndexRef(name: String, override val scope: Option[DatabaseRef])
        extends ObjectRef {
      override val rtype = Type.IndexRef
      override def toString = s"IndexRef($name,$scope)"
    }

    case class KeyRef(id: KeyID, override val scope: Option[DatabaseRef])
        extends ObjectRef {
      override val rtype = Type.KeyRef
      override def toString = s"KeyRef($id,$scope)"
    }

    case class UserFunctionRef(name: String, override val scope: Option[DatabaseRef])
        extends ObjectRef {
      override val rtype = Type.UserFunctionRef
      override def toString = s"UserFunctionRef($name,$scope)"
    }

    case class RoleRef(name: String, override val scope: Option[DatabaseRef])
        extends ObjectRef {
      override val rtype = Type.RoleRef
      override def toString = s"RoleRef($name,$scope)"
    }

    case class AccessProviderRef(name: String, override val scope: Option[DatabaseRef])
        extends ObjectRef {
      override val rtype = Type.AccessProviderRef
      override def toString = s"AccessProviderRef($name,$scope)"
    }

    sealed trait ObjectInstanceRef extends ObjectRef
    case class SelfRef(clsRef: CollectionRef) extends ObjectInstanceRef
    case class InstanceRef(id: Either[String, SubID], clsRef: CollectionRef)
        extends ObjectInstanceRef {
      override val scope = clsRef.scope
    }
  }
  import RefScope._

  def parse(res: Literal, pos: Position, apiVersion: APIVersion = APIVersion.Default): R[Ref] =
    (res match {
      case ObjectL(alist) => refParser(alist, pos, apiVersion)
      case _              => None
    }) getOrElse Left(List(InvalidArgument(List(Type.Ref), res.rtype, pos)))

  private def castOpt[T <: Ref](rType: Type, res: Option[Literal], apiVersion: APIVersion, pos: Position)(
    pf: PartialFunction[Ref, T]): R[Option[T]] =
    res match {
      case None | Some(NullL) => Right(None)
      case Some(res) =>
        parse(res, pos, apiVersion) match {
          case Right(ref) =>
            (pf andThen { r =>
              Right(Some(r))
            }).applyOrElse(ref, { ref: Ref =>
              Left(List(InvalidArgument(List(rType), ref.rtype, pos)))
            })
          case Left(res) => Left(res)
        }
    }

  private def mk(
    id: Literal,
    cls: Option[Literal],
    database: Option[Literal],
    apiVersion: APIVersion,
    pos: Position): R[Ref] = {
    val idE = id match {
      case StringL(str) => Right(Left(str))
      case LongL(lng)   => Right(Right(SubID(lng)))
      case _ =>
        Left(
          List(
            InvalidArgument(List(Type.String, Type.Integer), id.rtype, pos at "id")))
    }

    val classE = castOpt(Type.CollectionRef, cls, apiVersion, pos at "class") {
      case ref: CollectionRef => ref
    }
    val databaseE = castOpt(Type.DatabaseRef, database, apiVersion, pos at "database") {
      case ref: DatabaseRef => ref
    }

    (idE, classE, databaseE) match {
      case (Right(Left(collectionName)), Right(None), Right(None)) =>
        NativeCollectionID.fromName(collectionName) match {
          case Some(DatabaseClassRef.collectionID) => Right(DatabaseClassRef)
          case Some(CollectionCollectionRef.collectionID) =>
            Right(CollectionCollectionRef)
          case Some(IndexClassRef.collectionID)          => Right(IndexClassRef)
          case Some(KeyClassRef.collectionID)            => Right(KeyClassRef)
          case Some(TokenClassRef.collectionID)          => Right(TokenClassRef)
          case Some(CredentialsClassRef.collectionID)    => Right(CredentialsClassRef)
          case Some(UserFunctionClassRef.collectionID)   => Right(UserFunctionClassRef)
          case Some(RoleClassRef.collectionID)           => Right(RoleClassRef)
          case Some(AccessProviderClassRef.collectionID) => Right(AccessProviderClassRef)
          case _ =>
            Left(List(InvalidNativeClassRefError(collectionName, pos at "id")))
        }

      case (Right(Left(collectionName)), Right(None), Right(db)) =>
        NativeCollectionID.fromName(collectionName) match {
          case Some(clsID) => Right(NativeCollectionRef(clsID, db))
          case None =>
            Left(List(InvalidNativeClassRefError(collectionName, pos at "id")))
        }

      case (Right(Right(_)), Right(None), Right(_)) =>
        Left(List(InvalidArgument(List(Type.String), Type.Number, pos at "id")))

      case (Right(Left(dbName)), Right(Some(DatabaseClassRef)), Right(db)) =>
        Right(DatabaseRef(dbName, db))

      case (Right(Left(collectionName)),
            Right(Some(CollectionCollectionRef)),
            Right(db)) =>
        Right(UserCollectionRef(collectionName, db))

      case (Right(Left(idxName)), Right(Some(IndexClassRef)), Right(db)) =>
        Right(IndexRef(idxName, db))

      case (Right(Left(udfName)), Right(Some(UserFunctionClassRef)), Right(db)) =>
        Right(UserFunctionRef(udfName, db))

      case (Right(Left(roleName)), Right(Some(RoleClassRef)), Right(db)) =>
        Right(RoleRef(roleName, db))

      case (Right(Right(id)), Right(Some(KeyClassRef)), Right(db)) =>
        Right(KeyRef(KeyID(id.toLong), db))

      case (Right(Left(accessProviderName)), Right(Some(AccessProviderClassRef)), Right(db)) =>
        Right(AccessProviderRef(accessProviderName, db))

      case (Right(Left("self")), Right(Some(cls)), Right(None)) =>
        Right(SelfRef(cls))

      case (Right(id @ Right(_)), Right(Some(cls)), Right(None)) =>
        Right(InstanceRef(id, cls))

      case (Right(Left(id)), Right(Some(cls)), Right(None)) =>
        id.toLongOption match {
          case Some(idLong) =>
            Right(InstanceRef(Right(SubID(idLong)), cls))

          case None if id forall Character.isDigit =>
              Left(List(SubIDArgumentTooLarge(id, pos at "id")))

          case None =>
              Left(List(NonNumericSubIDArgument(id, pos at "id")))
        }

      case (Right(_), Right(Some(_)), Right(Some(_))) =>
        Left(
          List(
            InvalidArgument(List(Type.Null), Type.DatabaseRef, pos at "database")))

      case (id, cls, db) =>
        FParser.collectErrors(id, cls, db)
    }
  }

  private val refParser = MapRouter.build[Literal, R[Ref], Position] { refs =>
    refs.add("@ref") {
      case (obj, pos, apiVersion) => parse(obj, pos, apiVersion)
    }
    refs.add("id") {
      case (id, pos, apiVersion) => mk(id, None, None, apiVersion, pos)
    }
    refs.add("id", "class") {
      case (id, cls, pos, apiVersion) => mk(id, Some(cls), None, apiVersion, pos)
    }
    refs.add("id", "collection") {
      case (id, cls, pos, apiVersion) => mk(id, Some(cls), None, apiVersion, pos)
    }
    refs.add("id", "database") {
      case (id, db, pos, apiVersion) => mk(id, None, Some(db), apiVersion, pos)
    }
    refs.add("id", "class", "database") {
      case (id, cls, db, pos, apiVersion) => mk(id, Some(cls), Some(db), apiVersion, pos)
    }
    refs.add("id", "collection", "database") {
      case (id, cls, db, pos, apiVersion) => mk(id, Some(cls), Some(db), apiVersion, pos)
    }
  }
}

object LegacyRefParser {

  private val InvalidNames = Set("self", "events")

  private def isValidName(n: String) =
    Parsing.Name.isValid(n) && !InvalidNames.contains(n)

  object Ref {

    def parse(str: String) = Path.parse(str) collect {
      case r: RefParser.RefScope.Ref => r
    }
  }

  object Path {

    def parse(str: String) = new PathParser(str).top.run() match {
      case Success(p) => Some(p)
      case _          => None
    }
  }

  class PathParser(val input: ParserInput) extends Parser {

    import RefParser.RefScope._

    def databaseRef(name: String) = DatabaseRef(name, None)
    def indexRef(name: String) = IndexRef(name, None)
    def userFunctionRef(name: String) = UserFunctionRef(name, None)
    def roleRef(name: String) = RoleRef(name, None)
    def accessProviderRef(name: String) = AccessProviderRef(name, None)
    def userCollectionRef(name: String) = UserCollectionRef(name, None)

    def mkInstanceRef(
      classRef: CollectionRef,
      id: Option[SubID]): ObjectInstanceRef =
      id match {
        case Some(id) => InstanceRef(Right(id), classRef)
        case None     => SelfRef(classRef)
      }

    def pathEnd = rule { EOI }

    def top = rule { path ~ EOI }

    def path: Rule1[RefParser.RefScope.Path] = rule {
      classRef ~ (
        (pathEnd ~> identity[CollectionRef] _) |
          "/events" ~ (
            pathEnd ~> EventsRef |
              "/" ~ versionID ~> EventRef
          ) |
          instanceRefs
      ) |
        indexRefs |
        functionRefs |
        roleRefs |
        accessProverRefs |
        schemaClassRefs
    }

    def instanceRefs = rule {
      ('/' ~ idOrSelf ~> mkInstanceRef _) ~ (
        pathEnd ~> (identity[ObjectInstanceRef] _) |
          "/events" ~ (
            pathEnd ~> EventsRef |
              "/" ~ versionID ~> EventRef
          )
      )
    }

    def indexRefs = rule {
      "indexes/" ~ name ~> (indexRef _) ~ (
        pathEnd ~> (identity[IndexRef] _) |
          str("/events") ~> EventsRef
      )
    }

    def functionRefs = rule {
      "functions/" ~ name ~> (userFunctionRef _) ~ (
        pathEnd ~> (identity[UserFunctionRef] _) |
          str("/events") ~> EventsRef
      )
    }

    def roleRefs = rule {
      "roles/" ~ name ~> roleRef _ ~ (
        pathEnd ~> identity[RoleRef] _ |
          str("/events") ~> EventsRef
      )
    }

    def accessProverRefs = rule {
      "access_providers/" ~ name ~> accessProviderRef _ ~ (
        pathEnd ~> identity[AccessProviderRef] _ |
          str("/events") ~> EventsRef
      )
    }

    def schemaClassRefs = rule {
      "databases" ~ (
        pathEnd ~ push(DatabaseClassRef) |
          "/events" ~ push(EventsRef(DatabaseClassRef)) |
          ("/" ~ name ~> (databaseRef _)) ~ (
            pathEnd ~> (identity[DatabaseRef] _) |
              "/events" ~ (
                pathEnd ~> EventsRef |
                  "/" ~ versionID ~> EventRef
              )
          )
      ) |
        "classes" ~ (
          pathEnd ~ push(CollectionCollectionRef) |
            "/events" ~ push(EventsRef(CollectionCollectionRef))
        ) |
        "collections" ~ (
          pathEnd ~ push(CollectionCollectionRef) |
            "/events" ~ push(EventsRef(CollectionCollectionRef))
        ) |
        "indexes" ~ (
          pathEnd ~ push(IndexClassRef) |
            "/events" ~ push(EventsRef(IndexClassRef))
        ) |
        "functions" ~ (
          pathEnd ~ push(UserFunctionClassRef) |
            "/events" ~ push(EventsRef(UserFunctionClassRef))
        ) |
        "roles" ~ (
          pathEnd ~ push(RoleClassRef) |
            "/events" ~ push(EventsRef(RoleClassRef))
        ) |
        "access_providers" ~ (
          pathEnd ~ push(AccessProviderClassRef) |
            "/events" ~  push(EventsRef(AccessProviderClassRef))
        )
    }

    // elements

    def classRef: Rule1[CollectionRef] = rule {
      "keys" ~ push(KeyClassRef) |
        "tokens" ~ push(TokenClassRef) |
        "credentials" ~ push(CredentialsClassRef) |
        "classes/" ~ name ~> (userCollectionRef _) |
        "collections/" ~ name ~> (userCollectionRef _)
    }

    def instanceRef: Rule1[ObjectInstanceRef] = rule {
      classRef ~ "/" ~ idOrSelf ~> mkInstanceRef _
    }

    def psegment = rule { oneOrMore(noneOf("/,()&=")) }

    def digits = rule { oneOrMore(anyOf("0123456789")) }

    def id: Rule1[SubID] = rule {
      capture(digits) ~> { s: String =>
        s.toLongOption match {
          case Some(id) => push(SubID(id))
          case None     => MISMATCH
        }
      }
    }

    def idOrSelf: Rule1[Option[SubID]] = rule {
      ("self" ~ push(None)) | id ~> (Some[SubID] _)
    }

    def name: Rule1[String] = rule {
      capture(psegment) ~> { n: String =>
        if (isValidName(n)) push(n) else MISMATCH
      }
    }

    def versionID: Rule1[VersionID] = rule {
      capture(digits) ~ (
        str("/create") ~> { ts: String =>
          VersionID(Timestamp.ofMicros(ts.toLong), Create)
        } |
          str("/delete") ~> { ts: String =>
            VersionID(Timestamp.ofMicros(ts.toLong), Delete)
          }
      )
    }
  }
}
