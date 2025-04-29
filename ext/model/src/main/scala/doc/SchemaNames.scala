package fauna.model

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model.schema.{
  CollectionConfig,
  NamedCollectionID,
  NativeIndex,
  SchemaItemView,
  SchemaStatus
}
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.Store
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir.{ IRType, IRValue, StringV }
import fauna.storage.ops.VersionAdd
import scala.collection.immutable.Queue

object SchemaNames {

  // Cached lookup

  type PendingNames = Map[(ScopeID, DocID), String]

  def modifiedNamedCollections: Query[PendingNames] =
    Query.state map { state =>
      state.allPending() collect {
        case w @ VersionAdd(_, DocID(_, NamedCollectionID(_)), _, action, _, _, _)
            if action.isCreate =>
          (w.scope, w.id) -> findName(w.scope, w.data).toString
      } toMap
    }

  def lookupCachedName(scope: ScopeID, id: DocID): Query[Option[String]] =
    modifiedNamedCollections flatMap { lookupCachedName(scope, id, _) }

  def lookupCachedName(
    scope: ScopeID,
    id: DocID,
    pendingNames: PendingNames): Query[Option[String]] =
    pendingNames.get((scope, id)) match {
      case n @ Some(_) => Query.value(n)
      case None        => Cache.nameByID(scope, id)
    }

  def lookupNameUncached(scope: ScopeID, id: DocID) =
    RuntimeEnv.Static.Store(scope).getVersionLiveNoTTL(id) mapT {
      findName(_).toString
    }

  def idByNameUncachedView[I <: ID[I]](scope: ScopeID, name: String)(
    implicit comp: IDCompanion[I]): Query[Option[SchemaItemView[I]]] = {
    CollectionConfig(scope, comp.collID).flatMapT { config =>
      SchemaStatus.getAbstract(config.Schema) { ts =>
        val idx = NativeIndex.ByName(scope, comp.collID)
        val terms = Vector(IndexTerm(name))
        Store.uniqueIDForKey(idx, terms, ts).mapT { id =>
          comp.collIDTag.fromDocID(id).get
        }
      } { (a, b) => a == b }
    }
  }

  def idByAliasUncachedView[I <: ID[I]](scope: ScopeID, name: String)(
    implicit comp: IDCompanion[I]): Query[Option[SchemaItemView[I]]] = {
    CollectionConfig(scope, comp.collID).flatMapT { config =>
      SchemaStatus.getAbstract(config.Schema) { ts =>
        val idx = NativeIndex.ByAlias(scope, comp.collID)
        val terms = Vector(IndexTerm(name))
        Store.uniqueIDForKey(idx, terms, ts).mapT { id =>
          comp.collIDTag.fromDocID(id).get
        }
      } { (a, b) => a == b }
    }
  }

  def idByNameStagedUncached[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    name: String): Query[Option[I]] =
    idByNameUncachedView(scope, name).map(_.flatMap(_.staged))

  def idByNameActiveUncached[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    name: String): Query[Option[I]] =
    idByNameUncachedView(scope, name).map(_.flatMap(_.active))

  def idByAliasStagedUncached[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    name: String): Query[Option[I]] =
    idByAliasUncachedView(scope, name).map(_.flatMap(_.staged))

  def idByAliasActiveUncached[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    name: String): Query[Option[I]] =
    idByAliasUncachedView(scope, name).map(_.flatMap(_.active))

  // Version Data

  case class Name(override val toString: String) extends AnyVal

  implicit val NameFieldType = new FieldType[Name] {
    override val vtype: IRType = IRType.Custom("Name")

    override def encode(n: Name) = Some(StringV(n.toString))

    override def decode(value: Option[IRValue], path: Queue[String]) =
      value match {
        case Some(StringV(name)) if Parsing.Name.reserved(name) =>
          Left(List(ReservedName(path.toList, name, Parsing.ReservedNames)))
        case Some(StringV(name)) if Parsing.Name.invalidChars(name) =>
          Left(List(InvalidCharacters(path.toList, name, Parsing.ValidNameChars)))
        case Some(StringV(name)) => Right(Name(name))
        case Some(v) => Left(List(InvalidType(path.toList, vtype, v.vtype)))
        case None    => Left(List(ValueRequired(path.toList)))
      }
  }

  val NameField = Field[Name]("name")
  val AliasField = Field[Option[String]]("alias")

  def findName(version: Version): String =
    findName(version.parentScopeID, version.data)

  def findName(scope: ScopeID, data: Data) = NameField.read(data.fields) match {
    case Left(_) =>
      throw new IllegalArgumentException(
        s"required name field not found in $scope $data")
    case Right(v) => v.toString
  }

  def findAlias(version: Version): Option[String] =
    findAlias(version.parentScopeID, version.data)

  def findAlias(scope: ScopeID, data: Data) = AliasField.read(data.fields) match {
    case Left(_) =>
      throw new IllegalArgumentException(s"invalid alias field $scope $data")
    case Right(v) => v
  }
}
