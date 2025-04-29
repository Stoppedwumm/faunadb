package fauna.model.schema

import fauna.atoms.{ CollectionID, ScopeID }
import fauna.lang.syntax._
import fauna.model.Collection
import fauna.repo.query.Query
import fauna.repo.schema._
import fauna.repo.schema.SchemaType.StructSchema
import fql.ast.Name
import fql.typer.Type
import scala.language.implicitConversions

/** Utility to resolve a `Type` into a `SchemaType`. Used in schema validation.
  */
object SchemaTypeResolver {
  implicit def scalarToQuery(v: ScalarType): Query[Option[SchemaType]] =
    Query.value(Some(v))
  implicit def schemaToQuery(v: SchemaType): Query[Option[SchemaType]] =
    Query.value(Some(v))

  /** Returns the hydrated `SchemaType` for the given `Type`. Returns `None` if the type is not persistable.
    */
  def apply(ty: Type)(implicit scope: ScopeID): Query[Option[SchemaType]] =
    ty match {
      case Type.Any(_)  => ScalarType.Any
      case Type.Null    => ScalarType.Null
      case Type.Int     => ScalarType.Int
      case Type.Long    => ScalarType.Long
      case Type.Float   => ScalarType.Double
      case Type.Double  => ScalarType.Double
      case Type.Number  => ScalarType.Number
      case Type.Boolean => ScalarType.Boolean
      case Type.Str     => ScalarType.Str
      case Type.Time    => ScalarType.Time
      case Type.Date    => ScalarType.Date
      case Type.Bytes   => ScalarType.Bytes

      case Type.Singleton(lit, _) => ScalarType.Singleton(lit)

      case Type.Array(elem, _) =>
        SchemaTypeResolver(elem).mapT { SchemaType.Array(_) }

      case Type.Tuple(elems, _) =>
        elems
          .map { SchemaTypeResolver(_) }
          .sequence
          .map { elems =>
            if (elems.exists(_.isEmpty)) {
              None
            } else {
              Some(SchemaType.Tuple(elems.flatten.toVector))
            }
          }

      case Type.Record(elems, wildcard, _) =>
        val fieldsQ = elems.map { case (name, v) =>
          SchemaTypeResolver(v).mapT { name -> FieldSchema(_) }
        }.sequence

        // Some(Some(wildcard)) means the wildcard is present.
        // Some(None) means the wildcard is absent.
        // None means the type is invalid.
        val wildcardQ: Query[Option[Option[SchemaType]]] = wildcard match {
          case Some(w) =>
            SchemaTypeResolver(w).map {
              case Some(w) => Some(Some(w))
              case None    => None
            }
          case None => Query.value(Some(None))
        }

        (fieldsQ, wildcardQ) par { (fields, wildcard) =>
          // If any of the types cannot be converted, nothing can be converted.
          if (fields.exists(_.isEmpty) || wildcard.isEmpty) {
            Query.value(None)
          } else {
            Query.value(
              Some(SchemaType.ObjectType(
                StructSchema(fields.flatten.toMap, wildcard.flatten))))
          }
        }

      case Type.Union(elems, _) =>
        elems
          .map { SchemaTypeResolver(_) }
          .sequence
          .map { elems =>
            if (elems.exists(_.isEmpty)) {
              None
            } else {
              Some(SchemaType.Union(elems.flatten.toVector))
            }
          }

      case Type.EmptyRef(Type.Named(Name(col, _), _, _), _) =>
        // An EmptyRef<D> type. Ensure D is actually a document type.
        idByNameOrAlias(col) map {
          _.fold(Option.empty[SchemaType]) { id =>
            Some(ScalarType.NullDocType(id, col))
          }
        }

      case Type.Ref(Type.Named(Name(col, _), _, _), _) =>
        // A Ref<D> type. Ensure D is actually a document type.
        idByNameOrAlias(col) map {
          _.fold(Option.empty[SchemaType]) { id =>
            Some(
              SchemaType
                .Union(ScalarType.DocType(id, col), ScalarType.NullDocType(id, col)))
          }
        }

      case Type.Named(name, _, Seq()) =>
        if (name.str.startsWith("Null") && name.str.length > 4) {
          // This means we have a name like `NullUser`.
          //
          // This could either be the doc type for the collection `NullUser`, or
          // the null doc type for the collection `User`.
          //
          // So, we lookup both the collection `User` and `NullUser`. Only one
          // should exist, as allowing both to exist makes this ambiguous.

          val nameWithoutNull = name.str.drop(4)

          for {
            idByName     <- idByNameOrAlias(name.str)
            nullIdByName <- idByNameOrAlias(nameWithoutNull)
          } yield (idByName, nullIdByName) match {
            case (Some(id), None) => Some(ScalarType.DocType(id, name.str))
            case (None, Some(id)) =>
              Some(ScalarType.NullDocType(id, nameWithoutNull))

            // This means the name is a non-persistable type.
            case (None, None) => None

            // Not much else we can do about this case.
            case (Some(_), Some(_)) =>
              throw new IllegalStateException(
                s"ambiguous doc type name in $scope: $name")
          }
        } else {
          // For types that don't start with null, we can just look up the name as
          // a collection name.
          idByNameOrAlias(name.str).map {
            case Some(id) => Some(ScalarType.DocType(id, name.str))

            // This means the named type is not persistable.
            case None => None
          }
        }

      case _ => Query.value(None)
    }

  private def idByNameOrAlias(name: String)(
    implicit scope: ScopeID): Query[Option[CollectionID]] = {
    name match {
      case s if s == PublicCollection.Key.name =>
        Query.value(Some(PublicCollection.Key.id))
      case s if s == PublicCollection.Token.name =>
        Query.value(Some(PublicCollection.Token.id))
      case s if s == PublicCollection.Credential.name =>
        Query.value(Some(PublicCollection.Credential.id))

      // FIXME: Need to clear the cache before doing type env validation to avoid
      // this uncached lookup.
      case _ =>
        Collection.idByNameUncachedStaged(scope, name).flatMap {
          case res @ Some(_) => Query.value(res)
          case None          => Collection.idByAliasUncachedStaged(scope, name)
        }
    }
  }
}
