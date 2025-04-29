package fauna.model.schema

import fauna.atoms.{ DatabaseID, DocID, GlobalDatabaseID, ScopeID }
import fauna.auth.JWTToken
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.{ AccessProvider, Credentials, Database, Key, LookupHelpers }
import fauna.model.schema.Validators.NameValidationHook
import fauna.model.SchemaNames
import fauna.repo.query.Query
import fauna.repo.schema.{ CollectionSchema, Path, WriteHook }
import fauna.repo.schema.FieldSchema.ReadField
import fauna.repo.store.LookupStore
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.{ Data, Diff, Field }
import fauna.util.BCrypt

object AccessProvidersSchemaHooks {

  val writeHook = WriteHook {
    case (coll, WriteHook.OnCreate(id, _))    => addAudience(coll, id)
    case (coll, WriteHook.OnUpdate(id, _, _)) => addAudience(coll, id)
  }

  private def addAudience(coll: CollectionSchema, docID: DocID) =
    Database.forScope(coll.scope) flatMap {
      case Some(db) =>
        val audience = JWTToken.canonicalDBUrl(db.globalID)
        val updated = Diff(AccessProvider.AudienceField -> audience)
        Store.internalUpdate(coll, docID, updated).map(_ => Nil)

      case None =>
        Query.fail(new IllegalStateException(s"Invalid scope `${coll.scope}`"))
    }
}

object KeysSchemaHooks {

  val writeHook = WriteHook {
    case (coll, WriteHook.OnCreate(id, data)) =>
      patchDatabase(coll, id, data) andThen { Query.incrKeyCreates() }
    case (coll, WriteHook.OnUpdate(id, _, data)) =>
      patchDatabase(coll, id, data) andThen { Query.incrKeyUpdates() }
    case (_, WriteHook.OnDelete(_, _)) =>
      Query.value(Seq.empty) andThen { Query.incrKeyDeletes() }
  }

  private def patchDatabase(coll: CollectionSchema, id: DocID, data: Data) = {
    data(Field[Option[String]]("database")) match {
      case Some(name) =>
        Database.idByName(coll.scope, name) flatMap {
          case Some(dbID) =>
            val updated = Diff(Key.DatabaseField -> Some(dbID))

            Store.internalUpdate(coll, id, updated) map { _ => Nil }

          case None =>
            val failure = NameValidationHook
              .formatMessage(
                Path(Right("database")),
                Seq(NameValidationHook.FieldType.Database),
                name)

            Query.value(List(failure))
        }

      case None => Query.value(Nil)
    }
  }

  def databaseDocToName(scope: ScopeID): ReadField = {
    // This is an exception to the rule about Value.Doc. This is a named doc
    // that we store, because we can't break the existing format of keys.
    case Value.Doc(id, _, ts, _, _) =>
      SchemaCollection
        .Database(scope)
        .get(id.as[DatabaseID], ts.getOrElse(Timestamp.MaxMicros)) map {
        case Some(db) => Value.Str(SchemaNames.findName(db))
        case None     => Value.Doc(id, None, ts)
      }
    case v => Query.value(v)
  }
}

object CredentialsSchemaHooks {
  val writeHook = WriteHook { case (coll, ev) =>
    ev.newData match {
      case Some(data) =>
        data(Credentials.PasswordField) match {
          case Some(pwd) =>
            val diff = Diff(
              Credentials.HashedPasswordField -> Some(BCrypt.hash(pwd)),
              Credentials.PasswordField -> None
            )

            Store.internalUpdate(coll, ev.id, diff) map { _ => Nil }

          case None =>
            Query.value(Nil)
        }

      case None =>
        Query.value(Nil)
    }
  }
}

object RoleSchemaHooks {
  val writeHook = WriteHook {
    case (_, WriteHook.OnCreate(_, _)) =>
      Query.value(Seq.empty).andThen { Query.incrRoleCreates() }
    case (_, WriteHook.OnUpdate(_, _, _)) =>
      Query.value(Seq.empty).andThen { Query.incrRoleUpdates() }
    case (_, WriteHook.OnDelete(_, _)) =>
      Query.value(Seq.empty).andThen { Query.incrRoleDeletes() }
  }

}

object UserFunctionSchemaHooks {
  val writeHook = WriteHook {
    case (_, WriteHook.OnCreate(_, _)) =>
      Query.value(Seq.empty).andThen { Query.incrFunctionCreates() }
    case (_, WriteHook.OnUpdate(_, _, _)) =>
      Query.value(Seq.empty).andThen { Query.incrFunctionUpdates() }
    case (_, WriteHook.OnDelete(_, _)) =>
      Query.value(Seq.empty).andThen { Query.incrFunctionDeletes() }
  }
}

object DatabaseSchemaHooks {
  val writeHook = WriteHook {
    case (coll, WriteHook.OnCreate(id, _)) =>
      (Query.nextID, Query.nextID) par { case (scopeID, globalID) =>
        val internalIDs = Diff(
          Database.ScopeField -> ScopeID(scopeID),
          Database.GlobalIDField -> GlobalDatabaseID(globalID)
        )

        checkAccountSettings(coll.scope, internalIDs) flatMap { diff =>
          Store.internalUpdate(coll, id, diff) flatMap { vers =>
            val lookups = LookupHelpers.lookups(vers)
            lookups.map(LookupStore.add(_, occNonExistence = true)).join map { _ =>
              Nil
            }
          }
        }
      } andThen { Query.incrDatabaseCreates() }

    case (coll, WriteHook.OnUpdate(id, prev, _)) =>
      // TODO: Remove this global_id patch once we copy over
      // read-only fields on updates. See Schema.validateReplaceWithTransform()
      val globalID = Diff(
        Database.GlobalIDField -> prev(Database.GlobalIDField)
      )

      checkAccountSettings(coll.scope, globalID) flatMap { diff =>
        Store.internalUpdate(coll, id, diff) map { _ => Nil }
      } andThen { Query.incrDatabaseUpdates() }

    case (coll, WriteHook.OnDelete(id, _)) =>
      for {
        _ <- deleteSiblingKeys(coll.scope, id.as[DatabaseID])
        _ <- deleteDescendants(coll.scope, id.as[DatabaseID])
        _ <- Query.incrDatabaseDeletes()
      } yield Nil
  }

  private def checkAccountSettings(scope: ScopeID, diff: Diff) =
    Database.forScope(scope) map {
      // parent has an account - this write cannot override it
      case Some(parent) if parent.accountID != Database.DefaultAccount =>
        diff.update(Database.AccountField -> None)
      case _ =>
        diff
    }

  private def deleteDescendants(scope: ScopeID, db: DatabaseID): Query[Unit] = {
    for {
      vers <- SchemaCollection.Database(scope).getVersionNoTTL(db)

      res <- vers match {
        case Some(vers) =>
          // Un-delete the database in order to Database.foldDescendants() to work
          SchemaCollection
            .Database(scope)
            .removeVersion(db, vers.versionID)
            .flatMap(_ => Database.delete(scope, db, recursiveDelete = true).join)

        case None => Query.unit
      }
    } yield res
  }

  private def deleteSiblingKeys(scope: ScopeID, db: DatabaseID): Query[Unit] =
    Key.idsForDatabase(scope, db).foreachValueT { id =>
      PublicCollection.Key(scope).internalDelete(id).join
    }
}
