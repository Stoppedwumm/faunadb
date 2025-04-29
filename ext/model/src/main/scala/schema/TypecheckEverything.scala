package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.ConsoleControl
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.runtime.fql2.Result
import fauna.model.schema.index.CollectionIndexManager
import fauna.model.Index
import fauna.repo.query.Query
import fauna.repo.RepoContext
import scala.annotation.unused
import scala.concurrent.duration._

/** A helper to validate a scope. This is mostly used by the offline tool
  * `TypecheckEverything`, but may also be invoked in the debug console to
  * sanity-check a database.
  *
  * Additionally, it is defined in model so that it may be tested by model
  * tests.
  */
class TypecheckEverything(invalidScopes: Set[ScopeID] = Set.empty) {
  def check(scope: ScopeID)(fail: String => Unit): Query[Unit] = for {
    _ <- TypeEnvValidator(scope, Map.empty, FQLInterpreter.StackTrace(Seq()))
      .dryRun()
      .map {
        case Result.Ok(()) =>
          if (invalidScopes.contains(scope)) {
            fail(s"Expected a failure, but passed")
          }

        case Result.Err(e) =>
          if (!invalidScopes.contains(scope)) {
            fail(
              s"Unexpected error:\n${e.errors.map(_.renderWithSource(Map.empty)).mkString("\n\n")}")
          }
      }

    status <- SchemaStatus.forScope(scope)
    staged <- SchemaCollection.Collection(scope).allDocs().flattenT.mapT { coll =>
      CollectionIndexManager.BackingIndex.fromData(coll.data)
    }
    active <- status.activeSchemaVersion match {
      case Some(vers) =>
        SchemaCollection.Collection(scope).allDocs(vers.ts).flattenT.mapT { coll =>
          CollectionIndexManager.BackingIndex.fromData(coll.data)
        }

      case None => Query.value(Seq.empty)
    }

    _ <- SchemaCollection.Index(scope).allDocs().flattenT.map { indexes =>
      indexes.map { index =>
        if (index.data(Index.CollectionIndexField) == Some(true)) {
          val id = index.id.as[IndexID]
          if (
            !staged.exists(_.elems.exists(_.indexID == id)) &&
            !active.exists(_.elems.exists(_.indexID == id))
          ) {
            fail(
              s"Index ${id} is orphaned (no collections have it as a backing index).")
          }
        }
      }
    }
  } yield ()
}

object TypecheckEverything {

  /** NB: This isn't quite readonly! It'll generate schema. Should only be used
    * if the database has schema already generated.
    */
  def check(repo: RepoContext, scope: ScopeID)(
    implicit @unused ctl: ConsoleControl) = {
    repo.runSynchronously(
      new TypecheckEverything().check(scope) { msg =>
        println(s"Unexpected result for scope $scope: $msg")
      },
      30.seconds)
  }
}
