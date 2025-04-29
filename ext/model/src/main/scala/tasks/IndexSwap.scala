package fauna.model.tasks

import fauna.ast.MaximumIDException
import fauna.atoms._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.logging.ExceptionLogging
import fauna.model._
import fauna.model.schema.{ NativeIndex, SchemaCollection }
import fauna.model.schema.index.CollectionIndexManager
import fauna.repo.query.Query
import fauna.repo.store.CacheStore
import fauna.repo.Store
import fauna.storage.api.set._
import fauna.storage.doc._
import fauna.storage.index.IndexSources
import fauna.storage.ir.DocIDV
import scala.util.control.NoStackTrace

object IndexSwap {

  /** Thrown as a swap-specific variant of IllegalArgumentException. */
  final class BadSwapException(msg: String) extends Exception(msg) with NoStackTrace

  /** This suffix is appended to the name of a live index to produce a
    * (hopefully) unique index name under which the rebuild can
    * occur. After the swap completes, the old (presumably broken)
    * index will have this name.
    */
  val SwapSuffix = "_swap"

  val ScopeField = Field[ScopeID]("scope")
  val LiveIndexField = Field[IndexID]("live_id")
  val HiddenIndexField = Field[IndexID]("hidden_id")

  case object Root extends Type("index-swap-root") with ExceptionLogging {
    val StateField = Field[String]("state")

    def step(task: Task, ts: Timestamp): Query[Task.State] = {
      val scope = task.data(ScopeField)
      val live = task.data(LiveIndexField)
      val hidden = task.data(HiddenIndexField)
      val state = task.data(StateField)

      state match {
        case "start" =>
          task.fork {
            val build = IndexBuild.RootTask.create(
              scope,
              hidden,
              parent = Some(task.id),
              isOperational = true)

            build map { child =>
              (Vector(child), task.data.update(StateField -> "swap"))
            }
          }

        case "swap" =>
          task.joinThen() {
            case Left(_) => Query.value(Task.Cancelled())
            case Right(_) =>
              checkIndex(scope, live) flatMap { _ =>
                swap(scope, live, hidden) map { _ => Task.Completed() }
              } recover { case e: BadSwapException =>
                logException(e)
                Task.Cancelled()
              }
          }
      }
    }

    /** Rebuild the index at `live` using a new, hidden IndexID and swap
      * them after the build completes.
      */
    def create(scope: ScopeID, live: IndexID): Query[Task] =
      SchemaCollection.Index(scope) flatMap { config =>
        val idQ = config.Schema.nextID flatMap {
          case Some(id) => Query.value(id.as[IndexID])
          case None     => Query.fail(MaximumIDException)
        }

        idQ flatMap { id =>
          SchemaCollection.Index(scope).get(live) flatMap {
            case None =>
              Query.fail(new BadSwapException(s"Unable to find $live in $scope."))

            case Some(version) =>
              // Check this once on the way in, it shouldn't change
              // after active becomes true.
              if (version.data(Index.ActiveField) == false) {
                throw new BadSwapException(s"$live is not an active index.")
              }

              val name = s"${version.data(SchemaNames.NameField)}$SwapSuffix"
              val data = version.data.update(
                SchemaNames.NameField -> SchemaNames.Name(name),
                Index.HiddenField -> Some(true))

              SchemaCollection.Index(scope).insert(id, data) flatMap { _ =>
                create(scope, live, id)
              }
          }
        }
      }

    /** Start a new build of the index at `hidden` and swap it with `live`
      * after the build completes.
      *
      * `live` must not be deleted or refer to a hidden index.
      * `hidden` must not be deleted and must refer to a hidden index.
      *
      * !!! WARNING !!!
      * No assertions are made that `live` and `hidden` represent the
      * same shape of index.
      */
    private def create(
      scope: ScopeID,
      live: IndexID,
      hidden: IndexID): Query[Task] = {
      checkIndex(scope, live) flatMap { _ =>
        val data = Data(
          StateField -> "start",
          ScopeField -> scope,
          LiveIndexField -> live,
          HiddenIndexField -> hidden)

        Task.createRandom(scope, name, data, parent = None, isOperational = true)
      }
    }

    private def checkIndex(scope: ScopeID, live: IndexID): Query[Unit] =
      Index.get(scope, live) flatMap {
        case Some(live) => checkHidden(live, false)

        case None =>
          val msg = s"live index $live does not refer to a live index"
          Query.fail(new BadSwapException(msg))
      }

    private def checkHidden(idx: Index, expected: Boolean): Query[Unit] =
      if (idx.isHidden && !expected) {
        val msg = s"${idx.id} must not refer to a hidden index."
        Query.fail(new BadSwapException(msg))
      } else if (!idx.isHidden && expected) {
        val msg = s"${idx.id} must refer to a hidden index."
        Query.fail(new BadSwapException(msg))
      } else {
        Query.unit
      }

    private def swap(scope: ScopeID, live: IndexID, hidden: IndexID): Query[Unit] = {
      SchemaCollection.Index(scope).flatMap { coll =>
        val verA = Store.getVersionLiveNoTTL(coll.Schema, live.toDocID)
        val verB = Store.getVersionLiveNoTTL(coll.Schema, hidden.toDocID)

        // parT here assumes checkIndexes() has happened.
        (verA, verB) parT { case (a, b) =>
          // Leave the old index around - but hidden - for post-hoc
          // investigations.
          val aData = a.data.update(
            SchemaNames.NameField -> b.data(SchemaNames.NameField),
            Index.HiddenField -> Some(true)
          )
          val bData = b.data.update(
            SchemaNames.NameField -> a.data(SchemaNames.NameField),
            Index.HiddenField -> None
          )

          Query.snapshotTime flatMap { ts =>
            val aQ = Store.insertCreate(coll.Schema, a.id, ts, aData)
            val bQ = Store.insertCreate(coll.Schema, b.id, ts, bData)

            // Publish a new schema version to cause a cache refresh.
            CacheStore.updateSchemaVersion(scope) flatMap { _ =>
              aQ flatMap { _ => bQ map { Some(_) } }
            }
          }
        } flatMap { _ =>
          updateRoles(scope, live, hidden)
        } flatMap { _ =>
          updateCollection(scope, live, hidden)
        }
      }
    }

    private def updateRoles(
      scope: ScopeID,
      live: IndexID,
      hidden: IndexID): Query[Unit] =
      Query.snapshotTime flatMap { ts =>
        val terms = Vector(Scalar(DocIDV(RoleID.collID.toDocID)))
        val entries =
          Store.collection(NativeIndex.DocumentsByCollection(scope), terms, ts)

        val roles = entries flatMapValuesT { entry =>
          Role.get(scope, entry.docID.as[RoleID]) map { _.toSeq }
        }

        val needle = live.toDocID
        roles foreachValueT { role =>
          val needsUpdate = role.privileges.exists {
            _.resource match {
              case Right(`needle`) => true
              case _               => false
            }
          }

          if (needsUpdate) {
            for {
              roleColl <- SchemaCollection.Role(scope)
              latest   <- Store.getVersionLiveNoTTL(roleColl.Schema, role.id.toDocID)
              res <- latest match {
                case None => Query.unit // Weird, but okay.
                case Some(version) =>
                  val privs = version.data(Role.PrivilegesField) map { priv =>
                    priv.resource match {
                      case Right(`needle`) =>
                        priv.copy(resource = Right(hidden.toDocID))
                      case _ => priv
                    }
                  }

                  val data = version.data.update(Role.PrivilegesField -> privs)
                  Store.insertCreate(roleColl.Schema, version.id, ts, data) map {
                    _ => ()
                  }
              }
            } yield res
          } else {
            Query.unit
          }
        }
      }

    private def updateCollection(
      scope: ScopeID,
      live: IndexID,
      hidden: IndexID): Query[Unit] = {

      val liveQ = Index.get(scope, live)
      val hiddenQ = Index.get(scope, hidden)

      // parT here assumes checkIndexes() has happened.
      (liveQ, hiddenQ) parT { case (live, hidden) =>
        if (live.isCollectionIndex) {
          require(
            hidden.isCollectionIndex,
            s"$live is a collection index but $hidden is not!")

          live.sources match {
            case IndexSources.Limit(ids) =>
              SchemaCollection.Collection(scope).flatMap { coll =>
                // ids should always have size = 1, but don't assume
                // so.

                val updates = ids map { id =>
                  Store.get(coll.Schema, id.toDocID) flatMap {
                    case None => Query.unit // Weird, but okay.
                    case Some(version) =>
                      val mngr = CollectionIndexManager(scope, id, version.data)

                      val backing = mngr.backingIndexes
                        .replace(live.id, _.copy(indexID = hidden.id))

                      val updated = mngr.copy(backingIndexes = backing)
                      Store.internalUpdate(
                        coll.Schema,
                        id.toDocID,
                        Diff(updated.toCollectionData))
                  }
                }

                updates.join map { _ => None }

              }
            case _ => Query.none
          }
        } else {
          Query.none
        }
      } map { _ => () }
    }
  }
}
