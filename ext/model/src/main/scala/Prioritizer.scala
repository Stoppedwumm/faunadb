package fauna.model

import fauna.atoms._
import fauna.exec._
import fauna.repo._
import fauna.scheduler._
import scala.concurrent.Future

case class Prioritizer(repo: Future[RepoContext]) extends PriorityProvider {

  private[this] val Root = Future.successful(PriorityGroup.Root)

  def lookup(id: GlobalID): Future[PriorityGroup] = {
    implicit val ec = ImmediateExecutionContext

    if (id == ScopeID.RootID) return Root

    val query = id match {
      case s: ScopeID =>
        Database.forScope(s) map {
          case Some(db) => db.priorityGroup
          case None     => PriorityGroup.Background
        }

      case id: GlobalKeyID =>
        Key.forGlobalID(id) map {
          case Some(key) => key.priorityGroup
          case None      => PriorityGroup.Background
        }

      case id: GlobalDatabaseID =>
        Database.forGlobalID(id) map {
          case Some(db) => db.priorityGroup
          case None     => PriorityGroup.Background
        }
    }

    if (repo.isCompleted) {
      repo flatMap { _.withPriority(PriorityGroup.Root).result(query) map { _.value } }
    } else {
      Future.successful(PriorityGroup.Default)
    }
  }
}
