package fauna.model.tasks

import fauna.atoms._
import fauna.exec._
import fauna.lang.syntax._
import fauna.model._
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.Random

object TaskBouncer {
  val InitialSleepTime = 10.seconds
  val MaxSleepTime = 2.minutes

  trait Service {
    def selfID: HostID
    def runningHosts: Set[HostID]
    def isReady: Boolean
  }

  def cassandraService: Service =
    new Service {
      def service = CassandraService.instance
      def selfID = service.localID.get
      def runningHosts = service.runningHosts

      def isReady =
        service.isRunning && // can't bounce if not running
          service.localID.isDefined && // can't bounce if haven't joined the cluster
          service.runningHosts.sizeIs > 1 // can't bounce if there is no one else
    }
}

/** When enabled, it bounces tasks off the local host into other random hosts */
final class TaskBouncer(
  repo: RepoContext,
  service: TaskBouncer.Service = TaskBouncer.cassandraService)
    extends LoopThreadService("TaskBouncer", Thread.MIN_PRIORITY) {
  import TaskBouncer._

  private val backoff = new BackOff(InitialSleepTime, MaxSleepTime)
  private val logger = getLogger()

  protected def loop(): Unit =
    backoff { _ =>
      if (service.isReady) {
        try {
          bounceAllTasks()
        } catch {
          case NonFatal(err) =>
            logger.error("Failed to bounce tasks off the host.", err)
        }
      }
      BackOff.Operation.Wait
    }

  def bounceAllTasks(): Unit = { // public for testing
    val self = service.selfID
    val others = service.runningHosts - self

    if (others.sizeIs > 0) {
      val other = Random.choose(others.toSeq)
      val q = Task.drain(self, to = Some(other)) foreach { n =>
        logger.info(s"Bounced $n task(s) to $other.")
      }
      repo.runSynchronously(q, MaxSleepTime)
    }
  }
}
