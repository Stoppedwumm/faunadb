package fauna.model.test

import fauna.ast._
import fauna.atoms.AccountID
import fauna.auth._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.tasks._
import fauna.repo.cassandra.CassandraService
import fauna.repo.test.CassandraHelper
import fauna.trace.Sampler
import org.scalatest.tags.Slow
import scala.util.Random

@Slow
class TaskReprioritizerSpec extends Spec {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  val local = CassandraService.instance.localID.get

  /** Creates a new Auth, with a random account ID.
    */
  def newAuth(): Auth =
    Auth.forScope(ctx ! newScope(RootAuth, AccountID(Random.nextInt())))

  def newExecutor(batchSize: Int): TaskExecutor =
    new TaskExecutor(
      repo = ctx,
      router = TaskRouter,
      service = CassandraService.instance,
      reprioritizer = new TaskReprioritizer(ctx, CassandraService.instance),
      sampler = Sampler.Default,
      parallelism = 2,
      batchSize = batchSize,
      consumeTimeSlice = false
    )

  def makeLargeColl(auth: Auth, name: String) = {
    ctx ! mkCollection(auth, MkObject("name" -> name))
    (0 until 129) map { i =>
      ctx ! evalQuery(
        auth,
        CreateF(
          ClsRefV("coll"),
          MkObject("data" -> MkObject("num" -> i))
        ))
    }
  }

  def collectTasks(auths: Seq[Auth]): Map[String, Task] = {
    val localID = CassandraService.instance.localID.get
    ctx ! Task.getRunnableByHost(localID).flattenT flatMap {
      // Filter for just the tasks created in this test
      case task if auths.exists(_.scopeID == task.scopeID) =>
        // This get will throw if the task isn't for an index build, which
        // is intentional
        val index = (ctx ! IndexBuild.index(task)).get
        Some((index.name, task))
      case _ => None
    } toMap
  }

  /** Creates a scenario where the exception mentioned in issue ENG-XXX could occur.
    *
    * To re-create the issue uncomment InterruptedException case match
    * in [[TaskExecutor.foldErrors]] increase the number of iterations from 1 to 20.
    * and you should see this test print the error messages mentioned in the issue ticket.
    */
  "TaskReprioritizer" - {
    "reprioritized tasks are ordered correctly" in {
      val execOnTwoThreads = newExecutor(batchSize = 2)

      val auth1 = newAuth()
      makeLargeColl(auth1, "coll")

      val auth2 = newAuth()
      makeLargeColl(auth2, "coll")

      // Needs to be in order for Create calls
      val indexesList = List(
        "idx0" -> auth1,
        "idx1" -> auth1,
        "idx2" -> auth1,
        "idx3" -> auth1,
        "idx4" -> auth1,
        "otherIdx0" -> auth2)
      indexesList foreach { case (name, auth) =>
        ctx ! evalQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> name,
              "source" -> ClsRefV("coll"),
              "values" -> Seq(
                MkObject("field" -> Seq("data", "num"))
              )
            )))
      }

      val tasks0 = collectTasks(Seq(auth1, auth2))
      tasks0("idx0").priority should be(0)
      tasks0("otherIdx0").priority should be(0)
      tasks0("idx1").priority should be(-10)
      tasks0("idx2").priority should be(-20)
      tasks0("idx3").priority should be(-30)
      tasks0("idx4").priority should be(-40)

      execOnTwoThreads.reprioritizer.reprioritize()

      val tasks1 = collectTasks(Seq(auth1, auth2))
      tasks1("idx0").priority should be(0)
      tasks1("otherIdx0").priority should be(0)
      tasks1("idx1").priority should be(-10)
      tasks1("idx2").priority should be(-20)
      tasks1("idx3").priority should be(-30)
      tasks1("idx4").priority should be(-40)

      var idx0Active = false
      var otherIdx0Active = false
      while (!idx0Active || !otherIdx0Active) {
        execOnTwoThreads.step()
        if (!idx0Active) {
          idx0Active = ctx ! evalQuery(auth1, Get(IndexRef("idx0"))) match {
            case Right(v) => v.asInstanceOf[VersionL].version.data(Index.ActiveField)
            case Left(_)  => false
          }
        }
        if (!otherIdx0Active) {
          otherIdx0Active =
            ctx ! evalQuery(auth2, Get(IndexRef("otherIdx0"))) match {
              case Right(v) =>
                v.asInstanceOf[VersionL].version.data(Index.ActiveField)
              case Left(_) => false
            }
        }
      }

      val tasks2 = collectTasks(Seq(auth1, auth2))
      tasks2 should not contain "idx0"
      tasks2 should not contain "otherIdx0"
      tasks2("idx1").priority should be(-10)
      tasks2("idx2").priority should be(-20)
      tasks2("idx3").priority should be(-30)
      tasks2("idx4").priority should be(-40)

      execOnTwoThreads.reprioritizer.reprioritize()

      val tasks3 = collectTasks(Seq(auth1, auth2))
      tasks3 should not contain "idx0"
      tasks3 should not contain "otherIdx0"
      tasks3("idx1").priority should be(0)
      tasks3("idx2").priority should be(-10)
      tasks3("idx3").priority should be(-20)
      tasks3("idx4").priority should be(-30)
    }

    "initial tasks are executed in order" in {
      val execOnTwoThreads = newExecutor(batchSize = 2)

      val auth1 = newAuth()
      makeLargeColl(auth1, "coll")

      val auth2 = newAuth()
      makeLargeColl(auth2, "coll")

      // Needs to be in order for Create calls
      val indexesList = List(
        "idx0" -> auth1,
        "idx1" -> auth1,
        "idx2" -> auth1,
        "idx3" -> auth1,
        "idx4" -> auth1,
        "otherIdx0" -> auth2)
      indexesList foreach { case (name, auth) =>
        ctx ! evalQuery(
          auth,
          CreateIndex(
            MkObject(
              "name" -> name,
              "source" -> ClsRefV("coll"),
              "values" -> Seq(
                MkObject("field" -> Seq("data", "num"))
              )
            )))
      }
      var indexes = Map.from(indexesList)

      while (indexes.nonEmpty) {
        execOnTwoThreads.step()
        var completed = indexes filter { case (name, auth) =>
          ctx ! evalQuery(auth, Get(IndexRef(name))) match {
            case Right(v) => v.asInstanceOf[VersionL].version.data(Index.ActiveField)
            case Left(_)  => false
          }
        }
        completed.size should be <= 2
        while (completed.nonEmpty) {
          val keys = indexes.size match {
            case 5 | 6 => Set("idx0", "otherIdx0")
            case 4     => Set("idx1")
            case 3     => Set("idx2")
            case 2     => Set("idx3")
            case 1     => Set("idx4")
            case _     => fail(s"invalid size $indexes")
          }
          completed.keys should contain atLeastOneElementOf keys
          // remove only the ones that were completed
          indexes --= keys intersect completed.keys.toSet
          completed --= keys
        }
      }
    }

    "tasks created in one transaction are ordered correctly" in pendingUntilFixed {
      val auth = newAuth()
      makeLargeColl(auth, "coll")

      // Needs to be in order for Create calls
      val indexesList = List("idx0", "idx1", "idx2")
      val creates = indexesList.map { name =>
        CreateIndex(
          MkObject(
            "name" -> name,
            "source" -> ClsRefV("coll"),
            "values" -> Seq(
              MkObject("field" -> Seq("data", "num"))
            )
          ))
      }
      ctx ! evalQuery(auth, Do(creates))

      val tasks0 = collectTasks(Seq(auth))
      tasks0("idx0").priority should be(0)
      tasks0("idx1").priority should be(-10)
      tasks0("idx2").priority should be(-20)
    }
  }
}
