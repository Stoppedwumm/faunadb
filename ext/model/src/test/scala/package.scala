package fauna.model

import fauna.atoms._
import fauna.flags.{ test => _, _ }
import fauna.lang.{ ConsoleControl, TimeBound, Timestamp }
import fauna.lang.clocks.{ Clock, TestClock }
import fauna.model.tasks.Mapper
import fauna.prop.PropConfig
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.query._
import fauna.repo.service.rateLimits.PermissiveOpsLimiter
import fauna.repo.store.CacheStore
import fauna.repo.test.RepoMatchers
import fauna.storage.ops.Write
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

package test {

  trait VersionsFreeSpec extends AnyFreeSpecLike {

    implicit def convertStringToVersionWrapper(s: String): VersionWrapper =
      new VersionWrapper(s)

    final class VersionWrapper(name: String) {

      def before(version: APIVersion): PredicateWrapper =
        new PredicateWrapper(name, APIVersion.removedOn(version))

      def after(version: APIVersion): PredicateWrapper =
        new PredicateWrapper(name, APIVersion.introducedOn(version))

      def forAll(fn: APIVersion => Unit): Unit =
        new PredicateWrapper(name, _ => true).in(fn)
    }

    final class PredicateWrapper(name: String, pred: APIVersion => Boolean) {

      def in(fn: APIVersion => Unit): Unit = {
        APIVersion.Versions filter pred foreach { v =>
          s"$name on api version $v" in fn(v)
        }
      }
    }
  }

  trait Spec
      extends VersionsFreeSpec
      with BeforeAndAfter
      with BeforeAndAfterAll
      with Eventually
      with Matchers
      with OptionValues
      with EitherValues
      with RepoMatchers
      with Inside {

    def await[T](f: Future[T]): T =
      Await.result(f, 5.minutes)
  }

  object EnabledFeature extends Feature[HostID, BooleanValue] {
    val key = "__test"
    val default = true
  }

  object DisabledFeature extends Feature[HostID, BooleanValue] {
    val key = "__test"
    val default = false
  }

  final class TestScanner(
    val repo: RepoContext,
    val task: Mapper.Task,
    clock: Clock,
    feature: Feature[HostID, BooleanValue],
    totalTime: FiniteDuration = 0.seconds)
      extends Mapper(
        "Test-Scan",
        feature = feature,
        totalTime = totalTime,
        timeout = 5.minutes,
        clock = clock)

}

package object test {
  implicit val ctl = new ConsoleControl
  implicit val propConfig = PropConfig()

  implicit class RepoContextOps(ctx: RepoContext) {

    /** Set a static snapshot time for testing purposes */
    def withStaticSnapshotTime(ts: Timestamp) = ctx.copy(clock = new TestClock(ts))
  }

  def getWrites[T](
    ctx: RepoContext,
    q: Query[T],
    snapshotTS: Timestamp = Clock.time) = {

    val resF = QueryEvalContext
      .eval(
        q,
        ctx,
        snapshotTS,
        TimeBound.Max,
        16 * 1024 * 1024,
        PermissiveOpsLimiter,
        ScopeID.RootID
      )

    Await.result(resF, Duration.Inf) match {
      case (QDone(v), s)  => (Success(v), s.readsWrites.allWrites.toSeq)
      case (QFail(ex), _) => (Failure(ex), Nil)
    }
  }

  def applyWritesAt(txTime: Timestamp, writes: Iterable[Write]) =
    writes foreach { op =>
      CassandraService.instance.storage.applyMutationForKey(op.rowKey) { mut =>
        op.mutateAt(mut, txTime)
      }
    }

  implicit def writesToQuery(writes: Seq[Write]) =
    (writes foldLeft Query.unit) { (q, write) =>
      q flatMap { _ => Query.write(write) }
    }

  def writesWithSchemaOCC(scope: ScopeID)(writes: => Seq[Write]): Query[Unit] =
    CacheStore.getLastSeenSchema(scope) flatMap { _ =>
      writesToQuery(writes)
    }

  def runTask(
    ctx: RepoContext,
    task: Mapper.Task,
    clock: Clock,
    feature: Feature[HostID, BooleanValue] = EnabledFeature): Boolean = {
    val mapper = new TestScanner(ctx, task, clock, feature)

    val seed: Try[Unit] = Success(())
    val result = mapper.localSegments().foldLeft(seed) { case (acc, seg) =>
      acc flatMap { _ =>
        mapper.processSegment(seg)
      }
    }

    result.isSuccess
  }

  def runTasks(ctx: RepoContext, clock: Clock, tasks: Seq[Mapper.Task]): Boolean =
    tasks forall { t => runTask(ctx, t, clock) }

  def getTasks() =
    Task.getRunnableByHost(CassandraService.instance.localID.get).flattenT
}
