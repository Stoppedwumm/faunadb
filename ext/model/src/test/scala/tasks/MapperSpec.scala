package fauna.model.test

import fauna.atoms._
import fauna.auth.Auth
import fauna.flags.{ BooleanValue, Feature }
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.model.tasks._
import fauna.repo._
import fauna.repo.query.Query
import fauna.repo.store.VersionStore
import fauna.repo.test.CassandraHelper
import fauna.storage.api.version.StorageVersion
import fauna.trace._
import java.io.IOException
import java.util.concurrent.TimeoutException
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class MapperSpec extends Spec with Eventually {
  import SocialHelpers._

  object NullTask extends RangeIteratee[(ScopeID, DocID), StorageVersion] {
    def apply(v: (ScopeID, DocID)): Query[Option[ColIteratee[StorageVersion]]] =
      Query.some(colIter())

    private def colIter(): ColIteratee[StorageVersion] =
      ColIteratee { cols =>
        if (cols.nonEmpty) {
          Query.some(colIter())
        } else {
          Query.none
        }
      }
  }

  case class FailingTask(exception: Exception)
      extends RangeIteratee[(ScopeID, DocID), StorageVersion] {
    def apply(v: (ScopeID, DocID)): Query[Option[ColIteratee[StorageVersion]]] = {
      throw exception
    }
  }

  object NonFatalTask extends RangeIteratee[(ScopeID, DocID), StorageVersion] {
    def apply(v: (ScopeID, DocID)) =
      throw new IOException()
  }

  val exporter = new MemoryExporter
  val tracer = new SamplingTracer(new ProbabilitySampler(1.0))

  tracer.addExporter(exporter)

  GlobalTracer.registerIfAbsent { (None, tracer) }

  val ctx = CassandraHelper.context("model")

  before {
    exporter.reset()

    val scope = ctx ! newScope
    val auth = Auth.forScope(scope)

    socialSetup(ctx, auth)
  }

  def process(task: Mapper.Task): Try[Unit] = {
    val mapper = new TestScanner(ctx, task, Clock, feature = EnabledFeature)

    val seed: Try[Unit] = Success(())
    mapper.localSegments().foldLeft(seed) { case (acc, seg) =>
      acc flatMap { _ =>
        mapper.processSegment(seg)
      }
    }
  }

  "Mapper" - {
    "works" in {
      val task = Mapper.Task(VersionStore.sparseScan, { _ => NullTask })

      tracer.activeSpan shouldBe empty
      process(task).isSuccess should be (true)
      tracer.activeSpan shouldBe empty
    }

    "stops" in {
      val task = Mapper.Task(VersionStore.sparseScan, { _ => NullTask })
      val mapper = new TestScanner(ctx, task, Clock, feature = EnabledFeature, totalTime = 1.day)
      mapper.start()

      eventually {
        mapper.stop()
        mapper.isRunning should be (false)
      }
    }

    "responds to a feature flag" in {
      @volatile var sentinel = false

      object SentinelTask extends RangeIteratee[(ScopeID, DocID), StorageVersion] {
        def apply(v: (ScopeID, DocID)) =
          Query {
            if (sentinel) {
              None
            } else {
              sentinel = true
              None
            }
          }
      }

      def run(feature: Feature[HostID, BooleanValue]) = {
        val task = Mapper.Task(VersionStore.sparseScan, { _ => SentinelTask })
        val mapper = new TestScanner(ctx, task, Clock, feature = feature, totalTime = 1.day)
        mapper.start()
        Thread.sleep(1000) // plenty of time to process
        mapper.stop(graceful = false)
      }


      run(DisabledFeature)
      sentinel should be (false)

      run(EnabledFeature)
      sentinel should be (true)
    }

    "failure modes" - {

      def assertCause(task: Mapper.Task, ex: Throwable) =
        process(task) match {
          case Success(_) =>
            fail("unexpected success")
          case Failure(cause) =>
            cause.getSuppressed should contain (ex)
        }


      "timeout" in {
        val ex = new TimeoutException("flaky")
        val task = Mapper.Task(VersionStore.sparseScan, { _ => FailingTask(ex) })

        assertCause(task, ex)
      }

      "contention" in {
        val ex = DocContentionException(ScopeID(0), DocID(SubID(0), CollectionID(0)), Timestamp.Epoch)
        val task = Mapper.Task(VersionStore.sparseScan, { _ => FailingTask(ex) })

        a[DocContentionException] shouldBe thrownBy {
          process(task).get
        }
      }

      "transaction too large" in {
        val task = Mapper.Task(VersionStore.sparseScan, { _ =>
          FailingTask(TxnTooLargeException(10, 5, "bytes")) })

        a[TxnTooLargeException] shouldBe thrownBy {
          process(task).get
        }
      }

      "transaction key too large" in {
        val task = Mapper.Task(VersionStore.sparseScan, { _ =>
          FailingTask(TransactionKeyTooLargeException(42)) })

        a[TransactionKeyTooLargeException] shouldBe thrownBy {
          process(task).get
        }
      }

      "version too large" in {
        val task = Mapper.Task(VersionStore.sparseScan, { _ =>
          FailingTask(VersionTooLargeException(42)) })

        a[VersionTooLargeException] shouldBe thrownBy {
          process(task).get
        }
      }

      "max query width exceeded" in {
        val ex = MaxQueryWidthExceeded(10, 5)
        val task = Mapper.Task(VersionStore.sparseScan, { _ => FailingTask(ex) })

        assertCause(task, ex)
      }

      "nonfatal" in {
        val task = Mapper.Task(VersionStore.sparseScan, { _ => NonFatalTask })

        process(task).isFailure should be (true)
      }
    }
  }
}
