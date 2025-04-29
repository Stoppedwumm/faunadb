package fauna.exec

import fauna.lang.{ FailureReporter, Local, NamedPoolThreadFactory }
import fauna.trace.GlobalTracer
import java.util.concurrent.{
  ArrayBlockingQueue,
  ExecutorService,
  ForkJoinPool,
  LinkedBlockingQueue,
  RejectedExecutionException,
  SynchronousQueue,
  ThreadPoolExecutor,
  TimeUnit
}
import scala.concurrent.{
  Batchable,
  ExecutionContext,
  ExecutionContextExecutor,
  FaunaForkJoinPool
}

trait FaunaExecutionContext extends ExecutionContextExecutor { self =>

  // FIXME: Deprecated in 2.12. Required to support Local. Only used
  // by tracing subsystem at this point, which we should probably
  // migrate to Query.
  override def prepare(): ExecutionContext =
    new ExecutionContext {
      val tracer = GlobalTracer.instance
      val context = Local.save()

      // capture any span active when Futures are created
      val active = tracer.retainActive()

      def execute(r: Runnable): Unit =
        self.execute(new Runnable with Batchable {

          def run(): Unit =
            Local.let(context) {
              try {
                r.run()
              } finally {
                active foreach { _.release() }
              }
            }
        })

      def reportFailure(t: Throwable): Unit = self.reportFailure(t)
    }

  def name: String

  def reportStats(stats: FaunaExecutionContext.Stats): Unit

  def isRunning: Boolean

  def stop(graceful: Boolean = true): Unit

  def die(): Unit = stop(false)
}

object FaunaExecutionContext {

  trait Stats {
    def set(name: String, count: Int): Unit

    def reportPoolSize(name: String, count: Int) =
      set(s"ThreadPool.$name.Pool.Size", count)

    def reportPending(name: String, count: Int) =
      set(s"ThreadPool.$name.Pending", count)

    def reportActive(name: String, count: Int) =
      set(s"ThreadPool.$name.Active", count)

    def reportRunning(name: String, count: Int) =
      set(s"ThreadPool.$name.Running", count)

    def reportBlocked(name: String, count: Int) =
      set(s"ThreadPool.$name.Blocked", count)
  }

  @volatile private[this] var ecs = Set.empty[FaunaExecutionContext]
  @volatile private[this] var pools = Set.empty[NamedPoolThreadFactory]

  private[this] var availableProcessors: Int = 0

  def setAvailableProcessors(n: Int): Unit = synchronized {
    if (availableProcessors == 0) {
      availableProcessors = n
    } else {
      throw new IllegalStateException(s"availableProcessors is already set to $availableProcessors, rejecting $n.")
    }
  }

  private[exec] def register(ec: FaunaExecutionContext) = synchronized { ecs += ec }

  private[exec] def deregister(ec: FaunaExecutionContext) = synchronized {
    ecs -= ec
  }

  private[exec] def register(p: NamedPoolThreadFactory) = synchronized { pools += p }

  private[exec] def deregister(p: NamedPoolThreadFactory) = synchronized {
    pools -= p
  }

  def report(stats: Stats) = {
    ecs foreach { _.reportStats(stats) }
    pools foreach { pool =>
      stats.reportPoolSize(pool.name, pool.size)
    }
  }

  def stopAll(graceful: Boolean = true) = ecs foreach { _.stop(graceful) }

  object Implicits {
    implicit lazy val global = synchronized {
      if (availableProcessors <= 0) {
        // NB. This call returns incorrect results in containers running < Java 10.
        availableProcessors = Runtime.getRuntime.availableProcessors
      }

      val factory =
        new NamedPoolThreadFactory("FaunaGlobal", true, FailureReporter.Default)
      val pool = NamedForkJoinExecutionContext.makePool(
        factory,
        parallelism = availableProcessors * 2,
        FailureReporter.Default
      )

      new NamedForkJoinExecutionContext("FaunaGlobal",
                                        factory,
                                        pool,
                                        FailureReporter.Default) {
        override def stop(graceful: Boolean) = ()
      }
    }
  }
}

sealed abstract class NamedExecutorServiceContext extends FaunaExecutionContext {
  val name: String
  val threadFactory: NamedPoolThreadFactory
  val pool: ExecutorService
  val failureReporter: FailureReporter

  FaunaExecutionContext.register(threadFactory)
  FaunaExecutionContext.register(this)

  def execute(r: Runnable) =
    try {
      pool.execute(r)
    } catch {
      // if the pool has been shutdown, squelch spurious rejected
      // executions
      case _: RejectedExecutionException if pool.isShutdown => ()
    }

  def reportFailure(ex: Throwable) = failureReporter.report(None, ex)

  def isRunning = !pool.isTerminated

  def stop(graceful: Boolean) = {
    if (graceful) pool.shutdown() else pool.shutdownNow()
    FaunaExecutionContext.deregister(this)
    FaunaExecutionContext.deregister(threadFactory)
  }
}

object NamedForkJoinExecutionContext {

  private[exec] def makePool(
    factory: NamedPoolThreadFactory,
    parallelism: Int,
    reporter: FailureReporter) =
    new FaunaForkJoinPool(factory, parallelism, reporter)

  def apply(
    name: String,
    parallelism: Int,
    daemons: Boolean = false,
    reporter: FailureReporter = FailureReporter.Default) = {

    val factory = new NamedPoolThreadFactory(name, daemons, reporter)
    val pool = makePool(factory, parallelism, reporter)

    new NamedForkJoinExecutionContext(name, factory, pool, reporter)
  }
}

class NamedForkJoinExecutionContext(
  val name: String,
  val threadFactory: NamedPoolThreadFactory,
  val pool: ForkJoinPool,
  val failureReporter: FailureReporter)
    extends NamedExecutorServiceContext {

  def reportStats(stats: FaunaExecutionContext.Stats) = {
    stats.reportPending(name, pool.getQueuedSubmissionCount)
    stats.reportActive(name, pool.getActiveThreadCount)
    stats.reportRunning(name, pool.getRunningThreadCount)
    stats.reportBlocked(name, pool.getActiveThreadCount - pool.getRunningThreadCount)
  }
}

object NamedThreadPoolExecutionContext {
  private[exec] def makePool(
    factory: NamedPoolThreadFactory,
    parallelism: Int,
    capacity: Int) = {

    val queue = if (parallelism == Int.MaxValue) {
      new SynchronousQueue[Runnable]
    } else if (capacity < 2000) {
      new ArrayBlockingQueue[Runnable](capacity)
    } else {
      new LinkedBlockingQueue[Runnable](capacity)
    }

    new ThreadPoolExecutor(if (parallelism == Int.MaxValue) 0 else parallelism,
                           parallelism,
                           60,
                           TimeUnit.SECONDS,
                           queue,
                           factory)
  }

  def apply(
    name: String,
    parallelism: Int = Int.MaxValue,
    capacity: Int = Int.MaxValue,
    daemons: Boolean = false,
    reporter: FailureReporter = FailureReporter.Default) = {

    val factory = new NamedPoolThreadFactory(name, daemons, reporter)
    val pool = makePool(factory, parallelism, capacity)

    new NamedThreadPoolExecutionContext(name, factory, pool, reporter)
  }
}

class NamedThreadPoolExecutionContext(
  val name: String,
  val threadFactory: NamedPoolThreadFactory,
  val pool: ThreadPoolExecutor,
  val failureReporter: FailureReporter)
    extends NamedExecutorServiceContext {

  def reportStats(stats: FaunaExecutionContext.Stats) = {
    stats.reportPending(name, pool.getQueue.size)
    stats.reportActive(name, pool.getActiveCount)
  }
}
