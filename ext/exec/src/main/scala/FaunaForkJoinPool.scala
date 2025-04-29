// This is a workaround to get the benefits of batching execution without relying on
// the Scala's global thread pool since Fauna needs fine grain control over its
// execution life time for thread local variables tracking.
package scala.concurrent {
  import scala.concurrent.impl._
  import java.util.concurrent.{ ForkJoinPool, TimeUnit }

  /**
    * Pool configured to allow up to 256 blockers (see NamedPoolThreadFactory) and
    * maintain at least parallelism number of runnable threads. Based on:
    * https://github.com/scala/scala/blob/v2.13.3/src/library/scala/concurrent/impl/ExecutionContextImpl.scala#L101-L116
    */
  final class FaunaForkJoinPool(
    factory: ForkJoinPool.ForkJoinWorkerThreadFactory,
    parallelism: Int,
    reporter: Thread.UncaughtExceptionHandler
  ) extends ForkJoinPool(
        parallelism,
        factory,
        reporter,
        true, // asyncMode
        parallelism, // corePoolSize
        parallelism + 256, // maximumPoolSize
        parallelism, // minimumRunnable
        null, // saturate
        60, // keepAliveTime in seconds
        TimeUnit.SECONDS)
      with BatchingExecutor {

    final override def submitForExecution(runnable: Runnable): Unit =
      super[ForkJoinPool].execute(runnable)

    final override def execute(runnable: Runnable): Unit =
      if (
        (!runnable.isInstanceOf[Promise.Transformation[_, _]]
        || runnable.asInstanceOf[Promise.Transformation[_, _]].benefitsFromBatching)
        && runnable.isInstanceOf[Batchable]
      ) {
        submitAsyncBatched(runnable)
      } else {
        submitForExecution(runnable)
      }

    final override def reportFailure(cause: Throwable): Unit =
      getUncaughtExceptionHandler() match {
        case null =>
        case some => some.uncaughtException(Thread.currentThread, cause)
      }
  }
}
