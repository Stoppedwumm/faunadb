package fauna.exec

import fauna.lang.Local
import fauna.logging.ExceptionLogFailureReporter
import fauna.trace._
import io.netty.util.internal.ObjectPool
import java.util.{ ArrayDeque, Queue }
import scala.concurrent.{
  BlockContext,
  CanAwait,
  ExecutionContext,
  ExecutionContextExecutor
}

// An execution context that synchronously executes submitted
// runnables. May be used to attach future transformations to the
// running thread (Regain twitter-future style execution). Not to be
// used lightly!
//
// Blocking should be strictly avoided as it could hog the current
// thread.  Also, since we're running on a single thread, blocking
// code risks deadlock.
//
// See https://github.com/scala/scala/blob/v2.11.7/src/library/scala/concurrent/Future.scala#L597
object ImmediateExecutionContext extends ExecutionContextExecutor with BlockContext {
  self =>

  /*
   * A ThreadLocal value is used to track the state of the trampoline
   * in the current thread. When a Runnable is added to the trampoline
   * it uses the ThreadLocal to see if the trampoline is already
   * running in the thread. If so, it starts the trampoline. When it
   * finishes, it checks the ThreadLocal to see if any Runnables have
   * subsequently been scheduled for execution. It runs all the
   * Runnables until there are no more to execute, then it clears the
   * ThreadLocal and stops running.
   *
   * ThreadLocal states:
   * - null =>
   *       - no Runnable running: trampoline is inactive in the
   *         current thread
   * - Empty =>
   *       - a Runnable is running and trampoline is active
   *       - no more Runnables are enqueued for execution after the
   *         current Runnable completes
   * - next: Runnable =>
   *       - a Runnable is running and trampoline is active
   *       - one Runnable is scheduled for execution after the current
   *         Runnable completes
   * - queue: ArrayDeque[Runnable] =>
   *       - a Runnable is running and trampoline is active
   *       - two or more Runnables are scheduled for execution after
   *         the current Runnable completes
   */
  private[this] val local = new ThreadLocal[AnyRef]

  private final class RecycleQueue(handle: ObjectPool.Handle[RecycleQueue])
      extends ArrayDeque[Runnable](4) {

    def recycle() = {
      assert(this.isEmpty)
      handle.recycle(this)
    }
  }

  private[this] val pool =
    ObjectPool.newPool(new ObjectPool.ObjectCreator[RecycleQueue] {
      def newObject(handle: ObjectPool.Handle[RecycleQueue]) =
        new RecycleQueue(handle)
    })

  // marker for an empty queue
  private[this] object Empty

  override def prepare(): ExecutionContext =
    new ExecutionContext {
      val tracer = GlobalTracer.instance
      val context = Local.save()

      val active = tracer.retainActive()

      def execute(r: Runnable): Unit =
        self.execute(new Runnable {
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

  override def blockOn[T](thunk: => T)(implicit permission: CanAwait): T = {
    // If this thread is about to block, go execute tasks.
    executeScheduled()
    thunk
  }

  def execute(r: Runnable): Unit =
    local.get match {
      case null =>
        try {
          // start a new trampoline
          local.set(Empty)
          BlockContext.withBlockContext(this) {
            try {
              r.run()
              // Once complete, go find more tasks.
              executeScheduled()
            } finally {
              // Now that we've executed everything, recycle the queue if needed.
              cleanup()
            }
          }
        } finally {
          // shutdown the trampoline
          local.set(null)
        }
      case Empty => local.set(r)
      case next: Runnable =>
        // Convert the single queued Runnable into an ArrayDeque
        // so we can schedule 2+ Runnables
        val queue = pool.get()
        queue.add(next)
        queue.add(r)
        local.set(queue)
      case q: Queue[_] =>
        // push onto the end of the queue
        val queue = q.asInstanceOf[RecycleQueue]
        queue.add(r)
      case v =>
        throw new IllegalStateException(s"Unknown trampoline value: $v")
    }

  @annotation.tailrec
  private def executeScheduled(): Unit = {
    local.get match {
      case Empty => ()
      case r: Runnable =>
        // mark the queue empty
        local.set(Empty)
        r.run()
        // look for more work
        executeScheduled()
      case q: Queue[_] =>
        val queue = q.asInstanceOf[RecycleQueue]
        while (!queue.isEmpty) {
          val r = queue.remove
          r.run()
        }
      case v =>
        throw new IllegalStateException(s"Unknown trampoline value: $v")
    }
  }

  private def cleanup(): Unit = {
    local.get match {
      case Empty       => ()
      case _: Runnable => ()
      case q: Queue[_] =>
        val queue = q.asInstanceOf[RecycleQueue]
        queue.recycle()
      case v =>
        throw new IllegalStateException(s"Unknown trampoline value: $v")
    }
  }


  def reportFailure(t: Throwable): Unit =
    ExceptionLogFailureReporter.report(None, t)
}
