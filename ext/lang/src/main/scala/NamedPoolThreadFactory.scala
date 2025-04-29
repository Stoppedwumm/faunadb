package fauna.lang

import java.io.IOException
import java.util.concurrent.{ Semaphore, ForkJoinPool, ForkJoinWorkerThread, ThreadFactory }
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ BlockContext, CanAwait }

/**
 * A [[java.util.concurrent.ThreadFactory]] which creates threads with a name
 * indicating the pool from which they originated.
 *
 * A new [[java.lang.ThreadGroup]] (named `name`) is created as a sub-group of
 * whichever group to which the thread that created the factory belongs. Each
 * thread created by this factory will be a member of this group and have a
 * unique name including the group name and an monotonically increasing number.
 * The intention of this naming is to ease thread identification in debugging
 * output.
 *
 * For example, a `NamedPoolThreadFactory` with `name="writer"` will create a
 * `ThreadGroup` named "writer" and new threads will be named "writer-1",
 * "writer-2", etc.
 *
 * @param name the name of the new thread group
 * @param makeDaemons determines whether or not this factory will creates
 * daemon threads.
 */
class NamedPoolThreadFactory(
  val name: String,
  makeDaemons: Boolean = false,
  exceptionHandler: Thread.UncaughtExceptionHandler = null,
  maxBlockers: Int = 256)
    extends ThreadFactory with ForkJoinPool.ForkJoinWorkerThreadFactory {

  val group = new ThreadGroup(Thread.currentThread.getThreadGroup, name)
  val threadCount = new AtomicInteger(0)

  def size = threadCount.get

  def newThread(r: Runnable) = {
    val idx = threadCount.incrementAndGet

    configure(s"${name}-${idx}", new Thread(group, new Runnable {
      final override def run =
        try {
          r.run()
        } catch {
          // netty doesn't trap these, and we can't do anything about them.
          case ex: IOException if ex.getMessage == "Connection reset by peer" => ()
        } finally {
          threadCount.decrementAndGet
        }
    }))
  }

  private final val blockerPermits = new Semaphore(maxBlockers)

  def newThread(pool: ForkJoinPool) = {
    val idx = threadCount.incrementAndGet

    configure(s"${name}-${idx}", new ForkJoinWorkerThread(pool) with BlockContext {
      final override def onTermination(ex: Throwable): Unit = threadCount.decrementAndGet

      private[this] final var isBlocked: Boolean = false // This is only ever read & written if this thread is the current thread
      final override def blockOn[T](thunk: => T)(implicit permission: CanAwait): T =
        if ((Thread.currentThread eq this) && !isBlocked && blockerPermits.tryAcquire()) {
          try {
            val b: ForkJoinPool.ManagedBlocker with (() => T) =
              new ForkJoinPool.ManagedBlocker with (() => T) {
                private[this] final var result: T = null.asInstanceOf[T]
                private[this] final var done: Boolean = false
                final override def block(): Boolean = {
                  if (!done) {
                    result = thunk // If this throws then it will stop blocking.
                    done = true
                  }

                  isReleasable
                }

                final override def isReleasable = done
                final override def apply(): T = result
              }
            isBlocked = true
            ForkJoinPool.managedBlock(b)
            b()
          } finally {
            isBlocked = false
            blockerPermits.release()
          }
        } else thunk // Unmanaged blocking
    })
  }

  private def configure[T <: Thread](threadName: String, thread: T) = {
    thread.setDaemon(makeDaemons)
    thread.setName(threadName)

    if (thread.getPriority != Thread.NORM_PRIORITY) {
      thread.setPriority(Thread.NORM_PRIORITY)
    }

    if (exceptionHandler ne null) {
      thread.setUncaughtExceptionHandler(exceptionHandler)
    }

    thread
  }
}
