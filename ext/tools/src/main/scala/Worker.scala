package fauna.tools

import fauna.lang.NamedPoolThreadFactory
import java.util.concurrent.{ Executors, Phaser }

trait Worker extends Runnable

class Executor(val name: String, val nThreads: Int = 0) {

  private[this] val threadFactory = new NamedPoolThreadFactory(name)

  private[this] val executor =
    Executors.newFixedThreadPool(getThreads, threadFactory)

  private[this] val phaser = new Phaser(1)

  def getThreads =
    if (nThreads > 0) nThreads else Runtime.getRuntime.availableProcessors()

  def addWorker(worker: Worker): Unit = {
    phaser.register()
    executor execute { () =>
      try {
        worker.run()
      } finally {
        phaser.arrive()
      }
    }
  }

  def waitWorkers(): Unit = {
    phaser.awaitAdvanceInterruptibly(phaser.arrive())
    executor.shutdown()
  }
}
