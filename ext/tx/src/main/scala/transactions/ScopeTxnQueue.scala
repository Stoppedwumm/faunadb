package fauna.tx.transaction

import fauna.atoms.ScopeID
import fauna.exec.AsyncQueue
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.stats.StatsRecorder
import java.util.concurrent.{ ArrayBlockingQueue, TimeoutException }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicLong }
import scala.collection.immutable.SortedMap
import scala.concurrent.duration._
import scala.concurrent.{ blocking, Future, Promise }
import scala.util.Try

import Batch._

object ScopeTxnQueue {
  val QueueDepth = 128
}

/**
  * This queue handles the job of recombining the individual epoch
  * batch streams from each log segment into a single combined epoch
  * batch stream for the scope. There is inbound queue for each
  * segment, and a combined outbound queue for downstream transaction
  * processing.
  *
  * Upon receiving a batch from a lognode, the batch is added to its
  * corresponding inbound queue, and then the inbound queues are
  * inspected to see if it's possible to hand off a combined batch.
  *
  * If batch receipt idles, a re-sync with each upstream log segment
  * is triggered.
  */
final case class ScopeTxnQueue(
  epochClock: Clock,
  stats: TxnPipeline.Stats,
  maxBuffered: Int,
  maxTransactionsPerEpoch: Int) extends ExceptionLogging {

  private type CQ = ContiguousQueue[Batch]

  val queue = new AsyncQueue[Batch]

  private[this] val completed = new AtomicLong
  private[this] val pollRate = 10
  StatsRecorder.polling(pollRate.seconds) {
    stats.recordQueueProcessRate((completed.getAndSet(0) / pollRate).toDouble)
    stats.recordTransactionQueueSize(queue.size)
  }

  @volatile
  private[this] var segments = SortedMap.empty[SegmentID, (CQ, SegmentInfo)]

  private[this] val thunks = new ArrayBlockingQueue[Runnable](ScopeTxnQueue.QueueDepth)

  private[this] val processing = new AtomicBoolean(false)

  private[this] val BlockFor = 50.millis

  private val logger = getLogger

  private def mkCQ(e: Epoch) = new CQ(e.idx, maxBuffered, maxBuffered)

  /**
   * Scavenging scheme:
   * The idea is that if there is no work pending then just do what we want
   * then check if anyone added any work. If so kick that off.
   * Alternatives to using a queue might be to just have an atomic ref to a
   * promise that we just keep chaining work to.
   *
   * Public for testing.
   */
  final def work[A](thunk: => A): Future[A] = {
    if (processing.compareAndSet(false, true)) {
      val ret = Future.fromTry(Try(thunk))
      completed.getAndIncrement
      process()
      ret
    } else {
      val p = Promise[A]()
      if (!offer(thunk, p)) {
        stats.incrQueueFull()
        logger.warn("Blocked for 50ms accessing ScopeTxnQueue")
        p.failure(new TimeoutException("Failed to enqueue task after 50ms"))
      }
      p.future
    }
  }

  private def offer[A](thunk: => A, p: Promise[A]): Boolean = {
    val wrapped: Runnable = { () =>
      p.complete(Try(thunk))
      completed.getAndIncrement
    }
    thunks.offer(wrapped, BlockFor.length, BlockFor.unit)
  }

  /**
   * Run the task loop.
   * The first call in we budget the number of tasks we are willing to
   * run because we are blocking the return of some computation. Once
   * we have yielded at least once there is no point in context
   * switching so we pump the queue till it's empty before exiting or
   * yielding because we don't want to accidentally blow the stack.
   */
  private def process(onABudget: Boolean = true): Future[Unit] = {
    try {
      process0(onABudget)
    } finally {
      // Give someone else the chance to work
      processing.set(false)
    }

    // if it's still empty then we are definitely good.
    if (thunks.isEmpty) {
      Future.unit
    } else {
      // if it isn't empty then maybe someone managed to slip some work in
      // while we were switching the flag. Try to grab the conch again.
      if (processing.compareAndSet(false, true)) {
        // Switch the thread so the caller can get their result.
        logException(Future { process(false) })
      } else {
        // Someone else has taken up the slack.
        Future.unit
      }
    }
  }

  private def process0(onABudget: Boolean): Unit = {
    blocking {
      // this if statement is inelegant.
      if (onABudget) {
        // I guess we should measure this?
        var budget = 5
        while (!thunks.isEmpty && budget > 0) {
          thunks.poll().run()
          budget -= 1
        }
      } else {
        while (!thunks.isEmpty) {
          thunks.poll().run()
        }
      }
    }
  }

  // public for testing
  final def noTimeout[A](f: => Future[A]): Future[A] =
    f recoverWith { case _: TimeoutException => noTimeout(f) }

  def reinit(seg: SegmentID, epoch: Epoch): Future[Unit] =
    // Only reinit already known segments
    work {
      segments.get(seg) foreach { case (_, l) =>
        segments = segments.updated(seg, (mkCQ(epoch), l))
      }
    }

  def updateSegments(
    sl: Map[SegmentID, SegmentInfo],
    applyEpoch: Epoch): Future[Unit] =
    noTimeout(updateSegments0(sl, applyEpoch))

  private[this] def updateSegments0(
    sl: Map[SegmentID, SegmentInfo],
    applyEpoch: Epoch): Future[Unit] =
    work {
      segments.keys foreach { sID =>
        if (!sl.contains(sID)) {
          val (cq, segmentInfo) = segments(sID)
          logger.warn(s"Dropping $sID as of $applyEpoch (cq.lastIdx=${cq.lastIdx}, segmentInfo=$segmentInfo).")
        }
      }

      // Segments map should contain all existing started (incl. those since closed)
      // segments, with their up-to-date SegmentInfo, and create a CQ for those that
      // don't already have a CQ.
      segments = (sl.iterator collect {
        case (sid, info @ SegmentInfo(_, SegmentState.HasStart(start), _)) =>
          val cq = segments.get(sid).fold(mkCQ((start max applyEpoch) - 1)) { _._1 }
          sid -> ((cq, info))
      }).to(SortedMap)
    }

  def add(seg: SegmentID, prev: Epoch, in: Seq[Batch]): Future[Option[Epoch]] =
    work {
      segments.get(seg) match {
        case None =>
          logger.warn(s"ScopeTxnQueue received add for unknown $seg: $prev ${in map { b => b.epoch } }")
          None

        case Some((q, _)) =>

          val qlst = q.lastIdx
          val now = epochClock.time

          var p = prev.idx

          in foreach { b =>
            val ep = b.epoch.idx
            q.add(p, ep, b)
            p = ep
          }

          stats.recordPartialEpochReceiveLatency(now difference Epoch(p).ceilTimestamp)

          val out = List.newBuilder[Batch]
          var b = pollSegments
          var lstEpoch = Epoch.MinValue

          while (b.isDefined) {
            lstEpoch = b.get.epoch
            out += b.get
            b = pollSegments
          }

          val result = out.result()
          if (result.nonEmpty) {
            stats.recordFullEpochReceiveLatency(now difference lstEpoch.ceilTimestamp)
            queue.add(result)
          }

          if (qlst == q.lastIdx) None else Some(Epoch(q.lastIdx))
      }
    }

  def lastEpochs: Future[SortedMap[SegmentID, Epoch]] =
    noTimeout { lastEpochs0 }

  private[this] def lastEpochs0: Future[SortedMap[SegmentID, Epoch]] =
    work {
      val ret = segments map { case (s, (q, _)) => s -> Epoch(q.lastIdx) }
      ret
    }

  def poll(idle: FiniteDuration): Future[Option[Seq[Batch]]] =
    queue.poll(idle)

  def subscribe[R](idle: FiniteDuration)
    (f: Option[Seq[Batch]] => Future[Option[R]]): Future[R] =
    queue.subscribe(idle)(f)

  private def pollSegments = {
    if (segments.isEmpty) {
      None
    } else {
      val qs = List.newBuilder[CQ]
      var ep = Epoch.MaxValue

      segments foreach { case (_, (q, si)) =>
        q.peek match {
          case null =>
            if (q.lastIdx < ep.idx) {
              val lep = Epoch(q.lastIdx)
              // We need to consider this empty queue's last index as a minimum
              // if we expect to receive new batches in it (it isn't closed at
              // lep + 1). Note that Raft leader LogNode for a log segment will
              // emit at least one empty batch into the log after the segment's
              // end epoch to ensure that queues here end up with a lastIdx that
              // is beyond that end epoch.
              if (si.isLiveAt(lep + 1)) {
                if (logger.isDebugEnabled) {
                  logger.debug(s"DataNode: continuing empty queue at $lep.")
                }

                ep = lep
                qs.clear()
              } else if (logger.isDebugEnabled) {
                logger.debug(s"DataNode: ending empty queue at $lep (ep=$ep).")
              }
            }
          case e =>
            val vep = e.value.epoch
            if (vep <= ep) {
              if (si.isLiveAt(vep)) {
                if (vep < ep) {
                  ep = vep
                  qs.clear()
                }

                qs += q
              } else {
                // discard it; it's one of empty batches that LogNode emitted
                // after the segment was closed. We'll eventually drain the
                // queue, although it's more likely it'll get removed through
                // updateSegments before that happens.
                if (logger.isDebugEnabled) {
                  logger.debug(
                    s"DataNode: dropping $e (vep=$vep, ep=$ep). Empty batch?")
                }

                q.poll
              }
            } else if (logger.isDebugEnabled) {
              logger.debug(s"DataNode: dropping $e (vep=$vep, ep=$ep).")
            }
        }
      }

      val result = qs.result()
      if (result.isEmpty) {
        None
      } else {
        val txns = Vector.newBuilder[Txn]
        var offset = 0

        result foreach { q =>
          val b = q.poll.value

          b.txns foreach { t =>
            val pos = t.pos + offset

            if (pos < maxTransactionsPerEpoch) {
              txns += t.copy(pos = t.pos + offset)
            } else {
              logger.warn(
                s"DataNode: maximum transactions per epoch ($maxTransactionsPerEpoch) exceeded! " +
                  s"Dropping $t.")
            }
          }

          offset += b.count
        }

        val batch = Batch(ep, ScopeID.RootID, txns.result(), offset)

        if (logger.isDebugEnabled) {
          logger.debug(s"DataNode: Dequeued batch $batch.")
        }

        Some(batch)
      }
    }
  }
}
