package fauna.fuzz

import fauna.atoms.HostID
import fauna.exec.Timer
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.{ TimeBound, Timestamp, Timing, Logger }
import fauna.net.bus.{ HandlerID, MessageBus, MessageBusTestContext, SignalID }
import fauna.net.util.ByteBufStream
import fauna.net.{ Heartbeat, HostDest, HostInfo }
import fauna.stats.StatsRecorder
import fauna.tx.consensus.MessageBusTransport
import fauna.tx.transaction.Batch.Txn
import fauna.tx.transaction.{ BatchLog, Epoch, SegmentID }
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import java.io.{ PrintWriter, StringWriter }
import java.nio.file.{ Files, Path }
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicInteger }
import java.util.concurrent.{ LinkedBlockingQueue, ThreadLocalRandom }
import org.apache.commons.cli.{ DefaultParser, HelpFormatter, Options }
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

import BatchLogFuzzer.Config

/**
  * BatchLogFuzzer
  * 
  * Instantiates a 3 member log partition and drives a workload of transaction
  * submissions of randomized sizes. Network partitions and general
  * unreliability is simulated by wrapping the passed MessageBus with a shim.
  */
object BatchLogFuzzer {

  final case class Config(
    port: Int,
    tickDuration: FiniteDuration,
    idxCommitWindow: Int,
    cachedBatches: Int,
    heartbeatPeriod: FiniteDuration,
    maxPipelinedHeartbeats: Int,
    truncateInterval: FiniteDuration,
    truncateRetention: FiniteDuration,
    batchSizeRange: Range,
    entrySizeRange: Range,
    stallCheckInterval: FiniteDuration,
    stallWindow: FiniteDuration,
    maxConsecutiveStalls: Int,
  )

  final def main(args: Array[String]): Unit = {
    // By default, log at INFO level to stdout.
    System.setProperty("org.apache.logging.log4j.level", "INFO")
    val logger = getLogger

    val cli = {
      val opts = new Options

      opts.addOption("t", "test", true, "Fuzz test scenario to run.")
      opts.addOption("D", "dir", true, "Path to working directory.")
      opts.addOption("d", "duration", true, "Maximum test runtime, after which it exits successfully. Use ms, s, m, h, or d as a time unit suffix.")
      opts.addOption("h", "help", false, "Print this message.")

      val cli = (new DefaultParser).parse(opts, args)

      if (cli.hasOption("help")) {
        val sw = new StringWriter
        (new HelpFormatter).printOptions(new PrintWriter(sw), 80, opts, 2, 2)
        System.err.println("usage: faunadb [OPTIONS]\n")
        System.err.println(sw.toString)
        System.exit(0)
      }

      cli
    }

    val dir = Option(cli.getOptionValue("dir")) match {
      case Some(str) =>
        val d = Path.of(str)
        if (!Files.isDirectory(d)) {
          logger.error(s"$d is not a directory.")
          System.exit(2)
        }
        d
      case None =>
        val d = Files.createTempDirectory("BatchLogFuzzer")
        Runtime.getRuntime.addShutdownHook(new Thread({ () =>
          d.deleteRecursively()
        }))
        d
    }

    // Set a timer to shut down if specified.
    Option(cli.getOptionValue("duration")) foreach { s =>
      val duration = Duration(s)
      logger.info(s"Test scheduled with a maximum runtime of ${duration.toCoarsest}")
      Timer.Global.scheduleTimeout(duration) {
        logger.info(s"TEST RUN COMPLETE. Reached maximum runtime of ${duration.toCoarsest}.")
        System.exit(0)
      }
    }

    // FIXME: wire up with stats in CI?
    val config = Config(
      port = 7200,
      tickDuration = 200.millis,
      idxCommitWindow = 6000,
      cachedBatches = 500,            // See fauna.tx.transaction.PipelineConfig.
      heartbeatPeriod = 10.millis,    // See fauna.tx.transaction.PipelineConfig.
      maxPipelinedHeartbeats = 200,   // See fauna.tx.transaction.LogNode. Equals derived MaxPipelinedHeartbeats based on RTT of 1s.
      truncateInterval = 30.seconds,  // Next two items simulate quick and dirty truncation simulation.
      truncateRetention = 60.seconds,
      batchSizeRange = 0 to 100,      // Next two items result in max log entry size in bytes of 0 to 1MB.
      entrySizeRange = 1 to 10000,
      stallCheckInterval = 5.seconds,
      stallWindow = 30.seconds,       // Mirrors stall detection heuristic in fauna.tx.transaction.LogNode.
      maxConsecutiveStalls = 10,      // This is very generous compared to production, which triggers a restart after one equivalent stall.
    )

    val clock = Clock
    val stats = StatsRecorder.Null

    def tooShortTicks = new BatchLogFuzzer(config, dir, clock, logger, stats) {
      def test() = {
        // The max RTT is (90ms + 60ms) * 2 = 300ms, compared to tick time of 200ms
        // This would indicate that our tick time is configured to be perhaps too "aggressive".
        shims foreach { s =>
          s.inboundMessageDropRate = 0
          s.inboundMessageDelay = 90
          s.inboundMessageDelayJitter = 60
        }

        // Every scenario must sleep in order to not spin the main thread.
        Thread.sleep(10000)
      }
    }

    def networkStall = new BatchLogFuzzer(config, dir, clock, logger, stats) {
      def test() = {
        // Initial state.
        // The max RTT is (50ms + 45ms) * 2 = 190ms compared to tick time of 200ms.
        shims foreach { s =>
          s.inboundMessageDropRate = 0
          s.inboundMessageDelay = 50
          s.inboundMessageDelayJitter = 45
        }

        Thread.sleep(20000)

        logger.info("SLOWING NETWORK")

        // Slow state.
        // Add large spurious stream stalls which have a high likelihood of disrupting the ring.
        shims foreach { s =>
          s.inboundMessageDropRate = 0
          s.inboundMessageDelay = 50
          s.inboundMessageDelayJitter = 2500
        }

        Thread.sleep(10000)

        logger.info("RESETTING NETWORK")
      }
    }

    def badPeer = new BatchLogFuzzer(config, dir, clock, logger, stats) {
      def test() = {
        // Node 1 misbehaves periodically, dropping or delaying inbound or outbound messages.
        // NB: With how the test initializes itself, Node 1 will be the leader of term 1, so expect
        //     exactly one election for a test run longer than a couple of minutes.
        // NB: If generalizing to a group with n nodes, allow for multiple bad nodes but keep a quorum healthy.
        val troubleDuration = 10.seconds
        val tranquilityDuration = 10.second
        val (tranquilDelay, tranquilJitter) = (90.millis, 60.millis)

        val direction = ThreadLocalRandom.current.choose(0 to 2)
        val directionMsg = direction match {
          case 0 => "inbound"
          case 1 => "outbound"
          case 2 => "inbound and outbound"
        }

        logger.info(s"BAD PEER: for the next ${troubleDuration.toCoarsest}, dropping $directionMsg messages for ${log1.log.self}")
        direction match {
          case 0 =>
            shim1.inboundMessageDropRate = 1.0
          case 1 =>
            shim1.outboundMessageDropRate = 1.0
          case 2 =>
            shim1.inboundMessageDropRate = 1.0
            shim1.outboundMessageDropRate = 1.0
        }

        Thread.sleep(troubleDuration.toMillis)

        logger.info(s"GOOD PEER: ${log1.log.self} going back to normal for ${tranquilityDuration.toCoarsest}")
        shim1.inboundMessageDropRate = 0.0
        shim1.outboundMessageDropRate = 0.0
        shim1.inboundMessageDelay = tranquilDelay.toMillis.toInt
        shim1.inboundMessageDelayJitter = tranquilJitter.toMillis.toInt

        Thread.sleep(tranquilityDuration.toMillis)
      }
    }

    def partitions = new BatchLogFuzzer(config, dir, clock, logger, stats) {
      def test() = {
        // The nodes are periodically randomly bipartitioned. This scenario will cause
        // lots of elections and election failures.

        // Add significant delay and jitter so that calls for elections, vote requests,
        // etc. have a shot at being sent before a partition happened but being received after.
        val (delay, jitter) = (400.millis, 200.millis)
        shims.foreach { shim =>
          shim.inboundMessageDelay = delay.toMillis.toInt
          shim.inboundMessageDelayJitter = jitter.toMillis.toInt
        }

        // Make partitions long enough that elections happen when the leader is partitioned.
        val partitionPeriod = 3000.millis
        Thread.sleep(partitionPeriod.toMillis)
        ThreadLocalRandom.current().nextLong(4) match {
          case 0 => // No partition
            logger.info("REPARTITION: no partition: all nodes connected")
            shims foreach { shim =>
              shim.inboundMessageDropRate = 0.0
              shim.outboundMessageDropRate = 0.0
            }
          case i => // Partition peer i.
            logger.info(s"REPARTITION: node $i is cut off from the other peers")
            shims.zipWithIndex foreach { case (shim, j) =>
              if (j + 1 == i) {
                shim.inboundMessageDropRate = 1.0
                shim.outboundMessageDropRate = 1.0
              } else {
                shim.inboundMessageDropRate = 0.0
                shim.outboundMessageDropRate = 0.0
              }
            }
        }
      }
    }

    def chaos = new BatchLogFuzzer(config, dir, clock, logger, stats) {
      def test() = {
        // The network is unreliable. Some messages arrive promptly, others are delayed for
        // a long time. Most of the time, some messages are dropped, but some of the time
        // no messages get through and some of the time all messages get through.

        val tlr = ThreadLocalRandom.current()
        val chaosPeriodMillis = 500 + tlr.nextInt(1000)
        Thread.sleep(chaosPeriodMillis)

        def dropRate(): Double =
          tlr.nextDouble match {
            case r if r < 0.40 => 0.0
            case r if r < 0.80 => r / 2
            case _ => 1.0
          }
        val (delay, jitter) = (75.millis, 75.millis)
        val rates = shims map { shim =>
          val r = dropRate()
          shim.inboundMessageDropRate = r

          shim.inboundMessageDelay = delay.toMillis.toInt
          shim.inboundMessageDelayJitter = jitter.toMillis.toInt
          f"$r%1.2f"
        }

        logger.info(s"CHAOS: drop rates = ${rates.mkString(", ")}")
      }
    }

    val fuzzer = Option(cli.getOptionValue("test")) match {
      case Some("too-short-ticks") => Some(tooShortTicks)
      case Some("network-stall") => Some(networkStall)
      case Some("bad-peer") => Some(badPeer)
      case Some("partitions") => Some(partitions)
      case Some("chaos") => Some(chaos)

      case Some(scen) =>
        logger.error(s"No scenario '$scen' exists")
        System.exit(3)
        None

      case _ =>
        logger.error("No scenario chosen")
        System.exit(2)
        None
    }

    logger.info("Starting BatchLogFuzzer...")
    fuzzer foreach { _.run() }
    System.exit(1)
  }
}

abstract class BatchLogFuzzer(config: Config, dir: Path, clock: Clock, logger: Logger, stats: StatsRecorder) {

  def test(): Unit

  implicit val ec = ExecutionContext.Implicits.global

  @volatile var failed = false

  val builder = MessageBusTestContext.default
  if (!System.getProperty("os.name").contains("Mac OS")) {
    // Mac OS doesn't support different local addresses, at least not out-of-the-box.
    builder.withUniqueIPs.withFixedPort(7200)
  }
  val busctx = builder.build()

  val (bus1 @ _, shim1, log1) = newInstance()
  val (bus2 @ _, shim2, log2) = newInstance()
  val (bus3 @ _, shim3, log3) = newInstance()

  log1.log.init()
  log2.log.join(log1.log.self)
  log3.log.join(log1.log.self)

  val shims = Array(shim1, shim2, shim3)

  def run() = {
    while (!failed) {
      test()
    }

    for ((l, i) <- Seq(log1, log2, log3).zipWithIndex) {
      logger.error(s"Log $i: ${l.log}")
    }
  }

  def newInstance() = {
    var shim: FuzzerDispatcherShim = null
    val bus = busctx.createBus()
    bus.shimDispatcher { d =>
      shim = new FuzzerDispatcherShim(d)
      shim
    }

    val hostID = bus.hostID
    val id = hostID.uuid.getLeastSignificantBits
    val name = s"BatchLog$id"
    val transport = new MessageBusTransport(name, bus, SignalID(1), 1024, statsRecorder = stats, statsPrefix = name, dedicatedBusStream = true)
    val replog = BatchLog.Log.open(transport, hostID, dir / id.toString, name, config.tickDuration, stats, name, txnLogBackupPath = None)
    val batchlog = new BatchLog(SegmentID(1), replog, config.idxCommitWindow, config.cachedBatches)

    val pendingBeats = new AtomicInteger
    @volatile var lastTruncate = Timestamp.Epoch

    var stallIdx = Epoch.MinValue
    var consecutiveStalls = 0

    Heartbeat(config.stallCheckInterval, s"$name-Stall-Check") {
      val ts = clock.time
      val idx = batchlog.lastIdx

      logger.info(s"LOG DELAY: ${ts.millis - idx.ceilTimestamp.millis}ms behind clock.")

      if (ts > (idx.ceilTimestamp + config.stallWindow)) {
        if (stallIdx == idx) {
          consecutiveStalls += 1
        } else {
          consecutiveStalls = 1
        }

        stallIdx = idx

        logger.error(s"DETECTED COMMIT STATE STALL. batches pending ${pendingBeats.get}. stall #$consecutiveStalls at idx is $idx.")

        if (consecutiveStalls > config.maxConsecutiveStalls) {
          logger.error(s"FUZZ TEST FAILURE after $consecutiveStalls consecutive stalls")
          failed = true
        }

        batchlog.log.logRoleAndState()
      }
    }

    Heartbeat(config.heartbeatPeriod, name) {
      val ts = clock.time
      if (pendingBeats.get >= config.maxPipelinedHeartbeats) {
          logger.error(s"PENDING ENQUEUE STALL ${batchlog.lastIdx}")
          Thread.sleep(1000)
      } else {
        // This batch submission pipeline would be more accurate if we simulated
        // the Coordinator's log partition leader discovery mechanism.
        if (batchlog.isLeader) {
          val epoch = Epoch(ts)
          val batch = generateBatch()
          commitBatch(batchlog, pendingBeats, epoch, batch)
        }
      }

      if (ts > (lastTruncate + config.truncateInterval)) {
        lastTruncate = ts
        val ep = Epoch(clock.time - config.truncateRetention)
        val last = batchlog.lastIdx
        if (ep <= last) {
          val timing = Timing.start
          val prev = batchlog.prevIdx
          batchlog.truncate(ep)
          val newPrev = batchlog.prevIdx
          if (newPrev != prev) {
            logger.info(s"Truncated log file from $prev to $newPrev (in ${timing.elapsedMillis}ms)")
          }
        }
      }

    }

    (bus, shim, batchlog)
  }

  def generateBatch(): Vector[Txn] = 
    // FIXME: This could use TLR if it supported our random syntax.
    (0 until ThreadLocalRandom.current.choose(config.batchSizeRange)).iterator
      .map(_ => generateTxn())
      .toVector
  
  def generateTxn(): Txn = {
    val expr = new Array[Byte](ThreadLocalRandom.current.choose(config.entrySizeRange))
    val handler = SignalID(1000).at(HostID.NullID)
    Txn(expr, handler, clock.time + 30.seconds)
  }

  def commitBatch(log: BatchLog, pendingBeats: AtomicInteger, epoch: Epoch, txns: Vector[Txn]) = {
    pendingBeats.incrementAndGet()
    log.add(epoch, txns) ensure {
      pendingBeats.decrementAndGet()
    }
  }
}

class FuzzerDispatcherShim(dispatcher: MessageBus.Dispatcher) extends MessageBus.Dispatcher {
  @volatile var inboundMessageDropRate = 0.0
  @volatile var inboundMessageDelay = 0
  @volatile var inboundMessageDelayJitter = 0

  // TODO: Consider implementing outbound message delay and jitter.
  @volatile var outboundMessageDropRate = 0.0

  private val inboundQueue = new LinkedBlockingQueue[(TimeBound, HostInfo, SignalID, ByteBufStream, TimeBound)]
  private val inDelivery = new AtomicBoolean(false) 

  @annotation.tailrec
  private def deliver(): Unit = 
    if (inDelivery.compareAndSet(false, true)) {
      var ev = inboundQueue.poll()

      while (ev ne null) {
        val (deliverAt, f, s, b, d) = ev

        if (deliverAt.hasTimeLeft) {
          Timer.Global.scheduleTimeout(deliverAt.timeLeft) {
            implicit val ec = ExecutionContext.Implicits.global
            Future {
              dispatcher.recvMessage(f, s, b, d)
              inDelivery.set(false)
              deliver1()
            }
          }

          // break the delivery loop here, since the current message stalls the queue.
          return ()
        }

        dispatcher.recvMessage(f, s, b, d)
        ev = inboundQueue.poll()
      }

      inDelivery.set(false)
      deliver()
    }

  private def deliver1() = deliver()

  def recvMessage(from: HostInfo, signalID: SignalID, bytes: ByteBufStream, deadline: TimeBound): Unit = {
    val rnd = ThreadLocalRandom.current
    if (rnd.nextDouble < inboundMessageDropRate) {
      getLogger.trace(s"Dropping inbound message from $from bound for $signalID")
      bytes.close()
    } else {
      val delay = Math.max(0, inboundMessageDelay + rnd.nextInt(inboundMessageDelayJitter + 1))
      inboundQueue.add((delay.millis.bound, from, signalID, bytes, deadline))
      deliver()
    }
  }

  def recvTCPStream(from: HostInfo, signalID: SignalID, ch: Channel): Unit = {
    getLogger.warn("Shimming for TCP streams is not implemented yet.")
    dispatcher.recvTCPStream(from, signalID, ch)
  }

  def sendBytes(signalID: SignalID, to: HostDest, message: ByteBuf, deadline: TimeBound, reason: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    if (ThreadLocalRandom.current.nextDouble < outboundMessageDropRate) {
      getLogger.trace(s"Dropping outbound message to $to bound for $signalID")
      message.release()
      FutureTrue
    } else {
      dispatcher.sendBytes(signalID, to, message, deadline, reason)
    }
  }

  def openTCPStreamWithConfig(dest: HandlerID, reason: String)(configure: Channel => Unit)(implicit ec: ExecutionContext): Future[Channel] = {
    getLogger.warn("Shimming for TCP streams is not implemented yet.")
    dispatcher.openTCPStreamWithConfig(dest, reason)(configure)
  }
}
