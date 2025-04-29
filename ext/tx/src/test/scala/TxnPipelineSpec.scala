package fauna.tx.test

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.{ TimeBound, Timestamp }
import fauna.lang.clocks.{ Clock, WeightedDelayTrackingClock }
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.net.HostService
import fauna.tx.transaction._
import fauna.tx.transaction.Coordinator.Leadership
import fauna.tx.transaction.RevalidatorRole.MainRevalidator
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.nio.file.Path
import java.util.concurrent.{ ConcurrentHashMap, Semaphore, TimeoutException }
import java.util.TreeMap
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.math.abs
import scala.util.hashing.MurmurHash3
import scala.util.Random

class TxnPipelineSpec extends Spec {

  type Expr = (Map[ByteBuf, Timestamp], Map[ByteBuf, ByteBuf])

  val ec = global

  var dir: Path = _

  before {
    dir = aTestDir()
  }

  after {
    dir.deleteRecursively()
  }

  val replicaCount = 3
  val partitionCount = 2
  val nodeCount = replicaCount * partitionCount

  val ids = (1 to nodeCount map { _ => HostID.randomID }).toList
  val replicas = (ids grouped partitionCount).toList
  val partitions = (0 until partitionCount map { i => replicas map { _(i) } }).toList
  val logNodeProvider = new LogNodeProvider {
    var callbacks = List.empty[() => Future[Boolean]]
    def subscribeWithLogging(cb: => Future[Boolean]): Future[Unit] = {
      synchronized {
        callbacks = callbacks :+ { () => cb }
      }
      cb.unit
    }

    case class SimpleLogNodeInfo(
      topology: Map[SegmentID, SegmentInfo],
      validUntil: Epoch,
      version: Long)
        extends LogNodeInfo

    @volatile var logNodeInfo = SimpleLogNodeInfo(
      partitions.zipWithIndex map { case (p, i) =>
        SegmentID(i) -> SegmentInfo(
          p.toVector,
          Uninitialized,
          SegmentInfo.FirstInitRound)
      } toMap,
      Epoch.MinValue,
      0
    )

    def getLogNodeInfo = logNodeInfo

    def removeSegments(segs: Iterable[SegmentID], currVersion: Long) = Future.unit

    def revalidate(validUntil: Epoch, currVersion: Long) = synchronized {
      if (currVersion == logNodeInfo.version) {
        updateState(
          logNodeInfo
            .copy(validUntil = validUntil, version = logNodeInfo.version + 1))
      }
      Future.unit
    }

    def startSegment(seg: SegmentID, start: Epoch, currVersion: Long) =
      synchronized {
        if (currVersion == logNodeInfo.version) {
          val t = logNodeInfo.topology
          updateState(
            logNodeInfo.copy(topology =
              t.updated(seg, t(seg).copy(state = Started(start)))))
        }
        Future.unit
      }

    override def getRevalidatorRole = MainRevalidator

    private def updateState(newState: SimpleLogNodeInfo) = {
      logNodeInfo = newState
      val cbs = synchronized { callbacks }
      cbs foreach { cb => cb.apply() }
    }

    override def closeSegment(seg: SegmentID, currVersion: Long): Epoch = ???

    override def moveSegment(
      seg: SegmentID,
      from: HostID,
      to: HostID,
      currVersion: Long): Epoch = ???
  }

  val ports = ids map { _ => findFreePort() }
  val buses = ids zip ports map { case (id, p) =>
    MessageBus("txn", id, "localhost", port = p)
  }

  val conf = PipelineConfig(
    SignalID(1),
    SignalID(2),
    SignalID(3),
    null,
    512 * 1024,
    1024,
    None)

  buses foreach { b =>
    buses foreach { b2 => b.registerHostID(b2.hostID, b2.hostAddress) }
    b.start()
  }

  class TestHostService(id: HostID) extends HostService {
    private val local = (replicas find { _ contains id }).get.toSet

    def isLive(host: HostID) = true
    def isLocal(host: HostID) = local contains host
    def isNear(host: HostID) = false
    def subscribeStartsAndRestarts(f: HostID => Unit): Unit = ()
  }

  implicit val TestCodec: CBOR.Codec[Expr] = {
    def to(t: (Vector[(ByteBuf, Timestamp)], Vector[(ByteBuf, ByteBuf)])) =
      (t._1.toMap, t._2.toMap)
    def from(t: Expr) = (t._1.toVector, t._2.toVector)
    CBOR
      .AliasCodec[Expr, (Vector[(ByteBuf, Timestamp)], Vector[(ByteBuf, ByteBuf)])](
        to,
        from)
  }

  class TestPartitioner(host: HostID) extends Partitioner[ByteBuf, Expr] {
    val version = 0

    private val segs = partitions.zipWithIndex map { case (p, i) =>
      Segment(Location(i), Location(i + 1)) -> p.toSet
    } toMap

    private def getPart(key: ByteBuf) = {
      val loc = abs(MurmurHash3.bytesHash(key.toByteArray)) % partitionCount
      segs(Segment(Location(loc), Location(loc + 1)))
    }

    def hostsForRead(key: ByteBuf) =
      getPart(key)

    def hostsForWrite(expr: Expr) = {
      val b = Set.newBuilder[HostID]
      val (reads, writes) = expr
      reads.keys foreach { k => b ++= getPart(k) }
      writes.keys foreach { k => b ++= getPart(k) }
      b.result()
    }

    def coversRead(read: ByteBuf) =
      getPart(read) contains host

    def coversTxn(host: HostID, expr: Expr): Boolean = {
      val (reads, writes) = expr
      (reads.keys exists { getPart(_) contains host }) ||
      (writes.keys exists { getPart(_) contains host })
    }

    override def txnCovered(hosts: Set[HostID], expr: Expr) = {
      val (reads, writes) = expr
      (reads.keys ++ writes.keys) forall { k =>
        getPart(k) exists { hosts contains _ }
      }
    }

    // None of these are currently tested...
    def isReplicaForLocation(
      loc: Location,
      host: HostID,
      pending: Boolean): Boolean = ???
    def isReplicaForSegment(seg: Segment, host: HostID): Boolean = ???
    def primarySegments(host: HostID, pending: Boolean): Seq[Segment] = ???
    def replicas(seg: Segment): Seq[(Segment, Set[HostID])] = ???

    def segments(host: HostID, pending: Boolean): Seq[Segment] =
      segs collect {
        case (seg, ps) if ps.contains(host) => seg
      } toSeq

    def segmentsInReplica(replica: String): Map[HostID, Vector[Segment]] = ???
  }

  class TestStorage
      extends TransactionStorage[
        ByteBuf,
        ByteBuf,
        Timestamp,
        Expr,
        Option[Map[ByteBuf, Timestamp]]] {
    val data = new ConcurrentHashMap[ByteBuf, TreeMap[Timestamp, ByteBuf]]

    private def history(key: ByteBuf) =
      data.computeIfAbsent(key, { _ => new TreeMap })

    private def readEntry(ts: Timestamp, key: ByteBuf) = {
      val h = history(key)
      h.synchronized { Option(h.floorEntry(ts)) map { e => (e.getKey, e.getValue) } }
    }

    @annotation.nowarn("cat=unused-params")
    def read(scope: ScopeID, ts: Timestamp, key: ByteBuf) =
      waitForAppliedTimestamp(key, ts, 10.seconds.bound) map { _ =>
        readEntry(ts, key)
      } recover { case _: TimeoutException =>
        sys.error("timeout")
      }

    def readsForTxn(expr: Expr) = expr._1.keys.toSeq

    def evalTxnRead(ts: Timestamp, key: ByteBuf, deadline: TimeBound) =
      Future.successful(readEntry(ts, key) map { _._1 } getOrElse Timestamp.Epoch)

    def evalTxnApply(ts: Timestamp, reads: Map[ByteBuf, Timestamp], expr: Expr) = {
      val (occ, writes) = expr

      Future {
        val failed = reads filterNot { case (k, ts) => ts == occ(k) }

        if (failed.isEmpty) {
          writes foreach { case (k, v) =>
            val h = history(k)
            h.synchronized { h.put(ts, v) }
          }
          None
        } else {
          Some(failed)
        }
      }
    }

    def persistedTimestamp = Timestamp.Epoch
    def sync(syncMode: TransactionStorage.SyncMode): Unit = ()
    def lock(): Unit = ()
    def unlock(): Unit = ()
    def isUnlocked: Boolean = true
  }

  case class FixedPartitionerProvider[R, W](partitioner: Partitioner[R, W])
      extends PartitionerProvider[R, W]

  object TestKeyExtractor extends KeyExtractor[ByteBuf, ByteBuf, Expr] {
    def readKey(key: ByteBuf) = key
    def readKeysSize(expr: Expr) = expr._1.size
    def readKeysIterator(expr: Expr) = expr._1.keysIterator
    def writeKeys(expr: Expr) = expr._2.keys
  }

  object TestResetTxn extends (Expr => Option[Expr]) {
    def apply(e: Expr) = None
  }

  object EmptyDataFilter extends DataFilter[Expr] {
    def covers(expr: Expr) = true
  }

  val EmptyDataFilterCodec =
    CBOR.SumCodec[DataFilter[Expr]](CBOR.SingletonCodec(EmptyDataFilter))

  "TxnPipeline" - {
    "works" in {
      val storage = ids map { _ -> new TestStorage } toMap
      val clock = new WeightedDelayTrackingClock(Clock)
      val pipelines = buses map { b =>
        val p = TxnPipeline(
          conf.copy(rootPath = dir / b.hostAddress.port.toString),
          new TestHostService(b.hostID),
          logNodeProvider,
          ids,
          true,
          TestKeyExtractor,
          TestResetTxn,
          FixedPartitionerProvider(new TestPartitioner(b.hostID)),
          EmptyDataFilterCodec,
          storage(b.hostID),
          ec,
          b,
          clock,
          clock,
          Clock,
          0.1
        )
        p.ctx.markEpochSynced()
        p
      }

      Await.result(pipelines map { _.onStart } sequence, 10.seconds)

      val defaultPartitioner = new TestPartitioner(HostID.NullID)
      def readValue(ts: Timestamp, key: ByteBuf) = {
        val id = Random.choose(defaultPartitioner.hostsForRead(key).toSeq)
        storage(id).read(ScopeID.RootID, ts, key)
      }

      def snapTime() = Random.choose(pipelines).ctx.readClock.time

      Thread.sleep(2000)

      val entries = 1000
      val timestamps = List.newBuilder[Timestamp]
      val keys = 1 to 10 map { i => Unpooled.copyInt(i) }

      val sem = new Semaphore(512)

      val futs = 1 to entries map { _ =>
        sem.acquire()

        val f = Future.delegate {
          val snapTS = snapTime()
          val ks = 1 to 1 map { _ => Random.choose(keys) }

          (ks map { k => readValue(snapTS, k) map { k -> _ } }).sequence flatMap {
            readOpts =>
              val reads = readOpts collect { case (k, Some(v)) => k -> v } toMap
              val occ = ks map { k =>
                k -> (reads.get(k) map { _._1 } getOrElse Timestamp.Epoch)
              } toMap
              val writes = 1 to 1 map { _ =>
                Random.choose(keys) -> Unpooled.copyInt(Random.nextInt())
              } toMap
              val p = Random.choose(pipelines)

              p.coordinator.write((occ, writes), ScopeID.RootID, 1.hour.bound) map {
                case (ts, _, _) => timestamps.synchronized { timestamps += ts }
              }
          }
        }

        f onComplete { t =>
          t.failed foreach { _.printStackTrace }
          sem.release()
        }

        f
      }

      Await.result(futs.join, Duration.Inf)

      pipelines foreach { _.close(Duration.Zero) }

      // ensure all timestamps are unique
      timestamps.result().toSet.size should equal(entries)
    }

    "cleanup" in {
      Thread.sleep(1000)
      buses foreach { _.stop() }
    }

    "segmentLeaderCheck" - {

      /** Tests to ensure that when invalid SegmentIDs are returned from targeted hostID
        * then those responses are excluded from the quorum check.
        */

      val id1 = HostID.randomID
      val id2 = HostID.randomID
      val id3 = HostID.randomID

      "when all responses return valid SegmentID" in {
        val segmentID = SegmentID(1)
        val check = TxnPipeline.segmentLeaderCheck(segmentID, Set(id1, id2, id3))

        check.hasQuorum(
          Seq(
            Leadership.Leader(id1, segmentID),
            Leadership.Leader(id1, segmentID),
            Leadership.Leader(id2, segmentID)
          )
        ) should contain(id1)
      }

      "when majority return valid SegmentID" in {
        val segmentID1 = SegmentID(1)
        val segmentID2 = SegmentID(2)
        val check = TxnPipeline.segmentLeaderCheck(segmentID1, Set(id1, id2, id3))

        check.hasQuorum(
          Seq(
            Leadership.Leader(id1, segmentID1),
            Leadership.Leader(id1, segmentID1),
            // response 3 was returned by invalid SegmentID(2). But the request still
            // reaches quorum
            // since majority agree.
            Leadership.Leader(id1, segmentID2)
          )
        ) should contain(id1)
      }

      "when majority return invalid SegmentID" in {
        val segmentID1 = SegmentID(1)
        val segmentID2 = SegmentID(2)
        val check = TxnPipeline.segmentLeaderCheck(segmentID1, Set(id1, id2, id3))

        check.hasQuorum(
          Seq(
            Leadership.Leader(id1, segmentID1),
            // invalid SegmentID. SegmentID was expected.
            Leadership.Leader(id1, segmentID2),
            Leadership.Leader(id1, segmentID2)
          )
        ) shouldBe empty
      }

      "when all return invalid SegmentID" in {
        val segmentID1 = SegmentID(1)
        val segmentID2 = SegmentID(2)
        val check = TxnPipeline.segmentLeaderCheck(segmentID1, Set(id1, id2, id3))

        check.hasQuorum(
          Seq(
            // invalid SegmentID. SegmentID was expected.
            Leadership.Leader(id1, segmentID2),
            Leadership.Leader(id1, segmentID2),
            Leadership.Leader(id1, segmentID2)
          )
        ) shouldBe empty
      }

      "when empty" in {
        val segmentID1 = SegmentID(1)
        val check = TxnPipeline.segmentLeaderCheck(segmentID1, Set(id1, id2, id3))

        check.hasQuorum(Seq()) shouldBe empty
      }
    }
  }
}
