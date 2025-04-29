package fauna.storage

import fauna.atoms.{ HostID, Location, Segment }
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.prop.api.CoreLauncher
import fauna.prop.test.PropSpec
import fauna.scheduler.IOScheduler
import fauna.stats.StatsRecorder
import fauna.storage.cassandra.{ CassandraKeyLocator, CassandraStorageEngine }
import fauna.storage.ops.Write
import fauna.tx.transaction.Partitioner
import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.Collections
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import org.apache.cassandra.config._
import org.apache.cassandra.db._
import org.apache.cassandra.db.commitlog.CommitLog
import org.apache.cassandra.db.composites.{ CellName, CellNames }
import org.apache.cassandra.gms.Gossiper
import org.apache.cassandra.locator.SimpleStrategy
import org.apache.cassandra.service.{ MigrationManager, StorageService }
import org.apache.cassandra.utils.ByteBufferUtil
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration._

package test {

  abstract class Spec(val keyspaceName: String = "storageks")
      extends PropSpec(10, 100)
      with BeforeAndAfterAll
      with BeforeAndAfterEach
      with Matchers {

    val config = CassandraConfig(
      cluster = "test",
      regionGroup = "local",
      environment = "dev",
      rootPath = CoreLauncher.rootDir.toString,
      busPort = 7500,
      busSSLPort = 7501,
      busConnectTimeout = 5.seconds,
      busHandshakeTimeout = 5.seconds,
      busIdleTimeout = 5.seconds,
      busKeepaliveInterval = 5.seconds,
      busKeepaliveTimeout = 10.seconds,
      connectionsPerHost = 1,
      busMaxMessageSize = Int.MaxValue,
      busMaxPendingMessages = 1024,
      broadcastAddress = InetAddress.getLocalHost.getHostAddress,
      listenAddress = InetAddress.getLocalHost.getHostAddress,
      keyCacheSizeMB = 0,
      enableQoS = true,
      processors = 4,
      concurrentReads = 128,
      concurrentTxnReads = 16,
      concurrentWrites = 128,
      concurrentCompactors = 1,
      compactionThroughputMBPerSec = 0,
      streamTimeout = Duration(30, SECONDS),
      indexSummaryInterval = 1.hour,
      indexSummarySizeMB = 1,
      memtableSizeMB = 4,
      memtableFlushWriters = 1,
      commitLogSizeMB = 1,
      concurrentApplyingTransactions = 128,
      roundTripTime = 100.millis,
      syncPeriod = 1000.millis,
      syncOnShutdown = false,
      backupReadRatio = 0.1,
      minBackupReadDelay = 10.millis,
      readTimeout = 5.seconds,
      newReadTimeout = 5.seconds,
      repairHashDepth = 8,
      repairSyncParallelism = 16,
      ackDelayPercentile = 99.99,
      txnLogBackupPath = None,
      transactionLogBufferSize = 512 * 1024,
      backupDir = CoreLauncher.rootDir.toString + File.separator + "snapshots",
      dualWriteIndexCFs = false,
      index2CFValidationRatio = 0.0,
      consensusStallRestartPeriod = Duration.Inf,
      minWorkerID = 0,
      maxWorkerID = 1023,
      neighborReplica = None,
      enableDataTransferDigests = true,
      enableSnapshotTransferDigests = true,
      streamMaxOpenStreams = 10_000,
      openFileReserve = 1.0,
      unleveledSSTableLimit = Int.MaxValue,
      transferChunkSize = Int.MaxValue,
      recvMaxBytesPerSecond = Double.MaxValue,
      recvMaxBurstSeconds = Double.MaxValue,
      streamPlanThreads = 1,
      latWaitThreshold = 1.second,
      maxOCCReadsPerSecond = Int.MaxValue,
      occReadsBackoffThreshold = Int.MaxValue,
      limiterGossipInterval = 1.second,
      sstableDiskAccessMode = "auto"
    )

    CassandraConfig.writeConfiguration(config)

    System.setProperty("cassandra.config", s"file://${config.internalConfFilePath}")
    System.setProperty("cassandra.join_ring", "true")
    System.setProperty("cassandra.start_rpc", "false")
    System.setProperty("cassandra.start_native_transport", "false")

    def decoratedKey(key: String) =
      StorageService.getPartitioner.decorateKey(ByteBufferUtil.bytes(key))

    def cellname(bbs: Seq[ByteBuffer]): CellName =
      if (bbs.length == 1) {
        CellNames.simpleDense(bbs(0))
      } else {
        CellNames.compositeDense(bbs: _*)
      }

    def cellname(str: String, strs: String*): CellName = {
      val bbs = new Array[ByteBuffer](strs.length + 1)

      bbs(0) = ByteBufferUtil.bytes(str)

      (0 until strs.length) foreach { i =>
        bbs(i + 1) = ByteBufferUtil.bytes(strs(i))
      }

      cellname(ArraySeq.unsafeWrapArray(bbs))
    }

    override def beforeAll() = {
      if (Schema.instance.getKSMetaData(keyspaceName) eq null) {
        unregisterMBeans()
      }

      CommitLog.instance.allocator.enableReserveSegmentCreation()
      CommitLog.instance.resetUnsafe()
      Keyspace.setInitialized()

      Gossiper.instance.start((Clock.time.millis / 1000).toInt)

      if (Schema.instance.getKSMetaData(keyspaceName) eq null) {
        val md = KSMetaData.newKeyspace(
          keyspaceName,
          classOf[SimpleStrategy],
          Collections.singletonMap("replication_factor", "1"),
          true,
          Collections.emptyList())

        MigrationManager.announceNewKeyspace(md, true)
      }
    }

    private def unregisterMBeans(): Unit = {
      val mbs = ManagementFactory.getPlatformMBeanServer

      def unregister(name: ObjectName) = {
        if (mbs.isRegistered(name)) {
          mbs.unregisterMBean(name)
        }
      }

      mbs
        .queryNames(new ObjectName("org.apache.cassandra.db:type=*"), null)
        .forEach { unregister(_) }
      mbs
        .queryNames(
          new ObjectName("org.apache.cassandra.db:type=*,keyspace=*,columnfamily=*"),
          null)
        .forEach { unregister(_) }
      mbs
        .queryNames(new ObjectName("org.apache.cassandra.net:type=*"), null)
        .forEach { unregister(_) }
      mbs
        .queryNames(new ObjectName("org.apache.cassandra.internal:type=*"), null)
        .forEach { unregister(_) }
      mbs
        .queryNames(new ObjectName("org.apache.cassandra.metrics:type=*"), null)
        .forEach { unregister(_) }
      mbs
        .queryNames(new ObjectName("org.apache.cassandra.service:type=*"), null)
        .forEach { unregister(_) }
    }

    object FakeScheduler extends IOScheduler("Fake", 1, enableQoS = true)

    @annotation.nowarn("cat=unused-params")
    object NullPartitioner extends Partitioner[TxnRead, Txn] {
      val version: Long = 0

      def replicas(location: Location, pending: Boolean): Set[HostID] = Set.empty
      def replicas(seg: Segment) = Seq.empty
      def segments(host: HostID, pending: Boolean) = Seq.empty
      def segmentsInReplica(replica: String): Map[HostID, Vector[Segment]] = Map.empty
      def primarySegments(host: HostID, pending: Boolean) = Seq.empty
      def isReplicaForSegment(seg: Segment, host: HostID) = true
      def isReplicaForLocation(loc: Location, host: HostID, pending: Boolean) = true
      def coversRead(read: TxnRead): Boolean = true
      def coversTxn(host: HostID, expr: Txn): Boolean = true
      def hostsForRead(read: TxnRead) = Set.empty
      def hostsForWrite(write: (Map[TxnRead, Timestamp], Vector[Write])) = Set.empty
      def txnCovered(hosts: Set[HostID], expr: (Map[TxnRead, Timestamp], Vector[Write])) = true
    }

    def nameBuf(i: Int) = ByteBufferUtil.bytes(f"${i}%05d")

    val defaultRoot = Option(System.getenv("FAUNA_TEST_ROOT"))
    val baseDir = System.getProperty("os.name") match {
      case "Linux" => defaultRoot getOrElse "/dev/shm"
      case "Mac OS X" => defaultRoot getOrElse "/Volumes"
      case x if x.startsWith("Windows") => defaultRoot getOrElse System.getProperty("java.io.tmpdir")
      case _ => defaultRoot getOrElse System.getProperty("java.io.tmpdir")
    }

    def withStorageEngine[A](stats: StatsRecorder)(
      f: CassandraStorageEngine => A): A = {
      val engine =
        new CassandraStorageEngine(
          StorageEngine.Config(
            self = HostID.NullID,
            rootPath = CoreLauncher.rootDir,
            locator = CassandraKeyLocator,
            partitioner = { () => NullPartitioner },
            concurrentReads = Int.MaxValue,
            concurrentTxnReads = Int.MaxValue,
            readScheduler = FakeScheduler,
            syncPeriod = Int.MaxValue.nanoseconds,
            stats = StorageEngine.Stats(stats),
            syncOnShutdown = false,
            dualWriteIndexCFs = false,
            openFileReserve = 1.0,
            unleveledSSTableLimit = Int.MaxValue,
            latWaitThreshold = 1.second
          ),
          keyspaceName)

      try f(engine)
      finally engine.close()
    }
  }

}
