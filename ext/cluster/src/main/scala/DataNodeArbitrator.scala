package fauna.cluster

import fauna.atoms.HostID
import fauna.cluster.topology.{ ReplicaTopology, Topologies }
import fauna.cluster.workerid.WorkerIDs
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.tx.transaction.TransactionStorage
import scala.concurrent.Future

/**
  * An instance of this class primarily just determines whether this node is a
  * data node or not. It is typically used by `CassandraService` and it
  * propagates its `isDataNode` signal to its `TxnPipeline` which based on
  * whether it's true or false starts or stops its internal `DataNode`.
  * A node is considered a data node if it both has a worker ID and is in a
  * data replica.
  * This class has another important role, though. Before the first transition
  * from false to true, it will determine if it can fast-forward the LAT on
  * this node's storage to some recent timestamp. Fast-forwarding the LAT is
  * an overall cluster stability improvement; when a new node is added to a
  * replica, it needn't replay all of the transaction log on itself, as it can
  * take a long time to replay if for some reason the log couldn't be reasonably
  * truncated. If it is determined that it can safely do so, a data node can
  * instead just start replaying the log from a recent timestamp because the
  * effects of all the skipped transactions will already be present in the data
  * transferred from other nodes.
  */
final class DataNodeArbitrator(
  membership: Membership,
  workerIDs: WorkerIDs,
  topologies: Topologies,
  storage: TransactionStorage[_, _, _, _, _],
  clock: Clock = Clock
) {
  private[this] val internal = new DataNodeArbitratorInternal(
    membership.self,
    membership.replica,
    membership.hosts,
    workerIDs.workerID.idOpt.nonEmpty,
    topologies.snapshot.replicaTopologies,
    storage,
    clock
  )

  def isDataNode = internal.isDataNode

  ClusterService.subscribeWithLogging(workerIDs, topologies) {
    internal.tick() flatMap { _ =>
      FutureTrue // keep the subscription ticking
    }
  }

  // Should be invoked just before topology reconciler would announce ownership
  // of a segment. If the storage engine was never synced, sync it once now.
  // This ensures that the persisted time is later than epoch, so even if this
  // node were to crash it would not fast-forward LAT next time it starts up
  // (but it will persist a pretty recent LAT, so it won't need to FF anymore.)
  def beforeTakingOwnership(): Unit =
    if (storage.persistedTimestamp == Timestamp.Epoch) {
      storage.sync()
    }
}

// Most of actual logic lives in this class, extracted for testability.
final class DataNodeArbitratorInternal(
  self: HostID,
  replica: String,
  hosts: => Set[HostID],
  hasWorkerID: => Boolean,
  replicaTopologies: => Seq[(String, ReplicaTopology)],
  storage: TransactionStorage[_, _, _, _, _],
  clock: Clock
) {
  @volatile private[this] var _isDataNode = false

  def isDataNode = _isDataNode

  private[this] def appliedTimestamp = storage.appliedTimestamp

  def tick(): Future[Unit] =
    isDataNodeFut map { b =>
      // Once we returned true, we must no longer allow apply timestamp
      // fast-forwarding. Doing so could cause race conditions with data
      // transfer.
      if (b && !_isDataNode) {
        getLogger.info(s"Using timestamp $appliedTimestamp as the log replay timestamp.")
      }
      _isDataNode = b
    }

  private[this] def isDataNodeFut: Future[Boolean] =
    if (hasWorkerID) {
      val rts = replicaTopologies
      val myTopo = rts collectFirst {
        case (`replica`, t) => t
      }
      myTopo match {
        case None =>
          FutureFalse
        case Some(t) =>
          // Here we check if the criteria for fast-forwarding apply timestamp
          // is satisfied. We can FF the apply timestamp once at service startup
          // when:
          //
          // * this node's storage is empty (no data applied yet; note that
          //   since we haven't allowed TxnPipeline's DataNode to start yet the
          //   min applied timestamp and persisted TS are the same but using min
          //   applied timestamp here will short circuit all these evaluations
          //   as soon as we FF it), and
          //
          // * the node doesn't own any segments yet in topology "current" ring.

          def storageContainsData =
            appliedTimestamp != Timestamp.Epoch

          def firstNodeInCluster =
            hosts == Set(self)

          def nodeOwnsSomeSegments =
            t.current exists { _.host == self }

          if (isDataNode) {
            // Already a data node, don't forward LAT
            FutureTrue
          } else if (storageContainsData) {
            getLogger.info("Storage already contains data. Not fast-forwarding applied timestamp.")
            FutureTrue
          } else if (firstNodeInCluster) {
            getLogger.info("First node in cluster. Not fast-forwarding applied timestamp.")
            FutureTrue
          } else if (nodeOwnsSomeSegments) {
            // This should not happen, since we call beforeTakingOwnership()
            // to sync storage with a non-epoch timestamp before taking
            // ownership of the first segment.
            // Start data node without fast-forwarding LAT otherwise we could
            // introduce data loss in the already owned segments.
            getLogger.warn("Storage is empty, but the node already owns some segments. This is anomalous. Not fast-forwarding applied timestamp.")
            FutureTrue
          } else {
            val appliedTS = clock.time
            getLogger.info(s"Fast-forwarding applied timestamp to $appliedTS.")
            storage.updateMaxAppliedTimestamp(appliedTS)
            FutureTrue
          }
      }
    } else {
      FutureFalse
    }
}
