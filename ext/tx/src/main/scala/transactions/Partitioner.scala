package fauna.tx.transaction

import fauna.atoms._

trait KeyExtractor[K, R, W] {
  def readKey(expr: R): K
  def readKeysSize(expr: W): Int
  def readKeysIterator(expr: W): Iterator[K]
  def writeKeys(expr: W): Iterable[K]
}

trait KeyLocator[K, R] {

  /** Returns the position of a key within the ring.
    */
  def locate(key: K): Location
}

trait PartitionerProvider[R, W] {
  def partitioner: Partitioner[R, W]
}

/** A transaction partitioner matches keys in read/write expressions
  * with the hosts in the cluster topology responsible for replicating
  * those keys.
  */
trait Partitioner[R, W] {

  /** A monotonically increasing value which tracks changes to the
    * cluster's topology over time.
    */
  val version: Long

  /** Returns an ordered list of discrete sub-segments of seg, each with their
    * set of replicas.
    */
  def replicas(seg: Segment): Seq[(Segment, Set[HostID])]

  /** Returns an ordered list of all Segments belonging to a specific
    * host. If called for each host in the cluster, the results will
    * cover each token in the ring once per replica. Can be empty if
    * the host is unknown. Optionally includes pending topology
    * changes.
    */
  def segments(host: HostID, pending: Boolean): Seq[Segment]

  /** Returns an ordered list of all Segments belonging to all
    * hosts in the specified replica. Can be empty if the replica
    * is unknown.
    */
  def segmentsInReplica(replica: String): Map[HostID, Vector[Segment]]

  /** Returns an ordered list of Segments belonging primarily to a
    * specific host. If called for each host in the cluster, the
    * results will cover each token only once. Can be empty if the
    * host is unknown. Optionally includes pending topology changes.
    */
  def primarySegments(host: HostID, pending: Boolean): Seq[Segment]

  /** Returns true if the specified host is a replica for the specified segment.
    */
  def isReplicaForSegment(seg: Segment, host: HostID): Boolean

  /** Returns true if the specified host is a replica for the specified location.
    */
  def isReplicaForLocation(loc: Location, host: HostID, pending: Boolean): Boolean

  /** Returns true if this host is replicating the segment of scope's token
    * ring containing the key in the read expression.
    *
    * A host is replicating a segment for a read key if it is in the
    * current ring.
    */
  def coversRead(read: R): Boolean

  /** Returns true if host is replicating the segment of scope's
    * ring containing any keys in the write expression.
    *
    * A host is replicating a segment for a read key if it is in the
    * current ring.
    *
    * A host is replicating a segment for a write key if it is in
    * _either_ the current or pending ring.
    */
  def coversTxn(host: HostID, expr: W): Boolean

  /** Returns the set of hosts replicating the key in the read
    * expression.
    *
    * The resulting set must not include pending hosts.
    */
  def hostsForRead(read: R): Set[HostID]

  /** Returns the set of hosts replicating all keys in the write
    * expression.
    *
    * The resulting set must include pending hosts.
    */
  def hostsForWrite(write: W): Set[HostID]

  /** Returns true if every write in the expression is covered by the
    * set of hosts in scope's current ring.
    */
  def txnCovered(hosts: Set[HostID], expr: W): Boolean
}
