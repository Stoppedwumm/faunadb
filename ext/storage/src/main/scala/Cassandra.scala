package fauna.storage

import fauna.atoms._
import fauna.lang.LoggingSyntax
import java.util.{ Collections, HashSet }
import org.apache.cassandra.config.{ KSMetaData, Schema }
import org.apache.cassandra.cql3.QueryProcessor
import org.apache.cassandra.db.{ Keyspace => CKeyspace, _ }
import org.apache.cassandra.locator.{ AbstractReplicationStrategy, LocalStrategy }
import org.apache.cassandra.net.MessagingService
import org.apache.cassandra.service._
import org.apache.cassandra.utils.concurrent.SimpleCondition
import scala.collection.mutable.{ Map => MMap }
import scala.jdk.CollectionConverters._

sealed trait SetReplicaNameResult
object SetReplicaNameResult {
  case object Success extends SetReplicaNameResult
  case object InvalidReplicaName extends SetReplicaNameResult
  case class ReplicaNameAlreadySet(oldReplicaName: String) extends SetReplicaNameResult
}

object Cassandra extends LoggingSyntax {
  val KeyspaceName = "FAUNA"

  private[this] val logger = getLogger

  private[this] val replicaNameColumn = "data_center"
  private[this] val replicaNameKey = "replica_name"
  private[this] val cassandraSystem = "system.local"

  private[this] var daemon: CassandraDaemon = _
  private[this] var drainHook: Thread = null

  lazy val self: HostID =
    HostID(SystemKeyspace.getLocalHostId)

  // This terrible hack prevents C* from binding to its own messaging
  // port, and gossiping amongst itself. Essentially, this is what
  // makes Cassandra a local storage engine.
  private def disableMessagingService(): Unit = {
    val field = classOf[MessagingService].getDeclaredField("listenGate")
    field.setAccessible(true)
    val gate = field.get(MessagingService.instance).asInstanceOf[SimpleCondition]
    gate.signalAll()
  }

  /**
    * Sets the replicaName for the current node.
    * This function should not be called directly. Use [[setReplicaNameIfEmpty]] instead.
    */
  private def setReplicaName(replicaName: String): SetReplicaNameResult =
    if (ReplicaNameValidator.isValid(replicaName)) {
      val writeQuery = s"INSERT INTO $cassandraSystem (key, $replicaNameColumn) VALUES ('$replicaNameKey', '$replicaName')"
      QueryProcessor.executeInternal(writeQuery)
      SystemKeyspace.forceBlockingFlush("local") //force flush system.local so that replica_name gets persisted to disk.
      //log out the replica name set to console and to log file.
      val infoMessage = s"Replica name set to '$replicaName'."
      //FIXME - this is not being logged to core.log because storage package does not have logging.
      getLogger().info(infoMessage)
      println(infoMessage)
      SetReplicaNameResult.Success
    } else {
      SetReplicaNameResult.InvalidReplicaName
    }

  /**
    * Fetches the replicaName for the current node from Cassandra.
    */
  def getReplicaName: Option[String] = {
    val readQuery = s"SELECT $replicaNameColumn FROM $cassandraSystem WHERE key='$replicaNameKey'"
    val readQueryResult = QueryProcessor.executeInternal(readQuery)
    if (readQueryResult.isEmpty) {
      None
    } else {
      Some(readQueryResult.one.getString(replicaNameColumn))
    }
  }

  /**
    * Set's replica name if it's unset. Replica name is immutable.
    *
    * @return Left - with old replica name if it's already set
    *         Right - with new replica name if it's not already set.
    */
  def setReplicaNameIfEmpty(replicaName: String): SetReplicaNameResult =
    synchronized {
      getReplicaName map {
        oldReplicaName =>
          //if the new replica_name is the same as old replica_name return Success.
          if (oldReplicaName == replicaName) {
            SetReplicaNameResult.Success
          } else {
            SetReplicaNameResult.ReplicaNameAlreadySet(oldReplicaName)
          }
      } getOrElse {
        setReplicaName(replicaName)
      }
    }

  /**
    * Initializes the minimum amount of Cassandra surface area to
    * permit access to `SystemKeyspace.getLocalHostId`.
    */
  def init(config: CassandraConfig): Unit = {
    CassandraConfig.mkRootDir(config)
    CassandraConfig.writeConfiguration(config)

    System.setProperty("cassandra-foreground", "true")
    System.setProperty("cassandra.config", s"file:${config.internalConfFilePath}")
    System.setProperty("cassandra.join_ring", "true")
    System.setProperty("cassandra.start_rpc", "false")
    System.setProperty("cassandra.start_native_transport", "false")
    System.setProperty("cassandra.ignore_dc", "true")
    System.setProperty("cassandra.ignore_rack", "true")
    System.setProperty("cassandra.skip_wait_for_gossip_to_settle", "0")
    System.setProperty("cassandra.ring_delay_ms", "1")
    System.setProperty("cassandra.available_processors", config.processors.toString)

    disableMessagingService()

    assert(daemon == null)
    daemon = new CassandraDaemon
    daemon.init(null)

    initKeyspace(KeyspaceName)

    val drainField = classOf[StorageService].getDeclaredField("drainOnShutdown")
    drainField.setAccessible(true)
    drainHook = drainField.get(StorageService.instance).asInstanceOf[Thread]
    Runtime.getRuntime.removeShutdownHook(drainHook)
  }

  def initKeyspace(keyspaceName: String) = {
    while (!StorageService.instance.isJoined) {
      Thread.sleep(100)
    }

    val md = KSMetaData.newKeyspace(
      keyspaceName,
      classOf[LocalStrategy].asInstanceOf[Class[AbstractReplicationStrategy]],
      Collections.emptyMap(),
      true,
      Collections.emptyList())

    if (Schema.instance.getKSMetaData(keyspaceName) eq null) {
      MigrationManager.announceNewKeyspace(md, true)
    } else {
      MigrationManager.announceKeyspaceUpdate(md, true)
    }

    // ensure the keyspace changes are persisted
    for {
      ks <- CKeyspace.all.asScala
      cf <- ks.getColumnFamilyStores.asScala
    } cf.forceBlockingFlush()
  }

  /**
    * Generates a new snapshot of all column families in the FAUNA
    * keyspace with the given name.
    */
  def snapshot(name: String): Unit = {
    val ks = CKeyspace.open(KeyspaceName)
    ks.getColumnFamilyStores forEach { cfs =>
      cfs.snapshot(name)
    }
  }

  /**
    * Returns the names of all snapshots of column families in the
    * FAUNA keyspace, keyed by the name of the column family
    * containing the snapshot.
    */
  def snapshots: Map[String, Seq[String]] = {
    val ks = CKeyspace.open(KeyspaceName)

    val snaps = MMap.empty[String, Seq[String]].withDefaultValue(Seq.empty)

    ks.getColumnFamilyStores forEach { cfs =>
      cfs.getSnapshotDetails().keySet forEach { name =>
        snaps += cfs.name -> (snaps(cfs.name) :+ name)
      }
    }

    snaps.toMap
  }

  /**
    * Compares a snapshot of the provided column family in the FAUNA
    * keyspace to the sstables in the active data directory. If any
    * sstables exist in the snapshot, but _not_ in the active data
    * directory, a log message is emitted.
    */
  def validateSnapshot(cf: String, snapshot: String): Unit = {
    val ks = CKeyspace.open(KeyspaceName)
    val cfs = ks.getColumnFamilyStore(cf)

    require(cfs ne null, s"unknown column family $cf")

    if (!cfs.snapshotExists(snapshot)) {
      logger.warn(s"Attempted to validate a missing snapshot $cf/$snapshot.")
      return
    }

    val backup = new HashSet(cfs.directories
      .sstableLister
      .onlyBackups(true)
      .snapshots(snapshot)
      .list()
      .keySet())

    val active = cfs.directories
      .sstableLister
      .skipTemporary(true)
      .includeBackups(false)
      .list()
      .keySet()

    backup.removeAll(active)

    if (!backup.isEmpty) {
      backup forEach { desc =>
        logger.warn(s"Snapshot $cf/$snapshot contains extra sstable $desc.")
      }

      logger.info(s"Retaining $cf/$snapshot for analysis.")
    } else {
      logger.info(s"Snapshot $cf/$snapshot validated. Clearing snapshot.")
      cfs.clearSnapshot(snapshot)
    }
  }

  def stop(): Unit = {
    // need to shut down jmxServer if daemon.init starts it up.
    Option(CassandraDaemon.jmxServer) foreach { _.stop() }
    Option(drainHook) foreach { _.run() }
    daemon = null
  }
}
