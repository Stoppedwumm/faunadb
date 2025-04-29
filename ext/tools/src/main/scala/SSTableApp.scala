package fauna.tools

import fauna.api.FaunaApp
import fauna.storage.{ Cassandra, CassandraConfig, Storage, Tables }
import java.util.{ Map => JMap, Set => JSet }
import org.apache.cassandra.config.{ DatabaseDescriptor, Schema }
import org.apache.cassandra.db.compaction.CompactionManager
import org.apache.cassandra.db.{ ColumnFamilyStore, Keyspace }
import org.apache.cassandra.io.sstable._
import org.apache.cassandra.utils.OutputHandler
import scala.util.control.NonFatal

abstract class SSTableApp(name: String) extends FaunaApp(name) {

  override def printBanner() = {}

  override def setupLogging() =
    Some(config.logging.configure(None, debugConsole = true))

  lazy val handler = new OutputHandler.LogOutput

  private[this] var _init = false

  protected def openColumnFamily(cf: String, withSSTables: Boolean = false) = {
    if (!_init) {
      _init = true

      CassandraConfig.mkRootDir(config.cassandra)
      CassandraConfig.writeConfiguration(config.cassandra)

      System.setProperty(
        "cassandra.config",
        s"file://${config.cassandra.internalConfFilePath}")
      System.setProperty("cassandra.storagedir", config.storagePath)

      DatabaseDescriptor.loadSchemas(false)

      if (Schema.instance.getCFMetaData(Cassandra.KeyspaceName, cf) eq null) {
        Cassandra.init(config.cassandra)

        val schema = Seq(
          Tables.Lookups.Schema,
          Tables.Versions.Schema,
          Tables.HistoricalIndex.Schema,
          Tables.HistoricalIndex.Schema2,
          Tables.SortedIndex.Schema,
          Tables.SortedIndex.Schema2
        )

        Storage(Cassandra.KeyspaceName, schema).init()
      }
    }

    if (Schema.instance.getCFMetaData(Cassandra.KeyspaceName, cf) eq null) {
      throw new IllegalArgumentException(
        s"Unknown keyspace/columnfamily ${Cassandra.KeyspaceName}.$cf")
    }

    val ks = if (withSSTables) {
      Keyspace.open(Cassandra.KeyspaceName)
    } else {
      Keyspace.openWithoutSSTables(Cassandra.KeyspaceName)
    }

    val cfs = ks.getColumnFamilyStore(cf)

    try {
      cfs.disableAutoCompaction()
      CompactionManager.instance.forceShutdown()
    } catch {
      case NonFatal(ex) =>
        handler.warn(s"An error occurred trying to disable compaction in $cf", ex)
        sys.exit(1)
    }

    cfs
  }

  protected def forEachSSTable(prefix: String, cfs: ColumnFamilyStore)(
    fn: SSTableReader => Unit): Unit =
    forEachSSTablePath(prefix, cfs) { entry =>
      try {
        fn(SSTableReader.openNoValidation(entry.getKey, entry.getValue, cfs.metadata))
      } catch {
        case NonFatal(ex) =>
          handler.warn(s"Error loading ${entry.getKey}", ex)
          stats.incr(s"$prefix.Failure")
          sys.exit(1)
      }
    }

  protected def forEachSSTablePath(prefix: String, cfs: ColumnFamilyStore)(
    fn: JMap.Entry[Descriptor, JSet[Component]] => Unit): Unit = {
        val lister = cfs.directories.sstableLister().skipTemporary(true)

    val sstableOpen = new Executor(s"$prefix-SSTable-Open")

    lister.list.entrySet forEach { entry =>
      sstableOpen addWorker { () =>
        val comps = entry.getValue

        if (comps.contains(Component.DATA)) {
          try {
            fn(entry)
          } catch {
            case NonFatal(ex) =>
              handler.warn(s"Error loading ${entry.getKey}", ex)
              stats.incr(s"$prefix.Failure")
              sys.exit(1)
          }
        }
      }
    }

    sstableOpen.waitWorkers()
  }
}
