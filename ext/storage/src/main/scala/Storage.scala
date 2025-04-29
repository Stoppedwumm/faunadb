package fauna.storage

import fauna.storage.cassandra.{ Caching, ColumnFamilySchema, GCGrace }
import java.util.UUID
import org.apache.cassandra.config.Schema
import org.apache.cassandra.db.{ Keyspace, SystemKeyspace }
import org.apache.cassandra.exceptions.ConfigurationException
import org.apache.cassandra.service.{ MigrationManager, StorageService }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

case class SchemaException(msg: String) extends Exception(msg)

object Storage {

  /** The standard GC grace period. It should be long enough for data movement to
    * finish before GC kicks in, but not much longer.
    */
  val StandardGCGrace = 2.days

  /** An extended GC grace period used when joining a host to a
    * cluster. This prevents the new host from removing tombstones
    * until an operator resets the grace period to its default with
    * `faunadb-admin update-storage-version`.
    */
  val ExtendedGracePeriod = 1000.days

  // The Column Family ID is a C* anachronism that has no meaning in
  // FaunaDB. Migrating a comparator requires either a new Column
  // Family or a new replica.
  //
  // If a CF is not in this list, schema initialization will mint a
  // new UUID, and schema updates will re-use that UUID. If that
  // behavior is not desirable, add the CF to this list.
  lazy val CFIDs = Map(
    Tables.HealthChecks.CFName -> UUID.fromString(
      "650dee83-51dd-3980-9860-1f86f0feb6c5"),
    Tables.HistoricalIndex.CFName -> UUID.fromString(
      "5aff13b7-8633-3b11-b373-536f7e844681"),
    Tables.Lookups.CFName -> UUID.fromString("812b1bfc-dcad-3186-acbe-4367d4965b19"),
    Tables.RowTimestamps.CFName -> UUID.fromString(
      "86769dcd-03a8-3219-8787-e5f3e15b1b1d"),
    Tables.SortedIndex.CFName -> UUID.fromString(
      "39d428ec-3ba2-3832-87c7-ceea9975a95d"),
    Tables.Versions.CFName -> UUID.fromString(
      "2de7f3e2-6de5-3e8c-8c0d-4958a71de9c4"),
    Tables.SchemaVersions.CFName -> UUID.fromString(
      "30da90ba-df6b-39ce-b1be-0d99a0d8c9f1")
  )

  def apply(schema: Seq[ColumnFamilySchema]): Storage =
    Storage(Cassandra.KeyspaceName, schema)

  lazy val LocalIndexBuildKeyspace = {
    val keyspaceName = "LOCALINDEXBUILD"
    Cassandra.initKeyspace(keyspaceName)

    val storage = Storage(
      keyspaceName,
      Seq(
        Tables.SortedIndex.Schema2.withCaching(Caching.None),
        Tables.HistoricalIndex.Schema2.withCaching(Caching.None)))
    storage.init()
    storage.update()

    Keyspace.open(keyspaceName)
  }

  lazy val LocalMetadataIndexBuildKeyspace: Keyspace = {
    val keyspaceName = "LOCALMETADATAINDEXBUILD"
    Cassandra.initKeyspace(keyspaceName)

    val storage = Storage(
      keyspaceName,
      Seq(Tables.SortedIndex.Schema, Tables.HistoricalIndex.Schema))

    // Generate fresh UUIDs for these CFs in the rebuild keyspace - CF
    // UUIDs are _globally_ unique.
    storage.init(overrideIDs = false)

    Keyspace.open(keyspaceName)
  }

}

case class Storage(keyspaceName: String, schema: Seq[ColumnFamilySchema]) {

  /** Initializes the storage engine by creating column families as
    * appropriate.
    *
    * NOTE: Disable column family ID overrides in tests to allow multiple
    * keyspaces to contain the same column family definitions.
    */
  def init(overrideIDs: Boolean = true, extendedGCGrace: Boolean = false): Unit = {
    val ks = Option(Schema.instance.getKSMetaData(keyspaceName))
    val meta = ks map { _.cfMetaData.asScala.toMap }

    meta match {
      case None =>
        throw SchemaException("Database not initialized.")
      case Some(defs) =>
        schema foreach { s =>
          defs.get(s.name) match {
            case Some(_) => ()
            case None =>
              val schema = if (extendedGCGrace) {
                s.withGCGrace(GCGrace.Custom(Storage.ExtendedGracePeriod))
              } else {
                s
              }

              val metadata = schema.metadata(
                keyspaceName,
                Option.when(overrideIDs)(Storage.CFIDs.get(schema.name)).flatten)
              MigrationManager.announceNewColumnFamily(
                metadata, /* announceLocally */ true)
          }
        }

        SystemKeyspace.allSchemaCfs forEach { SystemKeyspace.forceBlockingFlush(_) }
    }
  }

  /** Compares column families defined in storage with the
    * currently-defined schema, and updates any column families in
    * storage which have changes in the schema.
    */
  def update(): Unit = {
    val ks = Option(Schema.instance.getKSMetaData(keyspaceName))
    val meta = ks map { _.cfMetaData.asScala.toMap }

    meta match {
      case None =>
        throw SchemaException("Database not initialized.")
      case Some(defs) =>
        schema foreach { s =>
          val cfmeta = s.metadata(keyspaceName, Storage.CFIDs.get(s.name))
          defs.get(s.name) match {
            case None =>
              MigrationManager.announceNewColumnFamily(
                cfmeta,
                /* announceLocally */ true)
            case Some(d) =>
              if (
                d.getDefaultValidator.getClass != cfmeta.getDefaultValidator.getClass
              ) {
                throw SchemaException(
                  "validators do not match or are not compatible.")
              }

              try {
                d.validateCompatility(cfmeta)
                MigrationManager.announceColumnFamilyUpdate(
                  cfmeta, /* fromThrift */ true, /* announceLocally */ false)
              } catch {
                case e: ConfigurationException =>
                  throw SchemaException(e.getMessage)
              }
          }
        }

        SystemKeyspace.allSchemaCfs forEach { SystemKeyspace.forceBlockingFlush(_) }
    }
  }

  /** Searches the data directory for each column family in the
    * schema, loading any new sstables present.
    */
  def reload(): Unit =
    schema foreach { s =>
      StorageService.instance.loadNewSSTables(keyspaceName, s.name)
    }

  /** Compares column families defined in storage with the
    * currently-defined schema, and removes any column families from
    * storage which no longer exist in the schema.
    */
  def cleanup(): Unit = {
    val ks = Option(Schema.instance.getKSMetaData(keyspaceName))
    val meta = ks map { _.cfMetaData.asScala.toMap }

    meta match {
      case None =>
        throw SchemaException("Database not initialized.")
      case Some(defs) =>
        val current = schema map { _.name } toSet
        val remove = defs.keySet -- current

        remove foreach { cf =>
          MigrationManager.announceColumnFamilyDrop(keyspaceName, cf, true)
        }
    }
  }

  /** Removes all defined column families; used only in creating a new
    * test environment.
    */
  def removeAll(): Unit = {
    val ks = Option(Schema.instance.getKSMetaData(keyspaceName))
    val meta = ks map { _.cfMetaData.keySet }

    meta match {
      case None => ()
      case Some(defs) =>
        defs forEach { name =>
          MigrationManager.announceColumnFamilyDrop(keyspaceName, name, true)
        }
    }
  }

  /** A time-based UUID indicating how recently the schema in storage
    * has been updated.
    *
    * NOTE: may not correspond to versions across hosts or clusters.
    */
  def version: UUID = Schema.instance.getVersion

  /** Returns true iff a column family named `name` exists in storage.
    *
    * WARNING: Does _not_ verify the correspondence of on-disk schema
    * with in-memory schema!
    */
  def isDefined(name: String): Boolean =
    Option(Schema.instance.getCFMetaData(keyspaceName, name)).isDefined

  // Returns an a-list CF name -> gc grace period in seconds, for all non-_2 CFs.
  def getGCGrace() = {
    val cfs = schema.filter { sch =>
      sch.name != "HistoricalIndex_2" && sch.name != "SortedIndex_2"
    }
    // NB: GC grace period in sch does not get updated; go to the KS.
    cfs map { sch =>
      sch.name -> Keyspace
        .open(keyspaceName)
        .getColumnFamilyStore(sch.name)
        .metadata
        .getGcGraceSeconds
    }
  }

  // Update the GC grace period of all non-_2 CFs to `d`.
  def updateGCGraceAll(d: FiniteDuration) =
    schema.filter { sch =>
      sch.name != "HistoricalIndex_2" && sch.name != "SortedIndex_2"
    } foreach { updateGCGrace(_, d) }

  def updateGCGrace(cfs: ColumnFamilySchema, d: FiniteDuration) = {
    val cfMeta = cfs
      .withGCGrace(GCGrace.Custom(d))
      .metadata(Cassandra.KeyspaceName, Storage.CFIDs.get(cfs.name))
    MigrationManager.announceColumnFamilyUpdate(cfMeta, true, true)
  }

  def getIndex2CFGcGrace() = {
    val ks = Keyspace.open(keyspaceName)
    val h_days = ks
      .getColumnFamilyStore(Tables.HistoricalIndex.CFName2)
      .metadata
      .getGcGraceSeconds / 1.day.toSeconds
    val s_days = ks
      .getColumnFamilyStore(Tables.SortedIndex.CFName2)
      .metadata
      .getGcGraceSeconds / 1.day.toSeconds
    (h_days, s_days)
  }
}
