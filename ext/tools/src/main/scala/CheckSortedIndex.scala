package fauna.tools

import com.github.benmanes.caffeine.cache.Caffeine
import fauna.api.FaunaApp
import fauna.atoms._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.{ Timestamp, Timing }
import fauna.lang.syntax._
import fauna.lang.LRU
import fauna.model.{ Collection, Database }
import fauna.model.schema.NativeIndex
import fauna.repo.{ IndexIterator, IndexRow, RepoContext, Store }
import fauna.repo.query.Query
import fauna.repo.schema.CollectionSchema
import fauna.storage._
import fauna.storage.cassandra.CassandraIterator
import fauna.storage.index.{ IndexTuple, NativeIndexID }
import java.nio.file.{ Files, Path }
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.utils.OutputHandler
import org.apache.commons.cli.Options
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** Reads the SortedIndex CF, and validates that there is a version for every
  *  index entry.
  *
  * This is a standalone tool, which is meant to be run in a one-off snapshot
  * broker host. It has been used in the past to scan for index mismatches in
  * eu-std. At the time of scanning (September 2024), it was estimated to take
  * around 8 days to scan through all 2TB in eu-std. So it is not a fast tool by
  * and means.
  *
  * The tool splits up the dataset into a large number of segments (4096 right
  * now), and then threads from a pool grab those segments and process them
  * individually.
  *
  * The scan can be filtered to a specific scope and/or index with the `--scope`
  * and `--index` flags, which makes this tool much faster (an hour for a single
  * index instead of days).
  */
object CheckSortedIndex extends FaunaApp("CheckSortedIndex") {
  case class Opts(
    threads: Int,
    scope: Option[ScopeID],
    index: Option[IndexID],
    skipSegments: Option[Int])

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption(null, "threads", true, "The number of threads to use.")
    o.addOption(null, "scope", true, "The scope to scan (default: all).")
    o.addOption(null, "index", true, "The index to scan (default: all).")
    // NB: This will not resume the scan! Segments are scanned on a thread pool, so
    // they may complete in any order. Instead, this is just used to skip to a
    // different point in the cluster (instead of scanning the same data at the start
    // every time).
    o.addOption(
      null,
      "skipSegments",
      true,
      "The number of segments to skip (default: 0).")
  }

  override def setupLogging() =
    Some(config.logging.configure(None, debugConsole = true))

  lazy val handler = new OutputHandler.LogOutput

  start {
    val opts = Opts(
      threads = Option(cli.getOptionValue("threads"))
        .flatMap { _.toIntOption }
        .getOrElse(sys.runtime.availableProcessors),
      scope = Option(cli.getOptionValue("scope")).map { s => ScopeID(s.toLong) },
      index = Option(cli.getOptionValue("index")).map { s => IndexID(s.toShort) },
      skipSegments = Option(cli.getOptionValue("skipSegments")).map(_.toInt)
    )

    snap = readSnapshotTime()

    run(opts)
  }

  var repo: RepoContext = null
  var snap: Timestamp = null

  def readSnapshotTime() = {
    // The host this runs on, which is a restore broker, has this metadata file in
    // place. This snapshot time is the time when all the data is valid. Reading
    // ahead of this time will produce inconsistent results, as there may be data
    // from some hosts, but not others.
    val path = "/media/ephemeral/fauna-backups/config/snapshots.json"

    val snap =
      try {
        val json = JSON
          .parse[JSValue](Files.readAllBytes(Path.of(path)))
          .as[JSObject]

        Timestamp.parseInstant((json / "snapshot_ts").as[String])
      } catch {
        case NonFatal(e) =>
          handler.warn(s"Failed to read snapshot time from $path: $e")
          sys.exit(1)
      }

    handler.output(s"Using snapshot time $snap")

    snap
  }

  def prettyDuration(d: Duration) = {
    if (d.isFinite) {
      val hours = d.toMinutes / 60
      val minutes = d.toMinutes % 60
      val seconds = d.toSeconds % 60

      s"${hours}h${minutes}m${seconds}s"
    } else {
      "<infinite>"
    }
  }

  private def run(opts: Opts) = {
    val start = Timing.start

    handler.output("Setting up repo...")
    repo = RepoTool.setupRepoWithoutCompactions(config, stats)
    handler.output("Repo is setup.")

    scanSortedIndex(opts)

    val end = start.elapsed
    handler.output(s"Total search time: ${prettyDuration(end)}")

    sys.exit(0)
  }

  val cacheSize = 1024 * 1024

  val mvtCache = TlsLru[(ScopeID, CollectionID), Timestamp](cacheSize) {
    case (scope, coll) => lookupMvtUncached(scope, coll)
  }
  val liveScopeCache = ConcurrentLru[ScopeID, Boolean](cacheSize) { scope =>
    repo
      .runSynchronously(Database.forScope(scope).map(_.isDefined), 30.seconds)
      .value
  }

  private def lookupMvtUncached(scope: ScopeID, coll: CollectionID): Timestamp = {
    try {
      val q = Store.getUnmigrated(scope, coll.toDocID, Timestamp.MaxMicros).flatMap {
        case Some(_) => Collection.deriveMinValidTime(scope, coll)

        // The collection has been deleted, so its all eligible for GC.
        case None => Query.value(Timestamp.MaxMicros)
      }
      repo.runSynchronously(q, 30.seconds).value
    } catch {
      case NonFatal(e) =>
        handler.output(s"Failed to lookup MVT for $scope/$coll: $e")
        // Assume its all bad.
        Timestamp.MaxMicros
    }
  }

  private def scanSortedIndex(opts: Opts): Unit = {
    // Find all sorted index entries.
    val ks = Keyspace.open(repo.storage.keyspaceName)
    val sortedIndexCF = ks.getColumnFamilyStore(Tables.SortedIndex.CFName)

    val exec = new SegmentExecutor(opts.threads, handler)
    opts.skipSegments.foreach { amount =>
      exec.skipSegments(amount)
    }

    val selector = opts.scope match {
      case Some(scope) => Selector.Scope(scope)
      case None        => Selector.All
    }
    exec.work { (segment, progress) =>
      val iter0 = new CassandraIterator(
        exec.iterCounter,
        sortedIndexCF,
        ScanBounds(segment),
        selector,
        snapTime = snap
      )

      val iter =
        new IndexIterator(iter0, (scope, coll) => mvtCache.get((scope, coll)))

      var i = 0
      while (iter.hasNext) {
        val row = iter.next()

        opts.index match {
          // Skip if the index doesn't match.
          case Some(index) if row.key.id != index => ()
          case _                                  => checkValue(row)
        }

        if (i % 1000 == 0) {
          progress(
            (iter0.currentLocation.token - segment.left.token).toDouble /
              (segment.right.token - segment.left.token).toDouble)
        }
        i += 1
      }
    }

    exec.runWithProgress()
  }

  private def checkValue(row: IndexRow): Unit = {
    // Skip if the row is not an add.
    if (row.value.action != SetAction.Add) {
      return
    }

    // Skip if the row is TTL'ed.
    row.value.tuple.ttl match {
      case Some(ttl) if ttl < snap => return
      case _                       => ()
    }

    // Skip deleted scopes and indexes.
    if (!liveScopeCache.get(row.key.scope)) {
      return
    }

    def mismatch(msg: String) = {
      handler.output(s"Mismatch!!! for ${row.key} and value ${row.value}: $msg")
    }

    val mvt = mvtCache.get((row.key.scope, row.value.docID.collID))

    // If the MVT is the max, we can't make any assumptions about the row.
    if (mvt == Timestamp.MaxMicros) {
      return
    }

    val validTS = row.value.ts.validTS.max(mvt)

    val scope = row.key.scope
    val doc = row.value.docID

    val checkDocByCollection =
      row.key.id != NativeIndexID.DocumentsByCollection.id

    val docByCollectionQ = if (checkDocByCollection) {
      Store
        .collection(
          NativeIndex.DocumentsByCollection(scope),
          Vector(doc.collID.toDocID),
          snapshotTS = validTS,
          from = IndexTuple(scope, doc),
          to = IndexTuple(scope, DocID(SubID(doc.subID.toLong + 1), doc.collID)),
          pageSize = 2,
          ascending = true
        )
        .flattenT
    } else {
      Query.value(Seq.empty)
    }

    val query = for {
      docByCollection <- docByCollectionQ
      doc <- Store.getVersionLiveNoTTL(
        CollectionSchema.empty(scope, doc.collID),
        doc,
        validTS)
    } yield {
      doc match {
        case Some(version) if version.isDeleted =>
          mismatch(s"found ADD with DELETED version (expected live version)")

        case Some(version) if version.ttl.exists(_ < snap) =>
          mismatch(s"found ADD with TTL'ed version (expected live version)")

        case Some(version) =>
          if (
            checkDocByCollection && !docByCollection.exists(_.docID == version.id)
          ) {
            mismatch(
              s"found ADD with no DocumentByCollection ADD in ${docByCollection}")
          }

        case None => mismatch(s"found ADD with no versions")
      }
    }

    try {
      repo.runSynchronously(query, 10.seconds)
    } catch {
      case NonFatal(e) => mismatch("failed to check: " + e)
    }
  }

  /** A small wrapper around an LRU cache per-thread.
    */
  class TlsLru[K, V](size: Int)(lookup: K => V) {
    private val cache = new ThreadLocal[LRU[K, V]] {
      override def initialValue = LRU.unpinneable[K, V](size)
    }

    def get(key: K): V = {
      val lru = cache.get()

      lru.containsKey(key) match {
        case true => lru.get(key)
        case false =>
          val value = lookup(key)
          lru.put(key, value)
          value
      }
    }
  }

  object TlsLru {
    def apply[K, V](size: Int)(lookup: K => V) = new TlsLru(size)(lookup)
  }

  class ConcurrentLru[K, V](size: Int)(lookup: K => V) {
    private val cache = Caffeine.newBuilder().maximumSize(size).build[K, V]()

    def get(key: K): V = cache.get(key, k => lookup(k))
  }

  object ConcurrentLru {
    def apply[K, V](size: Int)(lookup: K => V) = new ConcurrentLru(size)(lookup)
  }
}
