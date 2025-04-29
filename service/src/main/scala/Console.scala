package fauna.core

import fauna.api.API
import fauna.ast._
import fauna.atoms._
import fauna.auth.Auth
import fauna.cluster.topology.{ OwnedSegment, SegmentOwnership, Topologies }
import fauna.cluster.NodeInfo
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.{ ConsoleControl, Logger, TimeBound, Timestamp }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.{ InternalCollection, NativeIndex, SchemaSource }
import fauna.model.tasks.TaskExecutor
import fauna.repo._
import fauna.repo.doc._
import fauna.repo.query.{ QTry, Query, QueryEvalContext, State }
import fauna.repo.store._
import fauna.storage.{ Tables, VersionID }
import fauna.storage.api.set._
import fauna.storage.cassandra.CassandraKeyLocator
import fauna.storage.index.{ IndexTerm, IndexValue }
import fauna.util.{ REPL, REPLServer }
import fql.ast.{ SchemaItem, Src }
import fql.color.ColorKind
import fql.schema.{ Diff, DiffRender, SchemaDiff }
import io.netty.buffer.Unpooled
import org.apache.logging.log4j.{ Level, LogManager }
import org.apache.logging.log4j.core.LoggerContext
import scala.concurrent._
import scala.concurrent.duration._

object Console {
  def apply(getAPI: () => Option[API], address: String, port: Int) =
    new REPLServer(newREPL(getAPI), address, port)

  private def newREPL(getAPI: () => Option[API]) =
    new REPL {
      val ctx = ConsoleContext(getAPI)

      val prompt = "core> "

      def welcomeMsg =
        s"""|Debug console
            |""".stripMargin('|')

      val helpMsg = ""

      bind("help", helpMsg)
      bind("consoleContext", ctx)
      autoImport("consoleContext._")
      autoImport("language._")
      autoImport("fauna.ast._")
      autoImport("fauna.atoms._")
      autoImport("fauna.auth.Auth")
      autoImport("fauna.lang._")
      autoImport("fauna.lang.clocks.Clock")
      autoImport("fauna.lang.syntax._")
      autoImport("fauna.model._")
      autoImport("fauna.model.schema._")
      autoImport("fauna.repo._")
      autoImport("fauna.repo.store._")
      autoImport("fauna.storage._")
      autoImport("fauna.storage.doc._")
      autoImport("fauna.storage.index._")
      autoImport("fauna.storage.ir._")
      autoImport("org.apache.logging.log4j.Level")
      autoImport("scala.concurrent.duration._")
      autoImport("scala.concurrent.Await")
    }
}

case class ConsoleContext(getAPI: () => Option[API]) {
  // enable console-only functionality
  implicit val ctl = new ConsoleControl

  val Main = fauna.core.Service
  val RootScope = Database.RootScopeID

  private def tryGetAPI(svc: String): API =
    getAPI().getOrElse {
      throw new IllegalStateException(
        s"$svc has not been initialized. Is this node a member of a cluster yet?")
    }

  def api = tryGetAPI("API service")
  def repo = tryGetAPI("repo object").repo
  def stats = tryGetAPI("stats object").stats
  def service = tryGetAPI("CassandraService").repo.service

  def dynamic(obj: AnyRef, name: String = "*") = new DynAccessor(obj, name)

  def RunQuery(
    auth: Auth,
    q: Literal,
    ts: Timestamp = Clock.time): Either[List[Error], Literal] =
    repo ! EvalContext.write(auth, ts, APIVersion.Default).parseAndEvalTopLevel(q)

  /** Evaluates a query and returns the result and the post-eval state. The
    * query does *not* commit a transaction to the log. This is useful for
    * examining a query's read and write set and the results of some queries
    * that normally throw exceptions.
    *
    * See Query.execute for the normal context of this level of evaluation.
    */
  def EvalQuery[A](q: Query[A], ts: Timestamp = Clock.time): (QTry[A], State) =
    Await.result(
      QueryEvalContext.eval(
        q,
        repo,
        ts,
        TimeBound.Max,
        repo.txnSizeLimitBytes,
        fauna.repo.service.rateLimits.PermissiveOpsLimiter,
        ScopeID.RootID
      ), // Used just for logging.
      TimeBound.Max.duration
    )

  def GetSchema(
    scope: ScopeID,
    ts: Timestamp = Timestamp.MaxMicros): Map[String, String] = {
    repo ! SchemaSourceID
      .getAllUserDefined(scope, ts)
      .collectMT { SchemaSource.get(scope, _, ts) }
      .flattenT
      .map { sources =>
        sources.map { source => source.filename -> source.content }.toMap
      }
  }

  // An event in the history of a collection schema.
  // Contains the name of the FSL file the collection is defined in,
  // the schema version, the excerpt of the FSL file defining the
  // collection, and the diff from the previous version of the
  // collection to this one.
  case class ColSchemaEvent(
    filename: String,
    version: SchemaVersion,
    excerpt: String,
    diff: String)

  /** Given a scope and collection name, returns the history of the
    * collection's FSL schema, in decreasing version order, along with
    * the rendered diffs between versions where changes occurred. It's
    * very useful when trying to understand how a collection's schema
    * evolved over time, e.g. if the schema got into a bad state. It
    * includes staged schema, so be aware the active schema may not be
    * the first element of the sequence.
    *
    * The tuple is (file, version, excerpted collection FSL, diff to previous).
    * The diff will use spans for the original FSL, not the excerpt.
    *
    * Old schema files that have invalid syntax will be ignored.
    *
    * This function is not smart enough to track a collection through
    * a rename, nor is it smart enough to figure out when a collection
    * has been deleted and replaced with a new one with the same name.
    * It could be this smart, but I bet it doesn't need to be to be
    * really useful.
    */
  def GetCollectionSchemaHistory(
    scope: ScopeID,
    name: String): Seq[ColSchemaEvent] = {
    // Grab every live source.
    val sources = repo ! SchemaSourceID
      .getAllUserDefined(scope)
      .flatMapPagesT { id => InternalCollection.SchemaSource(scope).versions(id) }
      .collectT {
        case v: Version.Live => Some(SchemaSource(v))
        case _               => None
      }
      .flattenT

    // Parse files that mention the collection name, then
    // pick out just the collection item if it exists.
    val parsed = sources
      .flatMap { source =>
        if (!source.content.contains(name)) {
          None
        } else {
          source.file.parse().toOption.flatMap { items =>
            val colOpt = items.find { item =>
              item.kind == SchemaItem.Kind.Collection && item.name.str == name
            }
            if (colOpt.isEmpty) {
              None
            } else {
              Some((source.filename, source.version, source.content, colOpt.get))
            }
          }
        }
      }
      .sortWith {
        // The order is by source id, then decreasing version.
        // We want by decreasing version, in case the collection
        // moved between files.
        _._2 > _._2
      }

    // Bite me functional programmers.
    parsed.zipWithIndex.flatMap {
      case ((filename, version, content, item), i) if i < parsed.size - 1 =>
        val before = parsed(i + 1)
        SchemaDiff.diffItem(before._4, item).map { diff =>
          val rendered = DiffRender.renderSemantic(
            Map(Src.SourceFile(before._1) -> before._3),
            Map(Src.SourceFile(filename) -> content),
            Seq(diff),
            ColorKind.None)
          ColSchemaEvent(filename, version, item.span.extract(content), rendered)
        }
      case ((filename, version, content, item), _) =>
        val rendered = DiffRender.renderSemantic(
          Map(),
          Map(Src.SourceFile(filename) -> content),
          Seq(Diff.Add(item)),
          ColorKind.None)
        Some(ColSchemaEvent(filename, version, item.span.extract(content), rendered))
    }
  }

  def GetLiveIDs(
    idx: IndexConfig,
    terms: Vector[Term],
    ts: Timestamp = Timestamp.MaxMicros): Seq[DocID] =
    repo ! (Store.collection(idx, terms, ts) mapValuesT { _.docID } flattenT)

  def GetSet(
    idx: IndexConfig,
    terms: Vector[Term],
    ts: Timestamp = Timestamp.MaxMicros): Seq[Version] = {
    val vs = GetLiveIDs(idx, terms, ts) map {
      RuntimeEnv.Default.Store(idx.scopeID).get(_, ts)
    } sequence

    (repo ! vs).flatten
  }

  def GetAllIDs(idx: IndexConfig, terms: Vector[IndexTerm]): Set[DocID] =
    repo ! (Store.historicalIndex(idx, terms).flattenT) map { _.docID } toSet

  def GetDoc(
    scope: ScopeID,
    id: DocID,
    ts: Timestamp = Timestamp.MaxMicros): Version =
    repo ! RuntimeEnv.Default.Store(scope).get(id, ts) getOrElse sys.error(
      s"Document $id not found.")

  def DisabledDatabases(scope: ScopeID): Seq[Database] = {
    val terms = Vector(Scalar(true))
    val ids =
      GetLiveIDs(NativeIndex.DatabaseByDisabled(scope), terms, Timestamp.MaxMicros)

    val q = ids map { id =>
      Database.getUncached(scope, id.as[DatabaseID])
    } sequence

    (repo ! q).flatten
  }

  def DatabasesForDB(scope: ScopeID): Seq[Database] =
    repo ! DatabaseID
      .getAllUserDefined(scope)
      .collectMT(Database.getUncached(scope, _))
      .flattenT

  def CollectionsForDB(scope: ScopeID): Seq[Collection] =
    repo ! Collection.getAll(scope).flattenT

  def IndexesForDB(scope: ScopeID): Seq[Index] =
    repo ! IndexID
      .getAllUserDefined(scope)
      .collectMT(Index.getUncached(scope, _))
      .flattenT

  def RolesForDB(scope: ScopeID): Seq[Role] =
    repo ! RoleID
      .getAllUserDefined(scope)
      .collectMT(Role.getItemUncached(scope, _).map(_.flatMap(_.active)))
      .flattenT

  // warning: shouldn't be executed on a doc with an extreme number of versions
  def IndexEntriesForDoc(scope: ScopeID, doc: DocID): Seq[IndexRow] = {
    val indexer = repo ! Index.getIndexer(scope, doc.collID)
    val versions = repo ! RuntimeEnv.Default.Store(scope).versions(doc).flattenT
    versions flatMap { v => repo ! indexer.rows(v) }
  }

  def MetaEntriesForAllIndexes(scope: ScopeID): Seq[IndexRow] = {
    val ids = IndexesForDB(scope) map { _.id.toDocID }
    ids flatMap { IndexEntriesForDoc(scope, _) }
  }

  def ReindexMetaEntriesForAllIndexes(scope: ScopeID): Unit = {
    val ids = IndexesForDB(scope) map { _.id.toDocID }
    val indexer = repo ! Index.getIndexer(scope, IndexID.collID)

    ids foreach { id =>
      val versions = repo ! RuntimeEnv.Default.Store(scope).versions(id).flattenT

      versions foreach { version =>
        repo ! Store.build(version, indexer)
      }
    }
  }

  // warning: shouldn't be executed on a doc with an extreme number of versions
  def ReindexDoc(scope: ScopeID, doc: DocID): Unit = {
    val indexer = repo ! Index.getIndexer(scope, doc.collID)

    val versions = repo ! RuntimeEnv.Default.Store(scope).versions(doc).flattenT

    versions foreach { version =>
      repo ! Store.build(version, indexer)
    }
  }

  def RunnableTasksForHost(host: HostID): Seq[Task] =
    repo ! Task.getRunnableByHost(host).flattenT

  def LocalRunnableTasks(): Seq[Task] =
    service.localID match {
      case Some(id) => RunnableTasksForHost(id)
      case None     => Seq.empty
    }

  // Try to remember to turn this off, but it won't hurt much if you forget.
  def SetVerboseTaskExecutorLogging(enable: Boolean) =
    TaskExecutor.setVerboseLogging(enable)

  def GetDatabase(name: String): Database = {
    var db = Database.RootDatabase
    name.split("/").foreach { part =>
      db = GetDatabase(db.scopeID, part)
    }
    db
  }

  def GetDatabase(scope: ScopeID, name: String): Database = {
    val vers =
      GetLiveIDs(NativeIndex.DatabaseByName(scope), Vector(Scalar(name))) match {
        case Seq(id) => GetDoc(scope, id)
        case Seq()   => sys.error(s"`$name` not found.")
        case ids     => sys.error(s"Multiple documents named `$name` found: $ids")
      }
    (repo ! Database.getUncached(vers.parentScopeID, vers.id.as[DatabaseID])).get
  }

  def GetCollection(scope: ScopeID, name: String): Collection =
    repo ! SchemaNames.idByNameStagedUncached[CollectionID](scope, name).flatMap {
      id => Collection.getUncached(scope, id.get).map(_.get.staged.get)
    }

  def GetCollection(scope: ScopeID, collID: CollectionID): Collection =
    (repo ! Collection.getUncached(scope, collID)).get.staged.get

  def GetIndex(scope: ScopeID, name: String): Index =
    repo ! SchemaNames.idByNameStagedUncached[IndexID](scope, name).flatMap { id =>
      Index.getUncached(scope, id.get).map(_.get)
    }

  def GetIndex(scope: ScopeID, indexID: IndexID): Index =
    (repo ! Index.getUncached(scope, indexID)).get

  def GetAncestry(scope: ScopeID): Seq[String] =
    (repo ! Database.latestForScope(scope)) match {
      case Some(db) => db.namePath
      case None     => Seq.empty
    }

  def SetRootLogLevel(level: Level = Level.INFO): Unit =
    SetLogLevel(LogManager.ROOT_LOGGER_NAME, level)

  def SetLogLevel[T](target: T, level: Level = Level.INFO): Unit = {
    val targetStr = target match {
      case s: String => s
      case t         => t.getClass.toString
    }

    val ctx = LoggerContext.getContext(false)
    ctx.getConfiguration().getLoggerConfig(targetStr).setLevel(level)
    ctx.updateLoggers()
  }

  // See Tables.<ColumnFamily>.rowKey(..)
  def SegmentsForKey(rowKey: Array[Byte]): Seq[(Segment, Set[HostID])] = {
    val loc = CassandraKeyLocator.locate(Unpooled.wrappedBuffer(rowKey))
    val seg = Segment(loc, Location(loc.token + 1)) // single token segment
    service.partitioner.partitioner.replicas(seg)
  }

  /** Consolidate the topology (current ring) for the specified replica as the pending ring.
    * This should stop topology reconciliation swiftly.
    * Does not have to be invoked from within the replica for which the operation will be executed.
    */
  def ConsolidateCurrentTopology(replicaName: String): Unit = {

    // Get the Topologies object
    val t = (dynamic(service) / "topologies").get[Topologies]
    // Get the replica topology
    val rt = t.snapshot.getTopology(replicaName).get

    val balanced =
      OwnedSegment.increaseSegmentation(rt.currentSegments, Location.PerHostCount)
    val newPending = OwnedSegment.toSegmentOwnership(balanced)

    val current = rt.current
    // Balanced topology has no differences to reconcile
    require(
      OwnedSegment
        .differentHosts(
          rt.currentSegments,
          SegmentOwnership.toOwnedSegments(newPending))
        .isEmpty)
    // It has same amount of hosts as before
    require(
      (newPending groupBy { _.host }).size == (current groupBy { _.host }).size)
    // Each host has PerHostCount tokens
    require(newPending groupBy { _.host } forall { case (_, v) =>
      v.size == Location.PerHostCount
    })

    // Propose this new topology
    Await.result(
      t.proposeTopology(replicaName, newPending, rt.pending, rt.reusable),
      Duration.Inf)
  }

  // Count the number of documents in a collection.
  // Counts at most 5 million documents by default, to protect
  // the node.
  def CountCollection(
    scope: ScopeID,
    id: CollectionID,
    pageSize: Int = 50_000,
    atMost: Int = 5_000_000): Int =
    repo ! Store
      .collection(
        NativeIndex.DocumentsByCollection(scope),
        Vector(id),
        Clock.time,
        pageSize = pageSize)
      .takeT(atMost)
      .countT

  /** This is a helper for looking up a document by collection in SortedIndex.
    */
  def DocumentsByCollectionDebug(scope: ScopeID, doc: DocID, pageSize: Int = 16) = {
    val location = LocateDocumentByCollection(scope, doc)
    if (LocationIsLocal(location)) {
      val cfg = NativeIndex.DocumentsByCollection(scope)
      repo ! Store
        .sortedIndexDebug(
          cfg,
          Vector(doc.collID.toDocID, FieldIndexer(cfg).hashForConsole(doc).value),
          from = IndexValue(scope, doc),
          to = IndexValue(scope, doc.copy(subID = SubID(doc.subID.toLong - 1))),
          pageSize = pageSize
        )
        .takeT(pageSize)
        .flattenT
    } else {
      val sb = new StringBuilder
      sb ++= "The given document by collection entry doesn't live on this host.\n"
      sb ++= "It lives on one of these:\n"
      val hosts = HostsForLocation(location)
      if (hosts.isEmpty) {
        sb ++= "  <it lives nowhere>\n"
      } else {
        hosts.foreach { h => sb ++= s"  $h\n" }
      }
      throw new IllegalStateException(sb.result())
    }
  }

  def DocumentsDebug(scope: ScopeID, doc: DocID, pageSize: Int = 16) = {
    val location = LocateDocument(scope, doc)
    if (LocationIsLocal(location)) {
      repo ! Store
        .versions(
          scope,
          doc,
          VersionID.MaxValue,
          VersionID.MinValue,
          pageSize,
          reverse = false,
          MVTProvider.Default
        )
        .takeT(pageSize)
        .flattenT
    } else {
      val sb = new StringBuilder
      sb ++= "The given document doesn't live on this host.\n"
      sb ++= "It lives on one of these:\n"
      val hosts = HostsForLocation(location)
      if (hosts.isEmpty) {
        sb ++= "  <it lives nowhere>\n"
      } else {
        hosts.foreach { h => sb ++= s"  $h\n" }
      }
      throw new IllegalStateException(sb.result())
    }
  }

  def LocateDocumentByCollection(scope: ScopeID, doc: DocID): Location = {
    LocateIndexEntry(
      scope,
      NativeIndex.DocumentsByCollection(scope),
      Vector(IndexTerm(doc.collID.toDocID)),
      doc)
  }

  def LocateDocument(scope: ScopeID, doc: DocID): Location = {
    val buf = Tables.Versions.rowKeyByteBuf(scope, doc)
    repo.keyspace.locateKey(buf)
  }

  def LocateIndexEntry(
    scope: ScopeID,
    cfg: IndexConfig,
    terms: Vector[IndexTerm],
    doc: DocID): Location = {
    val termsWithPartition = if (cfg.isPartitioned) {
      terms :+ FieldIndexer(cfg).hashForConsole(doc)
    } else {
      terms
    }

    val buf = Tables.Indexes.rowKey(scope, cfg.id, termsWithPartition.map(_.value))
    repo.keyspace.locateKey(buf)
  }

  /** Returns all hosts that cover the given location.
    */
  def HostsForLocation(location: Location): Seq[NodeInfo] = {
    val hosts = repo.keyspace.allSegmentsByHost.collect {
      case (h, segs) if segs.exists { _.contains(location) } => h
    }
    val info = AllHostInfo()
    hosts.map { h => info.find(_.hostID == h) }.flatten.toSeq
  }

  def LocationIsLocal(location: Location): Boolean = {
    repo.keyspace.localSegments.exists { _.contains(location) }
  }

  /** Returns the host info for all nodes (this is the same information as
    * `faunadb-admin status`).
    */
  def AllHostInfo(): Seq[NodeInfo] = {
    val announcements =
      Await.result(
        repo.result(HealthCheckStore.getAll, Timestamp.Min).map { _.value },
        10.seconds)
    service.status(announcements)
  }

  /** This will scan through all documents in the documents index for the
    * provided scope and collection, and log documents that are
    * in the documents index but don't actually exit.
    * See IndexScanner to grab the log lines.
    * This also takes an optional function parameter that will be invoked
    * with every live document found to allow for additional processing
    * on each document if desired.
    */
  def ScanDocuments(
    scope: ScopeID,
    collection: CollectionID,
    docProcessor: Option[(Logger, Version.Live) => Query[Unit]] = None): Unit =
    DocumentScanner.scanDocs(repo, scope, collection, docProcessor)
}
