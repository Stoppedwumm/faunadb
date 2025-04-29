package fauna.storage.cassandra

import com.sun.management.UnixOperatingSystemMXBean
import fauna.atoms.{ Location, Segment }
import fauna.codex.cbor.CBOR.showBuffer
import fauna.exec.Timer
import fauna.lang._
import fauna.lang.syntax._
import fauna.net.bus.{ FileTransferContext, FileTransferManifest, MessageBus }
import fauna.stats.StatsRecorder
import fauna.storage._
import fauna.storage.ops.{ CassandraMutation, Mutation, Write }
import io.netty.buffer.{ ByteBuf, ByteBufAllocator, Unpooled }
import java.io.{ File, IOException, RandomAccessFile }
import java.lang.{ Boolean => JBoolean }
import java.lang.management.ManagementFactory
import java.nio.channels.FileChannel
import java.nio.file.{ Files, Path, StandardOpenOption }
import java.util.{
  ArrayList,
  Collections,
  ConcurrentModificationException,
  HashSet => JSet,
  UUID
}
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  Executors,
  Phaser,
  TimeoutException
}
import org.apache.cassandra.config.Schema
import org.apache.cassandra.db.{
  Cell => CCell,
  ColumnFamilyStore,
  DataRange,
  DataTracker,
  DecoratedKey,
  Keyspace,
  Mutation => CMutation,
  ReadCommand,
  RowPosition
}
import org.apache.cassandra.db.compaction.CompactionController
import org.apache.cassandra.db.compaction.CompactionManager
import org.apache.cassandra.db.composites.Composites
import org.apache.cassandra.db.filter.{
  ColumnSlice,
  ExtendedFilter,
  SliceQueryFilter
}
import org.apache.cassandra.dht.{ IncludingExcludingBounds, LongToken }
import org.apache.cassandra.io.sstable.{ Component, SSTable, SSTableReader }
import org.apache.cassandra.io.util.{ DataOutputBuffer, FileUtils }
import org.apache.cassandra.net.MessagingService
import org.apache.cassandra.service.StorageService
import scala.collection.immutable.ArraySeq
import scala.concurrent.{ blocking, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.Random

object CassandraStorageEngine {

  /**
    * Returned by `StorageEngine.readFailure`, for inspection of
    * failed transaction application.
    */
  case class ApplyFailure(
    key: ByteBuf,
    txnTime: Timestamp,
    failure: CMutation,
    mutations: Map[ByteBuf, CMutation])
}

final class CassandraStorageEngine(val config: StorageEngine.Config, keyspaceName: String = Cassandra.KeyspaceName)
    extends StorageEngine {

  // FIXME: This disgusting aberration exists because C* uses
  // reflection to allocate compaction strategies, which constrains
  // their ctor signature.
  CollectionStrategy.hints.setStatsRecorder(stats.stats)

  StatsRecorder.polling(10.seconds) {
    val cfs =
      StorageService.instance
        .getValidColumnFamilies(false /* allowIndexes */,
                                false /* autoAddIndexes */,
                                keyspaceName)
        .asScala

    val totalSSTablesPending =
      cfs.foldLeft(0) {
        case (acc, cf) =>
          try {
            stats.setTombstoneRatioForCF(cf.name, cf.getDroppableTombstoneRatio)
          } catch {
            // sporadically thrown by C* when calculating the sum of histograms
            case _: ConcurrentModificationException => ()
          }

          val count = cf.getUnleveledSSTables
          stats.setSSTablesPendingForCF(cf.name, count)
          acc + count
      }

    stats.setSSTablesPending(totalSSTablesPending)
    stats.setTombstonePurgePauses(CompactionController.ongoingTombstonePauses())
  }

  // FIXME: init order prevents us from passing this correctly
  final def keyspace: Keyspace = Keyspace.open(keyspaceName)

  override def applyMutationForKey(key: ByteBuf)(f: Mutation => Unit): Unit = {
    val mut = CassandraMutation(
      keyspaceName,
      key.nioBuffer(),
      index1CF = true,
      index2CF = config.dualWriteIndexCFs)
    f(mut)
    keyspace.apply(mut.cmut, false /* writeCommitLog */ )
  }

  override def compressionRatio(cf: String): Double =
    keyspace.getColumnFamilyStore(cf).getCompressionRatio

  override def columnFamilyNames: Set[String] = {
    val cfs = keyspace.getColumnFamilyStores
    val b = Set.newBuilder[String]

    cfs forEach { cf =>
      b += cf.name
    }

    b.result()
  }

  private def requireColumnFamily(cf: String): ColumnFamilyStore = {
    val cfs = keyspace.getColumnFamilyStores.stream

    val target = cfs.filter { _.name == cf } findFirst

    if (target.isEmpty) {
      throw new IllegalArgumentException(s"$cf does not exist in $keyspaceName")
    } else {
      target.get
    }
  }

  def needsCleanup(cf: String, keep: Seq[Segment]): Boolean = {
    val cfs = requireColumnFamily(cf)

    // Reference ALL THE THINGS!
    val range = CassandraHelpers.toRange(Segment.All)

    val sstables = cfs.selectAndReference { view =>
      val tree = DataTracker.buildIntervalTree(
        ColumnFamilyStore.CANONICAL_SSTABLES.apply(view))

      DataTracker.View.sstablesInBounds(range.toRowBounds,
        tree,
        StorageService.getPartitioner())
    }

    try {
      val ranges = keep map { CassandraHelpers.toRange(_) } asJava

      sstables.refs.stream
        .filter { sst => CompactionManager.needsCleanup(sst, ranges) }
        .findFirst
        .isPresent
    } finally {
      sstables.release()
    }
  }

  def cleanup(cf: String, keep: Seq[Segment]): Boolean = {
    import CompactionManager.AllSSTableOpStatus._

    val cfs = requireColumnFamily(cf)
    val ranges = keep map { CassandraHelpers.toRange(_) } asJava

    logger.debug(s"Storage cleanup of $cf started. Keeping $keep")

    val cm = CompactionManager.instance

    // jobs == 0 allows up to concurrent_compactors threads
    cm.performCleanup(cfs, ranges, 0 /* jobs */ ) match {
      case SUCCESSFUL =>
        logger.debug(s"Storage cleanup of $cf completed. Kept $keep")
        true

      case ABORTED =>
        logger.warn(s"Storage cleanup of $cf failed. Kept $keep")
        false
    }
  }

  override def hash(
    cf: String,
    segment: Segment,
    snapTime: Timestamp,
    deadline: TimeBound,
    maxDepth: Int,
    filterKey: ByteBuf => Boolean)(implicit ec: ExecutionContext): Future[HashTree] =
    if (columnFamilyNames contains cf) {
      if (segment == Segment(Location.MinValue,
                             CassandraHelpers.ForbiddenEndLocation)) {
        // Special case of [min, min + 1); in other words min-only single-token segment.
        // It can't be converted to a C* range (see CassandraHelpers.toRange), but we
        // know it's always empty.
        Future.successful(HashTree.newBuilder(segment, 0).result)
      } else {
        logger.info(s"Hashing segment $segment of $cf as of $snapTime")

        val syncF = if (snapTime <= persisted.get) {
          // no need to sync(); snapTime <= persisted <= LAT.
          logger.debug(
            s"Not waiting for applied timestamp: $snapTime <= $persisted.")
          Future.unit
        } else {
          waitForAppliedTimestamp(snapTime, deadline) map { _ =>
            // we have applied txns up to snapTime,
            // but their effects may still be in memtables.
            sync()
          }
        }

        syncF map { _ =>
          val cfs = keyspace.getColumnFamilyStore(cf)
          val range = CassandraHelpers.toRange(segment)
          val sstables = cfs.selectAndReference { view =>
            val tree = DataTracker.buildIntervalTree(
              ColumnFamilyStore.CANONICAL_SSTABLES.apply(view))
            DataTracker.View.sstablesInBounds(range.toRowBounds,
                                              tree,
                                              StorageService.getPartitioner())
          }

          try {
            val hash = new Hash(
              cfs,
              sstables.refs.asScala,
              segment,
              snapTime,
              maxDepth,
              deadline,
              stats.stats,
              HashFilter(filterKey))

            hash.get()
          } finally {
            sstables.release()
          }
        }
      }
    } else {
      val msg = s"Unknown column family $cf."
      Future.failed(new IllegalArgumentException(msg))
    }

  override def prepareTransfer(
    cf: String,
    segments: Vector[Segment],
    snapTime: Timestamp,
    ctx: FileTransferContext,
    bus: MessageBus,
    deadline: TimeBound,
    threads: Int = 1)(
    implicit ec: ExecutionContext): Future[Option[FileTransferManifest]] = {

    def prepare(): Future[Option[FileTransferManifest]] = Future {
      val cfs = keyspace.getColumnFamilyStore(cf)
      val view = cfs.selectAndReference { view =>
        val tree = DataTracker.buildIntervalTree(
          ColumnFamilyStore.CANONICAL_SSTABLES.apply(view))

        val sstables = new ArrayList[SSTableReader](segments.size)
        segments foreach { segment =>
          val ssts = DataTracker.View.sstablesInBounds(
            CassandraHelpers.toRange(segment).toRowBounds,
            tree,
            StorageService.getPartitioner())
          sstables.addAll(ssts)
        }

        sstables
      }

      try {
        val plan = blocking {
          streamPlan(view.sstables.asScala.toSeq, segments, threads)
        }

        if (plan.nonEmpty) {
          val handle = ctx.prepare(bus, plan map { t =>
            t.filename -> t
          } toMap)
          Some(handle.manifest)
        } else {
          None
        }
      } finally {
        view.release()
      }
    }

    if (columnFamilyNames contains cf) {
      if (snapTime <= persisted.get) {
        // no need to sync(); snapTime <= persisted <= LAT.
        logger.debug(
          s"Not waiting for applied timestamp: $snapTime <= $persisted.")
        prepare()
      } else {
        waitForAppliedTimestamp(snapTime, deadline) flatMap { _ =>
          // we have applied txns up to at least snapTime, but
          // their effects may still be in memtables.
          sync()
          prepare()
        } recover {
          case _: TimeoutException =>
            val msg =
              s"Transfer timed out: applied timestamp $appliedTimestamp is too far behind $snapTime."
            throw new TimeoutException(msg)
        }
      }

    } else {
      val msg = s"Unknown column family $cf."
      Future.failed(new IllegalArgumentException(msg))
    }
  }

  override def receiveTransfer(
    session: UUID,
    cf: String,
    tmpdir: Path,
    deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Unit] = {

    def timeout() =
      new TimeoutException(
        s"Transfer [$session]: Load of $cf from $tmpdir timed out.")

    if (columnFamilyNames contains cf) {
      val cfs = keyspace.getColumnFamilyStore(cf)

      val os = ManagementFactory.getOperatingSystemMXBean()

      val maxFiles = os match {
        case os: UnixOperatingSystemMXBean =>
          os.getMaxFileDescriptorCount
        case _ => Long.MaxValue
      }

      def openFiles = os match {
        case os: UnixOperatingSystemMXBean =>
          os.getOpenFileDescriptorCount
        case _ => 0
      }

      val fileLimit = maxFiles * config.openFileReserve

      def load(path: Path): Unit = {
        if (deadline.isOverdue) {
          throw timeout()
        }

        val file = FileChannel.open(path, StandardOpenOption.READ)

        val lock = cfs.directories.getWriteableLocationAsFile(file.size)

        if (lock eq null) {
          throw new IOException(s"Transfer [$session]: No disk space left.")
        }

        val sstable =
          try {
            val fileInfo = CompressedTransfer.loadFileInfo(path, file, stats.stats)
            CompressedTransfer.load(keyspaceName, fileInfo, file, stats.stats)
          } finally {
            lock.delete()
            FileUtils.closeQuietly(file)

            try {
              Files.delete(path)
            } catch {
              case NonFatal(ex) =>
                logger.warn(
                  s"Transfer [$session]: Failed to delete $path after transfer (${ex.getMessage})")
            }
          }

        val reader = sstable.closeAndOpenReader()

        // WARN: This _moves_ the reference to the DataTracker. Do not
        // release the reader ref here!
        cfs.addSSTable(reader)
      }

      def delay(path: Path): Future[Unit] =
        if (deadline.hasTimeLeft) {
          if (openFiles > fileLimit) {
            logger.warn(
              s"Transfer [$session]: Number of open files exceeds reserve percentage. " +
                s"Delaying load from $path. (open: $openFiles limit: $fileLimit.")

            Timer.Global.delay(30.seconds) {
              delay(path)
            }
          } else if (cfs.getUnleveledSSTables() > config.unleveledSSTableLimit) {
            logger.warn(
              s"Transfer: [$session]: Number of unleveled sstables exceeds limit. " +
                s"Delaying load from $path. " +
                s"(unleveled: ${cfs.getUnleveledSSTables} limit: ${config.unleveledSSTableLimit}.")

            Timer.Global.delay(30.seconds) {
              delay(path)
            }
          } else {
            Future { load(path) }
          }
        } else {
          Future.failed(timeout())
        }

      val paths = {
        val paths = Seq.newBuilder[Path]
        tmpdir foreachFile { paths += _ }
        paths.result()
      }

      // NB. SSTables are loaded incrementally for a given transferred segment. To
      // avoid compactions dropping tombstones that cover cells in later SSTable
      // loads, we MUST pause tombstone purges during the loading process.
      //
      // Note that SSTable pauses are ephemeral. Recovering from failures require the
      // entire segment to be loaded again. See `CompactionController` for safety
      // guarantees.
      val pause = JBoolean.getBoolean("fauna.transfer.pause-tombstone-purge")
      if (pause) {
        logger.info(s"Transfer [$session]: pausing tombstone purges.")
        CompactionController.pauseTombstonePurge()
      }

      paths.foldLeft(Future.unit) { case (fut, path) =>
        fut flatMap { _ => delay(path) }
      } ensure {
        if (pause) {
          CompactionController.unpauseTombstonePurge()
          logger.info(s"Transfer [$session]: tombstone purges were unpaused.")
        }
      }
    } else {
      val msg = s"Transfer [$session]: Unknown column family $cf."
      Future.failed(new IllegalArgumentException(msg))
    }
  }

  override def streamPlan(
    sstables: Seq[SSTableReader],
    segments: Seq[Segment],
    threads: Int = 1): Seq[CompressedTransfer] = {
    val transfers = new ConcurrentLinkedQueue[CompressedTransfer]
    val phaser = new Phaser(1)
    val pool =
      Executors.newFixedThreadPool(
        threads,
        new NamedPoolThreadFactory("Stream Plan"))

    val ranges = segments map { CassandraHelpers.toRange(_) } asJava

    sstables foreach { sstable =>
      phaser.register()
      pool.execute { () =>
        try {
          val ps = sstable.getPositionsForRanges(ranges)

          if (!ps.isEmpty) {
            val eks = sstable.estimatedKeysForRanges(ranges)
            transfers.add(new CompressedTransfer(sstable.ref, eks, ps, stats.stats))
          }
        } finally {
          phaser.arrive()
        }
      }
    }

    phaser.awaitAdvanceInterruptibly(phaser.arrive())
    pool.shutdown()

    ArraySeq.unsafeWrapArray(transfers.toArray(new Array[CompressedTransfer](0)))
  }

  override def bulkLoad(
    cf: String,
    root: File,
    threads: Int = 1): Seq[SSTableReader] = {
    val sstables = new ConcurrentLinkedQueue[SSTableReader]
    val phaser = new Phaser(1)
    val pool =
      Executors.newFixedThreadPool(threads, new NamedPoolThreadFactory("Bulk Load"))

    def visit(path: Path): Unit =
      if (!path.toFile.isDirectory && isValidData(path)) {
        val dir = path.getParent.toFile
        val name = path.getFileName.toString
        val desc = SSTable.tryComponentFromFilename(dir, name).left

        if (desc.cfname == cf) {
          getCFMetadata(desc.cfname) foreach { metadata =>
            val components = new JSet[Component]
            components.add(Component.DATA)
            components.add(Component.PRIMARY_INDEX)

            if (new File(desc.filenameFor(Component.SUMMARY)).exists()) {
              components.add(Component.SUMMARY)
            }

            if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists()) {
              components.add(Component.COMPRESSION_INFO)
            }

            if (new File(desc.filenameFor(Component.STATS)).exists()) {
              components.add(Component.STATS)
            }

            phaser.register()
            pool.execute { () =>
              try {
                val sstable = SSTableReader.openForBatch(
                  desc,
                  components,
                  metadata,
                  StorageService.getPartitioner)
                sstables.add(sstable)
              } catch {
                case ex: IOException => SSTableReader.logOpenException(desc, ex)
              } finally {
                phaser.arrive()
              }
            }
          }
        }
      }

    root.toPath.foreachFile(visit, ignoreErrors = true)

    phaser.awaitAdvanceInterruptibly(phaser.arrive())
    pool.shutdown()

    ArraySeq.unsafeWrapArray(sstables.toArray(new Array[SSTableReader](0)))
  }

  override def rowCFSlice(
    snapTime: Timestamp,
    key: ByteBuf,
    cfname: String,
    ranges: Seq[(ByteBuf, ByteBuf)],
    order: Order,
    count: Int,
    deadline: TimeBound): Vector[Cell] = {

    var scannedBytes = 0
    var unbornCount = 0
    var liveCount = 0
    var deadCount = 0

    val cells = Vector.newBuilder[Cell]

    @annotation.tailrec
    def getCells(qf: SliceQueryFilter, skipFirst: Boolean): Unit = {
      deadline.checkOrThrow()

      val cmd = ReadCommand.create(keyspaceName,
                                   key.nioBuffer,
                                   cfname,
                                   snapTime.millis,
                                   qf)
      val cf = cmd.getRow(keyspace).cf
      var lastCell: CCell = null.asInstanceOf[CCell]

      if ((cf ne null) && !cf.isMarkedForDelete()) {
        val iter = order match {
          case Order.Ascending =>
            cf.getReverseSortedColumns.iterator
          case Order.Descending =>
            cf.getSortedColumns.iterator
        }

        if (iter.hasNext && skipFirst) {
          lastCell = iter.next
        }

        var localUnborn = 0

        while (iter.hasNext && liveCount < count) {
          maybeCheckDeadline(deadline)

          lastCell = iter.next

          // This really shouldn't happen, but say something if it does.
          if (lastCell.timestamp < 0) {
            logger.warn(s"Negative cell timestamp for row ${key.toHexString} " +
              s"in cell ${lastCell.name.toByteBuffer.toHexString} in $cfname")
          }

          scannedBytes += lastCell.cellDataSize

          if (!lastCell.isLive(snapTime.millis)) {
            deadCount += 1
          } else if (lastCell.timestamp > snapTime.micros) {
            localUnborn += 1
            unbornCount += 1
          } else {
            val name = Unpooled.wrappedBuffer(lastCell.name.toByteBuffer)
            val value = Unpooled.wrappedBuffer(lastCell.value)
            val cts = Timestamp.ofMicros(lastCell.timestamp)

            cells += Cell(name, value, cts)
            liveCount += 1
          }
        }

        // We've filtered some unborn from the result set. Pull the next page.
        if (!iter.hasNext && localUnborn > 0 && liveCount < count && (lastCell ne null)) {
          val newQF = qf
            .withUpdatedCount((count - liveCount) + 1)
            .withUpdatedStart(lastCell.name, Tables.cfMetaData(keyspaceName, cfname).get)
          getCells(newQF, true)
        }
      }
    }

    deadline.checkOrThrow()

    val timing = Timing.start
    val cols = Array.newBuilder[ColumnSlice]

    cols.sizeHint(ranges)

    ranges foreach {
      case (f, t) =>
        cols += Tables.encodeSlice(keyspaceName, cfname, f, t)
    }
    val colsResult = cols.result()
    val slices =
      // optimize the most common case
      if (colsResult.lengthIs <= 1) {
        colsResult
      } else {
        val cfmetadata = Tables.cfMetaData(keyspaceName, cfname).get
        val comparator = order match {
          case Order.Ascending =>
            cfmetadata.comparator.reverseComparator
          case Order.Descending =>
            cfmetadata.comparator
        }
        ColumnSlice.deoverlapSlices(colsResult, comparator)
      }

    getCells(new SliceQueryFilter(slices, order == Order.Ascending, count), false)

    stats.recordReadTime(timing.elapsedMillis)
    stats.recordBytesRead(scannedBytes)
    stats.recordCellsRead(unbornCount + liveCount + deadCount)
    stats.recordDeletedCellsRead(deadCount)
    stats.recordUnbornCellsRead(unbornCount)

    cells.result()
  }

  override def scanSlice(
    snapTime: Timestamp,
    slice: ScanSlice,
    selector: Selector,
    count: Int,
    cellsPerRow: Int,
    deadline: TimeBound): (Vector[ScanRow], Option[(ByteBuf, ByteBuf)]) = {

    deadline.checkOrThrow()

    val cfs = keyspace.getColumnFamilyStore(slice.cf)

    val rowBounds = {
      val left = slice.bounds.left match {
        case Left(key) =>
          RowPosition.ForKey.get(key.nioBuffer, StorageService.getPartitioner)
        case Right(loc) => new LongToken(loc.token).minKeyBound()
      }
      new IncludingExcludingBounds[RowPosition](
        left,
        new LongToken(slice.bounds.right.token).minKeyBound())
    }

    val cs = Tables.encodeSlice(
      keyspaceName,
      slice.cf,
      slice.startCol,
      Unpooled.EMPTY_BUFFER)

    val dataRange = new DataRange.Paging(
      rowBounds,
      new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false /* reversed */, -1),
      cs.start,
      Composites.EMPTY,
      cfs.metadata) {

      override def contains(key: DecoratedKey): Boolean =
        selector.keep(slice.cf, Unpooled.wrappedBuffer(key.getKey))
    }

    val exFilter = ExtendedFilter.create(
      cfs,
      dataRange,
      null, // rowFilter
      count,
      true, // countCQL3Rows
      snapTime.millis)

    var scannedBytes = 0
    var unbornCount = 0
    var liveCount = 0
    var deadCount = 0

    val timing = Timing.start
    val rows = Vector.newBuilder[ScanRow]

    val rowIter = cfs.getRangeSlice(exFilter).iterator

    var key: ByteBuf = null
    var c: CCell = null

    while (rowIter.hasNext && liveCount < count) {
      deadline.checkOrThrow()

      val row = rowIter.next

      key = Unpooled.wrappedBuffer(row.key.getKey)
      c = null

      val cellIter = if ((row.cf eq null) || row.cf.isMarkedForDelete()) {
        Collections.emptyIterator
      } else {
        row.cf.getSortedColumns.iterator
      }

      val cells = Vector.newBuilder[Cell]
      var rowCellCount = 0

      while (cellIter.hasNext && liveCount < count && rowCellCount < cellsPerRow) {
        maybeCheckDeadline(deadline)

        c = cellIter.next

        scannedBytes += c.cellDataSize

        // This really shouldn't happen, but say something if it does.
        if (c.timestamp < 0) {
          logger.warn(
            s"Negative cell timestamp for row ${key.toHexString} " +
              s"in cell ${c.name.toByteBuffer.toHexString} in ${cfs.name}")
        }

        if (!c.isLive(snapTime.millis)) {
          deadCount += 1
        } else if (c.timestamp > snapTime.micros) {
          unbornCount += 1
        } else {
          val name = Unpooled.wrappedBuffer(c.name.toByteBuffer)
          val value = Unpooled.wrappedBuffer(c.value)
          val cts = Timestamp.ofMicros(c.timestamp)
          cells += Cell(name, value, cts)
          liveCount += 1
          rowCellCount += 1
        }
      }

      rows += ScanRow(slice.cf, key, cells.result())
    }

    logger.trace(
      s"Scanned $slice. $scannedBytes bytes in ${timing.elapsedMillis}msec. " +
        s"Found unborn=$unbornCount live=$liveCount dead=$deadCount as of $snapTime")

    stats.recordRangeReadTime(timing.elapsedMillis)
    stats.recordBytesRead(scannedBytes)
    val totalCellsCount = unbornCount + liveCount + deadCount
    stats.recordCellsRead(totalCellsCount)
    stats.recordDeletedCellsRead(deadCount)
    stats.recordUnbornCellsRead(unbornCount)

    val cursor = Option.when(
      (key ne null) && (c ne null) &&
        (totalCellsCount > 1 ||
          // If the caller only asked for one cell, and we only saw a
          // single dead/unborn cell, hand back a cursor.
          (count == 1 && liveCount == 0 && totalCellsCount >= 1))) {
      (key, Unpooled.wrappedBuffer(c.name.toByteBuffer))
    }

    (rows.result(), cursor)
  }

  protected def flushMemtables(
    cfsToFlush: Set[String],
    target: Timestamp,
    updatePersistedTimestamp: Boolean,
    totalTime: FiniteDuration) = {
    if (target > appliedTimestamp) {
      logger.warn("Memtables flushing with a target timestamp greater than appliedTimestamp.")
    }

    val cfs = Random.shuffle(if (cfsToFlush.isEmpty) {
      keyspace.getColumnFamilyStores.asScala
    } else {
      keyspace.getColumnFamilyStores.asScala.filter {
        cf => cfsToFlush.contains(cf.name)
      }
    })

    def randomizedSleep() =
      if (cfs.nonEmpty) {
        val interval = (totalTime.toMillis / cfs.size).toInt
        if (interval > 0) {
          try {
            Thread.sleep(Random.nextInt(interval) + (interval / 2))
          } catch {
            case _: InterruptedException => // ignore InterruptedException.
              ()
          }
        }
      }

    @annotation.tailrec
    def updatePersisted(): Unit = {
      val cur = persisted.get
      if (target > cur && isUnlocked) {
        if (persisted.compareAndSet(cur, target)) {
          logger.debug(s"Storage: Updating persisted timestamp to $target")
          updateMetadata(target, lockState.get)
        } else {
          updatePersisted()
        }
      }
    }

    if (persisted.get < target && isUnlocked) {
      cfs foreach { cf =>
        totalTime match {
          case Duration(0, _) => ()
          case _              => randomizedSleep()
        }

        logger.debug(s"Storage: Flushing CFStore $cf")
        val start = Timing.start
        cf.forceBlockingFlush()
        stats.flushTime(start.elapsedMillis)
      }

      if (updatePersistedTimestamp) updatePersisted()
    }
  }

  private def isValidData(candidate: Path) = {
    val pair = Option(candidate.getParent) flatMap { dir =>
      Option(
        SSTable.tryComponentFromFilename(dir.toFile, candidate.getFileName.toString))
    }

    pair exists { p =>
      val desc = p.left
      val index = new File(desc.filenameFor(Component.PRIMARY_INDEX))
      p.right.equals(Component.DATA) && !desc.`type`.isTemporary && index.exists()
    }
  }

  private def getCFMetadata(cf: String) =
    Option(Schema.instance.getKSMetaData(keyspaceName)) flatMap { ks =>
      Option(ks.cfMetaData.get(cf))
    }

  /**
    * Records a failed mutation and the set of mutations within the
    * same transaction to a file for later inspection. This method may
    * not throw a non-fatal exception.
    *
    * The file format is as follows:
    *
    * xx xx xx xx # transaction time (8B)
    * xx xx       # key size (4B)
    * ...         # key bytes
    * ...         # failed mutation
    * xx xx       # mutations count (4B)
    * ...         # mutations
    *
    * Each mutation is serialized using C*'s v2.1 format, which is as follows:
    * xx xx       # mutation key size (4B)
    * ...         # mutation key
    * xx xx       # modifications count (4B)
    * ...         # modifications
    *
    * See `ColumnFamilySerializer` for the format of each modification
    * within a mutation.
    */
  override protected[storage] def recordFailure(
    key: ByteBuf,
    txnTime: Timestamp,
    failure: Write,
    writes: Seq[Write]): Unit =
    try {
      val out = ByteBufAllocator.DEFAULT.buffer

      // dual with readKeyAndMutation()
      def writeKeyAndMutation(key: ByteBuf, mutation: CMutation) = {
        out.writeInt(key.readableBytes)
        out.writeBytes(key)

        val buf = new DataOutputBuffer()
        CMutation.serializer.serialize(mutation, buf, MessagingService.VERSION_21)
        out.writeBytes(buf.asByteBuffer)
      }

      val tsCellName = Tables.RowTimestamps.encode(txnTime)
      def convertWriteToMutation(write: Write): CMutation = {
        val m = CassandraMutation(
          keyspaceName,
          key.nioBuffer,
          index1CF = true,
          index2CF = config.dualWriteIndexCFs)
        m.add(Tables.RowTimestamps.CFName,
              tsCellName,
              Unpooled.EMPTY_BUFFER,
              txnTime,
              Tables.RowTimestamps.StorageEngineTTLSeconds)

        write.mutateAt(m, txnTime)
        m.cmut
      }

      out.writeLong(txnTime.micros)
      writeKeyAndMutation(key, convertWriteToMutation(failure))

      out.writeInt(writes.size)
      writes foreach { write =>
        writeKeyAndMutation(write.rowKey, convertWriteToMutation(write))
      }

      val name = s"transaction-failure-$txnTime"
      val path = config.rootPath / name
      val ch = FileChannel.open(path,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE_NEW)

      try {
        out.readAllBytes(ch)
        ch.force(true)
      } finally {
        logger.warn(
          s"Failed transaction for ${showBuffer(key)} at $txnTime recorded to $path.")
        ch.close()
      }
    } catch {
      case NonFatal(ex) =>
        logException(ex)
        logger.warn(
          s"Failed to record failed transaction for ${showBuffer(key)} at $txnTime.")
    }

  def readFailure(path: Path): CassandraStorageEngine.ApplyFailure = {
    val raf = new RandomAccessFile(path.toFile, "r")

    // dual with writeKeyAndMutation()
    def readKeyAndMutation(): (ByteBuf, CMutation) = {
      val keySize = raf.readInt
      val key = new Array[Byte](keySize)
      raf.read(key)

      val mut = CMutation.serializer.deserialize(raf, MessagingService.VERSION_21)
      (Unpooled.wrappedBuffer(key), mut)
    }

    try {
      val ts = Timestamp.ofMicros(raf.readLong)
      val (key, failure) = readKeyAndMutation()

      val m = Map.newBuilder[ByteBuf, CMutation]

      var mutations = raf.readInt
      while (mutations > 0) {
        val (k, mut) = readKeyAndMutation()
        m += k -> mut
        mutations = mutations - 1
      }

      CassandraStorageEngine.ApplyFailure(key, ts, failure, m.result())
    } finally {
      raf.close()
    }
  }

}
