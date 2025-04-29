package fauna.repo.service

import fauna.atoms._
import fauna.cluster._
import fauna.codex.cbor._
import fauna.codex.json.{ JSLong, JSObject }
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.lang.{ TimeBound, Timestamp, Timing }
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.{ HostInfo, RateLimiter }
import fauna.net.bus._
import fauna.repo.Restore
import fauna.stats._
import fauna.storage.{ Cassandra, CompressedTransfer, StorageEngine }
import fauna.tx.transaction.Partitioner
import java.io.{
  BufferedWriter,
  Closeable,
  File,
  FileNotFoundException,
  FileWriter,
  IOException
}
import java.nio.file.{
  DirectoryStream,
  FileAlreadyExistsException,
  FileVisitResult,
  Files,
  NoSuchFileException,
  Path,
  SimpleFileVisitor
}
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.{ ConcurrentHashMap, TimeoutException }
import java.util.regex.Pattern
import java.util.UUID
import org.apache.cassandra.io.sstable.SSTableReader
import org.apache.cassandra.service.StorageService
import scala.collection.mutable.{ ListBuffer, Map => MMap }
import scala.concurrent.{ blocking, Future, Promise }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace

final class IllegalBackupNameException(message: String)
    extends Exception(message)
    with NoStackTrace

final class UnexpectedBackupClusterException(message: String)
    extends Exception(message)
    with NoStackTrace

object RestoreStatusService {
  case class ColumnFamilyStatus(name: String, status: String, retries: Int)

  case class RestoreState(
    status: String,
    startTime: Timestamp,
    /** This can be searched in logs to track restore progress there.
      */
    sessionID: String,
    hostProgress: Map[String, Map[String, ColumnFamilyStatus]],
    endTime: Option[Timestamp] = None
  )
}

trait RestoreStatusService {
  import RestoreStatusService._

  /** Used to update the column family status for a given host.
    */
  def updateRestoreState(
    restoreID: UUID,
    hostID: HostID,
    columnFamilyStatus: ColumnFamilyStatus): Unit

  /** Returns the restore statuses for restores that have run
    * on this node.  Restore status is held in memory so if a node
    * is restarted or the fauna process is stopped any restore statuses
    * it had will not be returned on restart.
    */
  def getRestoreStatuses: Seq[RestoreState]
}

object BarService {
  /* This version should be changed if the ondisk data changes or major changes to
   * the BAR algorithm which would cause compatibility issues between versions. */
  private val BackupVersion = 1
  private val MetaDataFileName = "BackupInfo.json"

  // This character is not allowed in backup names
  val ClusterBackupSep = '.'
  val ClusterBackupExt = "CB"

  /* Backup Messaging -- sending */
  abstract sealed class BackupRequestMessage

  // FIXME: `ts` is unused
  case class BackupOpen(ts: Timestamp, replyTo: SignalID)
      extends BackupRequestMessage

  case class BackupExec(
    name: String,
    minTS: Timestamp,
    maxTS: Timestamp,
    replyTo: SignalID)
      extends BackupRequestMessage

  case class BackupClose(
    name: String,
    ok: Boolean,
    nodeCnt: Long,
    minTS: Timestamp,
    maxTS: Timestamp,
    replyTo: SignalID)
      extends BackupRequestMessage

  /* Backup Messaging -- Receive a Reply */
  abstract sealed class BackupReplyMessage

  trait ReplyV2Ack {
    def ok: Boolean
    def host: HostID
    def msg: String
  }

  case class BackupOpenAck(
    ok: Boolean,
    host: HostID,
    msg: String,
    ts: Timestamp,
    version: String,
    buildStamp: Long,
    gitHash: String)
      extends BackupReplyMessage
      with ReplyV2Ack {

    override def toString(): String =
      s"[Status: $ok, ts: $ts Msg: $msg  Host: $host]"
  }

  case class BackupExecAck(
    ok: Boolean,
    host: HostID,
    minTs: Timestamp,
    maxTs: Timestamp,
    msg: String)
      extends BackupReplyMessage
      with ReplyV2Ack

  case class BackupCloseAck(ok: Boolean, host: HostID, msg: String)
      extends BackupReplyMessage
      with ReplyV2Ack

  implicit val BackupRequestCodec =
    CBOR.SumCodec[BackupRequestMessage](
      CBOR.TupleCodec[BackupOpen],
      CBOR.TupleCodec[BackupExec],
      CBOR.TupleCodec[BackupClose]
    )

  implicit val BackupReplyCodec =
    CBOR.SumCodec[BackupReplyMessage](
      CBOR.TupleCodec[BackupOpenAck],
      CBOR.TupleCodec[BackupExecAck],
      CBOR.TupleCodec[BackupCloseAck]
    )

  implicit val BackupRequestProtocol =
    Protocol[BackupRequestMessage]("backup.request")

  implicit val BackupReplyProtocol =
    Protocol.Reply[BackupRequestMessage, BackupReplyMessage]("backup.reply")

  //  May contain only letter, numbers, underscore, dash
  //  must start with letter or number
  private final val pattern =
    Pattern.compile(
      """^[\p{IsAlphabetic}\d][\p{IsAlphabetic}\d_-]*(\\.[\p{IsAlphabetic}\d][\p{IsAlphabetic}\d_-]*)?""")

  /** Check for a valid backup name
    * only letters, digits dash or underscore
    * @param name  the snapshort name
    * @return true if a valid name is found
    */
  def isValidBackupName(name: String): Boolean = {
    name != null && pattern.matcher(name).matches
  }
}

class BarService(
  restoreSignal: SignalID,
  backupSignal: SignalID,
  bus: MessageBus,
  storage: StorageEngine,
  tmpDirectory: Path,
  chunkSize: Int,
  enableTransferDigests: Boolean,
  failureService: FailureService,
  membership: Membership,
  partitioner: => Partitioner[_, _],
  limiter: RateLimiter,
  stats: StatsRecorder,
  storagePath: Path,
  backupDir: Path,
  workerID: => Int)
    extends Closeable
    with ExceptionLogging
    with RestoreStatusService {

  import RestoreStatusService._

  // Ongoing transfers will register a promise here, which will be
  // completed if the FD observes the host restart.
  private[this] val restarts = new ConcurrentHashMap[HostID, Seq[Promise[Unit]]]

  /** Maps restore id to restore state for that restore.
    * Restore IDs are currently generated before kicking off the
    * restore and are used to map updates coming back from the
    * in progress Restore.scala.
    */
  private[this] val restoreStatus =
    new ConcurrentHashMap[UUID, RestoreState]

  failureService.subscribeStartsAndRestarts { host =>
    restarts.computeIfPresent(
      host,
      { case (_, ps) =>
        ps foreach { _.trySuccess(()) }
        null // Remove the mapping.
      })
  }

  import BarService._
  import Restore._

  private val transferCtx = FileTransferContext(
    tmpDirectory,
    chunkSize = chunkSize,
    enableDigests = enableTransferDigests,
    limiter = limiter,
    statsRecorder = stats,
    statsPrefix = "BackupService")

  private val restoreHandler = bus.handler(RestoreRequestProtocol, restoreSignal) {
    case (from, RestoreRequest(scope, cf, man, session, replyTo), deadline) =>
      require(scope == ScopeID.RootID)
      recvRestore(from, session, cf, man, replyTo.at(from.id), deadline)
  }

  private val backupHandler = bus.handler(BackupRequestProtocol, backupSignal) {

    case (from, BackupOpen(_, replyTo), deadline) =>
      recvBackupOpen(replyTo.at(from.id), deadline)

    case (from, BackupExec(name, minTs, maxTs, replyTo), deadline) =>
      recvBackupExec(name, minTs, maxTs, replyTo.at(from.id), deadline)

    case (from, BackupClose(name, ok, nodeCnt, minTs, maxTs, replyTo), deadline) =>
      recvBackupClose(
        from.id,
        name,
        ok,
        nodeCnt,
        minTs,
        maxTs,
        replyTo.at(from.id),
        deadline)

  }

  private val logger = getLogger()

  /** This function shutdown and release all resources of the barService
    */
  def close(): Unit = {
    restoreHandler.close()
    backupHandler.close()
  }

  def getRestoreStatuses: Seq[RestoreState] = restoreStatus.values().asScala.toSeq

  def updateRestoreState(
    restoreID: UUID,
    hostID: HostID,
    columnFamilyStatus: ColumnFamilyStatus) = {
    restoreStatus.computeIfPresent(
      restoreID,
      { case (_, v) =>
        v.copy(
          hostProgress = v.hostProgress + (hostID.toString -> (v.hostProgress
            .getOrElse(
              hostID.toString,
              Map.empty) + (columnFamilyStatus.name -> columnFamilyStatus))))
      }
    )
  }

  def restoreBackup(
    name: String,
    target: Option[String],
    threads: Int,
    deadline: TimeBound = TimeBound.Max): Future[Unit] = {
    val dir = new File(name)

    if (!dir.exists()) {
      return Future.failed(
        new IllegalArgumentException(s"Backup named [$name] does not exist."))
    }

    if (!dir.isDirectory) {
      return Future.failed(
        new IllegalArgumentException(s"Backup named [$dir] is not a directory"))
    }

    val restoreID = UUID.randomUUID()
    val session = UUID.randomUUID()
    val timer = Timing.start
    val timerMetricPoller = StatsRecorder.polling(1.minute) {
      stats.set("Restore.ElapsedTime", timer.elapsed.toSeconds)
    }
    restoreStatus.computeIfAbsent(
      restoreID,
      _ =>
        RestoreState(
          status = "InProgress",
          startTime = Clock.time,
          sessionID = session.toString,
          hostProgress = Map.empty))
    val dest = target map { t => s" to $t" } getOrElse ""
    logger.info(s"Transfer [$session]: Loading snapshot from $name$dest.")

    storage.columnFamilyNames
      .foldLeft(Future.unit) { (acc, cfName) =>
        acc flatMap { _ =>
          restoreOne(restoreID, session, cfName, dir, target, threads, deadline)
        }
      }
      .andThen {
        case Failure(_) =>
          restoreStatus.computeIfPresent(
            restoreID,
            { case (_, rs) =>
              rs.copy(status = "Failed", endTime = Some(Clock.time))
            })
        case Success(_) =>
          restoreStatus.computeIfPresent(
            restoreID,
            { case (_, rs) =>
              rs.copy(status = "Complete", endTime = Some(Clock.time))
            })
      } ensure { timerMetricPoller.cancel() }
  }

  def restoreOne(
    restoreID: UUID,
    session: UUID,
    cfName: String,
    sstables: Seq[SSTableReader],
    target: Option[String],
    deadline: TimeBound): Future[Unit] = {

    if (sstables.isEmpty) {
      logger.warn(
        s"Transfer [$session]: No sstables found for $cfName during restore.")
      Future.unit
    } else {
      val transfer =
        new Restore.CompressedBusTransfer(session, restoreSignal, bus, transferCtx)
      val restore =
        new Restore(
          restoreID,
          cfName,
          streamPlan(sstables, target),
          failureService,
          transfer,
          deadline,
          this
        )
      restore.run()
    }
  }

  private def restoreOne(
    restoreID: UUID,
    session: UUID,
    cfName: String,
    dir: File,
    target: Option[String],
    threads: Int,
    deadline: TimeBound): Future[Unit] = {

    if (!dir.isDirectory) {
      return Future.failed(new IllegalArgumentException(s"$dir is not a directory"))
    }

    val sstables = storage.bulkLoad(cfName, dir, threads)
    restoreOne(restoreID, session, cfName, sstables, target, deadline)
  }

  // computes the transfers necessary to move each sstable's contents
  // to each host currently responsible for same.
  private def streamPlan(
    sstables: Seq[SSTableReader],
    target: Option[String]): Map[HostID, Seq[CompressedTransfer]] = {
    val m = MMap.empty[HostID, Seq[CompressedTransfer]]

    val topology =
      target.fold {
        membership.hosts map { host => host -> partitioner.segments(host, true) }
      } { h =>
        val host = HostID(h)
        Set(host -> partitioner.segments(host, pending = true))
      }

    topology foreach { case (host, segments) =>
      val plan = storage.streamPlan(sstables, segments)

      if (plan.nonEmpty) {
        m += host -> plan
      }
    }

    m.toMap
  }

  private def recvRestore(
    from: HostInfo,
    session: UUID,
    cf: String,
    manifest: FileTransferManifest,
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {

    if (storage.columnFamilyNames contains cf) {
      transferCtx.receive(bus, manifest)(
        storage.receiveTransfer(session, cf, _, deadline)) flatMap { _ =>
        logger.info(
          s"Transfer [$session]: Restore of $cf from ${from.address.hostName}")
        bus.sink(RestoreReplyProtocol, replyTo).send(RestoreSuccess).unit
      } recover { case ex: FileTransferException =>
        logger.info(s"Transfer [$session]: " +
          s"Restore of $cf from ${from.address.hostName} failed. (Sender may retry.)")
        bus
          .sink(RestoreReplyProtocol, replyTo)
          .send(RestoreFailure(ex.getMessage))
          .unit
      }
    } else {
      logger.warn(
        s"Transfer [$session]: Received transfer from ${from.address.hostName}, but $cf is missing!")
      transferCtx.close(bus, manifest)
      bus
        .sink(RestoreReplyProtocol, replyTo)
        .send(RestoreFailure(s"$cf is missing"))
        .unit
    }
  }

  /** This returns a list of all the backups have have occurred on this node
    *
    * This is accomplished by looking at all directory names under the backup
    * directory.
    *
    * @return Future[TraversableOnce[String]]   A list of backup names
    */
  def listBackup(): Future[IterableOnce[String]] = {
    Future {
      blocking {
        try {
          val stream =
            Files.newDirectoryStream(
              backupDir,
              new DirectoryStream.Filter[Path] {
                override def accept(file: Path): Boolean = Files.isDirectory(file)
              })

          stream.iterator.asScala.map { _.getFileName.toString }
        } catch {
          case _: NoSuchFileException => Nil
        }
      }
    }
  }

  /* BACKUP ROUTINES */

  def createBackupReplica(
    backupName: Option[String],
    deadline: TimeBound = TimeBound.Max): Future[Seq[ReplyV2Ack]] = {
    val replicaHosts: Set[HostID] = membership.hosts filter {
      membership.isLocal
    }

    val name = (backupName, membership.localReplica) match {
      case (Some(n), _)    => n
      case (None, Some(r)) =>
        // Ensure to update the region separator
        r.replace('/', ClusterBackupSep)
      case (_, _) =>
        throw new IllegalStateException(
          s"Backup node not a member of a valid replica.")
    }

    createBackupCluster(replicaHosts, name, deadline)
  }

  /** Build a list of replicas to backup.
    *     In the future when we deal with data locality we might
    *     pass this in as a function.
    * Check for all nodes in the replica to be alive
    *     If not alive then abort with an error
    * TODO: Check all nodes to have transaction log backups enabled
    *     If not send a warning out that the backups are not that usefull
    *
    * Send a request to open a backup on all nodes in our list
    *     Each node will execute recvBackupOpen
    *     If successful then each node will responde with the timestamp
    *     of their persisted data.
    * Min and Max persisted timestamp is calculated for the backup
    *
    * Send a message to all nodes in our list to do a local backup
    *     Each node will execute recvBackupExec
    *
    * Send a message to all nodes in our list to close the backup
    *     Each node will execute recvBackupClose
    *
    *  At any point in time a failure can occur and the calling sequence
    *  open -> exec -> close can be interrupted.  This generally will occur
    *  because of network timeouts and unexpected network messages.
    *
    * @param hostList    The list of HostID to backup
    * @param backupName  The name give to this backup
    * @return
    */
  private def createBackupCluster(
    hostList: Set[HostID],
    backupName: String,
    deadline: TimeBound): Future[Seq[ReplyV2Ack]] = {
    /* Sanity check the system to make sure all the nodes we are going to send a
     * backup request are alive and well and we can get the names from the membership
     * service */

    if (!isValidBackupName(backupName)) {
      throw new IllegalBackupNameException(
        s"Snapshot name [${backupName}] contains illegal characters. The name must start with a letter followed by letters, numbers, dashes and underscores.")
    }

    if (!hostList.forall { failureService.fd.isAlive(_) }) {
      throw new IllegalStateException(
        s"All nodes in the replica(${membership.replica}) must be operational.")
    }

    val bum = membership.localHostName
    val hnList = hostList map {
      membership.getHostName(_)
    }

    // If we can not get name from the backup system then stop now
    if (bum.isEmpty || hnList.forall { _.isEmpty }) {
      throw new IllegalStateException(
        s"Membership does not contain all host names for the backup($backupName).")
    }

    logger.info(
      s"Backup(${backupName}) started on ${bum} hosts ${hnList.flatten.mkString(",")}.")

    // time the backup started
    val startTime: Timestamp = Clock.time

    // Send a message to each node in the replica to setup for a cluster backup
    val srcOpen = bus.tempSource(BackupReplyProtocol)
    hostList.foreach { host =>
      bus
        .sink(BackupRequestProtocol, backupSignal.at(host))
        .send(BackupOpen(startTime, srcOpen.id.signalID), deadline)
    }

    // Wait for all the open messages to complete
    val openAck = waitForAckOpen(srcOpen, hostList.size, deadline)

    val execAck = openAck flatMap { oa =>
      /* if all open message return success then exec the backup else close with
       * errors */
      if ((oa forall { _.ok }) && oa.size == hostList.size) {
        sendExecMsg(oa, backupName, hostList, deadline)
      } else {
        openAck
      }
    }
    execAck flatMap { ea =>
      sendCloseMsg(ea, backupName, hostList, deadline)
    }
  }

  private def sendExecMsg(
    openAck: Seq[ReplyV2Ack],
    backupName: String,
    hostList: Set[HostID],
    deadline: TimeBound): Future[Seq[ReplyV2Ack]] = {

    val openA = openAck.collect { case a: BackupOpenAck => a }
    /* minTs & maxTs define the critical region that must be played through the
     * transaction log. */
    val minTs = openA.minBy { _.ts }.ts
    val maxTs = openA.maxBy { _.ts }.ts

    val src = bus.tempSource(BackupReplyProtocol)

    // For each successful open message send an exec message
    // Send a message to each node in the replica to start the backup

    openA foreach { x =>
      bus
        .sink(BackupRequestProtocol, backupSignal.at(x.host))
        .send(BackupExec(backupName, minTs, maxTs, src.id.signalID), deadline)
    }

    waitForAckExec(src, hostList.size, deadline)
  }

  private def sendCloseMsg(
    execAck: Seq[ReplyV2Ack],
    backupName: String,
    hostList: Set[HostID],
    deadline: TimeBound): Future[Seq[ReplyV2Ack]] = {
    /* Ensure all message have responded success and we received messages from all
     * hosts */
    val backupResult = execAck.forall { _.ok } && (hostList.size == execAck.size)

    // Send a message to each node in the replica to close the backup
    val src = bus.tempSource(BackupReplyProtocol)
    execAck foreach {
      case BackupExecAck(_, host, minTs, maxTs, _) =>
        bus
          .sink(BackupRequestProtocol, backupSignal.at(host))
          .send(
            BackupClose(
              backupName,
              backupResult,
              hostList.size,
              minTs,
              maxTs,
              src.id.signalID),
            deadline)
      case BackupOpenAck(_, host, _, _, _, _, _) =>
        bus
          .sink(BackupRequestProtocol, backupSignal.at(host))
          .send(
            BackupClose(backupName, false, -1, null, null, src.id.signalID),
            deadline)

      case msg =>
        throw new IllegalStateException(s"Unexpected message $msg.")
    }

    //  Wait for the close to complete
    waitForAckClose(src, hostList.size, deadline)
  }

  /** This waits for all Open messages to responds and
    * decode them.  It will ensure all node responded or timeout.
    *
    * @param src      - The message source we recieve our message on
    * @param numMsg   - The number of messages we need to receive
    * @param deadline - The timeout
    * @return         - A Sequence of open acknowledgements
    */
  private def waitForAckOpen(
    src: Source[BackupReplyMessage],
    numMsg: Int,
    deadline: TimeBound): Future[Seq[ReplyV2Ack]] = {
    src.fold(Seq.empty[ReplyV2Ack], deadline.timeLeft) {
      case (acc, Source.Messages(rs)) =>
        val results: Seq[ReplyV2Ack] = rs map { reply =>
          reply.value match {
            case boa: BackupOpenAck => boa
            case _ =>
              throw new UnexpectedBackupClusterException(
                "Receiving an unexpected acknowlegement message for Open.")
          }
        }

        val n: Seq[ReplyV2Ack] = acc ++ results
        if (n.size == numMsg) {
          Future.successful(Right(n))
        } else {
          Future.successful(Left(n))
        }
      case _ =>
        val msg = s"Backup timed out."
        logger.error(msg)
        Future.failed(new TimeoutException(msg))
    }
  }

  /** This waits for all Exec messages to responds and
    * decode them.  It will ensure all node responded or timeout.
    *
    * @param src      - The message source we recieve our message on
    * @param numMsg   - The number of messages we need to receive
    * @param deadline - The timeout
    * @return         - A Sequence of open acknowledgements
    */
  private def waitForAckExec(
    src: Source[BackupReplyMessage],
    numMsg: Int,
    deadline: TimeBound): Future[Seq[ReplyV2Ack]] = {
    src.fold(Seq.empty[ReplyV2Ack], deadline.timeLeft) {
      case (acc, Source.Messages(rs)) =>
        val results: Seq[ReplyV2Ack] = rs map { reply =>
          reply.value match {
            case bea: BackupExecAck => bea
            case _ =>
              throw new UnexpectedBackupClusterException(
                "Receiving an unexpected acknowlegement message for execute.")
          }
        }

        val n: Seq[ReplyV2Ack] = acc ++ results
        if (n.size == numMsg) {
          Future.successful(Right(n))
        } else {
          Future.successful(Left(n))
        }
      case _ =>
        val msg = s"Backup timed out."
        logger.error(msg)
        Future.failed(new TimeoutException(msg))
    }
  }

  /** This waits for all Close messages to responds and
    * decode them.  It will ensure all node responded or timeout.
    *
    * @param src      - The message source we recieve our message on
    * @param numMsg   - The number of messages we need to receive
    * @param deadline - The timeout
    * @return         - A Sequence of open acknowledgements
    */
  private def waitForAckClose(
    src: Source[BackupReplyMessage],
    numMsg: Int,
    deadline: TimeBound): Future[Seq[ReplyV2Ack]] = {
    src.fold(Seq.empty[ReplyV2Ack], deadline.timeLeft) {
      case (acc, Source.Messages(rs)) =>
        val results: Seq[ReplyV2Ack] = rs map { reply =>
          reply.value match {
            case bca: BackupCloseAck => bca
            case _ =>
              throw new UnexpectedBackupClusterException(
                "Receiving an unexpected acknowlegement message for Close.")
          }
        }

        val n: Seq[ReplyV2Ack] = acc ++ results
        if (n.size == numMsg) {
          Future.successful(Right(n))
        } else {
          Future.successful(Left(n))
        }
      case _ =>
        val msg = s"Backup timed out."
        logger.error(msg)
        Future.failed(new TimeoutException(msg))
    }
  }

  /** This routine make a backup name
    * If this is a node snapshot then the timestamp is not provided
    * If this is a replica/cluster backup (i.e. more than one node) then
    * the timestamp is provided
    * *
    * @param name   - The name/label of the backup
    * @param minTs  - If this backup is a cluster backup then contains the
    *                 the timestamp
    * @return       - The on disk name of the backup
    */
  private def makeClusterBackupName(
    name: String,
    minTs: Option[Timestamp]): String = {
    minTs match {
      case Some(ts) =>
        new StringBuffer()
          .append(name)
          .append(ClusterBackupSep)
          .append(workerID)
          .append(ClusterBackupSep)
          .append(ts.seconds)
          .append(ClusterBackupSep)
          .append(ClusterBackupExt)
          .toString
      case None => name
    }
  }

  /** The backup Coordinator will call this function first on every node
    * participating in a backup.  The `recvBackupOpen` prepares the sytem
    * to execute a backup.
    * 1. TODO Check to see if the logs are being backup
    * 2. Flush the storage and try and advance persisted timestamp
    * 3. Get the minpersisted timestamp to send to the BUM coordinator
    *
    * If we perform all our work send a successful return
    * If we failed then return an error back to the coordinator
    *
    * @param backupCord   - The host which is started/managing the backup
    * @param replyTo      - Who to respond to
    * @param deadline     - The timeout
    * @return
    */
  private def recvBackupOpen(
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {

    // We should check that transaction log are being back up on this node
    val logBackupEnabled = true

    //  Flush the system and move up our persisted timestamp
    storage.sync()

    //  Get the timestamp of what has been saved to disk
    val ts: Timestamp = storage.persistedTimestamp

    logger.debug(s"recvBackupOpen Coord TS ${ts.toString}")

    bus
      .sink(BackupReplyProtocol, replyTo)
      .send(
        BackupOpenAck(
          logBackupEnabled,
          membership.self,
          "BackupOpen",
          ts,
          BuildInfo.version,
          BuildInfo.buildStamp,
          BuildInfo.gitFullHash),
        deadline)
      .unit
  }

  /**  The backup coordinator will call this function for all nodes executing
    *  a backup after all node have successfully executed a BackupOpen
    *  1. This function will backup all the data required for a restore.
    *
    * If we failed then return an error back to the coordinator
    *
    * @param backupCord   - The host which is started/managing the backup
    * @param name         - The name of the backup
    * @param replyTo      - Who to respond to
    * @param deadline     - The timeout
    * @return
    */
  private def recvBackupExec(
    name: String,
    minTs: Timestamp,
    maxTs: Timestamp,
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {
    logger.debug(s"recvBackupExec ${name} minTs: ${minTs}  maxTs: ${maxTs}")

    createBackup(Cassandra.KeyspaceName, name, Option(minTs)) transform {
      case Success(_) =>
        Success(BackupExecAck(true, membership.self, minTs, maxTs, "BackupExec"))
      case Failure(value) =>
        Success(
          BackupExecAck(
            false,
            membership.self,
            minTs,
            maxTs,
            "BackupExec Error " + value.getMessage))
    } flatMap { msg =>
      bus.sink(BackupReplyProtocol, replyTo).send(msg, deadline).unit
    }
  }

  /**  The backup coordinator will call this function to close down a backup.
    *  This could be a successful or failed backup.
    *  This function will report if the backup was successful or not,
    *  by creating a metadata file only if the backup was completed
    *  without errors.
    *
    * If we perform all our work send a successful return
    * If we failed then return an error back to the coordinator
    *
    * @param backupCord   - The host which is started/managing the backup
    * @param name         - The name/label of the backup
    * @param backupOK     - Did the backup complete OK
    * @param replyTo      - Who to respond to
    * @param deadline     - The timeout
    * @return
    */
  private def recvBackupClose(
    backupCord: HostID,
    name: String,
    backupOK: Boolean,
    nodeCount: Long,
    minTs: Timestamp,
    maxTs: Timestamp,
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {

    val bum = membership.getHostName(backupCord)
    val me = membership.localHostName

    logger.debug(s"recvBackupClose status = $backupOK")

    val (ret, msg) = (bum, me, backupOK) match {
      case (Some(bum), Some(me), true) =>
        createBackupMetaData(name, backupOK, nodeCount, minTs, maxTs, bum, me)
        (true, s"Backup($name) started by $bum on $me completed successfully.")
      case (_, _, true) =>
        (
          false,
          s"Backup($name) failed to retrieve membership information ME $me BUM $bum.")
      case (_, _, _) =>
        (false, s"Backup($name) started by $bum on $me failed.")
    }

    if (ret) {
      logger.info(msg)
    } else {
      logger.error(msg)
    }

    bus
      .sink(BackupReplyProtocol, replyTo)
      .send(BackupCloseAck(ret, membership.self, msg), deadline)
      .unit
  }

  /** Only after an individual node backup successfully completes then a
    * meta data file containing all the descriptive data about the backup
    * is generated in a descriptive format.
    *
    * @param backupName   The name of the backup
    * @param minTs        The timestamp of when the first backup started
    * @param maxTs        The timestamp of when the last backup started
    */
  private def createBackupMetaData(
    backupName: String,
    backupOk: Boolean,
    numberOfNodes: Long,
    minTs: Timestamp,
    maxTs: Timestamp,
    bum: String,
    me: String): Unit = {
    val targetRoot =
      backupDir.resolve(makeClusterBackupName(backupName, Option(minTs)))
    /* Make sure the backup does exists before writing meta data */
    if (!Files.exists(targetRoot)) {
      throw new FileNotFoundException("Backup directory does not exists.")
    }

    val meta = JSObject(
      "backup_version" -> JSLong(BackupVersion),
      "backup_manager" -> bum,
      "myself" -> me,
      "name" -> backupName,
      "tsmin" -> minTs.toString,
      "tsmax" -> maxTs.toString,
      "node_count" -> numberOfNodes,
      "fauna_version" -> BuildInfo.version,
      "revision" -> BuildInfo.revision,
      "githash" -> BuildInfo.gitFullHash,
      "build_stamp" -> BuildInfo.buildStamp,
      "build_timestamp" -> BuildInfo.buildTimestamp,
      "successful_backup" -> backupOk
    )

    val file = new File(targetRoot.toFile, MetaDataFileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(meta.toPrettyString)
    bw.close()
  }

  /** To take a backup/snapshot of the give node.  It does this by
    *   1. Ensure the name provided will create a unique backup directory for Fauna
    *   2. Requesting that cassandra takes a backup with the given name
    *   3. Creates a list of all directories that Cassandra created for its backup
    *   4. Create the Fauna directory where the backup will reside
    *   5. Move this list of cassandra directores and contents to Fauna's backup location
    *
    * @param backupName  The name give to this snapshot
    * @param ts          If this is a multi-node backup then provide a timestamp
    * @return
    */
  def createBackup(
    keyspaceName: String,
    backupName: String,
    ts: Option[Timestamp] = None): Future[(Timestamp, Path)] = {
    Future {
      blocking {
        /* If we are creating a replica backup then append the timestamp to each
         * backup name. */
        val name = if (!isValidBackupName(backupName)) {
          throw new IllegalBackupNameException(
            "Snapshot name contains illegal characters. The name must start with a letter followed by letters, numbers, dashes and underscores.")
        } else {
          makeClusterBackupName(backupName, ts)
        }
        val dataPath = storagePath.resolve("data").resolve(keyspaceName)
        val targetRoot = backupDir.resolve(name)

        /* Make sure the backup does not exists before going to C* */
        if (Files.exists(targetRoot)) {
          throw new FileAlreadyExistsException(
            s"Snapshot directory ${targetRoot}) already exists.")
        }

        val persisted = storage.persistedTimestamp

        try {
          StorageService.instance.takeSnapshot(name, keyspaceName)
        } catch {
          case _: IOException => throw new IOException("Backup Manager I/O error.")
          case ex: Throwable =>
            throw new IllegalArgumentException(
              "Backup Manager configuration error.",
              ex)
        }

        val dirsToMove = ListBuffer.empty[Path]

        Files.walkFileTree(
          dataPath,
          new SimpleFileVisitor[Path] {
            override def preVisitDirectory(
              dir: Path,
              attrs: BasicFileAttributes): FileVisitResult = {
              if (
                dir.getParent.getFileName.toString
                  .contentEquals("snapshots") && dir.getFileName.toString
                  .contentEquals(name)
              ) {
                dirsToMove += dir
                FileVisitResult.SKIP_SUBTREE
              } else FileVisitResult.CONTINUE
            }

            override def visitFileFailed(
              file: Path,
              ex: IOException): FileVisitResult = {
              if (
                file.getFileName.toString.contains("-tmp-")
                || !file.getFileName.toString.contains("snapshots")
              ) {
                // squelch non-fatal errors on tempfiles and any file _not_
                // found in the snapshots directory (other files subject to
                // compaction etc).
                FileVisitResult.CONTINUE
              } else {
                throw ex
              }
            }
          }
        )

        Files.createDirectories(targetRoot)
        dirsToMove foreach { dirToMove =>
          val cfName =
            dirToMove.getName(dirToMove.getNameCount - 3) // cf portion of the path
          val targetPath = targetRoot.resolve(cfName)
          Files.move(dirToMove, targetPath)
        }

        (persisted, targetRoot)
      }
    }
  }
}
