package fauna.repo.test

import fauna.atoms._
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.repo._
import fauna.repo.service.{ RestartService, RestoreStatusService }
import fauna.repo.service.RestoreStatusService._
import io.netty.buffer._
import java.nio.file.Files
import java.util.UUID
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._

class RestoreSpec extends Spec {

  val busContext = MessageBusTestContext.default.build()

  override def afterAll() = busContext.stopAll()

  class Restarter extends RestartService {
    def subscribeStartsAndRestarts(f: HostID => Unit): Handle = new Handle
    def unsubscribeStartsAndRestarts(handle: Handle): Unit = ()
  }

  class MockRestoreStatus extends RestoreStatusService {
    override def updateRestoreState(
      restoreID: UUID,
      hostID: HostID,
      columnFamilyStatus: ColumnFamilyStatus): Unit = ()

    /** Returns the restore statuses for restores that have run
      * on this node.  Restore status is held in memory so if a node
      * is restarted or the fauna process is stopped any restore statuses
      * it had will not be returned on restart.
      */
    override def getRestoreStatuses: Seq[RestoreState] = Seq.empty
  }

  class Transfer(val context: FileTransferContext)
      extends Restore.Transfer[ChunkedByteBuf] {

    val session = UUID.randomUUID()
    val bus = busContext.createBus()
    val source = bus.tempSource(Restore.RestoreReplyProtocol)

    def send(
      columnFamily: String,
      dest: HostID,
      transfers: Seq[ChunkedByteBuf],
      restart: Promise[Unit])(
      implicit ec: ExecutionContext): Future[Source[Restore.RestoreReply]] =
      Future.successful(source)
  }

  def makeTransfer(chunkSize: Int = 16): Transfer = {
    val tmpdir = Files.createTempDirectory("restore-")
    tmpdir.toFile.deleteOnExit()

    val ctx = FileTransferContext(tmpdir, chunkSize = chunkSize)
    new Transfer(ctx)
  }

  "Restore" - {
    implicit val ec = ExecutionContext.global

    "works" in {
      val cf = "TEST"
      val buf = PooledByteBufAllocator.DEFAULT.buffer(1024).writeZero(1024)

      val restarts = new Restarter
      val transfer = makeTransfer()

      val file = new ChunkedByteBuf(buf)
      val stream = Map(transfer.bus.hostID -> Seq(file))

      val restore = new Restore(
        UUID.randomUUID(),
        cf,
        stream,
        restarts,
        transfer,
        2.seconds.bound,
        new MockRestoreStatus())

      transfer.bus.send(
        transfer.source.id.signalID,
        transfer.bus.hostInfo.id,
        Restore.RestoreSuccess,
        "load complete")

      Await.result(restore.run(), 2.minutes)
    }

    "load failure" in {
      val cf = "TEST"
      val buf = PooledByteBufAllocator.DEFAULT.buffer(1024).writeZero(1024)

      val restarts = new Restarter
      val transfer = makeTransfer()

      val file = new ChunkedByteBuf(buf)
      val stream = Map(transfer.bus.hostID -> Seq(file))

      val restore = new Restore(
        UUID.randomUUID(),
        cf,
        stream,
        restarts,
        transfer,
        2.seconds.bound,
        new MockRestoreStatus())

      transfer.bus.send(
        transfer.source.id.signalID,
        transfer.bus.hostInfo.id,
        Restore.RestoreFailure("oops"),
        "failed")

      a[RestoreFailedException] shouldBe thrownBy {
        Await.result(restore.run(), 2.minutes)
      }
    }
  }
}
