package fauna.tx.consensus

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import fauna.exec.Timer
import fauna.lang.AtomicFile
import fauna.lang.syntax._
import fauna.net.bus._
import fauna.tx.log._
import io.netty.buffer.ByteBufAllocator
import java.nio.channels.FileChannel
import java.nio.file.Path
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, TimeoutException }
import scala.util.Random

final class ReplicatedStateTransport
  (logName: String, bus: MessageBus, signalID: SignalID, tmpDirectory: Path)
  (implicit ec: ExecutionContext) {

  private val protocol = Protocol[(TX, SignalID)](s"$logName.consensus.state.request")
  implicit private val replyProtocol =
    Protocol.Reply[(TX, SignalID), FileTransferManifest](s"$logName.consensus.state.reply")

  private val transfers = FileTransferContext(tmpDirectory, statsPrefix = s"$logName.State")

  def close() = bus.unbind(signalID)

  def setStateHandler(f: TX => Future[Option[Map[String, FileChannel]]]): Unit = {
    bus.handler(protocol, signalID) { case (from, (idx, replyTo), _) =>
      f(idx) map {
        _ foreach { files =>
          val manifest = transfers.prepareChannels(bus, files).manifest
          bus.sink(replyProtocol, replyTo.at(from)).send(manifest)
        }
      }
    }
  }

  def getState[T](peers: => Set[HostID], idx: TX)(f: Path => Future[T]): Future[T] = {
    val ps = peers
    if (ps.isEmpty) {
      // Logs that slave their ring to another log might end up in state
      // transfer following Reinit even before their master's ring was updated.
      // Instead of failing, we wait for peers to eventually become available
      // through the master log.
      Timer.Global.delay(1.second) { getState(peers, idx)(f) }
    } else {
      val sink = bus.sink(protocol, signalID.at(Random.choose(ps.toSeq)))

      sink.request((idx, _), 10.seconds.bound) flatMap {
        case Some(files) => transfers.receive(bus, files)(f)
        case None        => getState(peers, idx)(f) // try again
      } recoverWith {
        case _: TimeoutException => getState(peers, idx)(f) // try again
      }
    }
  }
}

object ReplicatedState {
  def apply[V, S: CBOR.Codec](
    log: ReplicatedLog[V],
    transport: ReplicatedStateTransport,
    dir: Path,
    name: String,
    init: => S,
    snapshotInterval: Int = 100,
    snapshotBeforeUpdate: Boolean = false)
    (f: (TX, S, V) => S)
    (implicit ec: ExecutionContext) = {

    val file = AtomicFile(dir / name + ".state")

    file create { c =>
      CBOR.encode(ByteBufAllocator.DEFAULT.buffer, (TX.MinValue, init)) releaseAfter { buf =>
        while (buf.isReadable) buf.readBytes(c, buf.readableBytes)
      }
    }

    new ReplicatedState[V, S](log, transport, file, snapshotInterval, snapshotBeforeUpdate, f)
  }
}

final class ReplicatedState[V, S: CBOR.Codec](
  log: ReplicatedLog[V],
  protected val transport: ReplicatedStateTransport,
  file: AtomicFile,
  snapshotInterval: Int,
  snapshotBeforeUpdate: Boolean,
  f: (TX, S, V) => S)(implicit ec: ExecutionContext)
    extends SnapshottedState[TX, V, S](log, file, snapshotInterval, snapshotBeforeUpdate, f) {

  override def close() = {
    transport.close()
    super.close()
  }

  def self = log.self
  def ring = log.ring
  def peers: Set[HostID] = ring - self

  override protected def logReinit(idx: TX) = {
    val loadFut = if (idxOrd.gt(idx, currentIdx)) {
      transport.getState(peers, idx)(loadState)
    } else {
      Future.unit
    }

    loadFut flatMap { _ =>
      // re-subscribe to the log at the current state's index
      super.subscribe()
      // end this subscription
      FutureFalse
    }
  }

  override protected def subscribe() = {
    transport.setStateHandler(prepareState)
    super.subscribe()
  }

  private def prepareState(minIdx: TX) = synchronized {
    // save first.
    if (persistedIdx < minIdx) save()

    if (persistedIdx < minIdx) FutureNone else {
      Future.successful(Some(Map("snapshot" -> file.read)))
    }
  }

  private def loadState(tmp: Path) = {
    val spath = tmp / "snapshot"
    val state = spath.read(decodeState)
    val sidx = state._1
    synchronized {
      if (idxOrd.lt(persistedIdx, sidx)) {
        // Received state is newer than persisted, so update persisted with it.
        setPersistedState(spath, sidx)
        if (idxOrd.lt(currentIdx, sidx)) {
          // Received state is also newer than current in-memory state, so update that too.
          setCurrentState(state)
        }
      } else {
        // Received state is not newer than persisted, so discard it
        spath.delete()
      }
    }
    Future.unit
  }
}
