package fauna.repo.service

import fauna.atoms.HostID
import fauna.cluster._
import fauna.codex.cbor._
import fauna.exec._
import fauna.lang._
import fauna.lang.syntax._
import fauna.logging.ExceptionLogging
import fauna.net.bus._
import fauna.stats._
import java.io.Closeable
import java.util.concurrent.{
  ConcurrentHashMap,
  CopyOnWriteArraySet => COWSet,
  Executors,
  TimeUnit,
  TimeoutException
}
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random

/** The failure service measures liveness across the cluster using a
  * play in three acts, (intentionally) very similar to the C* Gossip
  * protocol:
  *
  * === Act I ===
  *   Pick a random suspected-alive member of the cluster, and
  *   send it all of the Heartbeats we've seen.
  *
  * === Act II ===
  *   Upon receiving a list of Heartbeats from the first member, the
  *   second member finds all of _its_ Heartbeats which have higher
  *   generation or version numbers. It sends those back to the first
  *   member. It then finds all of its Heartbeats with _lower_
  *   generation or version numbers, and updates its local copy.
  *
  *   Any heartbeats which have been updated indicate that their
  *   corresponding processes are still alive; foreach of these,
  *   inform the failure detector.
  *
  * === Act III ===
  *   The first member, now receiving all Heartbeats for which the
  *   second member had newer information, updates its local copy and
  *   informs its failure detector.
  *
  * Concurrently, all of the same actions are attempted on a
  * suspected-dead member of the cluster to resurrect it.
  */
object FailureService {
  val MaxGenerationDelta = 365.days

  object Heartbeat {
    implicit val Codec = CBOR.TupleCodec[Heartbeat]
  }

  /** Heartbeats are maintained per active member of the cluster. Their
    * rules are simple:
    *
    * - generation is set to the current time when a process starts
    * - version is initialized to 0 when a process starts
    * - version is incremented when a failure detection pass is made
    *
    * Other members measure the rate at which these two values grow. If
    * a remote process fails to publish heartbeats growing at the
    * expected rate, the remote is considered unreachable.
    */
  case class Heartbeat(generation: Timestamp, version: Int)

  /** The protocol is a three-way handshake, like TCP:
    *
    * A                            B
    * | ----------> SYN ---------> |
    * |                            |
    * | <-------- SYN/ACK <------- |
    * |                            |
    * | ----------> ACK ---------> |
    */
  case class Syn(heartbeats: Map[HostID, Heartbeat], replyTo: SignalID)
  case class SynAck(heartbeats: Map[HostID, Heartbeat], replyTo: SignalID)
  case class Ack(heartbeats: Map[HostID, Heartbeat])

  implicit val SynCodec = CBOR.TupleCodec[Syn]
  implicit val SynAckCodec = CBOR.TupleCodec[SynAck]
  implicit val AckCodec = CBOR.TupleCodec[Ack]

  private val executor =
    Executors.newScheduledThreadPool(
      1,
      new NamedPoolThreadFactory("Failure Detector", true))

  def run(svc: FailureService): Unit =
    executor.scheduleWithFixedDelay(
      svc,
      FailureDetector.CheckInterval.toMillis,
      FailureDetector.CheckInterval.toMillis,
      TimeUnit.MILLISECONDS)
}

case class FailureService(
  signal: SignalID,
  bus: MessageBus,
  membership: Membership,
  fd: FailureDetector,
  stats: StatsRecorder)
    extends Closeable
    with Runnable
    with ExceptionLogging
    with RestartService {

  import FailureService._

  implicit val ec = FaunaExecutionContext.Implicits.global

  private val SynProtocol = Protocol[Syn]("failure.syn")
  private val SynAckProtocol = Protocol.Reply[Syn, SynAck]("failure.synack")
  private val AckProtocol = Protocol[SynAck]("failure.ack")
  private val AckReplyProtocol = Protocol.Reply[SynAck, Ack]("failure.ack.reply")

  private val synHandler = bus.handler(SynProtocol, signal) {
    case (from, Syn(hbs, replyTo), deadline) =>
      recvSyn(hbs, replyTo.at(from.id), deadline)
  }

  private val logger = getLogger

  @volatile private[this] var closed = false
  private[this] val peers = new COWSet[HostID]()
  private[this] val heartbeats = new ConcurrentHashMap[HostID, Heartbeat]

  membership.subscribeWithLogging(membershipChange)

  def close() = {
    closed = true
    synHandler.close()
    executor.shutdown()
  }

  private val subscribers = new ConcurrentHashMap[Handle, HostID => Unit]()

  def subscribeStartsAndRestarts(f: HostID => Unit): Handle = {
    val handle = new Handle
    subscribers.put(handle, f)
    handle
  }

  def unsubscribeStartsAndRestarts(handle: Handle) =
    subscribers.remove(handle)

  private def notifyStartOrRestart(hostID: HostID): Unit =
    subscribers.forEachValue(Long.MaxValue, { f => f(hostID) })

  def run(): Unit = {
    Option(heartbeats.get(membership.self)) match {
      case None =>
        // we're (re)starting, publish a fresh generation and version
        heartbeats.put(membership.self, Heartbeat(fd.clock.time, 0))
      case Some(hb) =>
        // just another loop; incr version.
        heartbeats.put(membership.self, hb.copy(version = hb.version + 1))
    }

    val alive = Seq.newBuilder[HostID]
    val dead = Seq.newBuilder[HostID]
    peers forEach { peer =>
      if (fd.isAlive(peer)) {
        alive += peer
      } else {
        dead += peer
      }
    }

    val liveF = Random.shuffle(alive.result()).headOption map {
      check(_, FailureDetector.CheckInterval.bound)
    } getOrElse Future.unit

    val deadF = Random.shuffle(dead.result()).headOption map {
      check(_, FailureDetector.CheckInterval.bound)
    } getOrElse Future.unit

    val msg = Future.sequence(Seq(liveF, deadF))

    msg.failed foreach {
      case ex: TimeoutException =>
        stats.incr("FailureDetection.Timeout")
        logger.debug(s"Failure Detection: ${ex.getMessage}")
      case ex => logException(ex)
    }

    // block for the live check to either succeed or fail; value is
    // irrelevant.
    try {
      // don't trust the deadline
      Await.ready(liveF, FailureDetector.CheckInterval * 2)
    } catch {
      case ex: TimeoutException =>
        stats.incr("FailureDetection.Timeout")
        logger.debug(s"Failure Detection: ${ex.getMessage}")
      case ex: Throwable => logException(ex)
    }

    peers forEach { fd.interpret(_) }
  }

  private def membershipChange: Future[Boolean] = {
    if (membership.hosts contains membership.self) {
      val current = membership.hosts - membership.self

      peers forEach { peer =>
        if (!current.contains(peer)) {
          fd.remove(peer)
          heartbeats.remove(peer)
          peers.remove(peer)
        }
      }

      current foreach { peer =>
        peers.add(peer)
      }
    }

    Future.successful(!closed)
  }

  def check(host: HostID, deadline: TimeBound)(
    implicit ec: ExecutionContext): Future[Unit] = {
    val src = bus.tempSource(SynAckProtocol)

    // A -> SYN -> B
    val msg = Syn(heartbeats.asScala.toMap, src.id.signalID)
    bus.sink(SynProtocol, signal.at(host)).send(msg, deadline)

    src.subscribe(deadline.timeLeft) {
      case Source.Messages(rs) =>
        rs foreach { case Source.Message(from, _, SynAck(hbs, replyTo)) =>
          val delta = Map.newBuilder[HostID, Heartbeat]

          hbs foreach { case (host, remote) =>
            Option(heartbeats.get(host)) match {
              case None =>
                if (membership.hosts contains host) {
                  notifyStartOrRestart(host)
                  heartbeats.put(host, remote)
                }
              case Some(local) =>
                val now = fd.clock.time
                val lgen = local.generation
                val rgen = remote.generation

                if (rgen > now + MaxGenerationDelta) {
                  logger.warn(
                    s"Invalid generation for peer $host; now = $now, gen = $rgen")
                } else if (rgen > lgen) {
                  hostRestarted(host, remote)
                } else if (rgen == lgen) {
                  markAlive(host, local, remote)

                  if (local.version > remote.version) {
                    delta += host -> local
                  }
                }
            }
          }

          // A -> ACK -> B
          bus
            .sink(AckReplyProtocol, replyTo.at(from))
            .send(Ack(delta.result()), deadline)
        }
        Future.successful(Some(()))
      case Source.Idle =>
        val msg = s"Failure check timed out for $host (SYN)."
        Future.failed(new TimeoutException(msg))
      case Source.Done =>
        Future.successful(Some(()))
    }
  }

  private def recvSyn(
    hbs: Map[HostID, Heartbeat],
    replyTo: HandlerID,
    deadline: TimeBound): Future[Unit] = {
    val src = bus.tempSource(AckReplyProtocol)

    val delta = Map.newBuilder[HostID, Heartbeat]

    hbs foreach { case (host, remote) =>
      val rgen = remote.generation
      val rver = remote.version

      Option(heartbeats.get(host)) match {
        case None =>
          /** The reason we filter out membership.self here is there is a race
            * condition when a node restarts it can receive gossip from
            * another node that contains the previous heartbeat of the restarted node.
            * Without this check, when that happens before the restarted node has set
            * its updated heartbeat timestamp, then it instead accepts the remote gossip,
            * the timestamp doesn't update and no restart notifications are received for the node.
            */
          if ((membership.hosts contains host) && host != membership.self) {
            notifyStartOrRestart(host)
            // save away the remote heartbeat that we didn't have,
            // but still ask for anything fresher in the ACK.
            heartbeats.put(host, remote)
            delta += host -> remote
          }

        case Some(local) =>
          val lgen = local.generation
          val lver = local.version

          if (rgen > lgen) {
            // note that the host has restarted, and ask the remote
            // for any newer version in the new generation
            hostRestarted(host, remote)
            delta += host -> Heartbeat(rgen, 0)
          } else if (rgen < lgen) {
            // send the remote our copy
            delta += host -> local
          } else if (rgen == lgen) {
            if (rver > lver) {
              // note that the host is still alive, and ask the
              // remote for any version greater than ours, in the
              // current generation
              markAlive(host, local, remote)
              delta += host -> Heartbeat(rgen, lver)
            } else if (rver < lver) {
              delta += host -> local
            }
          }
      }
    }

    // A <- SYN/ACK <- B
    bus
      .sink(AckProtocol, replyTo)
      .send(SynAck(delta.result(), src.id.signalID), deadline)

    val host = replyTo.host.toEither match {
      case Right(id) => id
      case Left(addr) =>
        bus.getFirstHostID(addr) getOrElse HostID.NullID
    }

    src.subscribe(deadline.timeLeft) {
      case Source.Messages(rs) =>
        // drop null IDs on the floor - they went away.
        if (host != HostID.NullID) {
          rs foreach { case Source.Message(_, _, Ack(hbs)) =>
            hbs foreach { case (host, remote) =>
              Option(heartbeats.get(host)) match {
                case None =>
                  if (membership.hosts contains host) {
                    notifyStartOrRestart(host)
                    heartbeats.put(host, remote)
                  }
                case Some(local) =>
                  val now = fd.clock.time
                  val lgen = local.generation
                  val rgen = remote.generation

                  if (rgen > now + MaxGenerationDelta) {
                    logger.warn(
                      s"Invalid generation for peer $host; now = $now, gen = $rgen")
                  } else if (rgen > lgen) {
                    hostRestarted(host, remote)
                  } else if (rgen == lgen) {
                    markAlive(host, local, remote)
                  }
              }
            }
          }
        }

        Future.successful(Some(()))
      case Source.Idle =>
        stats.incr("FailureDetection.Timeout")
        logger.debug(s"Failure Detection: Failure check timed out for $host (ACK)")

        // NB. we do _not_ fail the future here, because the handler
        // has no way to differentiate between a 'normal' timeout and
        // a 'bad' timeout.
        Future.successful(Some(()))
      case Source.Done =>
        Future.successful(Some(()))
    }
  }

  private def markAlive(host: HostID, local: Heartbeat, remote: Heartbeat) = {
    if (local.version < remote.version) {
      logger.debug(s"Marking $host alive at $remote")
      heartbeats.put(host, remote)
    } else {
      logger.debug(s"Marking $host alive at $local")
    }

    fd.mark(host)
  }

  // clear all samples from the previous
  // generation, and seed the current generation
  private def hostRestarted(host: HostID, heartbeat: Heartbeat) = {
    logger.debug(s"Detected new generation for $host at $heartbeat.")
    notifyStartOrRestart(host)
    fd.remove(host)
    fd.mark(host)
    heartbeats.put(host, heartbeat)
  }
}
