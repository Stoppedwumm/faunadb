package fauna.cluster

import fauna.atoms.HostID
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext.Implicits.global
import fauna.exec.Timer
import fauna.lang.TimeBound
import fauna.lang.syntax._
import fauna.net.bus.{ HandlerCtx, MessageBus, Protocol, SignalID }
import fauna.net.{ HostAddress, HostInfo }
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{ Failure, Success }

object HostIntroduction {
  implicit val codec = CBOR.TupleCodec[HostIntroduction]
}

/**
  * Used in the Hello protocol for a new node to introduce itself to the rest
  * of the cluster.
  * @param hostID the ID of the node
  * @param replica the name of the replica node belongs to
  */
case class HostIntroduction(hostID: HostID, replica: String)

object IntroductionResponse {
  implicit val codec = CBOR.SumCodec[IntroductionResponse](
    CBOR.TupleCodec[IntroductionAccepted],
    CBOR.SingletonCodec(NotMember),
    CBOR.SingletonCodec(AlreadyRemoved),
    CBOR.TupleCodec[WrongReplica],
    CBOR.TupleCodec[UnexpectedError]
  )
}

sealed trait IntroductionResponse

case class IntroductionAccepted(knownHosts: Vector[(HostID, HostAddress)]) extends IntroductionResponse

sealed trait IntroductionFailedResponse extends IntroductionResponse {
  def exception: Exception
  // The host should stop making future requests without changes
  def badRequest: Boolean = false
}

// The contacted seed is not a member of the cluster, so it can't process the introduction
case object NotMember extends IntroductionFailedResponse {
  def exception =
    new IllegalArgumentException("The contacted seed host is not a member of a cluster.")
}

// The node contacting the seed with an introduction is a known dead host - it has been removed
case object AlreadyRemoved extends IntroductionFailedResponse {
  def exception =
    new IllegalArgumentException("This node was previously removed from the cluster. It needs new identity before it can rejoin.")
  override def badRequest = true
}

// The node contacting the seed with an introduction is already known but it is registered with a
// different replica name. The returned response contains the name of the replica known to the cluster
// to contain the host.
case class WrongReplica(knownReplica: String) extends IntroductionFailedResponse {
  def exception =
    new IllegalArgumentException(s"Conflicting replica_name configuration: the consensus is this node belongs to replica $knownReplica")
  override def badRequest = true
}

// The seed processing the host introduction encountered an unexpected error
case class UnexpectedError(msg: String) extends IntroductionFailedResponse {
  def exception =
    new IllegalStateException(s"The contacted seed host encountered an unexpected error: $msg")
}

/**
  * "Hello" is a protocol used by a newly joining node to introduce itself
  * (with its HostID and replica name) to the rest of the cluster. It is
  * necessary so that the rest of the cluster knows the address of the new
  * node. Any node already in the cluster can receive the Hello request from
  * the new node. A request handler is embedded in Membership, and upon its
  * receipt it'll replicate an IntroduceHost message into the log and also
  * respond to the Hello request with a list of all known HostID->HostAddress
  * mappings. If the receiving node can not replicate the message (notably,
  * because it can not reach a quorum), it will respond with an empty sequence.
  * Hello protocol is also used by nodes that have moved to announce a change
  * in their IP address or replica name.
  */
object Hello {
  type RequestHandler = (HostInfo, HostIntroduction, Duration) => Future[IntroductionResponse]

  private type HelloMsg = (HostIntroduction, SignalID)
  private val HelloProtocol = Protocol[HelloMsg]("hello.request")
  private implicit val HelloReplyProtocol = Protocol.Reply[HelloMsg, IntroductionResponse]("hello.reply")

  private val HelloTimeout = 90.seconds
  private val HelloPollTimeout = 1.second

  def makeHandler(bus: MessageBus, signalID: SignalID)(onRequest: RequestHandler): HandlerCtx = {
    def sendReply(to: HostInfo, signal: SignalID, res: IntroductionResponse) =
      bus.sink(HelloReplyProtocol, signal.at(to.address)).send(res).unit

    bus.handler(HelloProtocol, signalID) {
      case (from, (intro, replyTo), deadline) =>
        onRequest(from, intro, deadline.timeLeft) transformWith {
          case Success(res) =>
            sendReply(from, replyTo, res)
          case Failure(t) =>
            t match {
              case _: TimeoutException =>
                getLogger.warn(s"Timed out processing a Hello request from $from.")
                Future.unit
              case e: Throwable =>
                getLogger.error(s"Membership Service: Unexpected exception while processing a Hello request from $from.", e)
                sendReply(from, replyTo, UnexpectedError(e.toString))
            }
        }
    }
  }

  def sendHello(bus: MessageBus, signalID: SignalID, peers: => Seq[HostAddress], intro: HostIntroduction): Future[(Option[HostID], HostAddress)] = {
    def repeatedlySendHello(currPeers: Seq[HostAddress], deadline: TimeBound): Future[(Option[HostID], HostAddress)] = {
      def retry() = {
        if (deadline.hasTimeLeft) {
          val newPeers = if (currPeers.size < 2) {
            peers // re-evaluate
          } else {
            currPeers.tail
          }
          repeatedlySendHello(newPeers, deadline)
        } else {
          Future.failed(new TimeoutException("Timed out trying to obtain a response from any peer"))
        }
      }

      currPeers.headOption.fold(Timer.Global.delay(1.second) { retry() }) { peer =>
        bus.sink(HelloProtocol, signalID.at(peer)).request((intro, _), HelloPollTimeout.bound) flatMap {
          _.fold(retry()) {
            case IntroductionAccepted(idToAddr) =>
              idToAddr foreach { case (id, addr) => bus.registerHostID(id, addr.copy(port = bus.hostAddress.port)) }
              val idOpt = idToAddr collectFirst {
                case (id, addr) if addr.hostName == peer.hostName => id
              }
              Future.successful((idOpt, peer))
            case failed: IntroductionFailedResponse =>
              if (failed.badRequest) {
                Future.failed(failed.exception)
              } else {
                getLogger.warn(s"Membership service: ${failed.exception.getMessage}")
                retry()
              }
          }
        }
      }
    }
    repeatedlySendHello(peers, HelloTimeout.bound)
  }
}
