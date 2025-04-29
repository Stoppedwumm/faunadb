package fauna.tx.consensus

import fauna.exec.Timer
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future, Promise, TimeoutException }

/**
  * A function that implements the logic to perform quorum check.
  *
  * For example the following [[QuorumCheck]] function implements the logic to return a valid RAFT leader
  * from multiple leader responses received from all the members of a RAFT clusters.
  *
  * {{{
  *    implicit val leaderQuorumCheck: QuorumCheck[Option[HostID], HostID] =
  *       (responses: Seq[Option[HostID]]) =>
  *         Ring(hostIDs).quorum(responses.flatten)
  * }}}
  *
  * To see more examples see test cases - QuorumSpec or TxnPipeline.logSegmentLeader.
  *
  * @tparam R The response type
  * @tparam Q The outcome of the quorum check.
  *           The first one to return Some[Q] will be returned.
  */
trait QuorumCheck[R, Q] {
  def hasQuorum(responses: Seq[R]): Option[Q]
}

object Quorum {

  val QuorumCheckTimeout = 10.seconds

  /**
    * Private state of the current quorum check.
    *
    * @param totalRequests total number of requests dispatched.
    * @tparam R type of response.
    */
  private class State[R, Q](totalRequests: Int) {

    private[this] val responses: ListBuffer[R] = ListBuffer.empty
    private[this] var hasReachedQuorum: Boolean = false

    def isQuorumReached: Boolean =
      hasReachedQuorum

    def hasEnoughResponses =
      responses.size + 1 > totalRequests / 2

    def hasAllResponses =
      responses.size == totalRequests

    def addResponse(newResponse: R,
                    check: QuorumCheck[R, Q]): Option[Q] = {
      //add all responses to the state.
      responses += newResponse
      //if quorum has not already reached and if there are enough responses to do a quorum check - run check!
      if (!isQuorumReached && hasEnoughResponses) {
        check.hasQuorum(responses.toSeq) map { yes =>
          //yep quorum reached.
          hasReachedQuorum = true
          yes
        }
      } else {
        None
      }
    }

    def onCheck(requests: => List[Future[R]],
                check: QuorumCheck[R, Q])(implicit ec: ExecutionContext): Future[Option[Q]] = {
      val promise = Promise[Option[Q]]()

      val timeout = Timer.Global.scheduleTimeout(QuorumCheckTimeout) {
        promise.tryFailure(new TimeoutException("Timed out on quorum check"))
      }

      requests foreach { _ map {
        response =>
          //Yep! got a response. Do quorum check and complete request early if this response reaches quorum.
          synchronized {
            val quorum = addResponse(response, check)
            // We either got a quorum, or all the requests have been received without quorum.
            if (quorum.isDefined || hasAllResponses) {
              promise.trySuccess(quorum)
              timeout.cancel()
            }
          }
        }
      }

      promise.future
    }
  }

  /**
    * Run quorum check and returns the first response that quorums success.
    */
  def check[R, Q](requests: => List[Future[R]],
                  check: QuorumCheck[R, Q])(implicit ec: ExecutionContext): Future[Option[Q]] = {
    val initialState = new State[R, Q](requests.size)
    initialState.onCheck(requests, check)
  }
}
