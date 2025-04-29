package fauna.qa.main

import fauna.exec.Timer
import fauna.lang.{ Timestamp, Timing }
import fauna.lang.syntax._
import fauna.qa._
import fauna.qa.net._
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.LockSupport
import java.util.ConcurrentModificationException
import org.HdrHistogram._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

/** Customers are synthetic load generators. A Worker may start multiple
  * Customers in order to simulate multiple streams of requests. Customer load can
  * be started, paused, continued, and stopped. Request latencies and error
  * counts are recorded in the provided objects.
  *
  * A Customer is effectively single threaded by use of the parasitic
  * ExecutionContext. This means it will run on the Netty thread it gets from the
  * provided FaunaClient. Pausing will block the thread it's currently running
  * on, thus the FaunaClient needs to have come from an EventLoopGroup that can
  * handle blocked threads.
  *
  * If `retryOnFailure` is enabled, customers will retry requests failing with 503s
  * and/or `TimeoutException` until they succeed or the Commander sends a reset
  * message.
  */

trait Customer {
  def host: Host

  def run(testGenerator: TestGenerator, schema: Schema.DB): Future[Unit]

  def runRequests(reqs: RequestIterator): Future[Unit]

  def pause(): Unit

  def continue(): Unit

  def close(): Unit

  def runValidationQuery(
    testGen: TestGenerator with ValidatingTestGenerator,
    schema: Schema.DB,
    minSnapTime: Timestamp,
    timeout: Duration): Future[String]
}

class LegacyCustomer(
  node: CoreNode,
  factory: FaunaClientFactory,
  recorder: Recorder,
  errorCounter: LongAdder,
  rate: Rate = Rate.Max,
  retryOnFailure: Boolean = false,
  lastSeenTxnTime: Timestamp = Timestamp.Epoch
) extends Customer {
  def host = node.host

  private[this] val timer = Timer.Global
  private[this] val log = getLogger()

  @volatile private[this] var closed: Boolean = false
  @volatile private[this] var paused: Boolean = false
  @volatile private[this] var done: Boolean = true

  implicit private val ec = ExecutionContext.parasitic

  @volatile private[this] var client: Future[FaunaClient] = {
    def mkClient: Future[FaunaClient] =
      if (!closed) {
        factory.connect(node) transformWith {
          case Success(c) =>
            c onClose { client = mkClient }
            c syncLastTxnTime lastSeenTxnTime
            Future.successful(c)
          case Failure(err) =>
            log.error("Connection Error", err)
            Timer.Global.delay(1.second)(mkClient)
        }
      } else {
        Future.failed(new Exception("closed"))
      }
    mkClient
  }

  private def closeClient(): Future[Unit] = {
    var closeF = Future.unit
    client.value foreach {
      _ foreach { c =>
        closeF = c.close()
      }
    }
    closeF
  }

  private[this] var parkedThread: Thread = _

  private def park(): Unit = {
    parkedThread = Thread.currentThread
    LockSupport.park(this)
  }

  private def unpark(): Unit = {
    val t = parkedThread
    parkedThread = null
    LockSupport.unpark(t)
  }

  private def recordResponse(timing: Timing, rep: FaunaResponse): Unit =
    if (rep.isSuccess) {
      recorder.recordValue(timing.elapsedMillis)
    } else {
      errorCounter.increment()
    }

  private def processIterator(reqs: RequestIterator): Future[Unit] = {
    if (closed) {
      closeClient()
    } else if (!reqs.hasNext) {
      done = true
      Future.unit
    } else {
      if (paused) park()
      processRequest(reqs.next()) recover { case _ =>
        () // skip errors...
      } flatMap { _ =>
        processIterator(reqs)
      }
    }
  }

  private def processRequest(req: FaunaQuery): Future[Unit] = {
    if (paused) park()
    if (closed) return closeClient()

    val repF = rate match {
      case Rate.Max =>
        runQuery(req)

      case Rate.Limited(value, unit) =>
        val timeout = Duration(1, unit) / value
        val deadline = timeout.fromNow
        runQuery(req, timeout) flatMap { rep =>
          timer.delay(deadline.timeLeft)(Future.successful(rep))
        }
    }

    repF flatMap { rep =>
      req.nextReq(rep.code, rep.body) match {
        case Some(q) => processRequest(q)
        case _       => Future.unit
      }
    }
  }

  def runValidationQuery(
    testGen: TestGenerator with ValidatingTestGenerator,
    schema: Schema.DB,
    minSnapTime: Timestamp = Timestamp.Epoch,
    timeout: Duration = Duration.Inf): Future[String] =
    client flatMap { cli =>
      val req = testGen.getValidateStateQuery(schema, minSnapTime)
      val passFailOnly = testGen.isPassFail
      val expectFailure = testGen.expectValidateQueryToFail

      cli.syncLastTxnTime(minSnapTime)
      cli.query(
        req.authKey,
        req.query,
        timeout,
        maxRetriesOnContention = req.maxContentionRetries,
        requestPathOverride = req.requestPathOverride) transformWith {
        case Success(x) =>
          x.code match {
            case 409 =>
              Future.failed(new ConcurrentModificationException(x.body.toString()))
            case 503 => Future.failed(new TimeoutException(x.body.toString()))
            case _ => {
              log.info(
                s"Host[${cli.coreNode.addr}] ${testGen.name} query: ${req.query}, response: ${x.body}")
              (passFailOnly, x.isSuccess != expectFailure) match {
                case (true, true)  => Future.successful("PASS")
                case (false, true) => Future.successful(x.body.toString())
                case (true, false) =>
                  Future.failed(new IllegalStateException("FAIL"))
                case (false, false) =>
                  Future.failed(new IllegalStateException(x.body.toString()))
              }
            }
          }
        case Failure(ex) =>
          log.error("Unhandled query error", ex)
          Future.failed(ex)
      }
    }

  private def runQuery(
    req: FaunaQuery,
    timeout: Duration = Duration.Inf,
    backOffDelay: Duration = 100.millis): Future[FaunaResponse] = {

    def retry(): Future[FaunaResponse] =
      timer.delay(backOffDelay) {
        runQuery(req, timeout, backOffDelay * 2)
      }

    def runQuery0(): Future[FaunaResponse] =
      client flatMap { cli =>
        val timing = Timing.start
        cli.query(
          req.authKey,
          req.query,
          timeout,
          requestPathOverride = req.requestPathOverride,
          source = req.name.map { name => s"qa-stream-$name" }) andThen {
          case Success(rep) => recordResponse(timing, rep)
          case Failure(_)   => errorCounter.increment()
        }
      }

    if (!retryOnFailure) {
      runQuery0()
    } else {
      if (paused) park()
      if (closed) {
        Future.failed(
          new IllegalStateException(
            "Customer was closed while running query with retryOnFailure=true."))
      } else {
        runQuery0() transformWith {
          case Success(rep) if rep.code == 503 => retry()
          case Failure(_: TimeoutException)    => retry()
          case other                           => Future.fromTry(other)
        }
      }
    }
  }

  def run(testGenerator: TestGenerator, schema: Schema.DB): Future[Unit] =
    runRequests(testGenerator.stream(schema))

  def runRequests(reqs: RequestIterator): Future[Unit] =
    if (!done) {
      Future.failed(new Exception("Stream still running"))
    } else {
      done = false
      closed = false
      paused = false
      unpark()
      processIterator(reqs)
    }

  def pause(): Unit =
    paused = true

  def continue(): Unit = {
    paused = false
    unpark()
  }

  def close(): Unit = {
    closed = true
    if (paused) {
      // let the process iterator catch the close flag and close the client
      paused = false
      unpark()
    } else if (done) {
      // process iterator isn't running, we need to close the client
      closeClient()
    }
  }
}
