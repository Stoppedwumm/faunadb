package fauna.api

import fauna.ast.MaximumIDException
import fauna.cluster.ClusterTopologyException
import fauna.logging.ExceptionLogging
import fauna.model.{ IndexBuildLimitExceeded, IndexBuildsDisabled }
import fauna.net.http.RequestTooLarge
import fauna.repo.{
  ContentionException,
  FutureReadException,
  MaxQueryWidthExceeded,
  TransactionKeyTooLargeException,
  TxnTooLargeException,
  TxnTooManyComputeOpsException,
  VersionIndexEntriesTooLargeException,
  VersionTooLargeException
}
import fauna.repo.service.rateLimits.OpsLimitException
import fauna.scheduler.TooBusyException
import fauna.stats.StatsRecorder
import fauna.storage.api.ReadValidTimeBelowMVT
import fauna.storage.ComponentTooLargeException
import fauna.tx.transaction.Coordinator
import java.io.IOException
import scala.concurrent.TimeoutException

object ExceptionResponseHelpers extends ExceptionLogging {
  import fauna.api.{ ProtocolErrorCode => Code }

  val ContactSupport = "Please create a ticket at https://support.fauna.com"

  /** Catchall handler for exceptions which kill the request/transaction.
    * Exceptions should be added here as a last resort, since we have lost any
    * contextual information about where the exception originated in the query
    * at this point.
    *
    * If at all possible, catch exceptions at the point of origin and return a
    * query failure instead.
    *
    * FIXME: Multiple exceptions currently here can be moved to a central hook at the
    * read/parser level.
    */
  def toResponse[Res](e: Throwable, stats: StatsRecorder)(
    format: (Code, String) => Res): Res = {
    e match {
      /** AccountDisabled is returned when a customer's account has the
        * RunQueries feature disabled. No queries are permitted after
        * authentication.
        */
      case AccountDisabledException =>
        format(Code.Disabled, s"Your account has been disabled. $ContactSupport")

      /** AccountShellDisabled is returned when a customer makes a request from the Fauna Shell
        * and has the RunShell feature disabled.
        */
      case AccountShellDisabledException =>
        format(
          Code.Disabled,
          s"The Fauna Shell has been disabled for your account. $ContactSupport")

      /** IndexBuildsDisabled is returned when a customer's account has
        * the RunTasks feature disabled. No index builds may be created or
        * executed.
        */
      case IndexBuildsDisabled =>
        format(
          Code.Disabled,
          s"Index builds have been disabled for your account. $ContactSupport")

      // FIXME: this could be caught within read functions.
      case err: ReadValidTimeBelowMVT =>
        format(
          Code.BadRequest,
          s"Requested timestamp ${err.validTS} less than minimum allowed timestamp ${err.mvt}."
        )

      // FIXME: this could be caught within read functions.
      case _: FutureReadException =>
        format(Code.BadRequest, e.getMessage)

      case Coordinator.BackoffException =>
        format(Code.TooManyRequests, "Too many pending requests")

      case _: TooBusyException =>
        format(Code.TooManyRequests, "Too many pending requests")

      case LimitExceededException =>
        format(Code.TooManyRequests, "Too many pending requests")

      case e: IndexBuildLimitExceeded =>
        format(
          Code.TooManyRequests,
          s"Too many pending index builds. Pending: ${e.pending} Limit: ${e.limit}")

      case e: OpsLimitException =>
        format(Code.TooManyRequests, s"Rate limit for ${e.op} exceeded")

      // FIXME: these could be caught within read functions. split from query
      // timeout
      case ex: TimeoutException =>
        if (ex.getMessage eq null) {
          format(Code.ServiceTimeout, "Read timed out")
        } else {
          format(Code.ServiceTimeout, s"Read timed out (${ex.getMessage})")
        }

      case _: AggressiveTimeoutException =>
        format(Code.ProcessingTimeLimitExceeded, e.getMessage)

      case _: ClusterTopologyException =>
        format(Code.OperatorError, e.getMessage)

      case RequestTooLarge =>
        format(Code.RequestTooLarge, "Request entity is too large")

      // FIXME: this could be caught within write functions.
      case _: TransactionKeyTooLargeException =>
        format(Code.BadRequest, e.getMessage)

      // FIXME: this could be caught within write functions.
      case _: VersionTooLargeException =>
        format(Code.BadRequest, e.getMessage)

      // FIXME: this could be caught within write functions.
      case _: ComponentTooLargeException =>
        format(Code.BadRequest, e.getMessage)

      // FIXME: this could be caught within write functions.
      case _: TxnTooLargeException =>
        format(Code.BadRequest, e.getMessage)

      // FIXME: this could be caught within write functions.
      case _: VersionIndexEntriesTooLargeException =>
        format(Code.BadRequest, e.getMessage)

      case _: TxnTooManyComputeOpsException =>
        format(Code.BadRequest, e.getMessage)

      case _: MaxQueryWidthExceeded =>
        format(Code.BadRequest, e.getMessage)

      case Coordinator.ShutDownException =>
        // hide this ugly error from users
        stats.incr("Query.TimeoutFromShutdown")
        format(Code.ServiceTimeout, "Read timed out.")

      // FIXME: this could be caught within write functions.
      case MaximumIDException =>
        format(Code.BadRequest, e.getMessage)

      case _: ContentionException =>
        format(
          Code.ContendedTransaction,
          "Transaction was aborted due to detection of concurrent modification.")

      case _: IOException => throw e

      case e =>
        logException(e)
        format(Code.InternalError, s"${e.toString} $ContactSupport")
    }
  }

}
