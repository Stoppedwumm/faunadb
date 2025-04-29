package fauna.model.test

import fauna.atoms._
import fauna.exec.{
  ImmediateExecutionContext,
  Observable,
  OverflowStrategy,
  Publisher
}
import fauna.model.stream.StreamContext
import fauna.repo.service.stream.TxnResult
import fauna.repo.service.StreamingService
import fauna.repo.IndexConfig
import fauna.snowflake.IDSource
import fauna.storage.index.IndexTerm
import scala.concurrent.duration.Duration

trait StreamContextHelpers { self: FQL2Spec =>

  private[this] var service: StreamingService = ctx.service.streamService
  private[this] var publisher: Publisher[TxnResult] = _

  lazy val streamCtx =
    new StreamContext(
      repo = ctx,
      stats = ctx.stats,
      logEvents = true,
      idGen = new IDSource(() => 42),
      streamService = service,
      eventReplayLimit = 128
    )

  after {
    // reset mock to concrete impl.
    if (publisher ne null) {
      publisher.close()
      service = ctx.service.streamService
    }
  }

  def mockService(): Publisher[TxnResult] = {
    implicit val ec = ImmediateExecutionContext
    val (pub, obs) = Observable.gathering(OverflowStrategy.unbounded[TxnResult])

    service = new StreamingService {
      def forDocument(
        scope: ScopeID,
        docID: DocID,
        idlePeriod: Duration = Duration.Inf
      ) = obs

      def forIndex(
        cfg: IndexConfig,
        terms: Vector[IndexTerm],
        idlePeriod: Duration = Duration.Inf
      ) = obs
    }

    publisher = pub
    pub
  }
}
