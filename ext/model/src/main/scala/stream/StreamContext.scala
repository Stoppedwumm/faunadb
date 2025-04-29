package fauna.model.stream

import fauna.repo._
import fauna.repo.service._
import fauna.snowflake._
import fauna.stats._

case class StreamID(toLong: Long) extends AnyVal

final class StreamContext(
  val repo: RepoContext,
  val stats: StatsRecorder,
  val logEvents: Boolean,
  val idGen: IDSource,
  streamService: => StreamingService,
  val eventReplayLimit: Int) {

  def service: StreamingService = streamService

  def nextID: StreamID = StreamID(idGen.getID)
}
