package fauna.tx.transaction

import fauna.logging.ExceptionLogging

abstract class Node[K, R, RV, W, WV] extends ExceptionLogging {
  val ctx: PipelineCtx[K, R, RV, W, WV]

  protected def onClose(): Unit

  @volatile private[this] var _closed = false
  def isClosed = _closed

  def close() = synchronized {
    if (!isClosed) {
      _closed = true
      onClose()
    }
  }
}
