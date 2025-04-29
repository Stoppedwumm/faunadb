package fauna.trace

import fauna.lang.Local
import fauna.logging.ExceptionLogging
import io.netty.util.{ AbstractReferenceCounted, IllegalReferenceCountException }
import java.io.Closeable

object Scope extends ExceptionLogging {
  private[this] val local = new Local[Scope]

  /**
    * Returns the active span within the executing scope, or none if
    * no span is active.
    */
  def activeSpan: Option[Span] =
    local() flatMap { _.span }

  /**
    * Opens a new scope for the provided span, setting it as the
    * active span.
    */
  def open(span: Span): Option[Scope] = {
    val cur = if (span.isSampled) {
      val prev = local()
      Some(new Scope(span, prev))
    } else {
      None
    }

    local.set(cur)
    cur
  }

  /**
    * Used by EC to capture an active scope when a Future is created
    * for re-activation when that Future is executed.
    *
    * Each retainActive() call MUST have a corresponding Scope.close().
    */
  def retainActive(): Option[Scope] =
    try {
      val so = local()
      so foreach { _.retain }
      so
    } catch {
      case t: IllegalReferenceCountException =>
        // The Scope object is unusable as its refCnt was 0. This indicates a
        // bug: we have more releases than retains, therefore we log the
        // exception.
        logException(t)
        // Forget the no longer usable Scope.
        restore(None)
        // Returning None allows callers to proceed; a failure in tracing
        // shouldn't break processing.
        None
    }

  /**
    * Restores the previously-active scope after a Scope is closed, if
    * any.
    */
  def restore(prev: Option[Scope]): Unit =
    local.set(prev)
}

/**
  * A scope represents an activation of a span.
  *
  * It saves any previously-active span and restores it after this
  * scope closes.
  *
  * When this scope closes, if it is the last retained activation of
  * this span, the span will be automatically finished. Typically,
  * there are only two activations of a scope: one when a
  * Runnable/Future is created, and one when it is executed.
  */
final class Scope(
  private[this] val _span: Span,
  private[this] val previous: Option[Scope])
    extends AbstractReferenceCounted
    with Closeable {

  def span = Some(_span)

  def touch(o: AnyRef) = this

  def close(): Unit = {
    release()
    Scope.restore(previous)
  }

  protected def deallocate(): Unit =
    _span.finish()

  override def toString =
    s"Scope(${_span}, $previous))"
}
