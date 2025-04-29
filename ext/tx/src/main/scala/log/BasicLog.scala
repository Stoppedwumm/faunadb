package fauna.tx.log

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

class BasicLog[I, V](val store: LogStore[I, V]) extends Log[I, V] {

  type E = store.E

  def prevIdx = store.prevIdx

  def lastIdx = store.lastIdx

  def entries(after: I): EntriesIterator[E] = store.entries(after)

  def poll(after: I, within: Duration) = store.poll(after, within)

  def subscribe(after: I, idle: Duration)(f: Log.Sink[I, E])(implicit ec: ExecutionContext) =
    store.subscribe(after, idle)(f)

  def add(value: V, within: Duration) = {
    val idx = store.add(Seq(value))
    store.flush()
    store.updateCommittedIdx(idx)
    Future.successful(idx)
  }

  def sync(within: Duration) = Future.successful(store.lastIdx)

  def truncate(to: I) = store.truncate(to)

  def close() = store.close()

  def isClosed = store.isClosed
}
