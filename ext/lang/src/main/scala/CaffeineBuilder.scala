package fauna.lang

import com.github.benmanes.caffeine.cache.{ Caffeine => JCaffeine, _ }
import java.util.concurrent.{ CompletableFuture, Executor => JExec }
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object Caffeine {
  type Unrefined = Caffeine[Nothing, AnyRef]

  def newBuilder = new Unrefined()

  implicit class UnrefinedOps(b: Unrefined) {
    def withTypes[K <: AnyRef, V <: AnyRef] = b.asInstanceOf[Caffeine[K, V]]
    def apply[K <: AnyRef, V <: AnyRef] = withTypes[K, V]
  }
}

case class Caffeine[K <: AnyRef, V <: AnyRef](
  caffeine: JCaffeine[AnyRef, AnyRef] = JCaffeine.newBuilder,
  hasMaxSize: Boolean = false,
  weigh: (K, V) => Int = null,
) {
  def maxSize(size: Long) = copy(caffeine = caffeine.maximumSize(size), hasMaxSize = true)
  def maxWeight(weight: Long) = copy(caffeine = caffeine.maximumWeight(weight))
  def expireAfterWrite(d: FiniteDuration) = copy(caffeine = caffeine.expireAfterWrite(d.length, d.unit))
  def expireAfterAccess(d: FiniteDuration) = copy(caffeine = caffeine.expireAfterAccess(d.length, d.unit))
  def refreshRate(d: FiniteDuration) = copy(caffeine = caffeine.refreshAfterWrite(d.length, d.unit))
  def ticker(ticker: Ticker) = copy(caffeine = caffeine.ticker(ticker))
  def executor(executor: Runnable => Unit) = copy(caffeine = caffeine.executor(executor(_)))
  def weigher(weigh: (K, V) => Int) = copy(weigh = weigh)

  def build(
    load: K => Option[V],
    reload: (K, V) => Option[V] = null): LoadingCache[K, V] = {

    val reloadOpt = Option(reload)
    val weighOpt = Option(weigh)

    if (hasMaxSize) {
      require(weighOpt.isEmpty, "Max size configured, but weigher provided.")
    }

    // Need to assign to a fresh val since the builder's type is refined at this
    // point. Raw match syntax (vs fold) allows the type system to infer K and V.
    val caff0 = weighOpt match {
      case Some(f) => caffeine.weigher(f(_, _))
      case None    => caffeine
    }

    val loader = reloadOpt.fold(newLoader(load))(newReloader(load, _))
    caff0.recordStats().build(loader)
  }

  private abstract class Loader[K, V <: AnyRef] extends CacheLoader[K, V] {
    override def asyncLoad(key: K, exec: JExec) =
      try {
        CompletableFuture.completedFuture(load(key))
      } catch {
        case NonFatal(e) => CompletableFuture.failedFuture(e)
      }

    override def asyncReload(key: K, oldValue: V, exec: JExec) =
      try {
        CompletableFuture.completedFuture(reload(key, oldValue))
      } catch {
        case NonFatal(e) => CompletableFuture.failedFuture(e)
      }
  }

  private def newLoader[K, V <: AnyRef](loader: K => Option[V]) =
    new Loader[K, V] {
      def load(key: K) = loader(key).getOrElse(null.asInstanceOf[V])
    }

  private def newReloader[K, V <: AnyRef](loader: K => Option[V], reloader: (K, V) => Option[V]) =
    new Loader[K, V] {
      def load(key: K) = loader(key).getOrElse(null.asInstanceOf[V])
      override def reload(key: K, oldValue: V) = reloader(key, oldValue).getOrElse(null.asInstanceOf[V])
    }
}
