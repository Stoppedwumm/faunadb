package fauna.model.runtime.stream

import fauna.atoms.ScopeID
import fauna.exec.{
  AsyncRingBuffer,
  ImmediateExecutionContext,
  Observable,
  Observer
}
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.ValueSet
import fauna.repo.schema.Path
import fauna.repo.service.stream.TxnResult
import io.netty.util.{ AbstractReferenceCounted, IllegalReferenceCountException }
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import scala.reflect.ClassTag

object SharedTranslation {
  val SharedBufferSize = 4096

  final case class Key(scopeID: ScopeID, source: ValueSet, watchedFields: Seq[Path])
}

/** Normalizes event translation for streams with the same shape: same source set and
  * same watched fields.
  */
final class SharedTranslation[A: ClassTag] {
  import SharedTranslation._

  private final class Translation(
    key: Key,
    sharedObs: Observable[Seq[TxnResult]],
    translate: Seq[TxnResult] => Future[Iterable[A]])
      extends AbstractReferenceCounted
      with Observer[Seq[TxnResult]]
      with Observable[A] {

    implicit val ec = ImmediateExecutionContext

    private val sharedBuffer = new AsyncRingBuffer[A](SharedBufferSize)
    private lazy val subscription = sharedObs.subscribe(this)
    @volatile private var lastPublishedTS = Timestamp.Min

    def subscribe(observer: Observer[A]) = {
      // NB. use Scala's lazy semantics to subscribe to the upstream only once. Note
      // that removing lazy semantics may cause concurrent modification issues with
      // the `translations` map since a race can cause upstream to be already closed,
      // therefore immediately deallocating this shared translation, which then
      // removes it from the mapping.
      subscription

      val obs =
        synchronized {
          // No events seen yet. Subscribe to the buffer.
          if (lastPublishedTS == Timestamp.Min) {
            sharedBuffer
          } else {
            // We've seen some events already, we'll use the last seen txn time as
            // the shared stream starting point.
            val start =
              new TxnResult(
                lastPublishedTS,
                Clock.time,
                Vector.empty
              )

            Observable
              .single(Seq(start))
              .flatMapAsync {
                translate(_) map { Observable.from(_) }
              }
              .concat(sharedBuffer)
          }
        }

      obs.ensure(release()).subscribe(observer)
    }

    def onNext(txns: Seq[TxnResult]) =
      translate(txns) flatMap { events =>
        synchronized {
          events foreach { sharedBuffer.publish(_) }
          lastPublishedTS = txns.last.txnTS
          Observer.ContinueF
        }
      }

    def onError(err: Throwable) = close(err)
    def onComplete() = close()
    def deallocate() = close()
    def touch(hint: Object) = this

    def close(err: Throwable = null) = {
      translations.remove(key, this)
      sharedBuffer.close(err)
      subscription.cancel()
    }
  }

  private val translations = new ConcurrentHashMap[Key, Translation]()

  def size: Int = translations.size

  def share(
    key: Key,
    obs: => Observable[Seq[TxnResult]],
    translate: Seq[TxnResult] => Future[Iterable[A]]): Observable[A] =
    translations.compute(
      key,
      {
        case (_, null) => new Translation(key, obs, translate)
        case (_, shared) =>
          try {
            shared.retain()
            shared
          } catch {
            case _: IllegalReferenceCountException =>
              new Translation(key, obs, translate)
          }
      }
    )
}
