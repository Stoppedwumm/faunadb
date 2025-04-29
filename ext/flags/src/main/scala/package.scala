package fauna

import fauna.atoms.{ AccountID, HostID }
import fauna.codex.cbor.CBOR
import fauna.stats.StatsRecorder
import scala.concurrent.{ ExecutionContext, Future }

package flags {

  object Service {

    /** Useful in the absence of a Feature Flags service with which to
      * communicate, this class will return nothing for all queries.
      */
    final class Null extends Service {

      def version = 0

      def getAllUncached[ID, P <: Properties[ID], F <: Flags[ID]](props: Vector[P])(
        implicit propCodec: CBOR.Encoder[Vector[P]],
        flagsCodec: CBOR.Decoder[Vector[F]],
        companion: FlagsCompanion[ID, F]): Future[Vector[F]] =
        Future.successful(Vector.empty)
    }

    /** This service allows extending a primary Service object with a static set
      * of flag fallbacks, in order to allow for dynamically configurable flag
      * defaults.
      *
      * Resolution is:
      *   - If a flag is present in the primary response, that value is returned.
      *   - Otherwise, if a flag is present in the static map for accounts or hosts, that value is returned.
      *   - Otherwise the flag default is returned.
      */
    final class Fallback private[Service] (
      primary: Service,
      accountFallbacks: Map[String, Value],
      hostFallbacks: Map[String, Value])
        extends Service {

      def version = primary.version

      def getAllUncached[ID, P <: Properties[ID], F <: Flags[ID]](props: Vector[P])(
        implicit propCodec: CBOR.Encoder[Vector[P]],
        flagsCodec: CBOR.Decoder[Vector[F]],
        companion: FlagsCompanion[ID, F]): Future[Vector[F]] = {

        // XXX: can't access IEC
        implicit val ec = ExecutionContext.parasitic
        primary.getAllUncached[ID, P, F](props) map { vec =>
          props flatMap { p =>
            addFallbacks[ID, F](p.id, vec find { _.id == p.id })
          }
        }
      }

      override def getUncached[ID, P <: Properties[ID], F <: Flags[ID]](props: P)(
        implicit propCodec: CBOR.Encoder[Vector[P]],
        flagsCodec: CBOR.Decoder[Vector[F]],
        companion: FlagsCompanion[ID, F]): Future[Option[F]] = {

        // XXX: can't access IEC
        implicit val ec = ExecutionContext.parasitic
        primary.getUncached[ID, P, F](props) map { opt =>
          addFallbacks[ID, F](props.id, opt)
        }
      }

      private def addFallbacks[ID, F <: Flags[ID]](
        id: ID,
        flags: Option[F]): Option[F] =
        (id, flags) match {
          case (_: AccountID, _) if accountFallbacks.isEmpty => flags
          case (id: AccountID, opt) =>
            Some(
              AccountFlags(
                id,
                opt.fold(accountFallbacks)(accountFallbacks ++ _.values))
                .asInstanceOf[F])

          case (_: HostID, _) if hostFallbacks.isEmpty => flags
          case (id: HostID, opt) =>
            Some(
              HostFlags(id, opt.fold(hostFallbacks)(hostFallbacks ++ _.values))
                .asInstanceOf[F])

          case _ =>
            throw new IllegalStateException(
              s"Invalid ID, Flag type pair: $id, $flags")
        }
    }
  }

  trait Service {

    /** The current flags state version. Used downstream for cache invalidation. */
    def version: Int

    /** Issues a request to the feature flags service for each of the
      * provided Properties.
      *
      * Returns the set of associated flags from the service's response.
      *
      * The returned flags MAY be a subset of the requested properties.
      */
    def getAllUncached[ID, P <: Properties[ID], F <: Flags[ID]](props: Vector[P])(
      implicit propCodec: CBOR.Encoder[Vector[P]],
      flagsCodec: CBOR.Decoder[Vector[F]],
      companion: FlagsCompanion[ID, F]): Future[Vector[F]]

    def getAllCached[ID, P <: Properties[ID], F <: Flags[ID]](props: Vector[P])(
      implicit propCodec: CBOR.Encoder[Vector[P]],
      flagsCodec: CBOR.Decoder[Vector[F]],
      companion: FlagsCompanion[ID, F]): CachedResult[Vector[F]] =
      CachedResult(this) {
        getAllUncached[ID, P, F](props)
      }

    def getUncached[ID, P <: Properties[ID], F <: Flags[ID]](prop: P)(
      implicit propCodec: CBOR.Encoder[Vector[P]],
      flagsCodec: CBOR.Decoder[Vector[F]],
      companion: FlagsCompanion[ID, F]): Future[Option[F]] = {
      implicit val ec = ExecutionContext.parasitic // XXX: can't access IEC

      getAllUncached[ID, P, F](Vector(prop)) map { fs =>
        fs find { _.id == prop.id }
      }
    }

    def getCached[ID, P <: Properties[ID], F <: Flags[ID]](prop: P)(
      implicit propCodec: CBOR.Encoder[Vector[P]],
      flagsCodec: CBOR.Decoder[Vector[F]],
      companion: FlagsCompanion[ID, F]): CachedResult[Option[F]] =
      CachedResult(this) {
        getUncached[ID, P, F](prop)
      }

    def withFallbacks(
      account: Map[Feature[AccountID, Value], Value] = Map.empty,
      host: Map[Feature[HostID, Value], Value] = Map.empty): Service =
      new Service.Fallback(
        this,
        account map { case (f, v) => f.key -> v },
        host map { case (f, v) => f.key -> v })
  }

  case class Stats(recorder: StatsRecorder) {
    def incrRequests() = recorder.incr("FeatureFlags.Requests")
    def incrResponses() = recorder.incr("FeatureFlags.Responses")
    def incrFailures() = recorder.incr("FeatureFlags.Failures")
    def incrErrors() = recorder.incr("FeatureFlags.Errors")
    def incrTimeouts() = recorder.incr("FeatureFlags.Timeouts")

    def incrConnects() = recorder.incr("FeatureFlags.Connects")
    def incrConnectFailures() = recorder.incr("FeatureFlags.Connect.Failures")
    def incrPoolExhausted() = recorder.incr("FeatureFlags.Pool.Exhausted")

    def responseTime(elapsedMillis: Long) =
      recorder.timing("FeatureFlags.Response.Time", elapsedMillis)
  }

  sealed trait CachedResult[+A] {
    def get: Future[A]
  }

  object CachedResult {
    def apply[A](service: Service)(thunk: => Future[A]): CachedResult[A] =
      new CachedResult[A] {
        @volatile private[this] var cachedState: (Int, Future[A]) =
          (Int.MinValue, null)

        // recalculate/get ordering:
        // - get/recalc thread:
        //     - reads cachedState
        //     - reads service.version
        //     - synchronized:
        //       - reads service.version
        //       - re-reads cachedState
        //       - reads service state
        //       - updates cachedState
        // - service thread:
        //     - MUST update internal state, _then_ update version
        //
        // * reader can read internal state after the service has updated it but
        //   before the version has been updated. This leads to the cached value
        //   for V and version V-1. A subsequent read will redundantly
        //   recalculate the service state.
        // * two readers can read a new service.version and compete on
        //   recalculate. the first into the synchronized lock and update the
        //   state. the second will recheck the service.version and see that it
        //   matches, and no-op.
        def recalculate(): Unit =
          synchronized {
            val vers = service.version
            if (cachedState._1 != vers) {
              val fut = thunk
              cachedState = (vers, fut)
            }
          }

        @annotation.tailrec
        def get: Future[A] = {
          val (vers, fut) = cachedState
          if (vers != service.version) {
            recalculate()
            get
          } else {
            fut
          }
        }
      }
  }
}
