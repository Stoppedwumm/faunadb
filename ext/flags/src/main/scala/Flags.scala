package fauna.flags

import fauna.atoms.{ AccountID, HostID }
import fauna.codex.cbor._
import fauna.logging.ExceptionLogging
import scala.reflect.ClassTag

sealed abstract class FlagsCompanion[ID: CBOR.Decoder, F <: Flags[ID]] {

  private val FlagsKey = "flags"

  implicit def FlagsVecDecoder = new FlagsVecDecoder

  /** This codec decodes a Map of Flags from a service response to a
    * Vector. The wire protocol looks like:
    *
    * {
    *   id: {
    *     "flags": values
    *   }
    * }
    */
  final class FlagsVecDecoder extends CBOR.Decoder[Vector[F]] {

    def decode(stream: CBOR.In): Vector[F] = {
      // XXX: eliminate the intermediate Map with a CBORSwitch?
      val flags = CBOR.decode[Map[ID, Map[String, Map[String, Value]]]](stream)
      val b = Vector.newBuilder[F]

      flags foreach { case (id, flags) =>
        flags.get(FlagsKey) foreach { vs =>
          b += apply(id, vs)
        }
      }

      b.result()
    }
  }

  implicit def flagsCompanion = this

  def apply(id: ID, values: Map[String, Value]): F
}

/** A bag of properties, represented as key/value pairs, similar to
  * Properties.
  *
  * These are returned as responses by the service, and correlate by
  * ID with the Properties in the request.
  */
sealed abstract class Flags[ID] extends ExceptionLogging {
  def id: ID
  def values: Map[String, Value]

  /** Returns the unwrapped value, if any is associated, or the
    * default value of the Feature.
    *
    * If the value is not of the correct type, logs an exception and
    * returns the default.
    */
  def get[T, V <: Value: ClassTag](feature: Feature[ID, V])(
    implicit ev: V#T <:< T): T =
    values.getOrElse(feature.key, feature.default) match {
      case v: V => ev(v.value)
      case v =>
        logException(new RuntimeException(s"unexpected value for $feature: $v"))
        feature.default.value
    }
}

object AccountFlags extends FlagsCompanion[AccountID, AccountFlags] {
  def forRoot = {
    val acct = AccountID.Root
    val values = Map[String, Value](Properties.AccountID -> acct.toLong)

    val flags = FileService.EnvState.flagsForValues(values)
    AccountFlags(acct, flags)
  }
}

case class AccountFlags(id: AccountID, values: Map[String, Value] = Map.empty)
    extends Flags[AccountID]

object HostFlags extends FlagsCompanion[HostID, HostFlags]

case class HostFlags(id: HostID, values: Map[String, Value] = Map.empty)
    extends Flags[HostID]
