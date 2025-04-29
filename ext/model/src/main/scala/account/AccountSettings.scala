package fauna.model.account

import fauna.atoms.AccountID
import fauna.codex.json._
import fauna.flags.{ Value => FFValue }
import fauna.limits.AccountLimits
import fauna.logging.ExceptionLogging
import fauna.storage.doc.Data
import fauna.storage.ir._

object AccountSettings extends ExceptionLogging {

  /** Projects settings out of raw data, if any are defined. */
  def fromData(data: Data) = {
    val id = data.fields.get(List("id")) match {
      case Some(LongV(id)) => Some(AccountID(id))
      case _               => None
    }

    def flattenedProps(data: Data) = {
      val b = Map.newBuilder[String, FFValue]

      def path(prefix: List[String]) = prefix.reverse.mkString(".")

      def go(prefix: List[String], v: IRValue): Unit =
        v match {
          case StringV(v)  => b += path(prefix) -> v
          case LongV(v)    => b += path(prefix) -> v
          case DoubleV(v)  => b += path(prefix) -> v
          case BooleanV(v) => b += path(prefix) -> v

          case MapV(vs) =>
            vs.foreach { case (n, v) => go(n :: prefix, v) }
          case ArrayV(vs) =>
            vs.zipWithIndex.foreach { case (v, i) => go(i.toString :: prefix, v) }

          // Skip any other IR type
          case _ =>
            squelchAndLogException(
              throw new IllegalStateException(
                s"Unexpected value $v in db account data for $id"))
        }

      go(Nil, data.fields)

      b.result()
    }

    def getLimit(strength: String, op: String): Option[Double] =
      data.fields.get(List("limits", op, strength)).collect { case DoubleV(value) =>
        value
      }

    val props = flattenedProps(data)

    val limits =
      AccountLimits(
        getLimit("hard", "read_ops"),
        getLimit("hard", "write_ops"),
        getLimit("hard", "compute_ops"),
        getLimit("soft", "read_ops"),
        getLimit("soft", "write_ops"),
        getLimit("soft", "compute_ops")
      )

    AccountSettings(id, props, limits)
  }
}

final case class AccountSettings(
  id: Option[AccountID],
  ffProps: Map[String, FFValue],
  limits: AccountLimits) {
  def toJSON: JSValue =
    JSObject(
      "id" -> id.fold("N/A")(_.toLong.toString),
      "flag_props" -> ffProps,
      "limits" -> limits.toJSON)
}
