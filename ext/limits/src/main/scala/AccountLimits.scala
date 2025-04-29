package fauna.limits

import fauna.atoms._
import fauna.codex.json._
import fauna.flags._
import fauna.stats.QueryMetrics

object AccountLimits {
  val empty = AccountLimits(None, None, None, None, None, None)
}

/** This structure is created by the settings in the Database model
  * object.
  *
  * See AccountSettings.
  */
final case class AccountLimits(
  hardRead: Option[Double],
  hardWrite: Option[Double],
  hardCompute: Option[Double],
  softRead: Option[Double],
  softWrite: Option[Double],
  softCompute: Option[Double]) {

  def toJSON: JSObject = {
    def limit(v: Option[Double]): JSValue =
      v map { JS(_) } getOrElse JSString("N/A")

    JSObject(
      "read_ops" -> JSObject(
        "hard" -> limit(hardRead),
        "soft" -> limit(softRead)
      ),
      "write_ops" -> JSObject(
        "hard" -> limit(hardWrite),
        "soft" -> limit(softWrite)
      ),
      "compute_ops" -> JSObject(
        "hard" -> limit(hardCompute),
        "soft" -> limit(softCompute))
    )
  }

  def getHardLimits(
    accountFlags: AccountFlags,
    readsFlag: RateLimitsReads,
    writesFlag: RateLimitsWrites,
    computeFlag: RateLimitsCompute,
    systemReadLimit: Double,
    systemWriteLimit: Double,
    systemComputeLimit: Double,
    burstSeconds: Double): OpsLimits = {
    val readLimit =
      opLimit(accountFlags, readsFlag, hardRead, maximum = systemReadLimit)
    val readBurst = burst(readLimit, systemReadLimit, burstSeconds)

    val writeLimit =
      opLimit(accountFlags, writesFlag, hardWrite, maximum = systemWriteLimit)
    val writeBurst = burst(writeLimit, systemWriteLimit, burstSeconds)

    val computeLimit =
      opLimit(
        accountFlags,
        computeFlag,
        hardCompute,
        QueryMetrics.BaselineCompute,
        maximum = systemComputeLimit * QueryMetrics.BaselineCompute)
    val computeBurst =
      burst(
        computeLimit,
        systemComputeLimit * QueryMetrics.BaselineCompute,
        burstSeconds)

    OpsLimits(
      OpLimit(readLimit, readBurst),
      OpLimit(writeLimit, writeBurst),
      OpLimit(computeLimit, computeBurst))
  }

  def getSoftLimits(
    accountFlags: AccountFlags,
    readsFlag: RateLimitsReads,
    writesFlag: RateLimitsWrites,
    computeFlag: RateLimitsCompute,
    hardLimits: OpsLimits): OpsLimits =
    OpsLimits(
      OpLimit(
        opLimit(
          accountFlags,
          readsFlag,
          softRead,
          maximum = hardLimits.read.permitsPerSecond),
        0.0),
      OpLimit(
        opLimit(
          accountFlags,
          writesFlag,
          softWrite,
          maximum = hardLimits.write.permitsPerSecond),
        0.0),
      OpLimit(
        opLimit(
          accountFlags,
          computeFlag,
          softCompute,
          QueryMetrics.BaselineCompute,
          maximum = hardLimits.compute.permitsPerSecond),
        0.0)
    )

  // this is a little gross, but the order of precedence is:
  // 1. AccountFlag
  // 2. AccountSettings
  // 3. AccountFlag default value
  private def opLimit(
    accountFlags: AccountFlags,
    feature: Feature[AccountID, DoubleValue],
    setting: Option[Double],
    multiplier: Double = 1.0,
    maximum: Double): Double = {
    val value: Double = accountFlags.get(feature)
    val limit: Double =
      if (accountFlags.values.contains(feature.key)) {
        value
      } else {
        setting.getOrElse(value)
      }

    (limit * multiplier) min maximum
  }

  // Compute the number of burst seconds at the base rate which is the
  // minimum of burstSeconds and maxPerSecond. This prevents idle time
  // accumulating more permits than are allowed in a single second at
  // the max rate.
  private def burst(
    basePerSecond: Double,
    maxPerSecond: Double,
    burstSeconds: Double): Double =
    if (basePerSecond + (basePerSecond * burstSeconds) <= maxPerSecond) {
      burstSeconds
    } else {
      // The # of seconds at the base rate it would take to accumulate
      // the max no. of permits, minus 1 sec. of base rate.
      Math.floor(maxPerSecond / basePerSecond) - 1.0
    }
}
