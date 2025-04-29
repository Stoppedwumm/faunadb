package fauna.lang

object PID {

  final class Clamp(min: Double, max: Double) {
    require(min <= max, "Clamp's min must be less than or equal to max.")

    def clamp(value: Double): Double =
      math.max(math.min(value, max), min)
  }

  val NoClamp = new Clamp(Double.NegativeInfinity, Double.PositiveInfinity)

  def limit(min: Double, max: Double): Clamp = new Clamp(min, max)

  def window(value: Double): Clamp = new Clamp(-value, value)
}

/**
 * Cheesy PID.
 *
 * See wikpedia / the internet for the gory details.
 *
 * Cliff's notes:
 * A controller takes a setpoint (desired system state), and then samples the
 * system state and compares it to the setpoint and hands out its best guess of
 * an adjustment that will bring the system to the setpoint. This adjustment
 * can be anything and isn't related to the setpoint. For instance:
 *
 * You want to ensure that 10% of all reads issue a backup read if they
 * cross some latency threshold, but you don't know what that threshold is. A
 * PID controller can provide that threshold by sampling the observed rate of
 * backup reads for a given threshold. If you were a complete loser you might
 * call this machine learning.
 *
 * WARNING ASSUMPTION: Currently assumes that the sampling is done at a
 *                     constant rate. YOU MUST OBEY THIS OR YOUR RESULTS WILL
 *                     SUCK.
 *
 * Anti integral windup is implemented by limiting integral gain to a pre-defined
 * acceptable window. Anti-windup prevents excessive integral gain upon a noisy
 * input. The proper window must be calculated in top of the expected normal
 * deviation of the input signal.
 *
 * Thoughts on tuning:
 * 1. You must set an integral term to account for the fact that magitudes might
 *    change. If you just set a proportional term then it must be large enough
 *    to cover the entire error domain. Unless it is bounded don't do that.
 * 2. Even if the error domain is bounded massive prop gain will probably give
 *    you a pretty unstable controller. Tamp down the prop gain and use the
 *    integral term to bring the output value to the magitude you need.
 * 3. Use derivative gain with caution. Apparently only 25% controllers in the
 *    wild use it, but perhaps software controllers do better this?
 *
 * TODO:
 * 1. Investigate stencils for derivatives.
 * 2. Insist on timestamps. This would mean that variable sampling rates
 *    wouldn't completely guff up the integral / derivative terms.
 * 3. Check all signs positive negative? Have a direction parameter?
 * 4. Add all the stats.
 * 5. Feed forward?
 *
 * @constructor Create a new PID with the given paramters
 * @param p Proportional gain.
 * @param i Integral gain.
 * @param d Derivative gain.
 * @param limit PID output boundaries.
 * @param windup PID anti-windup boundaries.
 */
class PID(
  p: Double,
  i: Double,
  d: Double,
  limit: PID.Clamp = PID.NoClamp,
  windup: PID.Clamp = PID.NoClamp) {

  private var integralErr = 0.0
  private var previousErr = 0.0
  private var bootstrapped = false

  var setPoint = 0.0

  /**
   * Sample the current observed state.
   *
   * @param observed The currently observed state.
   * @return output that will bring the system (closer) to the setpoint.
   */
  def apply(observed: Double): Double = {
    val err = setPoint - observed
    val prop = p * err
    integralErr = limit.clamp(integralErr + windup.clamp(err * i))
    val deri =
      if (bootstrapped) {
        (err - previousErr) * d
      } else {
        bootstrapped = true
        0
      }
    val output = limit.clamp(prop + integralErr + deri)
    previousErr = err
    output
  }
}
