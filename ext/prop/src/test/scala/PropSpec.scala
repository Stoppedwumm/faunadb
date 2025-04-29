package fauna.prop.test

import fauna.prop._
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import scala.util.Random

object PropSpecConfig {

  def apply(default: Int = 5, exhaustive: Int = 200) = {
    val min = Option(System.getenv("EXHAUSTIVE")).fold(default) { _ =>
      exhaustive
    }
    val seedOpt = Option(System.getenv("SEED")) flatMap { _.toLongOption }
    val seed = seedOpt getOrElse new Random().nextLong()
    PropConfig(minSuccessful = min, seed = seed)
  }
}

abstract class PropSpec(default: Int, exhaustive: Int)
    extends AnyFunSuite
    with Retries {
  implicit val propConfig = PropSpecConfig(default, exhaustive)

  override def withFixture(test: NoArgTest): Outcome = {
    val outcome = if (isRetryable(test)) {
      withRetryOnFailure { super.withFixture(test) }
    } else {
      super.withFixture(test)
    }

    outcome match {
      case out: Exceptional => info(s"TEST SEED: ${propConfig.seed}"); out
      case other            => other
    }
  }

  def prop(name: String, testTags: Tag*)(
    prop: => Prop[Unit]
  )(implicit conf: PropConfig) =
    test(name, testTags: _*) { prop.test(conf) }

  def once(name: String, testTags: Tag*)(
    prop: => Prop[Unit]
  )(implicit conf: PropConfig) =
    test(name, testTags: _*) { prop.sample(conf) }
}
