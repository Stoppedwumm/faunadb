package fauna.model.test

import fauna.model.runtime.fql2.{ QueryCheckFailure, QueryRuntimeFailure, Result }
import fql.error.TypeError
import org.scalacheck.rng.Seed
import org.scalacheck.Prop.propBoolean
import org.scalacheck.Properties
import org.scalacheck.Test.Parameters

// The ==> syntax allows for reporting test results like this:
// [is this a valid test?] ==> [did the test succeed?]
// Only "valid" tests that failed will go through the shrinking logic
// to find the smallest input that results in a failure.
case object PropTestResult {
  val ValidTestPassed = true ==> true
  val ValidTestFailed = true ==> false
  val InvalidTestPassed = false ==> true
  val InvalidTestFailed = false ==> false
}

abstract class FQL2BaseFuzzSpec(name: String)
    extends Properties(name)
    with FQL2Spec
    with FuzzGenerators {

  val fuzzTestsEnabled = sys.env.get("RUN_FUZZ_TESTS").contains("true")

  val initialTestSeed = sys.env.get("FUZZ_TEST_SEED") match {
    case Some(value) => Seed.fromBase64(value).get
    case None        => Seed.random()
  }

  if (fuzzTestsEnabled) println(f"FUZZ TEST SEED: ${initialTestSeed.toBase64}")

  override def overrideParameters(p: Parameters) = {
    if (fuzzTestsEnabled)
      p.withInitialSeed(initialTestSeed)
        .withMinSuccessfulTests(5000)
        .withMaxSize(20)
    else
      // Skip all props
      p.withPropFilter(Some("^$"))
  }

  protected def isValidRes(res: Result[_]): Boolean = {
    res match {
      case Result.Err(QueryRuntimeFailure.Simple("stack_overflow", _, _, _)) => false
      case Result.Err(QueryRuntimeFailure.Simple("divide_by_zero", _, _, _)) => false
      case Result.Err(
            QueryRuntimeFailure.Simple(
              "invalid_bounds",
              "expected `limit` to be greater than 0",
              _,
              _)) =>
        false
      case Result.Err(QueryCheckFailure(Seq(TypeError(_, _, _, _)))) => false
      case _                                                         => true
    }
  }

  protected def isKnownException(ex: Throwable): Boolean = {
    ex match {
      // add known exceptions here, along with ticket #
      // case _: IllegalStateException =>
      //   ex.getMessage().contains("invalid ord token")
      case _ => false
    }
  }
}
