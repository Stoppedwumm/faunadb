package fauna.flags.test

import fauna.flags._

class FeatureSpec extends Spec {
  test("toString") {
    RunQueries.toString should equal("Feature(run_queries, default = true)")
  }
}
