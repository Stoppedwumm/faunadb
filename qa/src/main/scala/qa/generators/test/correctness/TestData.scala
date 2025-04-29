package fauna.qa.generators.test.correctness

import fauna.qa.DBResource
import fauna.prop._
import fauna.prop.api._

object TestData {
  val class1Ref = DefaultQueryHelpers.ClassRef("class1")
}

class TestData(authKey: String, propConfig: PropConfig, config: TestConfig)
    extends JSGenerators {

  implicit val propConf: PropConfig = propConfig
  val minSize = config.MaxInstancePadding / 2

  val class1 = {
    for {
      obj <- jsObject(
        "data" -> jsObject(
          "p1" -> jsValue,
          "p2" -> jsValue,
          "p3" -> jsValue,
          "p4" -> jsValue,
          "p5" -> jsString(minSize, config.MaxInstancePadding)
        )
      )
    } yield DBResource(authKey, TestData.class1Ref, obj)
  }
}
