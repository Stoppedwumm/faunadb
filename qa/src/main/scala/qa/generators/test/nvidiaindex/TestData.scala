package fauna.qa.generators.test.nvidiaindex

import fauna.qa.DBResource
import fauna.prop._
import fauna.prop.api._

class TestData(authKey: String, propConfig: PropConfig, config: TestConfig)
    extends JSGenerators
    with DefaultQueryHelpers {

  implicit val propConf: PropConfig = propConfig

  private val ids = jsAlphaNumString(18, 18).times(config.NumIds).sample

  val userToken = {
    for {
      obj <- jsObject(
        "data" -> jsObject(
          "id" -> Prop.choose(ids),
          "se" -> jsAlphaNumString(32, 32),
          "token" -> jsAlphaNumString(32, 32),
          "deviceId" -> jsAlphaNumString(44, 44),
          "expiration" -> jsDate,
          "lastLogIn" -> jsDate,
          "deviceDescription" -> jsString(8, 20)
        )
      )
    } yield DBResource(authKey, ClassRef("user_tokens"), obj)
  }
}
