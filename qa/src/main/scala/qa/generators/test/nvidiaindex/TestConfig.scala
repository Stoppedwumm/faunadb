package fauna.qa.generators.test.nvidiaindex

import fauna.qa.QAConfig

class TestConfig(config: QAConfig) {
  val NumIds = config.getInt("nvidiaindex.data.ids")
}
