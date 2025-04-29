package fauna.qa.generators.op

import fauna.qa._

/**
  * InitOnly does not mutate any part of the system. It will only initialized the
  * system with the default setup.
  */
class InitOnly(config: QAConfig) extends TimedRun(config)
