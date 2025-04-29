package fauna.qa.generators.test

import fauna.qa._

/** DriverTestGenerator is a basic class that provides the minimal interface
  * for the Commander and Worker processes to instantiate and pass to the
  * DriverCustomer class, which will handle executing the queries via the drivers.
  *
  * When adding new external driver test generators, add a new class def in this file
  * with the name of the test generator (e.g. StreamingWithDriver below), which must
  * have a matching directory under {core}/qa/DriverTestGenerators - see the README
  * in that location for more details.
  */

class DriverTestGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf) {
  override def useDriver: Boolean = true

  val schema = Schema.DB(name)

  def stream(schema: Schema.DB): RequestStream =
    throw new UnsupportedOperationException(
      "DriverTestGenerator does not support calling stream() directly")
}

class StreamingWithDriver(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(
    new DriverTestGenerator("StreamingWithDriver", config))
}

class SchemaManipulation(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(
    new DriverTestGenerator("SchemaManipulation", config))
}
