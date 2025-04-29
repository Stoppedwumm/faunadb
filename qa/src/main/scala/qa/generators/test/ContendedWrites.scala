package fauna.qa.generators.test

import fauna.qa._
import fauna.codex.json._
import fauna.prop._

/**
  * Create a set of documents with a counter field. Then run updates against that
  * field that increment it by one each time.
  *  - contended-writes.predefined-counters = initial number of counter instances
  *  to create
  *  - contended-writes.contention-factor = a way to configure the amount of
  *  contention by determining how many counters are incremented.
  */
class ContendedWritesGenerator(name: String, benchConfig: QAConfig)
    extends TestGenerator(name, benchConfig) {

  private val tConfig = new ContendedWritesConfig(benchConfig)
  private val knownCounterIds = Prop.int(tConfig.nCounters)

  private val refs = Prop[Option[JSValue]] { conf =>
    if (conf.rand.nextDouble() > tConfig.contentionFactor) None
    else {
      Some(counterRef(knownCounterIds.sample))
    }
  }

  def schema: Schema.DB =
    Schema.DB(name, Schema.Collection("counters"))

  override protected def initializer(schema: Schema.DB): RequestIterator = {
    val serverKey = schema.serverKey.get
    RequestIterator("init-counters", Iterator.range(0, tConfig.nCounters) map { id =>
      createCounter(serverKey, Some(counterRef(id)))
    })
  }

  def stream(schema: Schema.DB) = {
    val serverKey = schema.serverKey.get
    RequestStream(
      name,
      () =>
        refs.sample match {
          case Some(ref) => incrementCounter(serverKey, ref)
          case None      => createCounter(serverKey)
        }
    )
  }

  private def counterRef(id: Int): JSValue =
    MkRef(ClassRef("counters"), id.toString)

  private def createCounter(
    auth: String,
    ref: Option[JSValue] = None
  ): FaunaQuery = {
    FaunaQuery(
      auth,
      CreateF(
        ref.getOrElse(ClassRef("counters")),
        MkObject("data" -> MkObject("counter" -> 0))
      )
    )
  }

  private def incrementCounter(auth: String, ref: JSValue): FaunaQuery = {
    FaunaQuery(
      auth,
      Update(
        ref,
        MkObject(
          "data" -> MkObject(
            "counter" -> AddF(1, Select(JSArray("data", "counter"), Get(ref)))
          )
        )
      )
    )
  }
}

private class ContendedWritesConfig(benchConfig: QAConfig) {
  val nCounters = benchConfig.getInt("contended-writes.predefined-counters")

  val contentionFactor =
    benchConfig.getDouble("contended-writes.contention-factor")
}

class ContendedWrites(config: QAConfig) extends QATest {

  def generators: Array[TestGenerator] = Array(
    new ContendedWritesGenerator("contended-writes", config)
  )
}
