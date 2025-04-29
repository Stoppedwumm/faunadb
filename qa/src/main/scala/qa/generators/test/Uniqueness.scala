package fauna.qa.generators.test

import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.prop.api.JSGenerators
import fauna.qa._

/**
  * This test creates documents with a unique constraint, and
  * remembers a sample of them. During validation, it attempts to
  * create documents which violate the unique constraint; if any
  * succeed, the validation fails.
  */
class UniquenessGenerator(name: String, config: QAConfig)
    extends TestGenerator(name, config)
    with JSGenerators
    with ValidatingTestGenerator {

  override val isTimeAware: Boolean = false
  override val isPassFail: Boolean = true
  override val expectValidateQueryToFail: Boolean = true

  val schema =
    Schema.DB(
      name,
      Schema.Collection("docs"),
      Schema.Index(
        "unique_docs",
        "docs",
        Vector(Schema.Index.Term(Vector("data", "field"))),
        Vector.empty,
        unique = true))

  protected val samples = new SamplingBuffer(
    config.getInt("uniqueness.buffer-size"))

  def stream(schema: Schema.DB) = {
    RequestStream(
      "uniqueness",
      () => {
        val data = MkObject("data" -> MkObject("field" -> jsString(5, 25).sample))

        new FaunaQuery(schema.serverKey.get, CreateF("docs", data)) {

          // Only sample successful results: the query may
          // legitimately encounter a constraint failure, or any
          // number of other errors, but errors are safely ignored
          // here.
          override def result(body: JSObject) = {
            samples.add(Seq(data))
            None
          }
        }
      }
    )
  }

  override def getValidateStateQuery(
    schema: Schema.DB,
    ts: Timestamp): FaunaQuery = {
    val data = samples.take(1)

    val query = if (data.isEmpty) {
      Abort("no samples yet!")
    } else {
      Do(data map { data =>
        CreateF("docs", data)
      })
    }
    new FaunaQuery(schema.serverKey.get, query)
  }
}

/**
  * This test creates and deletes documents with a unique constraint.
  * During validation, it samples the deleted documents and attempts to
  * recreate them which should be allowed by the unique constraint; if any
  * fail, the validation fails because the index still has the deleted docs.
  */
class NegativeUniquenessGenerator(name: String, config: QAConfig)
    extends UniquenessGenerator(name, config)
    with JSGenerators
    with ValidatingTestGenerator {

  override val expectValidateQueryToFail: Boolean = false

  override protected def initializer(schema: Schema.DB): RequestIterator = {
    // Prepopulate some random documents
    val range = 0 until 200
    RequestIterator("init-data", range.to(LazyList) map { _ =>
      create(schema.serverKey.get)
    })
  }

  override def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get

    new MultiStream(
      IndexedSeq(
        RequestStream("create", () => create(auth)),
        RequestStream("delete", () => delete(auth))
      )
    )
  }

  private def create(auth: String) = {
    val data = MkObject(
      "data" -> MkObject(
        "field" -> Concat(JSArray(ToString(Now()), jsString(5, 25).sample), "-")
      ))
    FaunaQuery(
      auth,
      CreateF("docs", data),
      maxContentionRetries = Some(1))
  }

  private def delete(auth: String) = {
    new FaunaQuery(
      auth,
      DeleteF(Select(0, Paginate(Documents(ClassRef("docs"))))),
      maxContentionRetries = Some(1)) {
      override def result(body: JSObject) = {
        samples.add(Seq((body / "resource").as[JSObject]))
        None
      }
    }
  }

  override def getValidateStateQuery(
    schema: Schema.DB,
    ts: Timestamp): FaunaQuery = {
    // Evenly distribute the sample buffer across the number of core nodes
    val sampleCount = (samples.size / config.coreCluster.length) min 1
    val resources = samples.take(sampleCount)

    new FaunaQuery(
      schema.serverKey.get,
      Do(
        resources map { resource =>
          val prev = (resource / "data").as[JSObject]
          val txnTS = (resource / "ts").as[Long]
          // NB. Merge the txn time so that we know it if the validation fails.
          val next = Quote(prev merge JSObject("txnTS" -> Timestamp.ofMicros(txnTS).toString))
          CreateF("docs", MkObject("data" -> next))
        },
        JSNull
      ))
  }
}

class Uniqueness(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new UniquenessGenerator("Uniqueness", config)
  )
}

class NegativeUniqueness(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new NegativeUniquenessGenerator("NegativeUniqueness", config)
  )
}
