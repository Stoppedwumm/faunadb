package fauna.qa.generators.test

import fauna.prop._
import fauna.prop.api._
import fauna.qa._
import scala.math.pow

/**
  * A read-only workload.
  * `ReadsGenerator` runs get requests against a set of instances.
  * `IndexedReadsGenerator` requests a randomly sized page from a match against
  * indexed data.
  * `DocumentsIndexReadsGenerator` uses the documents index to page through the
  * entire data set using randomized page sizes.
  * Config fields:
  *  - reads.instances = the number of instances to create
  *  - reads.per-transaction = the number of get or paginate calls per request
  */
abstract class AbstractReadsGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val schema = Schema.DB(
    name,
    Schema.Collection("totally_normal_class"),
    Schema.Index(
      "index_of_field_with_tiny_data",
      "totally_normal_class",
      Vector("data", "field_with_tiny_data")
    ),
    Schema.Index(
      "index_of_field_with_short_data",
      "totally_normal_class",
      Vector("data", "field_with_short_data")
    ),
    Schema.Index(
      "index_of_field_with_medium_data",
      "totally_normal_class",
      Vector("data", "field_with_medium_data")
    ),
    Schema.Index(
      "index_of_field_with_long_data",
      "totally_normal_class",
      Vector("data", "field_with_long_data")
    )
  )

  val instances = conf.getInt("reads.instances")
  val instsPerInit = instances / conf.clients
  val readsPerTransaction = conf.getInt("reads.per-transaction")
  val indexDensity = conf.getInt("reads.density")
  val fixedSizeRead = conf.opt("reads.fixed-size", _.toInt)

  val indexValueLimit = instances / indexDensity

  override def initializer(schema: Schema.DB, clientID: Int) = {
    val start = (clientID * instsPerInit)
    val ids = start until (start + instsPerInit)
    val auth = schema.serverKey.get

    RequestIterator("init", ids.to(LazyList) map { id =>
      FaunaQuery(
        auth,
        CreateF(Ref(s"classes/totally_normal_class/$id"), Quote(instanceP sample))
      )
    })
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("reads", () => query(auth))
  }

  val data = fixedSizeRead map { size =>
    jsObject("f" -> jsString(size, size))
  } getOrElse {
    jsObject(
      "field_with_tiny_data" -> jsString(2, 16),
      "field_with_short_data" -> jsString(2, 64),
      "field_with_medium_data" -> jsString(2, 256),
      "field_with_long_data" -> jsString(2, 1024)
    )
  }

  val instanceP = jsObject("data" -> data)

  def query(auth: String): FaunaQuery
}

class ReadsGenerator(name: String, conf: QAConfig)
    extends AbstractReadsGenerator(name, conf) {

  val refP = Prop.int(instances) map { id =>
    Ref(s"classes/totally_normal_class/$id")
  }

  def query(auth: String) =
    FaunaQuery(auth, List.fill(readsPerTransaction)(Get(refP.sample)))
}

class Reads(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(new ReadsGenerator("reads", config))
}

class IndexedReadsGenerator(name: String, conf: QAConfig)
    extends AbstractReadsGenerator(name, conf) {

  val matchP = Prop.int(indexValueLimit) map { term =>
    Match(Ref(s"indexes/index_of_field_with_medium_data"), term)
  }

  val pagesSizeP = Prop.int(5) map { exp =>
    pow(2, exp).toInt
  }

  def query(auth: String) =
    FaunaQuery(
      auth,
      List.fill(readsPerTransaction)(
        Paginate(matchP.sample, size = pagesSizeP.sample)
      )
    )
}

class IndexedReads(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new IndexedReadsGenerator("mixed-indexed-reads", config)
  )
}

class DocumentsIndexReadsGenerator(name: String, config: QAConfig)
    extends AbstractReadsGenerator(name, config) {

  val pagesSizeP = Prop.int(7 to 16) map { pow(2, _).toInt }

  override def query(auth: String) =
    FaunaQuery(
      auth,
      Paginate(
        Documents(ClassRef("totally_normal_class")),
        size = pagesSizeP.sample),
      Some(CursorDir.After))
}

class DocumentsIndexReads(config: QAConfig) extends QATest {

  val generators = Array(
    new DocumentsIndexReadsGenerator("documents-index-reads", config))
}
