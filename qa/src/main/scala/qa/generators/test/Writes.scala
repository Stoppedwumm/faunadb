package fauna.qa.generators.test

import fauna.codex.json.{ JSNull, JSValue }
import fauna.prop.api._
import fauna.qa._

/**
  * A write-only work load that continuously creates new documents of a collection.
  * `WritesGenerator` writes to a collection without an index.
  * `IndexedWritesGenerator` creates four indexes on the collection, one for each field
  * holding progressively large strings as values.
  * Config:
  *  - writes.per-transaction = the number of creates to execute per query
  */
abstract class AbstractWritesGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val schema: Schema.DB

  val documentsPerTransaction = conf.getInt("writes.per-transaction")
  // When configured, only emit one fixed-size field
  val fixedSizeWrite = conf.opt("writes.fixed-size", _.toInt)

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("writes", () => query(auth))
  }

  val data = fixedSizeWrite match {
    case Some(size) =>
      jsObject("f" -> jsString(size, size))
    case None =>
      jsObject(
        "field_with_tiny_data" -> jsString(2, 16),
        "field_with_short_data" -> jsString(2, 64),
        "field_with_medium_data" -> jsString(2, 256),
        "field_with_long_data" -> jsString(2, 1024)
      )
  }
  val documentP = jsObject("data" -> data)

  private def query(auth: String) = {
    val creates: List[JSValue] = List.fill(documentsPerTransaction) {
      CreateF(ClassRef("totally_normal_class"), Quote(documentP sample))
    }

    FaunaQuery(auth, Do(creates, JSNull))
  }
}

class WritesGenerator(name: String, conf: QAConfig)
    extends AbstractWritesGenerator(name, conf) {
  val schema = Schema.DB(name, Schema.Collection("totally_normal_class"))
}

class IndexedWritesGenerator(name: String, conf: QAConfig)
    extends AbstractWritesGenerator(name, conf) {

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
}

class Writes(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(new WritesGenerator("writes", config))
}

class IndexedWrites(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new IndexedWritesGenerator("indexed-writes", config)
  )
}
