package fauna.qa.generators.test

import fauna.codex.json.JSNull
import fauna.prop.api._
import fauna.qa._

/**
  * Generates a number of documents in a collection named "docs". Each
  * document contains randomly-generated data, and a well-known field
  * named "field" with a randomly-generated string value.
  *
  * The well-known field can be indexed easily, for example:
  *
  *   CreateIndex(
  *     MkObject(
  *       "name"   -> "docs_by_field",
  *       "source" -> Collection("docs"),
  *       "terms"  -> JSArray(MkObject("field" -> JSArray("data", "field")))))
  *
  * Config fields:
  *   - docs.count = the total number of documents to generate
  *   - docs.depth.min = the minimum depth of each document body (must be > 0)
  *   - docs.depth.max = the maximum depth of each document body (must be >= docs.depth.min)
  *   - docs.field.min = the minimum width of "field" (must be > 0)
  *   - docs.field.max = the maximum width of "field" (must be >= docs.field.min)
  */
final class DocumentsGenerator(name: String, config: QAConfig)
    extends TestGenerator(name, config)
    with JSGenerators {

  val CollectionName = "docs"
  val FieldName = "field"

  /* total number of docs to generate */
  private[this] val docs =
    config.opt("docs.count", _.toLong) getOrElse 10_000_000L

  /* minimum depth of each doc body */
  private[this] val minDepth =
    config.opt("docs.depth.min", _.toInt).getOrElse(10) max 0

  /* maximum depth of each doc body */
  private[this] val maxDepth =
    config.opt("docs.depth.max", _.toInt) getOrElse 20

  /* minimum width of each doc's well-known field */
  private[this] val minField =
    config.opt("docs.field.min", _.toInt).getOrElse(10) max 0

  /* maximum width of each doc's well-known field */
  private[this] val maxField =
    config.opt("docs.field.max", _.toInt) getOrElse 1024

  /* number of docs each worker will generate */
  private[this] val batchSize = docs / config.clients

  /* data generator, with one well-known field for indexing */
  private[this] val data = jsObject(minDepth, maxDepth max minDepth) flatMap { data =>
    jsObject(FieldName -> jsString(minField, maxField max minField)) map { field =>
      data merge field
    }
  }

  val schema = Schema.DB(name, Schema.Collection(CollectionName))

  override def initializer(schema: Schema.DB, worker: Int) = {
    val offset = worker * batchSize
    val range = offset until (offset + batchSize)
    val auth = schema.serverKey.get

    RequestIterator("init", range.to(LazyList) map { _ =>
      FaunaQuery(
        auth,
        CreateF(ClassRef(CollectionName), Quote(jsObject("data" -> data).sample)))
    })
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get

    RequestStream("empty", () => FaunaQuery(auth, JSNull))
  }
}

final class Documents(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] =
    Array(new DocumentsGenerator("documents", config))
}
