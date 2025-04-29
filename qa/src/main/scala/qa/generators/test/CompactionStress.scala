package fauna.qa.generators.test

import fauna.prop._
import fauna.prop.api._
import fauna.qa._

/**
  * Generates creates and removes at a configurable ratio. If the workload doesn't
  * write faster than the compaction rate, tombstones should not accumulate to higher
  * ratios than the configured workload remove ratio.
  */
final class CompactionStressGenerator(name: String, config: QAConfig)
    extends TestGenerator(name, config)
    with JSGenerators {

  private val initialDocs = config.getInt("compaction.initial-docs")
  private val bytesPerDoc = config.getInt("compaction.bytes-per-doc")
  private val keysRange = config.getInt("compaction.key-range")
  private val batchSize = config.getInt("compaction.batch-size")
  private val removeRatio = config.getDouble("compaction.remove-ratio")
  private val removesPerPage = math.ceil(batchSize * removeRatio).toInt

  private val keyP = Prop.int(keysRange)
  private val bytesP = Prop { _.rand.nextBytes(bytesPerDoc) }

  def schema: Schema.DB =
    Schema.DB(
      name = name,
      Schema.Collection("docs"),
      Schema.Index("docs_per_key", "docs", Vector("data", "key"))
    )

  override def initializer(db: Schema.DB, clientID: Int): RequestIterator = {
    val docsToAdd = math.ceil(initialDocs.toFloat / config.clients / batchSize).toInt
    val docsToRemove = math.ceil(docsToAdd * removeRatio / removesPerPage).toInt
    val creates = Iterator.fill(docsToAdd) { runCreate(db, clientID) }
    val removes = Iterator.fill(docsToRemove) { runRemove(db, clientID) }
    RequestIterator("init", creates.concat(removes))
  }

  def stream(db: Schema.DB): RequestStream =
    new MultiStream(
      IndexedSeq(
        RequestStream("create", () => runCreate(db)),
        RequestStream("delete", () => runRemove(db))
      ))

  private def runCreate(db: Schema.DB, key: Int = keyP.sample): FaunaQuery =
    FaunaQuery(
      db.serverKey.get,
      Do(Seq.fill(batchSize) {
        CreateF(
          ClassRef("docs"),
          MkObject(
            "data" -> MkObject(
              "key" -> key,
              "bytes" -> bytesP.sample
            ))
        )
      }: _*)
    )

  private def runRemove(db: Schema.DB, key: Int = keyP.sample): FaunaQuery =
    FaunaQuery(
      db.serverKey.get,
      Foreach(
        Lambda(
          "setEvt" -> Foreach(
            Lambda(
              "docEvt" -> RemoveVers(
                Select("document", Var("docEvt")),
                Select("ts", Var("docEvt")),
                Select("action", Var("docEvt"))
              )),
            Paginate(Events(Select("document", Var("setEvt"))))
          )),
        Paginate(
          Events(Match(IndexRef("docs_per_key"), key)),
          size = removesPerPage
        )
      )
    )
}

class CompactionStress(config: QAConfig) extends QATest {

  def generators: Array[TestGenerator] =
    Array(new CompactionStressGenerator("compaction-stress", config))
}
