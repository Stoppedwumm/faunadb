package fauna.qa.generators.test

import fauna.codex.json._
import fauna.qa._
import fauna.prop._
import fauna.prop.api._

/**
  * A contentious mixed workload. A configurable number of instances are created.
  * Then three streams are started: reads, inserts, and removes. Each stream will
  * randomly pick an instance ref and run either a get, insert, or remove on it.
  * Fewer instances will cause higher contention. The generator can also be
  * configured to create a new index over the data at some point in the run.
  * Doing so will cause the background tasks to run causing more load.
  * Config fields:
  *  - simple.ids = the number of instances to create
  *  - simple.reads-per-minute = rate at which the read stream will run
  *  - simple.inserts-per-minute = rate at which the inserts stream will run
  *  - simple.removes-per-minute = rate at which the removes stream will run
  *  - simple.add-index-after = point after start a new index should be
  *  created. If this is null, a new index will not be created.
  */
class SimpleGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val ids = conf.getInt("simple.ids")

  val instsPerInit = ids / conf.clients

  val schema = Schema.DB(
    name,
    Schema.Collection("a_class"),
    Schema.Index("an_index", "a_class", Vector("data", "indexed_field"))
  )

  def newIndexQuery(auth: String) =
    FaunaQuery(
      auth,
      CreateF(
        Ref("indexes"),
        MkObject(
          "name" -> "another_index",
          "source" -> ClassRef("a_class"),
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "indexed_field")))
        )
      )
    )

  override def initializer(schema: Schema.DB, clientID: Int) = {
    val start = (clientID * instsPerInit)
    val idRange = start until (start + instsPerInit)
    val auth = schema.serverKey.get

    RequestIterator("init", idRange.to(LazyList) map { id =>
      FaunaQuery(auth, CreateF(Ref(s"classes/a_class/$id"), dataP sample))
    })
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get

    new MultiStream(
      IndexedSeq(
        RequestStream("reads", () => read(auth)),
        RequestStream("inserts", () => insert(auth)),
        RequestStream("removes", () => remove(auth))
      )
    )
  }

  val refP = Prop.int(ids) map { id =>
    Ref(s"classes/a_class/$id")
  }

  val dataP =
    jsObject(
      "data" ->
        jsObject("indexed_field" -> jsString(8, 8, 'a' to 'z'))
    ) map { Quote(_) }

  private def insert(auth: String) =
    FaunaQuery(
      auth,
      List(InsertVers(refP sample, Time("now"), "create", dataP sample))
    )

  private def remove(auth: String) =
    FaunaQuery(auth, List(RemoveVers(refP sample, Time("now"), "create")))

  private def read(auth: String) =
    FaunaQuery(auth, Get(refP sample))
}

class Simple(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(new SimpleGenerator("simple", config))
}
