package fauna.qa.generators.test

import fauna.qa._
import fauna.codex.json.JSObject
import fauna.prop.api._

/**
  * LargeSet creates a very large index set by indexing on a boolean field and
  * inserting a large number of instances with field randomly set to true or
  * false. There are two generators run in sequence: the first only matches
  * against the index and returns the resulting refs. The second maps over the
  * results and does a get on every instance in the set.
  */
abstract class LargeSetGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val schema = Schema.DB(
    name,
    Schema.Collection("aclass"),
    Schema.Index("bool_index", "aclass", Vector("data", "theindex"))
  )

  def insts(schema: Schema.DB) = {
    val dataP = jsObject(1, 20) flatMap { obj =>
      jsObject("theindex" -> jsBoolean) map { _ ++ obj }
    }

    jsObject("data" -> dataP) map {
      DBResource(schema.serverKey.get, ClassRef("aclass"), _)
    } times (conf.getInt("baseline.largeset.instances")) sample
  }

  def query(auth: String): FaunaQuery

  override protected def initializer(schema: Schema.DB) = {
    val data = insts(schema)
    RequestIterator("init", data.map(_.toRequest))
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("reads", () => query(auth))
  }
}

class LargeSetRefsOnlyGenerator(name: String, conf: QAConfig)
    extends LargeSetGenerator(name, conf) {

  def query(auth: String): FaunaQuery =
    FaunaQuery(
      auth,
      Paginate(
        Match(Ref("indexes/bool_index"), true),
        size = conf.getInt("baseline.largeset.results")
      ),
      Some(CursorDir.After)
    )
}

class LargeSetWithDataGenerator(name: String, conf: QAConfig)
    extends LargeSetGenerator(name, conf) {
  def query(auth: String): FaunaQuery = mkQuery(auth, JSObject.empty)

  def mkQuery(auth: String, cursor: JSObject): FaunaQuery = {
    val query =
      MapF(
        Lambda("ref" -> Get(Var("ref"))),
        Paginate(
          Match(Ref("indexes/bool_index"), true),
          size = conf.getInt("baseline.largeset.results")
        ) ++ cursor
      )

    new FaunaQuery(auth, query, Some(CursorDir.After)) {
      override def nextPage(cursor: JSObject) = mkQuery(auth, cursor)
    }
  }
}

class Baseline(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new LargeSetRefsOnlyGenerator("largeset-refs-only", config),
    new LargeSetWithDataGenerator("largeset-with-data", config)
  )
}
