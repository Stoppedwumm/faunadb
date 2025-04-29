package fauna.qa.generators.test

import fauna.qa._
import fauna.prop.api._
import scala.util.Random

/**
  * Continuously run join queries between two index sets.
  * Config fields:
  *  - joins.instances = the total number of instances to create
  *  - joins.density = a smaller number here will create denser sets
  *  - joins.per-transaction = the number of intersections to run per
  *  query
  */
class JoinsGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val schema = Schema.DB(
    name,
    Schema.Collection("nodes"),
    Schema.Index("edges", "nodes", Vector("data", "left"), Vector("data", "right"))
  )

  val instances = conf.getInt("joins.instances")
  val density = conf.getInt("joins.density")
  val perTransaction = conf.getInt("joins.per-transaction")

  override protected def initializer(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestIterator("init", LazyList.tabulate(instances) { id =>
      CreateF(Ref(s"classes/nodes/$id"), Quote(instanceP sample))
    } grouped (100) map {
      FaunaQuery(auth, _)
    })
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("joins", () => query(auth))
  }

  val instanceP = jsObject(
    "data" -> jsObject("left" -> jsLong(density), "right" -> jsLong(density))
  )

  def query(auth: String) =
    FaunaQuery(
      auth,
      List.fill(perTransaction) {
        val term = Random.nextInt(density).toLong
        Paginate(
          Join(
            Match(Ref(s"indexes/edges"), term),
            Lambda(
              "num" ->
                Match(Ref(s"indexes/edges"), Var("num"))
            )
          )
        )
      }
    )
}

class Joins(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(new JoinsGenerator("joins", config))
}
