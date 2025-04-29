package fauna.qa.generators.test

import fauna.qa._
import fauna.prop.api._
import scala.util.Random

/**
  * Continuously run intersection queries between two index sets.
  * Config fields:
  *  - intersections.instances = the total number of instances to create
  *  - intersections.density = a smaller number here will create denser sets
  *  - intersections.per_transaction = the number of intersections to run per
  *  query
  */
class IntersectionsGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val schema =
    Schema.DB(
      name,
      Schema.Collection("items"),
      Schema.Index("lists", "items", Vector("data", "num"))
    )

  val instances = conf.getInt("intersections.instances")
  val density = conf.getInt("intersections.density")
  val perTransaction = conf.getInt("intersections.per-transaction")

  override protected def initializer(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestIterator("init", LazyList.tabulate(instances) { id =>
      CreateF(Ref(s"classes/items/$id"), Quote(instanceP sample))
    } grouped (100) map {
      FaunaQuery(auth, _)
    })
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("intersections", () => query(auth))
  }

  val instanceP = jsObject("data" -> jsObject("num" -> jsLong(density)))

  def query(auth: String) =
    FaunaQuery(
      auth,
      List.fill(perTransaction) {
        val term1 = Random.nextInt(density).toLong
        val term2 = Random.nextInt(density).toLong
        Paginate(
          Intersection(
            Match(Ref(s"indexes/lists"), term1),
            Match(Ref(s"indexes/lists"), term2)
          )
        )
      }
    )
}

class Intersections(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new IntersectionsGenerator("intersections", config)
  )
}
