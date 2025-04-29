package fauna.qa.generators.test

import fauna.prop.Prop
import fauna.prop.api._
import fauna.qa._

/**
  * A Jepsen style test meant to ensure a set is unique. The test
  * will continuously attempt to insert instances with a random
  * value. The field of that value is configured to be unique.
  * There should be no more than the total unique instances created.
  *  - sets.total-unique-instances = the total number of unique instances allowed
  */
class SetGenerator(name: String, fConfig: QAConfig)
    extends TestGenerator(name, fConfig)
    with JSGenerators {

  override val expectFailures = true

  val valP = Prop.int(1 to fConfig.getInt("sets.total-unique-instances"))

  val schema = Schema.DB(
    "jepsensets",
    Schema.Collection("jepsenset"),
    Schema.Index(
      "unique_set",
      "jepsenset",
      Vector(Schema.Index.Term(Vector("data", "val"))),
      Vector.empty,
      true
    )
  )

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream(
      "write",
      () =>
        FaunaQuery(
          auth,
          CreateF(
            ClassRef("jepsenset"),
            MkObject("data" -> MkObject("val" -> valP.sample))
          )
        )
    )
  }
}

class JepsenSet(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(new SetGenerator("sets", config))
}
