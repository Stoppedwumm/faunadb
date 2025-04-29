package fauna.qa.generators.test

import fauna.prop.Prop
import fauna.prop.api._
import fauna.qa._
import java.util.concurrent.ThreadLocalRandom

/**
  * A pure CPU bound workload. It aims to exercise query parallelism whiling
  * performing only CPU bound queries.
  */
class CPUGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val schema = Schema.DB(name)
  val sizeP = Prop.int(2048 to 4096)

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("CPU", () => compute(auth))
  }

  def compute(auth: String) =
    FaunaQuery(
      auth,
      Let(
        "arr" -> Seq.fill(sizeP.sample) {
          ThreadLocalRandom
            .current()
            .nextInt(0, Int.MaxValue)
            .toString
        },
        "nums" -> MapF(
          Lambda("s" -> ToInteger(Var("s"))),
          Var("arr")
        )
      ) {
        Seq(
          Var("nums"),
          MapF(Lambda("n" -> Pow(Var("n"))), Var("nums")),
          MapF(Lambda("n" -> Pow(Var("n"), 4)), Var("nums")),
          MapF(Lambda("n" -> Sqrt(Var("n"))), Var("nums"))
        )
      }
    )
}

class CPU(config: QAConfig) extends QATest {
  val generators = Array(new CPUGenerator("CPU", config))
}
