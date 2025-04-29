package fauna.qa.generators.test

import fauna.codex.json._
import fauna.prop.Prop
import fauna.prop.api._
import fauna.qa._

/**
  * A Jepsen style register test. This will continuously run three
  * streams:
  *  * reads - attempt to get a pre-determined ref and return its
  *    value or null if it doesn't exist.
  *  * writes - create or update an instance settings its value to
  *    a new random number
  *  * cas - if a ref exists add 1 to its value otherwise create it
  *    with a new value
  */
class RegisterGenerator(name: String, fConfig: QAConfig)
    extends TestGenerator(name, fConfig)
    with JSGenerators {

  val schema = Schema.DB(
    name,
    Schema.Collection("register"),
    Schema.Index(
      "register-by-val",
      "register",
      Vector(
        Schema.Index.Term(Vector("data", "val")),
        Schema.Index.Term(Vector("ref"))
      ),
      Vector.empty,
      false
    )
  )

  val intP = Prop.int(5)

  val refP = intP map { i =>
    (i, Ref(s"classes/register/$i"))
  }

  def readStream(auth: String) =
    RequestStream("reads", () => {
      val (i, ref) = refP.sample
      FaunaQuery(
        auth,
        If(
          Exists(ref),
          JSArray(i, Select(JSArray("data", "val"), Get(ref))),
          JSArray(i, "null")
        )
      )
    })

  def writeStream(auth: String) =
    RequestStream(
      "writes",
      () => {
        val (_, ref) = refP.sample
        val newVal = intP.sample
        FaunaQuery(
          auth,
          If(
            Exists(ref),
            Update(ref, MkObject("data" -> MkObject("val" -> newVal))),
            CreateF(ref, MkObject("data" -> MkObject("val" -> newVal)))
          )
        )
      }
    )

  def casStream(auth: String, n: String) =
    RequestStream(
      s"cas-$n",
      () => {
        val (_, ref) = refP.sample
        val newVal = intP.sample
        FaunaQuery(
          auth,
          If(
            Exists(ref),
            Let("cur" -> Select(JSArray("data", "val"), Get(ref))) {
              JSArray(
                Var("cur"),
                Update(
                  ref,
                  MkObject("data" -> MkObject("val" -> AddF(1, Var("cur"))))
                )
              )
            },
            CreateF(ref, MkObject("data" -> MkObject("val" -> newVal)))
          )
        )
      }
    )

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    new MultiStream(
      IndexedSeq(
        readStream(auth),
        writeStream(auth),
        casStream(auth, "1"),
        casStream(auth, "2")
      )
    )
  }
}

class JepsenRegister(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new RegisterGenerator("register", config)
  )
}
