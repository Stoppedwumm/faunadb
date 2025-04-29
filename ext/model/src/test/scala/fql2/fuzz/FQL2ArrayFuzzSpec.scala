package fauna.model.test

import fauna.model.runtime.fql2.{ QueryRuntimeFailure, Result }
import org.scalacheck.{ Gen, Prop }
import org.scalacheck.Prop.propBoolean
import scala.util.control.NonFatal

object FQL2ArrayFuzzSpec extends FQL2BaseFuzzSpec("arrayfuzzer") {

  val auth = newDB

  // Schema setup
  evalOk(
    auth,
    """|Collection.create({
       |  name: "User",
       |  indexes: {
       |    byLetter: {
       |      terms: [{ field: "letter" }],
       |    },
       |    byName: {
       |      terms: [{ field: "name" }]
       |    }
       |  },
       |  constraints: [{unique: ["name", "number"]}]
       |})
       |
       |Collection.create({ name: "Junk" })""".stripMargin
  )
  // Create docs
  evalOk(
    auth,
    """|[{ name: "Adam", number: 1, letter: "A" },
       |{ name: "Benjamin", number: 2, letter: "A" },
       |{ name: "Christina", number: 3, letter: "A" },
       |{ name: "David", number: 4, letter: "A" },
       |{ name: "Erica", number: 5, letter: "B" },
       |{ name: "Francis", number: 6, letter: "B" },
       |{ name: "Gilligan", number: 7, letter: "B" },
       |{ name: "Harriet", number: 8, letter: "B" }].map(User.create)
       |""".stripMargin
  )

  property("array.at()") = Prop.forAll(
    standaloneArray.map(WrappedExpr(_)),
    arrayChain("arr").map(WrappedExpr(_)),
    Gen.posNum[Int]) { (initial, chain, num) =>
    try {
      val q = s"""|let arr=$initial
                    |$chain.at($num)""".stripMargin
      val res = eval(auth, q, typecheck = false).res
      val worked = res match {
        case Result.Ok(_) => true
        case Result.Err(
              QueryRuntimeFailure.Simple("index_out_of_bounds", _, _, _)) =>
          true
        case _ => false
      }
      (isValidRes(res)) ==> worked
    } catch {
      case NonFatal(_) => PropTestResult.ValidTestFailed
    }
  }
}
