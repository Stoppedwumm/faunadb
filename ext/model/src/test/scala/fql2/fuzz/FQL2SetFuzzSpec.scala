package fauna.model.test

import fauna.model.runtime.fql2.{ QueryRuntimeFailure, Result }
import fauna.repo.values.Value
import fql.ast.display._
import org.scalacheck.{ Gen, Prop, Shrink }
import org.scalacheck.Prop.propBoolean
import scala.collection.SeqMap
import scala.util.control.NonFatal

object FQL2SetFuzzSpec extends FQL2BaseFuzzSpec("setfuzzer") {

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

  // Check the paginate response, including recursing into after tokens
  def isValidPageResponse(
    fields: SeqMap[String, Value],
    pageSize: Int,
    maxRecurse: Int): Boolean = {
    var isValid = false

    if (maxRecurse == 0)
      return true

    if (!fields.contains("data"))
      return false
    else
      isValid = fields("data").isInstanceOf[Value.Array]

    if (!fields.contains("after")) {
      isValid &&= fields("data").asInstanceOf[Value.Array].elems.length <= pageSize
    } else {
      isValid &&= fields("data").asInstanceOf[Value.Array].elems.length == pageSize

      val cursor =
        Value.SetCursor.toBase64(fields("after").asInstanceOf[Value.SetCursor], None)
      val afterRes = eval(
        auth,
        s"Set.paginate('${cursor}')"
      ).res

      isValid &&= (afterRes match {
        case Result.Ok(out) =>
          out.value match {
            case Value.Struct.Full(internalFields, _, _, _) =>
              isValidPageResponse(internalFields, pageSize, maxRecurse - 1)
            case _ => false
          }
        case _ => false
      })
    }

    isValid
  }

  // here are the actual fuzz tests. These all map(WrappedExpr(_)) so that failures
  // will .display the expression (see the WrappedExpr.toString() above)
  property("simple") = Prop.forAll(basicExpr(Some("v")).map(WrappedExpr(_))) {
    expr =>
      // TODO: Validate the output (somehow)
      try {
        val res =
          eval(auth, s"let v = 5; ${expr.expr.display}", typecheck = false).res
        val worked = res match {
          case Result.Ok(out) =>
            out.value match {
              case Value.Int(_) | Value.Long(_) | Value.Double(_) => true
              case _                                              => false
            }
          case _ => false
        }
        (isValidRes(res)) ==> worked
      } catch {
        // NOTE: Stack overflows won't be caught here, so those will skip
        // simplification.
        case NonFatal(_) => PropTestResult.ValidTestFailed
      }
  }

  // don't shrink the initial set, as that will break the test. The set chain
  // should be shrunk, so that we see a simpler set of operations on the set.
  property("set.toArray()") =
    Prop.forAllNoShrink(standaloneSet.map(WrappedExpr(_))) { initial =>
      Prop.forAll(setChain("set").map(WrappedExpr(_))) { chain =>
        try {
          val res = eval(
            auth,
            s"""|let set = $initial
                |$chain.toArray()
                |""".stripMargin,
            typecheck = false).res
          val worked = res match {
            case Result.Ok(out) =>
              out.value match {
                case Value.Array(_) => true
                case _              => false
              }
            case _ => false
          }
          (isValidRes(res)) ==> worked
        } catch {
          case e: java.lang.IllegalStateException =>
            if (!isKnownException(e))
              throw e
            PropTestResult.InvalidTestFailed
          case _: fauna.repo.TxnTooManyComputeOpsException =>
            PropTestResult.InvalidTestPassed
        }
      }
    }

  property("set.paginate()") =
    Prop.forAllNoShrink(standaloneSet.map(WrappedExpr(_))) { initial =>
      implicit val intShrinkPositive = implicitly[Shrink[Int]].suchThat(_ > 0)
      Prop.forAll(setChain("set").map(WrappedExpr(_)), Gen.choose(1, 30)) {
        (chain, pageSize) =>
          try {
            val res = eval(
              auth,
              s"""|let set = $initial
                  |$chain.paginate($pageSize)
                  |""".stripMargin,
              typecheck = false).res
            val worked = res match {
              case Result.Ok(out) =>
                out.value match {
                  case Value.Struct.Full(fields, _, _, _) =>
                    isValidPageResponse(fields, pageSize, 10)
                  case _ => false
                }
              case _ => false
            }
            (isValidRes(res)) ==> worked
          } catch {
            case e: java.lang.IllegalStateException =>
              if (!isKnownException(e))
                throw e
              PropTestResult.InvalidTestFailed
            case _: fauna.repo.TxnTooManyComputeOpsException =>
              PropTestResult.InvalidTestPassed
          }
      }
    }

  property("set.map(create).map()") =
    Prop.forAllNoShrink(standaloneSet.map(WrappedExpr(_))) { initial =>
      Prop.forAll(setChain("set").map(WrappedExpr(_))) { chain =>
        try {
          val res = eval(
            auth,
            s"""|let set = $initial
                |let docs = $chain.toArray().map(x => Junk.create({"value":x}))
                |let set = docs.map(.value).toSet()
                |$chain.toArray()
                |""".stripMargin,
            typecheck = false
          ).res
          val worked = res match {
            case Result.Ok(out) =>
              out.value match {
                case Value.Array(_) => true
                case _              => false
              }
            // TODO: this unbound variable bug shouldn't happen
            case Result.Err(
                  QueryRuntimeFailure.Simple(
                    "unbound_variable",
                    "Unbound variable `Junk`",
                    _,
                    _)) =>
              true
            case _ => false
          }
          (isValidRes(res)) ==> worked
        } catch {
          case e: java.lang.IllegalStateException =>
            if (!isKnownException(e))
              throw e
            PropTestResult.InvalidTestFailed
          case _: fauna.repo.TxnTooManyComputeOpsException =>
            PropTestResult.InvalidTestPassed
        }
      }
    }
}
