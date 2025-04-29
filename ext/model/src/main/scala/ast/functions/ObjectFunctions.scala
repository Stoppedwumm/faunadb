package fauna.ast

import fauna.lang.syntax._
import fauna.model.runtime.Effect
import fauna.repo.query.Query

object MergeFunction extends QFunction {
  val effect: Effect = Effect.Pure

  val DefaultLambda =
    LambdaL("key", "left", "right")(VarE("right"))

  def apply(
    leftLit: Literal,
    rightLit: Literal,
    merge: Option[LambdaL],
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {

    val errs = List.newBuilder[InvalidArgument]

    def castObj(o: Literal, pos: => Position): ObjectL =
      o match {
        case o: ObjectL  => o
        case o: VersionL => ObjectL(o.fields(ec.apiVers))
        case o =>
          errs += InvalidArgument(List(Type.Object), o.rtype, pos)
          ObjectL.empty
      }

    val left = castObj(leftLit, pos at "merge")

    val right = FunctionHelpers
      .toElemsIter(rightLit, pos at "with")
      .map { case (o, p) =>
        castObj(o, p) // side-effect: fills `errs` list
      } toList

    errs.result() match {
      case Nil  => mergeObjects(left, right, merge getOrElse DefaultLambda, ec, pos)
      case errs => Query.value(Left(errs))
    }
  }

  private def mergeObjects(
    left: ObjectL,
    right: Seq[ObjectL],
    mergeFunction: LambdaL,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    val zeroQ: Query[R[Map[String, Literal]]] = Query(Right(left.elems.toMap))

    val finalMapQ = right.foldLeft(zeroQ) { (objQ, rightObj) =>
      rightObj.elems.foldLeft(objQ) { case (leftMapQ, (key, rightValue)) =>
        leftMapQ flatMapT { leftMap =>
          val leftValue = leftMap.getOrElse(key, NullL)

          val args = ArrayL(List(StringL(key), leftValue, rightValue))

          ec.evalLambdaApply(mergeFunction, args, pos) mapT {
            case NullL => leftMap - key
            case value => leftMap + (key -> value)
          }
        }
      }
    }

    finalMapQ mapT { finalMap =>
      ObjectL(finalMap.toList)
    }
  }
}
