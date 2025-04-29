package fauna.model.test

import fql.ast.{ Expr, Name }
import org.scalacheck.Gen

trait ArrayGenerators { self: FuzzGenerators =>
  import WrappedExpr.ExprOps

  override protected def isShrinkableComponent(
    c: Expr.MethodChain.Component): Boolean = {
    c match {
      case Expr.MethodChain.MethodCall(
            _,
            Name("at" | "prepend" | "append" | "flatten" | "filter" | "slice", _),
            _,
            _,
            _,
            _) =>
        true
      case _ => false
    }
  }

  /** Builds an array literal.
    */
  def standaloneArray: Gen[Expr] = Gen.oneOf(
    // A basic array of numbers
    for {
      elem <- Gen.nonEmptyListOf(
        Gen.choose(-1000, 1000).map(num => WrappedExpr.int(num)))
    } yield WrappedExpr.array(elem: _*),
    // A set.sequence chained to toArray
    for {
      from <- Gen.choose(-100, 100)
      to   <- Gen.choose(from, 100)
    } yield WrappedExpr
      .id("Set")
      .call(
        "sequence",
        Seq(
          WrappedExpr.int(from),
          WrappedExpr.int(to)
        ))
      .call("toArray", Seq.empty),
    Gen.const(
      WrappedExpr
        .id("User")
        .call("all", Seq.empty)
        .call("map", Seq(WrappedExpr.shortLambda(WrappedExpr.ths.select("number"))))
        .call("toArray", Seq.empty)
    ),
    Gen.const(
      WrappedExpr
        .id("User")
        .call("byLetter", Seq(WrappedExpr.str("A")))
        .call("map", Seq(WrappedExpr.lambda(Seq("x"), WrappedExpr.id("x.number"))))
        .call("toArray", Seq.empty)
    ),
    Gen.const(
      WrappedExpr
        .id("User")
        .call("byName", Seq(WrappedExpr.str("Adam")))
        .call(
          "map",
          Seq(WrappedExpr.lambda(Seq("x"), WrappedExpr.id("x.name.length"))))
        .call("toArray", Seq.empty)
    )
  )

  /** Builds a chain of operations on an array.
    */
  def arrayChain(array: String): Gen[Expr] = Gen.oneOf(
    // just the initial array variable
    Gen.const(WrappedExpr.id(array)),
    // prepends to an array
    for {
      left    <- Gen.lzy(arrayChain(array))
      newElem <- Gen.choose(-1000, 1000).map(WrappedExpr.int(_))
    } yield left.call("prepend", Seq(newElem)),
    // appends to an array
    for {
      left    <- Gen.lzy(arrayChain(array))
      newElem <- Gen.choose(-1000, 1000).map(WrappedExpr.int(_))
    } yield left.call("append", Seq(newElem)),
    // flattens two arrays
    for {
      newArr1 <- Gen.lzy(arrayChain(array))
      newArr2 <- standaloneArray
    } yield WrappedExpr.array(newArr1, newArr2).call("flatten", Seq.empty),
    // filters an array with .filter()
    for {
      left   <- Gen.lzy(arrayChain(array))
      filter <- basicExpr(None, 2)
      oper   <- Gen.oneOf(Seq("<", "<=", ">", ">=", "==", "!="))
    } yield left.call(
      "filter",
      Seq(WrappedExpr.lambda(Seq("v"), WrappedExpr.id("v").op(oper, Some(filter))))),
    // take a slice of the array
    for {
      left  <- Gen.lzy(arrayChain(array))
      count <- Gen.choose(1, 10).map(num => WrappedExpr.int(num))
    } yield left.call("slice", Seq(count)),
    // chain toSet into toArray
    for {
      left <- Gen.lzy(arrayChain(array))
    } yield left.call("toSet", Seq.empty).call("toArray", Seq.empty)
  )
}
