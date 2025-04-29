package fauna.model.test

import fql.ast.{ Expr, Name }
import org.scalacheck.Gen

trait SetGenerators { self: FuzzGenerators =>
  import WrappedExpr.ExprOps

  // We don't want to shrink the `.all()` or `.byLetter()` calls, so we only allow
  // shrinking these calls in method chains.
  //
  // Shrinking will only occur on the `setChain` result, so we only need to worry
  // about those calls.
  override protected def isShrinkableComponent(
    c: Expr.MethodChain.Component): Boolean = {
    c match {
      case Expr.MethodChain.MethodCall(
            _,
            Name(
              "map" | "flatMap" | "where" | "take" | "drop" | "reverse" | "concat" |
              "distinct",
              _),
            _,
            _,
            _,
            _) =>
        true
      case _ => false
    }
  }

  /** Builds a set literal.
    */
  def standaloneSet: Gen[Expr] = Gen.oneOf(
    // an array set containing some numbers
    for {
      elem <- Gen.nonEmptyListOf(
        Gen.choose(-1000, 1000).map(num => WrappedExpr.int(num)))
    } yield WrappedExpr.array(elem: _*).call("toSet", Seq.empty),
    // a singleton set
    for {
      elem <- Gen.choose(-100, 100)
    } yield WrappedExpr
      .id("Set")
      .call(
        "single",
        Seq(WrappedExpr.int(elem))
      ),
    // a sequence of numbers
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
        )),
    // a set from a user's collection (User.all().map(.number))
    Gen.const(
      WrappedExpr
        .id("User")
        .call("all", Seq.empty)
        .call("map", Seq(WrappedExpr.shortLambda(WrappedExpr.ths.select("number"))))
    ),
    // a set from an index (User.byLetter("A").map(.number))
    Gen.const(
      WrappedExpr
        .id("User")
        .call("byLetter", Seq(WrappedExpr.str("A")))
        .call("map", Seq(WrappedExpr.lambda(Seq("x"), WrappedExpr.id("x.number"))))
    ),
    // a set from another index
    Gen.const(
      WrappedExpr
        .id("User")
        .call("byName", Seq(WrappedExpr.str("Adam")))
        .call(
          "map",
          Seq(WrappedExpr.lambda(Seq("x"), WrappedExpr.id("x.name.length"))))
    )
  )

  /** Builds a chain of operations on a set.
    */
  def setChain(set: String): Gen[Expr] = Gen.oneOf(
    // just the initial set variable
    Gen.const(WrappedExpr.id(set)),

    // maps a set
    for {
      left   <- Gen.lzy(setChain(set))
      mapper <- basicExpr(Some("v"))
    } yield left.call("map", Seq(WrappedExpr.lambda(Seq("v"), mapper))),
    // flatMaps a set
    for {
      left    <- Gen.lzy(setChain(set))
      mapperA <- basicExpr(Some("v"))
      mapperB <- basicExpr(Some("v"))
    } yield left.call(
      "flatMap",
      Seq(
        WrappedExpr.lambda(
          Seq("v"),
          WrappedExpr.array(mapperA, mapperB).call("toSet", Seq.empty)))),
    // filters a set with .where()
    for {
      left   <- Gen.lzy(setChain(set))
      filter <- basicExpr(None, 2)
      oper   <- Gen.oneOf(Seq("<", "<=", ">", ">=", "==", "!="))
    } yield left.call(
      "where",
      Seq(WrappedExpr.lambda(Seq("v"), WrappedExpr.id("v").op(oper, Some(filter))))),
    // takes some off the set
    for {
      left  <- Gen.lzy(setChain(set))
      count <- Gen.choose(1, 10).map(num => WrappedExpr.int(num))
    } yield left.call("take", Seq(count)),
    // drop some off the set
    for {
      left  <- Gen.lzy(setChain(set))
      count <- Gen.choose(1, 10).map(num => WrappedExpr.int(num))
    } yield left.call("drop", Seq(count)),
    // concat to another set
    for {
      left    <- Gen.lzy(setChain(set))
      nextSet <- standaloneSet
    } yield left.call("concat", Seq(nextSet)),
    // reverse the set
    for {
      left <- Gen.lzy(setChain(set))
    } yield left.call("reverse", Seq.empty),
    // chain toArray into toSet
    for {
      left <- Gen.lzy(setChain(set))
    } yield left.call("toArray", Seq.empty).call("toSet", Seq.empty),
    // get distinct values
    for {
      left <- Gen.lzy(setChain(set))
    } yield left.call("distinct", Seq.empty)
  )
}
