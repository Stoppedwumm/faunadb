package fauna.model.test

import fql.ast.Expr
import org.scalacheck.Gen

trait CommonGenerators extends ExprShrinker {
  import WrappedExpr.ExprOps

  // FIXME: Stop stack overflowing (and then increase this max)
  def basicExpr(
    variable: Option[String],
    maxDepth: Int = 5,
    depth: Int = 0): Gen[Expr] = {
    // this is a list of generators, which we build up throughout this function.
    val o = Seq.newBuilder[Gen[Expr]]
    o.addAll(
      Seq(
        // number literal
        Gen.choose(0, 100).map(num => WrappedExpr.int(num))
      ))
    if (depth < maxDepth) {
      o.addAll(
        Seq(
          // add
          for {
            a <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
            b <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
          } yield WrappedExpr.tuple(a.op("+", Some(b))),
          // subtract
          for {
            a <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
            b <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
          } yield WrappedExpr.tuple(a.op("-", Some(b))),
          // multiply
          for {
            a <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
            b <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
          } yield a.op("*", Some(b)),
          // divide
          for {
            a <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
            b <- Gen.const(2).map(WrappedExpr.int(_))
          } yield a.op("/", Some(b)),
          // modulo
          for {
            a <- Gen.lzy(basicExpr(variable, maxDepth, depth + 1))
            b <- Gen.const(2).map(WrappedExpr.int(_))
          } yield a.op("%", Some(b))
        ))
    }
    variable.foreach { v =>
      o += Gen.const(WrappedExpr.id(v))
    }
    val options = o.result()
    if (options.size == 1) {
      options(0)
    } else {
      // Gen.oneOf is dumb, and doesn't take an Iterable[Gen[T]]
      Gen.oneOf(options(0), options(1), options.drop(2): _*)
    }
  }
}
