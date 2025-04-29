package fauna.ast

import fauna.model.runtime.Effect

object FunctionHelpers {

  // argument extraction

  def toElemsIter(arg: Literal, pos: Position): Iterator[(Literal, Position)] =
    arg match {
      case ArrayL(es) =>
        es.iterator.zipWithIndex.map { case (e, i) => e -> (pos at i) }
      case e => Iterator.single(e -> pos)
    }
}

trait QFunction {
  def effect: Effect
}
