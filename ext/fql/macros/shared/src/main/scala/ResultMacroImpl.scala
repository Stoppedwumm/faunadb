package fql.result.macros

import scala.reflect.macros._

class ResultMacroImpl(val c: blackbox.Context) {
  import c.universe._

  val R = c.prefix.tree
  val RFail = q"$R.fail"

  lazy val transformer = new Transformer {
    override def transform(tree: c.Tree) =
      tree match {
        // result.getOrFail form
        case q"$t.getOrFail" =>
          c.typecheck(q"${transform(t)}.getOr { e => $R.unsafeFail(e) }", c.TERMmode)

        // Result.fail(err) form
        case q"$ap($err)" if ap equalsStructure RFail =>
          c.typecheck(q"$R.unsafeFail(${transform(err)})", c.TERMmode)

        case t =>
          super.transform(t)
      }
  }

  def guard(body: c.Tree): c.Tree =
    q"""try {
        ${transformer.transform(body)}
      } catch {
        case $R.ThrownFailure(err) => $R.Err(err)
      }"""
}
