package fauna.storage.macros

import scala.collection.immutable.TreeSet
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * A macro to produce a TreeSet of all instances of a sealed trait.
  * Based on Travis Brown's work:
  * http://stackoverflow.com/questions/13671734/iteration-over-a-sealed-trait-in-scala
  *
  * WARN: must be used at the END of a code block containing the instances.
*/
object EnumerationMacros {
  def sealedInstancesOf[A]: TreeSet[A] = macro sealedInstancesOfImpl[A]

  def sealedInstancesOfImpl[A: c.WeakTypeTag](c: blackbox.Context) = {
    import c.universe._

    val symbol = weakTypeOf[A].typeSymbol.asClass

    if  (!symbol.isClass || !symbol.isSealed) {
      c.abort(c.enclosingPosition, "Can only enumerate values of a sealed trait or class.")
    }

    val children = symbol.knownDirectSubclasses.toList

    if (!children.forall { _.isModuleClass }) {
      c.abort(c.enclosingPosition, "All children must be objects.")
    }

    c.Expr[TreeSet[A]] {
      def sourceModuleRef(sym: Symbol) =
        Ident(sym.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
          .sourceModule
          .asInstanceOf[Symbol])

      Apply(
        Select(
          reify(TreeSet).tree,
          TermName("apply")
        ),
        children.map { sourceModuleRef(_) }
      )
    }
  }
}
