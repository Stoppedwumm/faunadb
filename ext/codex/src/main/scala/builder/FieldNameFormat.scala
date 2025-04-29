package fauna.codex.builder

import scala.reflect.macros._

sealed trait FieldNameFormat {
  def apply(c: whitebox.Context, name: String): c.Tree
}

object FieldNameFormat {

  def apply(c: whitebox.Context)(container: c.Tree, name: c.Name) = {
    import c.universe._
    c.typecheck(container, c.TYPEmode).tpe.member(name) match {
      case m: TypeSymbol if m.toType =:= typeOf[Underscore] => new Underscore
      case _                                                => new Identity
    }
  }

  class Underscore extends FieldNameFormat {
    def apply(c: whitebox.Context, str: String): c.Tree = {
      import c.universe._
      var rv = str
      rv = """([A-Z]+)([A-Z][a-z])""".r.replaceAllIn (rv, m => s"${m.group(1)}_${m.group(2)}")
      rv = """([a-z\d])([A-Z])""".r.    replaceAllIn (rv, m => s"${m.group(1)}_${m.group(2)}")
      rv = """-""".r.                   replaceAllIn (rv, "_")
      rv = """\W""".r.                  replaceAllIn (rv, "")
      q"${rv.toLowerCase}"
    }
  }

  class Identity extends FieldNameFormat {
    def apply(c: whitebox.Context, str: String): c.Tree = {
      import c.universe._
      q"$str"
    }
  }
}
