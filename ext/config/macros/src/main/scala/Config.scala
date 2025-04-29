package fauna.config

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

sealed trait ConfigMod
case object Required extends ConfigMod
case object Deprecated extends ConfigMod
case class Renamed(name: String) extends ConfigMod

class Config(mods: Seq[ConfigMod]) extends StaticAnnotation {
  def this() = this(Seq.empty)
  def this(mod: ConfigMod, mods: ConfigMod*) = this(mod +: mods)
  def macroTransform(annottees: Any*): Any = macro ConfigImpl.impl
}

object ConfigImpl {

  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    annottees.map(_.tree).toList match {
      case ValDef(_, name, typ, default) :: Nil =>
        val Apply(Select(Apply(_, mods), _), _) = c.macroApplication

        val nameStr = c.parse(s"""\"${name.decodedName.toString}\"""")
        val fieldName = TermName(s"_${name.decodedName}")
        val setterName = TermName(name.decodedName.toString + "_$eq")

        val modStrs = mods map { _.toString }
        val setterGetter =
          if (modStrs contains "Deprecated") {
            q""
          } else if (modStrs contains "Renamed") {
            q"def $name = $fieldName.get"
          } else {
            q"""
              def $name = $fieldName.get
              def $setterName(v: $typ): Unit = $fieldName.set(v)
            """
          }

        val expr = q"""
          val $fieldName = fauna.config.ConfigField[$typ]($nameStr, $mods, $default)
          _fields += ($nameStr -> $fieldName)
          ..$setterGetter
        """

        c.Expr[Any](expr)

      case _ => c.abort(c.enclosingPosition, "Invalid config field")
    }
  }
}
