package fauna.storage.macros

import fauna.codex.builder.MacroUtils
import scala.reflect.macros._

class SumMacros(val c: whitebox.Context) extends MacroUtils {
  import c.universe._

  def genCodec[U: WeakTypeTag](field: Tree, variants: Tree*): Tree =
    codecImpl(weakTypeOf[U], field, variants)

  private def codecImpl(T: Type, field: Tree, variants: Seq[Tree]): Tree = {
    val FieldType = tq"_root_.fauna.storage.doc.FieldType"
    val InvalidType = q"_root_.fauna.storage.doc.InvalidType"
    val ValueRequired = q"_root_.fauna.storage.doc.ValueRequired"

    val codecs = variants map {
      case q"scala.Predef.ArrowAssoc[$_]($tag).->[$_]($codec)" =>
        val term = TermName(c.freshName("codec"))
        (tag, term, codec)
      case _ =>
        c.abort(c.enclosingPosition, "Invalid sum type definition.")
    }
    val cDefs = codecs map { case (_, t, c) => q"private[this] val $t = $c" }

    val decode = codecs map {
      case (t, c, _) => cq"$t => $c.decode(value, path)"
    }

    val encode = codecs map {
      case (tag, term, codec) =>
        val base = typed(FieldType).typeSymbol
        val ctor = typed(codec).baseType(base).typeArgs.head
        cq"""t: $ctor =>
          $term.encode(t) match {
            case Some(m: _root_.fauna.storage.ir.MapV) =>
              field.ftype.encode($tag) map { m.update(field.path, _) }
            case Some(_) =>
              throw new AssertionError("variants must be objects")
            case None =>
              field.ftype.encode($tag) map {
                _root_.fauna.storage.ir.MapV.empty.update(field.path, _)
              }
          }"""
    }


    val tr = q"""new _root_.fauna.storage.doc.FieldType[$T] {
      val vtype = _root_.fauna.storage.ir.IRType.Custom(${T.typeSymbol.name.toString})

      private[this] val field = $field

      ..$cDefs
      def decode(value: Option[_root_.fauna.storage.ir.IRValue], path: scala.collection.immutable.Queue[String]) = {
        value match {
          case Some(vs: _root_.fauna.storage.ir.MapV) =>
            field.read(vs) flatMap {
              case ..$decode
            }
          case Some(v) => Left(List($InvalidType(path.toList, vtype, v.vtype)))
          case None => Left(List($ValueRequired(path.toList)))
        }
      }

      def encode(t: $T): Option[_root_.fauna.storage.ir.MapV] = t match { case ..$encode }
    }
    """
    tr
  }
}
