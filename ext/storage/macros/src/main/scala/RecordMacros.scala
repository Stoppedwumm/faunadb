package fauna.storage.macros

import fauna.codex.builder.MacroUtils
import scala.reflect.macros._

class RecordMacros(val c: whitebox.Context) extends MacroUtils {
  import c.universe._

  def genCodec[T: WeakTypeTag]: Tree =
    codecImpl(TypeInfo(weakTypeOf[T]), Nil)

  def genOverrideCodec[T: WeakTypeTag](overrides: Tree*): Tree =
    codecImpl(TypeInfo(weakTypeOf[T]), overrides)

  private def codecImpl(info: TypeInfo, overrides: Seq[Tree]): Tree = {
    val T = info.T
    val Field = q"_root_.fauna.storage.doc.Field"
    val InvalidType = q"_root_.fauna.storage.doc.InvalidType"
    val ValueRequired = q"_root_.fauna.storage.doc.ValueRequired"

    val overs = overrides map { eval[(String, String)] } toMap
    val getters = info.fields map { _ => TermName(c.freshName("field")) }
    val defs = getters zip info.fields map { case (v, f) =>
      q"private[this] val $v = $Field[${f.tpe}](${overs.getOrElse(f.name, f.name)})"
    }
    val chain = getters zip info.fields map { case (v, f) =>
      fq"${f.getter} <- $v.read(vs)"
    }
    val args = info.fields map { f => q"${f.getter}" }

    val encs = getters zip info.fields map {
      case (v, f) =>
        q"""$v.ftype.encode(t.${f.getter}) foreach { v =>
           enc += ${overs.getOrElse(f.name, f.name)} -> v
         }
         """
    }

    val tr = q"""new _root_.fauna.storage.doc.FieldType[$T] {
      val vtype = _root_.fauna.storage.ir.IRType.Custom(${T.typeSymbol.name.toString})
      ..$defs

      def decode(value: Option[_root_.fauna.storage.ir.IRValue], path: scala.collection.immutable.Queue[String]) = {
        value match {
          case Some(vs: _root_.fauna.storage.ir.MapV) =>
            for (..$chain) yield ${info.construct(args)}
          case Some(v) => Left(List($InvalidType(path.toList, vtype, v.vtype)))
          case None    => Left(List($ValueRequired(path.toList)))
        }
      }
      def encode(t: $T): Option[_root_.fauna.storage.ir.IRValue] = {
        val enc = List.newBuilder[(String, _root_.fauna.storage.ir.IRValue)]
        ..$encs
        Some(_root_.fauna.storage.ir.MapV(enc.result()))
      }
    }
    """

    tr
  }
}
