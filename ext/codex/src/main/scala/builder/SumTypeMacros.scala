package fauna.codex.builder

import scala.reflect.macros._

class SumTypeMacros(val c: whitebox.Context) extends MacroUtils {
  import c.universe._

  def genDecoder[T: WeakTypeTag](variants: Tree*): Tree =
    decoderImpl(weakTypeOf[T], variants.toList, None)

  def genEncoder[T: WeakTypeTag](variants: Tree*): Tree =
    encoderImpl(weakTypeOf[T], variants.toList, None)

  def genCodec[T: WeakTypeTag](variants: Tree*): Tree =
    codecImpl(weakTypeOf[T], variants.toList, None)

  // Implementation

  // Indirection through an intermediate def closes over the codec
  // symbol and allows use of lazy val in body for recursive memoization.
  private def impl(variants: List[Tree], tagsOpt: Option[List[Tree]]) = {
    val tags = tagsOpt getOrElse { variants.indices.toList map { i => q"$i" } }
    val types = variants map { v => encoderType(typed(v)).typeArgs.head }

    val vs = variants map { _ => TermName(c.freshName("v")) }
    val ms = variants map { _ => TermName(c.freshName("m")) }

    val defs = ms zip variants map { case (m, d) => q"def $m = $d" }
    val vals = vs zip ms map { case (v, m) => q"private lazy val $v = $m" }

    (defs, vals, vs, tags, types)
  }

  private def codecImpl(T: Type, variants: List[Tree], tagsOpt: Option[List[Tree]]): Tree = {
    val (defs, vals, names, tags, types) = impl(variants, tagsOpt)

    val tr = q"""{
      ..$defs
      new $M.SumTypeDecoder with $M.SumTypeEncoder with $M.SumTypeValidator with $M.Codec[$T] {
        ..$vals
        validateTags($tags)
        ${genDecode(T, tags, names)}
        ${genEncode(T, tags, names, types)}
      }: $M.Codec[$T]
    }"""

    tr
  }

  private def decoderImpl(T: Type, variants: List[Tree], tagsOpt: Option[List[Tree]]): Tree = {
    val (defs, vals, names, tags, _) = impl(variants, tagsOpt)

    val tr = q"""{
      ..$defs
      new $M.SumTypeDecoder with $M.SumTypeValidator with $M.Decoder[$T] {
        ..$vals
        validateTags($tags)
        ${genDecode(T, tags, names)}
      }: $M.Decoder[$T]
    }"""

    tr
  }

  private def encoderImpl(T: Type, variants: List[Tree], tagsOpt: Option[List[Tree]]): Tree = {
    val (defs, vals, names, tags, types) = impl(variants, tagsOpt)

    val tr = q"""{
      ..$defs
      new $M.SumTypeEncoder with $M.SumTypeValidator with $M.Encoder[$T] {
        ..$vals
        validateTags($tags)
        ${genEncode(T, tags, names, types)}
      }: $M.Encoder[$T]
    }"""

    tr
  }

  // Helpers

  private def encoderType(dec: Type) = {
    val base = dec.baseType(typed(tq"$M.Encoder").typeSymbol)
    if (base == NoType) c.abort(c.enclosingPosition, s"$dec is not a $M.Encoder")
    base
  }

  private def genDecode(T: Type, tags: List[Tree], decoders: List[TermName]) = {
    val decodes = tags zip decoders map {
      case (tag, dec) => cq"$tag => $dec.decode(stream)"
    }

    val t = T.toString
    val msg = q"""s"Unable to decode variant $${v} for type " + $t"""

    q"""final def decode(stream: $M.In): $T = {
      val tag = decodeStart(stream)
      val rv = tag match {
        case ..$decodes
        case v => throw new _root_.fauna.codex.DecodingException($msg)
      }
      decodeEnd(stream)
      rv
    }"""
  }

  private def genEncode(T: Type, tags: List[Tree], encoders: List[TermName], types: List[Type]) = {
    val encodes = tags zip encoders zip types map {
      case ((tag, enc), tpe) => cq"v: $tpe => encodeStart(stream, $tag); $enc.encode(stream, v)"
    }

    val t = T.toString
    val msg = q"""s"Unable to encode variant $${v} for type " + $t"""

    q"""final def encode(stream: $M.Out, t: $T): $M.Out = {
      t match {
        case ..$encodes
        case v => throw new _root_.fauna.codex.EncodingException($msg)
      }
      encodeEnd(stream)
      stream
    }"""
  }
}
