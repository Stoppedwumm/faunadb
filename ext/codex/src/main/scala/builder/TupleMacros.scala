package fauna.codex.builder

import scala.reflect.macros._

class TupleMacros(val c: whitebox.Context) extends MacroUtils {
  import c.universe._

  private def refinedType(cons: Type, alt: Type) =
    c.openImplicits.headOption map { ic =>
      if (cons =:= ic.pt.typeConstructor) ic.pt.typeArgs.head else {
        c.abort(c.enclosingPosition, s"refined typeclass ${ic.pt.typeConstructor} is not $cons")
      }
    } getOrElse alt

  // Built-in Tuple implicits

  def implicitDecoder[T: WeakTypeTag]: Tree =
    decoderImpl(TupleInfo(refinedType(typed(tq"$M.Decoder"), weakTypeOf[T]).dealias))

  def implicitEncoder[T: WeakTypeTag]: Tree =
    encoderImpl(TupleInfo(refinedType(typed(tq"$M.Encoder"), weakTypeOf[T]).dealias))

  def implicitCodec[T: WeakTypeTag]: Tree =
    codecImpl(TupleInfo(refinedType(typed(tq"$M.Codec"), weakTypeOf[T]).dealias))

  // Constructed codecs

  def genDecoder[T: WeakTypeTag]: Tree =
    decoderImpl(TypeInfo(weakTypeOf[T]))

  def genEncoder[T: WeakTypeTag]: Tree =
    encoderImpl(TypeInfo(weakTypeOf[T]))

  def genCodec[T: WeakTypeTag]: Tree =
    codecImpl(TypeInfo(weakTypeOf[T]))

  // Implementation

  // Indirection through an intermediate def closes over the codec
  // symbol and allows use of lazy val in body for recursive memoization.
  private def impl(info: TypeInfo, tpe: FieldInfo => Tree) = {
    val vs = info.fields map { _ => TermName(c.freshName("v")) }
    val ms = info.fields map { _ => TermName(c.freshName("m")) }

    val defs = ms zip info.fields map { case (m, f) => q"final def $m = ${tpe(f)}" }
    val vals = vs zip ms map { case (v, m) => q"private lazy val $v = $m" }

    (info.T, defs, vals, vs)
  }

  private def codecImpl(info: TypeInfo): Tree = {
    val (t, defs, vals, names) = impl(info, _.codec)

    val tr = q"""{
      ..$defs
      new $M.TupleDecoder with $M.TupleEncoder with $M.Codec[$t] {
        ..$vals
        ${genDecode(info, names)}
        ${genEncode(info, names)}
      }: $M.Codec[$t]
    }"""

    tr
  }

  private def decoderImpl(info: TypeInfo): Tree = {
    val (t, defs, vals, names) = impl(info, _.decoder)

    val tr = q"""{
      ..$defs
      new $M.TupleDecoder with $M.Decoder[$t] {
        ..$vals
        ${genDecode(info, names)}
      }: $M.Decoder[$t]
    }"""

    tr
  }

  private def encoderImpl(info: TypeInfo): Tree = {
    val (t, defs, vals, names) = impl(info, _.encoder)

    val tr = q"""{
      ..$defs
      new $M.TupleEncoder with $M.Encoder[$t] {
        ..$vals
        ${genEncode(info, names)}
      }: $M.Encoder[$t]
    }"""

    tr
  }

  // Helpers

  private def genDecode(info: TypeInfo, decoders: List[TermName]) = {
    val T = info.T
    val vals = info.fields map { _ => TermName(c.freshName("value")) }
    val start = if (info.arity > 1) q"decodeStart(stream, ${info.arity})" else q"{}"
    val end = if (info.arity > 1) q"decodeEnd(stream)" else q"{}"
    val decodes = vals zip decoders map {
      case (v, d) => q"val $v = $d.decode(stream)"
    }

    q"""final def decode(stream: $M.In): $T = {
      $start
      ..$decodes
      $end
      ${info.construct(vals map (Ident(_)))}
    }"""
  }

  private def genEncode(info: TypeInfo, encoders: List[TermName]) = {
    val T = info.T
    val start = if (info.arity > 1) q"encodeStart(stream, ${info.arity})" else q"{}"
    val end = if (info.arity > 1) q"encodeEnd(stream)" else q"{}"
    val encodes = encoders zip info.fields map {
      case (v, f) => q"$v.encode(stream, t.${f.getter})"
    }

    q"""final def encode(stream: $M.Out, t: $T): $M.Out = {
      $start
      ..$encodes
      $end
      stream
    }"""
  }
}
