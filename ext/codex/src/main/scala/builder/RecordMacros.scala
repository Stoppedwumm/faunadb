package fauna.codex.builder

import scala.reflect.macros._

class RecordMacros(val c: whitebox.Context) extends MacroUtils {
  import c.universe._

  def genDecoder[T: WeakTypeTag]: Tree =
    decoderImpl(TypeInfo(weakTypeOf[T]))

  def genEncoder[T: WeakTypeTag]: Tree =
    encoderImpl(TypeInfo(weakTypeOf[T]))

  def genCodec[T: WeakTypeTag]: Tree =
    codecImpl(TypeInfo(weakTypeOf[T]))

  // Implementation

  private val fieldFormat = FieldNameFormat(c)(tq"$M", TypeName("RecordFieldNameFormat"))

  // Indirection through an intermediate def closes over the codec
  // symbol and allows use of lazy val in body for recursive memoization.
  private def impl(info: TypeInfo, tpe: FieldInfo => Tree) = {
    val vs = info.fields map { _ => TermName(c.freshName("v")) }
    val ms = info.fields map { _ => TermName(c.freshName("m")) }

    val defs = ms zip info.fields map { case (m, f) => q"final def $m = ${tpe(f)}" }
    val vals = vs zip ms map { case (v, m) => q"private lazy val $v = $m" }

    val fields = info.fields map { _ => TermName(c.freshName("f")) }
    val fieldDefs = fields zip info.fields map {
      case (n, f) => q"final val $n = ${fieldFormat(c, f.name)}"
    }

    (info.T, defs, vals, vs, fieldDefs, fields)
  }

  private def codecImpl(info: TypeInfo): Tree = {
    val (t, defs, vals, names, fieldDefs, fields) = impl(info, _.codec)

    val tr = q"""{
      ..$defs
      new $M.RecordDecoder with $M.RecordEncoder with $M.Codec[$t] {
        ..$fieldDefs
        ..$vals
        ${genDecode(info, fields, names)}
        ${genEncode(info, fields, names)}
      }: $M.Codec[$t]
    }"""

    tr
  }

  private def decoderImpl(info: TypeInfo): Tree = {
    val (t, defs, vals, names, fieldDefs, fields) = impl(info, _.decoder)

    val tr = q"""{
      ..$defs
      new $M.RecordDecoder with $M.Decoder[$t] {
        ..$fieldDefs
        ..$vals
        ${genDecode(info, fields, names)}
      }: $M.Decoder[$t]
    }"""

    tr
  }

  private def encoderImpl(info: TypeInfo): Tree = {
    val (t, defs, vals, names, fieldDefs, fields) = impl(info, _.encoder)

    val tr = q"""{
      ..$defs
      new $M.RecordEncoder with $M.Encoder[$t] {
        ..$fieldDefs
        ..$vals
        ${genEncode(info, fields, names)}
      }: $M.Encoder[$t]
    }"""

    tr
  }

  // Helpers

  private def genDecode(info: TypeInfo, names: List[TermName], decoders: List[TermName]) = {
    val T = info.T

    val flags = info.fields map { _ => TermName(c.freshName("decoded")) }
    val flagDefs = flags map { f => q"var $f = false" }

    val slots = info.fields map { _ => TermName(c.freshName("slot")) }
    val slotDefs = slots zip info.fields map {
      case (v, f) => q"var $v: ${f.tpe} = null.asInstanceOf[${f.tpe}]"
    }

    val defaults = info.fields map { f =>
      f.default map { d => q"$d" } getOrElse {
        q"""throw new NoSuchElementException("No field " + ${f.name})"""
      }
    }

    val decodes = names zip flags zip slots zip decoders map {
      case (((n, fl), v), d) => cq"n if n == $n => $fl = true; $v = $d.decode(stream)"
    }

    val cargs = flags zip slots zip defaults map { case ((f, v), d) => q"if ($f) $v else $d" }

    q"""final def decode(stream: $M.In): $T = {
      ..$flagDefs
      ..$slotDefs
      val left = decodeStart(stream)
      while (left.hasNext) {
        (decodeFieldName(stream): @unchecked) match { case ..$decodes }
        left.next()
      }
      decodeEnd(stream)

      ${info.construct(cargs)}
    }"""
  }

  private def genEncode(info: TypeInfo, names: List[TermName], encoders: List[TermName]) = {
    val T = info.T

    val slots = info.fields map { _ => TermName(c.freshName("slot")) }
    val slotDefs = slots zip info.fields map { case (v, f) => q"val $v = t.${f.getter}" }

    val flags = info.fields map { _ => TermName(c.freshName("isDefault")) }
    val flagDefs = flags zip slots zip info.fields map {
      case ((fl, v), f) => if (f.hasDefault) q"val $fl = $v == ${f.default.get}" else q""
    }

    val start = if (info.hasDefault) {
      val size = q"var size = ${info.minArity}"
      val incrs = flags zip info.fields map {
        case (fl, f) => if (f.hasDefault) q"if (!$fl) size += 1" else q""
      }
      q"""$size; ..$incrs; encodeStart(stream, size)"""
    } else {
      q"encodeStart(stream, ${info.arity})"
    }

    val encodes = names zip flags zip slots zip encoders zip info.fields map {
      case ((((n, fl), v), e), f) =>
        if (f.hasDefault) {
          q"if (!$fl) { encodeFieldName(stream, $n); $e.encode(stream, $v) }"
        } else {
          q"encodeFieldName(stream, $n); $e.encode(stream, $v)"
        }
    }

    q"""final def encode(stream: $M.Out, t: $T): $M.Out = {
      ..$slotDefs
      ..$flagDefs
      $start
      ..$encodes
      encodeEnd(stream)
      stream
    }"""
  }
}
