package fauna.model.runtime.fql2.serialization

import fauna.codex.json2.JSON
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.fql2._
import fauna.repo.query.Query
import fauna.repo.values._
import fauna.repo.values.Value.Null.Cause
import java.util.Objects
import scala.collection.mutable.{ Buffer => MBuffer, Map => MMap, Set => MSet }

object MaterializedValue {
  sealed trait Format { def isTagged: Boolean }
  case object Simple extends Format { val isTagged = false }
  case object Tagged extends Format { val isTagged = true }

  private def escape0(sb: StringBuilder, v: String) = {
    // This is copied from fql/Parser
    val unescaped = Map(
      '\\' -> '\\',
      '\u0000' -> '0',
      '\'' -> '\'',
      '"' -> '"',
      '`' -> '`',
      '\n' -> 'n',
      '\r' -> 'r',
      '\u000b' -> 'v',
      '\t' -> 't',
      '\b' -> 'b',
      '\f' -> 'f',
      '#' -> '#')
    sb.append("\"")
    v.foreach { c =>
      unescaped.get(c) match {
        case Some(replace) =>
          sb.append('\\')
          sb.append(replace)
        case None => sb.append(c)
      }
    }
    sb.append("\"")
  }
  def escape(v: String): String = {
    val sb = new StringBuilder
    escape0(sb, v)
    sb.result()
  }
  // FIXME: Check for valid idents using fql/ functions.
  def isIdent(v: String): Boolean =
    v.size > 0 && v(0).isLetter && v.forall(c => c == '_' || c.isLetterOrDigit)

  /** A `Value.Doc`, where `readTS` and `versionOverride` are both considered in `equals` and `hashCode`.
    */
  case class DocKey(doc: Value.Doc) {
    override def hashCode: Int = Objects.hash(doc, doc.readTS, doc.versionOverride)
    override def equals(other: Any): Boolean = other match {
      case o: DocKey =>
        doc == o.doc && doc.readTS == o.doc.readTS && doc.versionOverride == o.doc.versionOverride
      case _ => false
    }
  }
}

/** A marker wrapper to ensure that this value has been materialized to a
  * JSON-encodable structure. I.E. No bare doc references or sets.
  */
case class MaterializedValue(
  value: Value,
  refs: Map[Value.Doc, AnyDocRef] = Map.empty,
  docs: Map[MaterializedValue.DocKey, Value] = Map.empty,
  sets: Map[Value.Set, Value] = Map.empty,
  partials: Map[Value.Struct.Partial, Value] = Map.empty) {

  private def nullCauseStr(cause: Value.Null.Cause) =
    cause match {
      case Cause.ReadPermissionDenied(_, _) => "permission denied"
      case Cause.CollectionDeleted(_, _)    => "collection deleted"
      case Cause.DocDeleted(_, _, _)        => "deleted"
      case _                                => "not found"
    }

  private def writeTag(out: JSON.Out, tag: String, value: => JSON.Out): JSON.Out = {
    out.writeObjectStart()
    out.writeObjectField(tag.toUTF8Buf, value)
    out.writeObjectEnd()
  }

  private def tag(out: JSON.Out, tag: String, value: => JSON.Out)(
    implicit format: MaterializedValue.Format): JSON.Out =
    if (format.isTagged) writeTag(out, tag, value) else value

  /** With SetCursors it is possible for 2 value types to end up in the cursor that we don't currently support
    * encoding over the wire:
    * 1. Partials
    * 2. SetCursor
    * for 1, we need to replace the partials with the fully materialized partials
    * for 2, we need to encode the cursor into its string token so it can be sent over the wire
    *
    * There are tests in FQL2PaginationSpec that cover both of these scenarios.
    */
  private def replaceSetCursorValues(
    sc: Value.SetCursor,
    txnTime: Timestamp): Value.SetCursor = {
    def replaceSetCursorValues0(v: Value): Value = {
      v match {
        case Value.Array(vs) =>
          Value.Array(vs.map { replaceSetCursorValues0(_) })

        case Value.Struct.Full(vs, _, _, _) =>
          Value.Struct.Full(vs.map { case (k, v) =>
            k -> replaceSetCursorValues0(v)
          })

        case st: Value.SetCursor =>
          Value.Str(
            Value.SetCursor
              .toBase64(
                replaceSetCursorValues(st, txnTime),
                Some(txnTime)
              )
              .toString
          )

        case p: Value.Struct.Partial => partials(p)

        case other => other
      }
    }

    val scVals = sc.values.map { replaceSetCursorValues0(_) }
    sc.copy(values = scVals)
  }

  def encodeSimple(out: JSON.Out, txnTime: Timestamp): JSON.Out =
    encode(out, txnTime)(MaterializedValue.Simple)

  def encodeTagged(out: JSON.Out, txnTime: Timestamp): JSON.Out =
    encode(out, txnTime)(MaterializedValue.Tagged)

  private def encode(out: JSON.Out, txnTime: Timestamp)(
    implicit format: MaterializedValue.Format): JSON.Out =
    value match {
      case Value.ID(v) => out.writeString(v.toString)
      case Value.Int(v) =>
        if (format.isTagged) {
          writeTag(out, ValueFormat.IntTag, out.writeString(v.toString))
        } else {
          out.writeNumber(v)
        }
      case Value.Long(v) =>
        if (format.isTagged) {
          writeTag(out, ValueFormat.LongTag, out.writeString(v.toString))
        } else {
          out.writeNumber(v)
        }
      case Value.Double(v) =>
        if (format.isTagged) {
          writeTag(out, ValueFormat.DoubleTag, out.writeString(v.toString))
        } else {
          out.writeNumber(v)
        }
      case Value.True   => out.writeBoolean(true)
      case Value.False  => out.writeBoolean(false)
      case Value.Str(v) => out.writeString(v)
      case v: Value.Bytes =>
        tag(out, ValueFormat.BytesTag, out.writeString(v.toBase64))
      case Value.Time(v) =>
        tag(out, ValueFormat.TimeTag, out.writeString(v.toString))

      case Value.TransactionTime =>
        tag(out, ValueFormat.TimeTag, out.writeString(txnTime.toString))
      case Value.Date(v) =>
        tag(out, ValueFormat.DateTag, out.writeString(v.toString))
      case Value.UUID(v) =>
        tag(out, ValueFormat.UUIDTag, out.writeString(v.toString))
      case Value.Null(_) => out.writeNull()
      case Value.Array(vs) =>
        out.writeArrayStart()
        vs.foreach { v => copy(value = v).encode(out, txnTime) }
        out.writeArrayEnd()
      case Value.Struct.Full(vs, _, _, _) =>
        def writeObject() = {
          out.writeObjectStart()
          vs.foreach { case (k, v) =>
            out.writeObjectField(k.toUTF8Buf, copy(value = v).encode(out, txnTime))
          }
          out.writeObjectEnd()
        }
        if (vs.keys.exists(_.startsWith("@"))) {
          tag(out, ValueFormat.ObjectTag, writeObject())
        } else {
          writeObject()
        }

      case p: Value.Struct.Partial =>
        copy(value = partials(p)).encode(out, txnTime)

      case sc: Value.SetCursor =>
        out.writeString(
          Value.SetCursor.toBase64(
            replaceSetCursorValues(sc, txnTime),
            Some(txnTime)
          ))

      case v: Value.SingletonObject =>
        tag(out, ValueFormat.ModTag, out.writeString(v.name))

      // Functions are not round-tripable, so just squash them into the simple
      // format.
      case _: Value.Lambda     => out.writeString(s"[function <lambda>]")
      case f: Value.NativeFunc => out.writeString(s"[function ${f.name}()]")

      case d: Value.Doc =>
        (refs(d), docs.get(MaterializedValue.DocKey(d))) match {
          // legacy ref unsupported in v10
          case (r: LegacyRef, _) =>
            out.writeString(r.toRefString)

          // unresolved
          case (ref, None) =>
            tag(
              out,
              ValueFormat.RefTag,
              copy(value = ref.toValue).encode(out, txnTime))

          // missing doc
          case (ref, Some(Value.Null(cause))) =>
            if (format.isTagged) {
              val refV = Value.Struct(
                ref.toValue.fields.concat(
                  Seq(
                    "exists" -> Value.Boolean(false),
                    "cause" -> Value.Str(nullCauseStr(cause))
                  )
                )
              )
              writeTag(
                out,
                ValueFormat.RefTag,
                copy(value = refV).encode(out, txnTime))

            } else {
              out.writeNull()
            }

          // live doc
          case (_, Some(value)) =>
            tag(
              out,
              ValueFormat.DocTag,
              copy(value = value, docs = Map.empty).encode(out, txnTime))
        }

      case set: Value.Set =>
        sets.get(set) match {
          case Some(page) =>
            tag(
              out,
              ValueFormat.SetTag,
              // remove the current set to prevent infinite recursion in
              // materialize
              copy(value = page, sets = sets - set)
                .encode(out, txnTime)
            )
          case None =>
            tag(
              out,
              ValueFormat.SetTag,
              copy(value = ValueSet.setCursor(set))
                .encode(out, txnTime))
        }

      case stream: Value.EventSource =>
        tag(
          out,
          ValueFormat.StreamTag,
          out.writeString(Value.EventSource.toBase64(stream, txnTime)))
    }

  case class Decorator(txnTime: Timestamp) {
    val sb = new StringBuilder
    var _indent = 0
    var needsIndent = true

    // Docs that have been expanded already. Expanding only once prevents
    // infinite recursion on reference cycles.
    val seenDocs = MSet.empty[Value.Doc]

    def write(v: String) = {
      if (needsIndent) {
        sb.append(s"  ".repeat(_indent))
        needsIndent = false
      }
      sb.append(v)
      if (v.endsWith("\n")) {
        needsIndent = true
      }
    }

    def writeln(v: String) = {
      write(v)
      write("\n")
    }

    def writeStrLit(v: String): Unit = {
      // This handles any indents
      write("")
      if (v.contains("\n")) {
        val sigil = {
          if (v.contains("END")) {
            val num = (0 until 10).find { i => !v.contains(s"END$i") }
            num.map { i => s"END$i" }
          } else {
            Some("END")
          }
        }
        sigil match {
          case Some(sigil) =>
            writeln(s"<<-$sigil")
            indent()
            v.lines.forEach(writeln)
            deindent()
            write(sigil)
          case None =>
            // give up and just use double quotes.
            MaterializedValue.escape0(sb, v)
        }
      } else {
        // Now that we won't be appending any newlines, we can write to `sb`
        // directly.
        MaterializedValue.escape0(sb, v)
      }
    }

    def writeIdent(v: String) = {
      if (MaterializedValue.isIdent(v)) {
        write(v)
      } else {
        writeStrLit(v)
      }
    }

    def indent() = {
      _indent += 1
    }
    def deindent() = {
      _indent -= 1
    }

    def encode(value: Value): Unit = {
      value match {
        case Value.ID(v)   => writeStrLit(v.toString)
        case Value.Int(v)  => write(v.toString)
        case Value.Long(v) => write(v.toString)
        case Value.Double(v) =>
          if (v.isNaN) write("Math.NaN")
          else if (v.isPosInfinity) write("Math.Infinity")
          else if (v.isNegInfinity) write("-Math.Infinity")
          else write(v.toString)
        case Value.True   => write("true")
        case Value.False  => write("false")
        case Value.Str(v) => writeStrLit(v)
        case v: Value.Bytes =>
          write("Bytes(")
          writeStrLit(v.toBase64.toString)
          write(")")
        case Value.Time(v) =>
          write("Time(")
          writeStrLit(v.toString)
          write(")")
        case Value.TransactionTime => encode(Value.Time(txnTime))
        case Value.Date(v) =>
          write("Date(")
          writeStrLit(v.toString)
          write(")")
        case Value.UUID(_) => ???
        case Value.Null(_) => write("null")
        case Value.Array(vs) =>
          if (vs.isEmpty) {
            write("[]")
          } else {
            writeln("[")
            indent()
            vs.zipWithIndex.foreach { case (v, i) =>
              encode(v)
              if (i == vs.size - 1) {
                writeln("")
              } else {
                writeln(",")
              }
            }
            deindent()
            write("]")
          }
        case Value.Struct.Full(vs, _, _, _) =>
          if (vs.isEmpty) {
            write("{}")
          } else {
            writeln("{")
            indent()
            vs.zipWithIndex.foreach { case ((k, v), i) =>
              writeIdent(k)
              write(": ")
              encode(v)
              if (i == vs.size - 1) {
                writeln("")
              } else {
                writeln(",")
              }
            }
            deindent()
            write("}")
          }

        case p: Value.Struct.Partial =>
          encode(partials(p))

        case sc: Value.SetCursor =>
          val vctx = sc.values map {
            case p: Value.Struct.Partial => partials(p)
            case other                   => other
          }
          writeStrLit(
            Value.SetCursor
              .toBase64(
                sc.copy(values = vctx),
                Some(txnTime)
              )
              .toString
          )

        // copy of ToString, but doesn't use Query
        case Value.SingletonObject((name, Some(parent))) =>
          encode(parent)
          write(".")
          write(name)
        case Value.SingletonObject((name, None)) => write(name)

        case _: Value.Lambda     => writeStrLit("[function <lambda>]")
        case f: Value.NativeFunc => writeStrLit(s"[function ${f.name}]")

        case d: Value.Doc =>
          (refs(d), docs.get(MaterializedValue.DocKey(d))) match {
            // legacy ref unsupported in v10
            case (ref: LegacyRef, _) => renderDocRef(ref)

            // unresolved
            case (ref, None) => renderDocRef(ref)

            // missing doc
            case (ref, Some(Value.Null(cause))) =>
              renderDocRef(ref)
              write(s" /* ${nullCauseStr(cause)} */")

            // live doc
            case (ref, Some(v)) =>
              // Prevent infinite recursion by expanding a doc only once.
              if (seenDocs.contains(d)) {
                renderDocRef(ref)
              } else {
                seenDocs.add(d)
                encode(v)
                seenDocs.remove(d)
              }
          }

        case set: Value.Set =>
          sets.get(set) match {
            case Some(page) => encode(page)
            case None       => encode(ValueSet.setCursor(set))
          }

        case stream: Value.EventSource =>
          writeStrLit(Value.EventSource.toBase64(stream, txnTime).toString)
      }
    }

    private def renderDocRef(ref: AnyDocRef) =
      ref match {
        case DocRef(id, coll) =>
          encode(coll)
          write("(\"")
          write(id.toLong.toString)
          write("\")")
        case SchemaRef(name, coll) =>
          encode(coll)
          write(".byName(")
          writeStrLit(name)
          write(")")
        case r: LegacyRef =>
          write(r.toRefString)
      }
  }

  def encodeDecorated(out: JSON.Out, txnTime: Timestamp): JSON.Out = {
    val dec = Decorator(txnTime)
    dec.encode(value)
    out.writeString(dec.sb.result())
  }
}

object FQL2ValueMaterializer {
  import Result._

  val SetMaterializationDepth = 10

  def materialize(
    ctx: FQLInterpCtx,
    value: Value): Query[Result[MaterializedValue]] = {
    // NB: mutation of MMaps in this method is OK because query execution is
    // serialized.

    val refQs = MMap.empty[Value.Doc, () => Query[AnyDocRef]]
    val docQs = MMap.empty[MaterializedValue.DocKey, () => Query[Result[Value]]]
    val partials = MMap.empty[Value.Struct.Partial, Value]
    val setPages = MMap.empty[Value.Set, Value]
    val hintQs = MBuffer.empty[Query[Unit]]

    type PendingSets = MMap[Value.Set, () => Query[Result[ValueSet.Page]]]
    type PendingPartials = MMap[Value.Struct.Partial, () => Query[Value]]

    def gatherReads(v: Value, setQs: PendingSets, partialQs: PendingPartials): Unit =
      v match {
        case Value.Array(vs) =>
          vs.foreach { gatherReads(_, setQs, partialQs) }

        case Value.Struct.Full(vs, srcHints, path, accessSpan) =>
          if (ctx.performanceDiagnosticsEnabled) {
            hintQs.addOne(
              ReadBroker.maybeEmitHint(
                ctx,
                path,
                srcHints,
                accessSpan,
                checkPaths = false
              )
            )
          }
          vs.foreach { case (_, v) => gatherReads(v, setQs, partialQs) }

        case p: Value.Struct.Partial =>
          partialQs += (p -> (() => ReadBroker.materializeStruct(ctx, p)))

        case d: Value.Doc =>
          gatherRefs(ctx, refQs, d)
          docQs += (MaterializedValue.DocKey(d) -> (() =>
            ReadBroker.getAllFields(ctx, d)))

        case s: Value.Set =>
          setQs += (s -> (() => s.materializedPage(ctx, None)))

        case c: Value.SetCursor =>
          c.values.foreach {
            gatherReadsForSetCursors(_, partialQs)
          }

        case _ =>
      }

    /** For SetCursors we don't want to materialize docs that end up in the cursor. This makes it so that we only
      * materialize the documents that are actually returned in the response.
      */
    def gatherReadsForSetCursors(v: Value, partialQs: PendingPartials): Unit =
      v match {
        case Value.Array(vs) =>
          vs.foreach { gatherReadsForSetCursors(_, partialQs) }

        case Value.Struct.Full(vs, _, _, _) =>
          vs.foreach { case (_, v) => gatherReadsForSetCursors(v, partialQs) }

        case p: Value.Struct.Partial =>
          partialQs += (p -> (() => ReadBroker.materializeStruct(ctx, p)))

        case s: Value.Set =>
          throw new IllegalStateException(
            s"Found unexpected Value.Set $s in Value.SetCursor")

        case c: Value.SetCursor =>
          c.values.foreach {
            gatherReadsForSetCursors(_, partialQs)
          }

        case _ =>
      }

    // step 1: walk sets to finite depth, gathering refs and docs to materialize.
    def go(values: Iterable[(_, Value)], steps: Int): Query[Result[Unit]] = {
      val setQs: PendingSets = MMap.empty
      val partialQs: PendingPartials = MMap.empty

      values.foreach { case (_, v) =>
        gatherReads(v, setQs, partialQs)
      }

      if (steps == 0 || (setQs.isEmpty && partialQs.isEmpty)) {
        Ok(()).toQuery
      } else {
        val matPartialsQ =
          partialQs
            .map { case (p, q) => q().map { p -> _ } }
            .sequence
            .flatMap { matPartials =>
              partials ++= matPartials
              go(matPartials, steps - 1)
            }

        val matSetPagesQ =
          setQs
            .map { case (s, q) =>
              q().flatMapT(p => p.toValue(ctx).map(p => Ok(s -> p)))
            }
            .sequenceT
            .flatMapT { pages =>
              setPages ++= pages
              go(pages, steps - 1)
            }
        matPartialsQ.flatMapT { _ => matSetPagesQ }
      }
    }

    go(Seq(() -> value), SetMaterializationDepth).flatMapT { _ =>
      for {
        // emit any hints if present, this will be empty if perf hints aren't enabled
        _ <- hintQs.sequence
        // step 2: materialize docs
        docsR <- docQs.map { case (d, q) => q().mapT(d -> _) }.sequenceT
        // step 3: get all refs to render from materialized docs
        _ = docsR.map { vs =>
          vs foreach { case (_, v) => gatherRefs(ctx, refQs, v) }
        }
        // step 4: materialize refs and convert to value
        refs <- refQs.map { case (d, q) => q().map(d -> _) }.sequence
      } yield {
        docsR map { docs =>
          new MaterializedValue(
            value,
            refs.toMap,
            docs.toMap,
            setPages.toMap,
            partials.toMap
          )
        }
      }
    }
  }

  def materializeShallow(
    ctx: FQLInterpCtx,
    value: Value): Query[MaterializedValue] = {
    val refQs = MMap.empty[Value.Doc, () => Query[AnyDocRef]]

    gatherRefs(ctx, refQs, value)

    refQs
      .map { case (d, q) => q().map(d -> _) }
      .sequence
      .map { refs => MaterializedValue(value, refs.toMap) }
  }

  private def gatherRefs(
    ctx: FQLInterpCtx,
    refQs: MMap[Value.Doc, () => Query[AnyDocRef]],
    value: Value): Unit = value match {
    case Value.Array(vs) => vs.foreach(gatherRefs(ctx, refQs, _))

    case Value.Struct.Full(vs, _, _, _) =>
      vs.foreach { case (_, v) => gatherRefs(ctx, refQs, v) }

    case d: Value.Doc =>
      refQs += (d -> (() => ReadBroker.materializeRef(ctx, d)))

    case _ =>
  }
}

object FQL2ValueEncoder {

  def encode(
    format: ValueFormat,
    out: JSON.Out,
    v: MaterializedValue,
    txnTime: Timestamp): JSON.Out =
    format match {
      case ValueFormat.Simple    => v.encodeSimple(out, txnTime)
      case ValueFormat.Tagged    => v.encodeTagged(out, txnTime)
      case ValueFormat.Decorated => v.encodeDecorated(out, txnTime)
    }
}
