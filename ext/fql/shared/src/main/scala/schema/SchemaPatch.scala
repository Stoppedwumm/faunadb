package fql.schema

import fql.ast._
import fql.ast.display._

object SchemaPatch {

  def apply(diffs: Seq[Diff]): SchemaPatch =
    new SchemaPatch(diffs)

  def applyTo(
    main: Src,
    srcs: Map[Src, String],
    diffs: Seq[Diff]): Map[Src, String] =
    SchemaPatch(diffs).applyTo(main, srcs)
}

final class SchemaPatch private (val diffs: Seq[Diff]) extends AnyVal {

  def applyTo(main: Src, srcs: Map[Src, String]): Map[Src, String] =
    diffs
      .groupBy { diff =>
        val src = diff.span.src
        if (src == Src.Null) main else src
      }
      .foldLeft(srcs) { case (srcs, (src, diffs)) =>
        srcs.updated(src, applyDiffs(srcs.getOrElse(src, ""), diffs))
      }

  private def applyDiffs(src: String, diffs: Seq[Diff]): String =
    diffs
      .sortBy { diff =>
        // Start patching from the end of the file so that additions/modifications
        // don't change previously computed diff spans.
        -diff.span.end
      }
      .foldLeft(src) {
        case (src, Diff.Add(item)) =>
          if (src.isBlank) {
            item.display
          } else {
            val lastLine =
              src.substring(
                src.lastIndexOf("\n").max(0),
                src.length
              )

            val sb = new StringBuilder
            sb.append(src).append("\n")
            if (!lastLine.isBlank()) sb.append("\n")
            sb.append(item.display).result()
          }

        case (src, Diff.Remove(item)) =>
          item.span.replaceWith(src, "")

        case (src, Diff.Modify(before, _, changes, _)) =>
          val startSpan =
            before.docComment.fold(before.span) { doc =>
              before.span.from(doc.end)
            }
          applyChanges(src, changes, startSpan)
      }

  private def applyChanges(
    src: String,
    changes: Seq[Change],
    span: Span,
    level: Int = 0
  ): String = {
    // Similarily to `applyDiffs`, start applying the last change first.
    def endOf(change: Change): Int =
      change match {
        case Change.Add(_: Change.Element.Annotation) => span.start
        case Change.Add(_: Change.Element.Field)      => span.end
        case Change.Add(_: Change.Element.Member)     => span.end
        case Change.Rename(before, _)                 => before.span.end
        case Change.Remove(elem)                      => elem.span.end
        case Change.Modify(elem, _)                   => elem.span.end
        case Change.ReplaceBody(before, _)            => before.span.end
        case Change.ReplaceSig(before, _)             => before.span.end
        case Change.ReplaceType(before, _)            => before.span.end
        case Change.ReplaceSchemaType(before, _)      => before.span.end
        case Change.RemoveFieldDefault(before)        => before.span.end
        case Change.Block(_, nested) => nested.view.map { endOf(_) }.max
      }

    changes
      .sortBy { -endOf(_) }
      .foldLeft(src) { case (src, change) =>
        change match {
          case Change.Rename(b, a)        => rename(b, a, src)
          case Change.Add(e)              => addElement(e, src, span, level)
          case Change.Modify(e, cfg)      => modifyElement(e, cfg, src, level)
          case Change.Remove(e)           => replaceElement(e.span, src, "")
          case Change.Block(e, cs)        => applyChanges(src, cs, e.span, level + 1)
          case Change.ReplaceSig(b, sig)  => replaceElement(b.span, src, sig.display)
          case Change.ReplaceBody(b, exp) => replaceElement(b.span, src, exp.display)
          case Change.ReplaceType(b, a)   => modifyType(b, src, a.display)
          case Change.ReplaceSchemaType(b, a) => modifyType(b, src, a.display)
          case Change.RemoveFieldDefault(b)   =>
            // Patch like so:
            // foo: Int = 42 -> foo: Int
            //         -----
            //         |   ^ end
            //         ^ start
            val start = b.ty match {
              case Some(ty) => ty.span.end
              case None     => b.name.span.end
            }
            val span = b.value.get.span.copy(start = start)
            replaceElement(span, src, "")
        }
      }
  }

  private def rename(before: Name, after: Name, src: String): String =
    before.span.replaceWith(src, after.str)

  private def addElement(
    elem: Change.Element,
    src: String,
    span: Span,
    level: Int
  ): String = {
    val startSpan =
      elem match {
        case Change.Element.Annotation(_)                       => span.start
        case Change.Element.Field(_) | Change.Element.Member(_) => span.end - 1
      }

    val prefix = src.substring(0, startSpan).stripTrailing
    val suffix = src.substring(startSpan, src.length)
    val sb = new StringBuilder

    if (prefix.nonEmpty) {
      sb.append(prefix)
        .append('\n')
    }

    indentStr(
      sb,
      indentPrefix(elem, level),
      indentFirstLine = true,
      elem.display
    )

    if (!Span.LeadingNLRegex.matches(suffix)) {
      sb.append('\n')
        .append(
          indentPrefix(
            elem,
            level - 1
          ))
    }

    sb.append(suffix)
    sb.result()
  }

  private def modifyElement(
    elem: Change.Element,
    config: Config,
    src: String,
    level: Int
  ): String = {
    elem match {
      // FIXME: This is a dumb hack to fix `MigrationBlock` being both a
      // member and its own config.
      case Change.Element.Member(Member.Typed(_, MigrationBlock(_, _), span)) =>
        val sb = new StringBuilder
        // `config.display` doesn't include the keyword, so add it here.
        sb ++= "migrations "

        indentStr(
          sb,
          indentPrefix(elem, level),
          indentFirstLine = false,
          config.display
        )

        span.replaceWith(src, sb.result())

      case _ =>
        val sb = new StringBuilder
        val span =
          elem.configSpan match {
            case Span.Null => // optional config not previously set
              elem match {
                case Change.Element.Field(_) => sb.append(" = ")
                case _                       => sb.append(' ')
              }
              elem.span.from(elem.span.end)
            case other =>
              other
          }

        indentStr(
          sb,
          indentPrefix(elem, level),
          indentFirstLine = false,
          config.display
        )

        span.replaceWith(src, sb.result())
    }
  }

  private def modifyType(
    field: Field,
    src: String,
    newType: String
  ): String = field.ty match {
    case Some(ty) => replaceElement(ty.span, src, newType)
    case None =>
      replaceElement(
        field.name.span.copy(start = field.name.span.end),
        src,
        s": $newType")
  }

  private def replaceElement(span: Span, src: String, sub: String): String =
    span.replaceWith(src, sub)

  private def indentPrefix(elem: Change.Element, level: Int): String =
    elem match {
      case _: Change.Element.Annotation => ""
      case _: Change.Element.Field      => " " * (level + 1) * 2
      case _: Change.Element.Member     => " " * (level + 1) * 2
    }

  private def indentStr(
    sb: StringBuilder,
    prefix: String,
    indentFirstLine: Boolean,
    str: String): Unit =
    str.linesIterator.zipWithIndex foreach { case (line, i) =>
      if (i > 0) {
        sb.append('\n')
          .append(prefix)
      } else if (indentFirstLine) {
        sb.append(prefix)
      }
      sb.append(line)
    }
}
