package fql.schema

import com.github.difflib.text.{ DiffRow, DiffRowGenerator }
import fql.ast._
import fql.ast.display._
import fql.color.{ ColorBuilder, ColorKind }
import fql.color.Color
import fql.error.{ Diagnostic, RenderCtx }
import fql.migration.SchemaMigration
import scala.jdk.CollectionConverters._

object DiffRender {
  // We will show 3 non-modified lines between each hunk.
  val UnchangedLinesAroundDiff = 3
  // We will split a diff into sections if there are 7 sequential unchanged lines.
  val UnchangedLinesForSplit = 7

  // A diff with the semantic summary, but without the line-by-line diff.
  def renderSemantic(
    before: Map[Src.Id, String],
    after: Map[Src.Id, String],
    diffs: Seq[Diff],
    colors: ColorKind): String = renderDiffs(before, after, diffs, colors) {
    case (render, diff: Diff.Add)    => render.renderAdd(diff)
    case (render, diff: Diff.Modify) => render.renderModify(diff)
    case (render, diff: Diff.Remove) => render.renderRemove(diff)
  }

  // A diff with a single line per item change.
  def renderSummary(
    before: Map[Src.Id, String],
    after: Map[Src.Id, String],
    diffs: Seq[Diff],
    colors: ColorKind): String = renderDiffs(before, after, diffs, colors) {
    case (render, diff: Diff.Add)    => render.renderAddTitle(diff)
    case (render, diff: Diff.Modify) => render.renderModifyTitle(diff)
    case (render, diff: Diff.Remove) => render.renderRemoveTitle(diff)
  }

  // A diff that only shows the textual diff.
  def renderTextual(
    before: Map[Src.Id, String],
    after: Map[Src.Id, String],
    colors: ColorKind): String = {
    val render = new DiffRender(before, after, colors.createBuilder)

    val files = (before.keySet ++ after.keySet).toSeq.sortBy(_.name)
    var isFirst = true
    files.foreach { id =>
      val beforeSrc = before.getOrElse(id, "")
      val afterSrc = after.getOrElse(id, "")

      if (beforeSrc != afterSrc) {
        if (isFirst) {
          isFirst = false
        } else {
          render.writeln()
        }

        render.b.bold()
        render.write(id.name)
        render.b.reset()
        render.writeln()

        render.renderFileDiff(before.getOrElse(id, ""), after.getOrElse(id, ""))
      }
    }

    render.result
  }

  private def renderDiffs(
    before: Map[Src.Id, String],
    after: Map[Src.Id, String],
    diffs: Seq[Diff],
    colors: ColorKind)(f: (DiffRender, Diff) => Unit): String = {
    val render = new DiffRender(before, after, colors.createBuilder)

    diffs.sorted foreach { f(render, _) }

    render.result
  }
}

private final class DiffRender(
  before: Map[Src.Id, String],
  after: Map[Src.Id, String],
  val b: ColorBuilder
) {
  def result = b.result

  private val beforeCtx = RenderCtx(before)
  private val afterCtx = RenderCtx(after)

  private var needsIndent = false
  private var indentLevel = 0

  private def indent() = indentLevel += 2
  private def deindent() = indentLevel -= 2

  private def writeln(): Unit = writeln("")
  private def writeln(text: AnyRef): Unit = {
    write(text)
    write("\n")
    needsIndent = true
  }

  private def writeBackticked(text: AnyRef): Unit = {
    write("`")
    write(text)
    write("`")
  }

  private def write(text: AnyRef): Unit = {
    val str = text.toString
    if (needsIndent && str.nonEmpty) {
      for (_ <- 0 until indentLevel) b.append(' ')
    }
    b.append(str)
    needsIndent = false
  }

  private def extract(srcMap: Map[Src.Id, String], span: Span) = {
    val src = span.src match {
      case id: Src.Id             => srcMap(id)
      case Src.Inline(_, content) => content
    }
    span.extract(src)
  }

  // FIXME: Normalize defaults.
  def renderAddTitle(diff: Diff.Add, colon: Boolean = false): Unit = {
    b.bold()
    b.select(Color.Blue)
    write("* Adding ")
    write(diff.item.kind)
    write(" ")
    writeBackticked(diff.item.name.str)
    b.reset()

    write(" to ")
    Diagnostic.writeSpan(b, diff.item.span)(afterCtx)
    if (colon) {
      write(":")
    }
    writeln()
  }

  def renderRemoveTitle(diff: Diff.Remove, colon: Boolean = false): Unit = {
    b.bold()
    b.select(Color.Blue)
    write("* Removing ")
    write(diff.item.kind)
    write(" ")
    writeBackticked(diff.item.name.str)
    b.reset()

    write(" from ")
    Diagnostic.writeSpan(b, diff.item.span)(beforeCtx)
    if (colon) {
      write(":")
    }
    writeln()
  }

  def renderAdd(diff: Diff.Add): Unit = {
    val before = makeEmptyItem(diff.item)

    SchemaDiff.diffItem(before, diff.item) match {
      case Some(modify: Diff.Modify) =>
        renderAddTitle(diff, colon = true)

        indent()
        renderModifySummary(modify)
        deindent()

      case None =>
        // FIXME: Not really sure what else to put here.
        //
        // This comes up when adding new functions. We don't have all that much to
        // say, as we would normally just render "modifying body".
        renderAddTitle(diff, colon = false)
        writeln()
    }
  }

  def renderRemove(diff: Diff.Remove): Unit = {
    val after = makeEmptyItem(diff.item)

    SchemaDiff.diffItem(diff.item, after) match {
      case Some(modify: Diff.Modify) =>
        renderRemoveTitle(diff, colon = true)

        indent()
        renderModifySummary(modify)
        deindent()

      case None =>
        renderRemoveTitle(diff, colon = false)
        writeln()
    }
  }

  // Returns a new item with all the optional fields removed. Used when displaying
  // item adds and item removes.
  def makeEmptyItem(item: SchemaItem): SchemaItem = item match {
    case i: SchemaItem.Collection =>
      SchemaItem.Collection(name = i.name, span = i.span)
    case i: SchemaItem.Function =>
      SchemaItem.Function(name = i.name, sig = i.sig, body = i.body, span = i.span)
    case i: SchemaItem.AccessProvider =>
      SchemaItem.AccessProvider(
        name = i.name,
        issuer = i.issuer,
        jwksURI = i.jwksURI,
        span = i.span)
    case i: SchemaItem.Role =>
      SchemaItem.Role(name = i.name, span = i.span)
  }

  private def renderModifySummary(diff: Diff.Modify): Unit = {
    var changed = false
    if (diff.migrations.nonEmpty) {
      changed = true
      writeln("* Defined fields:")

      diff.migrations.foreach(renderSchemaMigration)
      writeln()
    }

    // No need to bother with categories for non-collections (for functions, the
    // change is always just "signature changed" or "body changed". For everything
    // else, all the changes are configuration).
    if (diff.item.kind != SchemaItem.Kind.Collection) {
      if (diff.changes.nonEmpty) {
        changed = true
        diff.changes.foreach(renderChange)
        writeln()
      }
    } else {
      val categorized = diff.changes.groupBy { _.category }

      Category.all.foreach { c =>
        val changes = categorized.getOrElse(Some(c), Nil)
        if (changes.nonEmpty) {
          changed = true
          writeln(s"* ${c.name}:")
          changes.foreach(renderChange)
          writeln()
        }
      }
    }

    if (!changed) {
      writeln("No semantic changes.")
      writeln()
    }
  }

  def renderSchemaMigration(m: SchemaMigration): Unit = {
    m match {
      case SchemaMigration.Add(_, _, None) =>
      // No data needs to be migrated, so say nothing.
      case SchemaMigration.Add(field, _, Some(backfill)) =>
        b.select(Color.Green)
        write(s"+ add field `${field.display}` with backfill `${backfill.display}`")
        b.reset()
        writeln()

      case SchemaMigration.Drop(field) =>
        b.select(Color.Red)
        write(s"- drop field `${field.display}`")
        b.reset()
        writeln()

      case SchemaMigration.Move(field, to) =>
        b.select(Color.Yellow)
        write(s"~ move field `${field.display}` to field `${to.display}`")
        b.reset()
        writeln()

      case SchemaMigration.Split(source, targets) =>
        // Header.
        b.select(Color.Yellow)
        write(s"~ split field `${source.display}` into fields ${targets
            .map({ case (t, _, _) => s"`${t.display}`" })
            .mkString(",")}")
        b.reset()
        writeln()

        // Da splits.
        indent()
        var first = true
        targets foreach { case (t, ty, _) =>
          val pre = if (first) "" else "else "
          val phr = if (source == t) {
            "the value stays in field"
          } else {
            "the value moves to field"
          }
          writeln(
            s"- ${pre}if the value matches type `${ty.display}`, $phr `${t.display}`")
          first = false
        }
        deindent()

        // Da backfills.
        targets.foreach {
          case (t, _, Some(backfill)) =>
            b.select(Color.Yellow)
            write(
              s"+ backfill field `${t.display}` with value `${backfill.display}`")
            b.reset()
            writeln()

          case _ => ()
        }

      case SchemaMigration.MoveWildcardConflicts(into) =>
        b.select(Color.Yellow)
        write(s"~ move values with type conflicts into `${into.display}`")
        b.reset()
        writeln()

      case SchemaMigration.MoveWildcard(field, fields) =>
        b.select(Color.Yellow)
        write(
          s"~ move all fields into the field `${field.display}` except fields in the list `[${fields
              .map(Path(_).display)
              .mkString(", ")}]`")
        b.reset()
        writeln()

      case SchemaMigration.AddWildcard =>
        b.select(Color.Green)
        write("+ add a top-level wildcard")
        b.reset()
        writeln()
    }
  }

  def renderModifyDiff(diff: Diff.Modify): Unit = {
    renderSummaryDiff(
      extract(before, diff.before.span),
      extract(after, diff.after.span))
  }

  def writePrefixLine(prefix: String, line: String): Unit = {
    if (line.isEmpty) {
      if (prefix != " ") {
        write(prefix)
        b.reset()
        writeln()
      } else {
        b.reset()
        writeln()
      }
    } else {
      write(prefix)
      write(" ")
      write(line)
      b.reset()
      writeln()
    }
  }

  def writeDiffRow(row: DiffRow) = {
    row.getTag match {
      case DiffRow.Tag.INSERT =>
        b.select(Color.Green)
        writePrefixLine("+", row.getNewLine)

      case DiffRow.Tag.DELETE =>
        b.select(Color.Red)
        writePrefixLine("-", row.getOldLine)

      case DiffRow.Tag.CHANGE =>
        b.select(Color.Red)
        writePrefixLine("-", row.getOldLine)
        b.select(Color.Green)
        writePrefixLine("+", row.getNewLine)

      case DiffRow.Tag.EQUAL =>
        writePrefixLine(" ", row.getOldLine)
    }
  }

  def renderSummaryDiff(before: String, after: String): Unit = {
    val rows = DiffRowGenerator
      .create()
      .lineNormalizer(identity) // do not escape special chars as html
      .build()
      .generateDiffRows(
        before.linesIterator.toList.asJava,
        after.linesIterator.toList.asJava
      )
      .asScala

    rows.foreach(writeDiffRow)
  }

  def renderFileDiff(before: String, after: String): Unit = {
    var row = 0 // The current row in the diff rows.
    var afterLine = 0 // The current line in the `after` text.

    val rows = DiffRowGenerator
      .create()
      .lineNormalizer(identity) // do not escape special chars as html
      .build()
      .generateDiffRows(
        before.linesIterator.toList.asJava,
        after.linesIterator.toList.asJava
      )
      .asScala

    while (row < rows.length) {
      // Determine the distance to the next hunk, and the size of that hunk.
      val hunkDist = rows.iterator
        .drop(row)
        .takeWhile(_.getTag == DiffRow.Tag.EQUAL)
        .length

      // The length of the hunk (in rows), without any padding for unchanged lines.
      var hunkRows = 0
      // The length of the hunk in lines in the `after` text, without any padding for
      // unchanged lines.
      var hunkLines = 0
      var currentUnchangedLines = 0

      rows.iterator
        .drop(row + hunkDist)
        .takeWhile { row =>
          if (row.getTag == DiffRow.Tag.EQUAL) {
            currentUnchangedLines += 1
          } else {
            hunkRows += currentUnchangedLines
            hunkLines += currentUnchangedLines
            currentUnchangedLines = 0

            hunkRows += 1
            // Adds and changes count a line in the `after` file, but deletes do not.
            if (row.getTag != DiffRow.Tag.DELETE) {
              hunkLines += 1
            }
          }

          // Stop iterating once we have enough unchanged lines.
          currentUnchangedLines < DiffRender.UnchangedLinesForSplit
        }
        .foreach { _ => () }

      if (hunkRows == 0) {
        // We're done.
        row = rows.length
      } else {
        val startRow =
          (row + hunkDist - DiffRender.UnchangedLinesAroundDiff).max(0)
        val endRow =
          (row + hunkDist + hunkRows + DiffRender.UnchangedLinesAroundDiff)
            .min(rows.length)

        val startLine =
          (afterLine + hunkDist - DiffRender.UnchangedLinesAroundDiff).max(0)
        val endLine =
          (afterLine + hunkDist + hunkLines + DiffRender.UnchangedLinesAroundDiff)
            .min(rows.length)

        // Write the hunk.
        b.select(Color.Cyan)
        write("@ line ")
        write((startLine + 1).toString)
        write(" to ")
        write(endLine.toString)
        b.reset()
        writeln()
        rows.slice(startRow, endRow).foreach(writeDiffRow)

        row = endRow
        afterLine = endLine
      }
    }
  }

  private def renderModify(diff: Diff.Modify): Unit = {
    renderModifyTitle(diff, colon = true)

    indent()
    renderModifySummary(diff)
    deindent()
  }

  def renderModifyTitle(diff: Diff.Modify, colon: Boolean = false): Unit = {
    b.bold()
    b.select(Color.Blue)
    write("* Modifying ")
    write(diff.item.kind)
    write(" ")
    writeBackticked(diff.item.name.str)
    b.reset()

    write(" at ")
    Diagnostic.writeSpan(b, diff.after.span)(afterCtx)
    if (diff.isMove) {
      write(" (previously defined at ")
      Diagnostic.writeSpan(b, diff.before.span)(beforeCtx)
      write(")")
    }
    if (colon) {
      write(":")
    }
    writeln()
  }

  private def renderChange(change: Change): Unit = {
    change match {
      case Change.Add(elem) =>
        b.select(Color.Green)
        write("+ add ")
        renderElement(elem)
        b.reset()
        writeln()

      case Change.Remove(elem) =>
        b.select(Color.Red)
        write("- remove ")
        renderElement(elem)
        b.reset()
        writeln()

      case Change.Rename(_, after) =>
        b.select(Color.Yellow)
        write(s"~ rename to `${after.str}`")
        b.reset()
        writeln()

      case Change.Modify(elem, config) =>
        b.select(Color.Yellow)
        write("~ change ")
        renderConfigChange(elem, config)
        b.reset()
        writeln()

      case Change.Block(member, changes) =>
        b.select(Color.Yellow)
        write("~ change ")
        renderMember(member)
        b.reset()
        writeln()

        indent()
        changes foreach { renderChange(_) }
        deindent()

      case Change.ReplaceBody(_, _) =>
        b.select(Color.Yellow)
        write("~ change body")
        b.reset()
        writeln()

      case Change.ReplaceSig(_, _) =>
        b.select(Color.Yellow)
        write("~ change signature")
        b.reset()
        writeln()

      case Change.RemoveFieldDefault(f) =>
        b.select(Color.Red)
        write(s"- remove default value from field `.${f.name.str}`")
        b.reset()
        writeln()

      // These are only used for `SchemaPatch`, not for diffs. Diffs have migrations,
      // and those show all the information that these changes do.
      case Change.ReplaceType(_, _)       => sys.error("unreachable")
      case Change.ReplaceSchemaType(_, _) => sys.error("unreachable")
    }
  }

  private def renderElement(elem: Change.Element): Unit = {
    elem match {
      case Change.Element.Annotation(ann) => renderAnnotation(ann)
      case Change.Element.Field(f)        => renderField(f)
      case Change.Element.Member(mem)     => renderMember(mem)
    }
  }

  private def renderAnnotation(ann: Annotation): Unit = {
    write(ann.kind)
    write(" set to ")
    renderConfig(ann.config)
  }

  private def renderField(f: Field): Unit = {
    val prefix = f.kind match {
      case FSL.FieldKind.Defined => "field"
      case FSL.FieldKind.Compute => "computed field"
      // Be sneaky and detect an implicit wildcard by its null span.
      case FSL.FieldKind.Wildcard if f.span == Span.Null => "implicit wildcard"
      case FSL.FieldKind.Wildcard                        => "wildcard"
    }
    val name = f.kind match {
      case FSL.FieldKind.Defined | FSL.FieldKind.Compute => s" `${f.name.str}`"
      case FSL.FieldKind.Wildcard                        => ""
    }
    write(s"$prefix$name")
  }

  private def renderMember(mem: Member): Unit = {
    renderMemberKindAndName(mem)
    mem.kind match {
      case Member.Kind.Unique =>
        renderConfig(mem.config)

      case _ =>
        mem.config match {
          case _: Config.Opt | _: Config.Block | _: Config.CheckPredicate |
              _: MigrationBlock =>
            ()
          case _ =>
            write(" set to ")
            renderConfig(mem.config)
        }
    }
  }

  private def renderMemberKindAndName(mem: Member): Unit = {
    write(mem.kind)
    mem.kind match {
      case Member.Kind.Unique =>
        write(" constraint on ")

      case Member.Kind.Check =>
        write(s" constraint ")
        writeBackticked(mem.nameOpt.get.str)

      case Member.Kind.Membership | Member.Kind.Privileges =>
        write(" on ")
        writeBackticked(mem.nameOpt.get.str)

      case _ =>
        mem.nameOpt foreach { name =>
          write(" ")
          writeBackticked(name.str)
        }
    }
  }

  private def renderConfigChange(elem: Change.Element, after: Config): Unit = {
    def summarizeMemberChange(before: Config, after: Config): Unit =
      (before, after) match {
        case (a: Config.Opt, b: Config.Opt) =>
          (a.config, b.config) match {
            case (None, Some(c)) =>
              write(" by adding ")
              renderConfig(c)

            case (Some(c), None) =>
              write(" by removing ")
              renderConfig(c)

            case (Some(cA), Some(cB)) =>
              write(" by modifying")
              summarizeMemberChange(cA, cB)

            case (None, None) => () // can't happen
          }

        case (_: Config.CheckPredicate, _: Config.CheckPredicate) =>
          write(" body")

        case (_: Config.Predicate, _: Config.Predicate) =>
          write(" its predicate")

        case (_: MigrationBlock, _: MigrationBlock) => ()

        case _ =>
          write(" from ")
          renderConfig(before)
          write(" to ")
          renderConfig(after)
      }

    elem match {
      case Change.Element.Annotation(ann) =>
        write(ann.kind)
        write(" set to ")
        renderConfig(ann.config)

      case Change.Element.Field(f) =>
        renderField(f)

      case Change.Element.Member(mem) =>
        renderMemberKindAndName(mem)
        summarizeMemberChange(mem.config, after)
    }
  }

  private def renderConfig(config: Config): Unit =
    config match {
      case _: Config.Block | _: Config.CheckPredicate => ()
      case _: Config.Predicate                        => write("a predicate")
      case _: Config.Scalar | _: Config.Seq           => write(config.display)
      case opt: Config.Opt =>
        opt.config match {
          case Some(cfg) => renderConfig(cfg)
          case None      => write(opt.display)
        }
    }
}
