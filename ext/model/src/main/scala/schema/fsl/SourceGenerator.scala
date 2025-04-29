package fauna.model.schema.fsl

import fauna.atoms._
import fauna.logging.ExceptionLogging
import fauna.model.schema.{ SchemaSource, SchemaTranslator }
import fauna.repo.query.Query
import fql.ast._
import fql.ast.display._
import fql.parser.Parser
import fql.schema.{ SchemaDiff, SchemaPatch }

object SourceGenerator extends ExceptionLogging {

  // This text precedes generated source in main.fsl. The extra newline prevents this
  // comment from attaching to a schema item as a doc comment.
  val Preamble =
    s"""|// The following schema is auto-generated.
        |// This file contains FSL for FQL10-compatible schema items.
        |""".stripMargin

  val InvalidItemsWarning =
    """|// WARNING: the following auto-generated items contain invalid FSL syntax.
       |// Please fix its syntax and re-submit the file. Contact support if you need
       |// assistance via https://support.fauna.com.
       |""".stripMargin

  /** Derive a sequence of source files matching the current database schema. */
  def deriveSchema(scope: ScopeID): Query[Seq[SourceFile.FSL]] =
    SchemaTranslator.translateSchema(scope) map { items =>
      if (items.isEmpty) {
        Seq.empty
      } else {
        val sb = new StringBuilder
        sb.append(Preamble)

        items foreach { item =>
          sb.append('\n')
            .append(item.display)
        }

        Seq(
          SourceFile.FSL(
            SourceFile.Builtin.Main.filename,
            annotateIfInvalid(scope, sb.result())
          ))
      }
    }

  /** Derive a sequence of source files patching the existing sources based on the
    * current database schema. Note: entries that are not mapped into the current
    * sources are appended to the main.fsl builtin file.
    */
  def patchSchema(
    scope: ScopeID,
    srcs: Seq[SchemaSource]): Query[Seq[SourceFile.FSL]] = {

    val (beforeSrcs, beforeItems) = {
      val beforeSrcs = Map.newBuilder[Src, String]
      val beforeItems = Seq.newBuilder[SchemaItem]

      srcs foreach { src =>
        src.file match {
          case f: SourceFile.FSL =>
            f.parse() match {
              case fql.Result.Ok(items) =>
                beforeItems ++= items
                beforeSrcs += f.src -> f.content

              // Log the error, and don't add in the file, so that the schema diff
              // below replaces this file with auto-generated items.
              case fql.Result.Err(e) =>
                logException(
                  new RuntimeException(
                    s"Replacing invalid FSL in $scope: ${e.mkString("\n\n")}"))
            }
        }
      }

      (beforeSrcs.result(), beforeItems.result())
    }

    SchemaTranslator.translateSchema(scope) map { afterItems =>
      val mainSrc = SourceFile.Builtin.Main.src
      var afterSrcs =
        SchemaPatch.applyTo(
          mainSrc,
          beforeSrcs,
          SchemaDiff.diffItems(
            beforeItems,
            afterItems,
            renames = Map.empty
          )
        )

      if (afterSrcs.contains(mainSrc) && !beforeSrcs.contains(mainSrc)) {
        // If the main.fsl file didn't exist before, add the preamble to it.
        afterSrcs = afterSrcs.updated(mainSrc, s"$Preamble\n${afterSrcs(mainSrc)}")
      }

      afterSrcs.view.map { case (src, content) =>
        SourceFile.FSL(src.name, annotateIfInvalid(scope, content))
      }.toSeq
    }
  }

  private def annotateIfInvalid(scope: ScopeID, src: String): String = {
    if (Parser.schemaItems(src).isErr) {
      squelchAndLogException {
        throw new RuntimeException(s"Invalid FSL syntax generated in $scope.\n$src")
      }

      if (src.startsWith(InvalidItemsWarning)) {
        src
      } else {
        s"$InvalidItemsWarning\n\n$src"
      }
    } else {
      src
    }
  }
}
