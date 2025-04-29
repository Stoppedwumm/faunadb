package fauna.model.schema.fsl

import fauna.storage.doc.{ Data, Field }
import fql.ast.{ FSL => FSLTree, _ }
import fql.parser.Parser
import fql.Result

/** Represents a schema source file either auto-generated or user-defined. */
sealed abstract class SourceFile(ext: SourceFile.Ext) {
  import SourceFile._

  require(ext.check(filename).isOk, s"invalid filename $filename")

  def filename: String
  def content: String

  final def isBlank = content.isBlank
  final def src = Src.SourceFile(filename)

  final def toData: Data =
    Data(
      Fields.Name -> filename,
      Fields.Ext -> ext.name,
      Fields.Content -> content
    )
}

object SourceFile {

  object Fields {
    val Name = Field[String]("name")
    val Ext = Field[String]("ext")
    val Content = Field[String]("content")
  }

  sealed abstract class Builtin[F <: SourceFile](val name: String, val ext: Ext) {
    val filename = s"$name${ext.dotted}"
    final def src = Src.SourceFile(filename)
  }
  object Builtin {
    case object Main extends Builtin[FSL]("main", Ext.FSL)
    def isBuiltin(file: SourceFile): Boolean = file.filename == Main.filename
  }

  sealed abstract class Ext(val name: String) {
    val dotted = s".$name"
    override def toString = dotted

    def check(filename: String): Result[Unit] =
      if (!filename.startsWith("*") && filename.endsWith(dotted)) {
        Result.Ok(())
      } else {
        Result.Err(Nil)
      }
  }
  object Ext {
    case object FSL extends Ext("fsl")
    def unapply(name: String) = Option.when(name == "fsl") { FSL }
  }

  final case class FSL(filename: String, content: String)
      extends SourceFile(Ext.FSL) {

    def parse(): Result[Seq[SchemaItem]] = parsed

    private[this] lazy val parsed = Parser.schemaItems(content, src)
    private[this] lazy val tree = Parser.fslNodes(content, src)

    private[schema] def locateItem(
      kind: SchemaItem.Kind,
      name: String): Either[Option[FSLTree.Node], Option[SchemaItem]] =
      Either.cond(
        parsed.isOk,
        parsed.getOrElse(Nil) find { it =>
          it.kind == kind && it.name.str == name
        },
        tree.getOrElse(Nil) find { it =>
          it.keyword match {
            case kind(_) => it.name exists { _.str == name }
            case _       => false
          }
        }
      )

    def append(snippet: String) =
      copy(content = s"$content\n\n$snippet")

    def remove(span: Span) =
      patch(span, "")

    def patch(span: Span, snippet: String) =
      copy(content = span.replaceWith(content, snippet))
  }

  object FSL {
    def checkFilename(filename: String): Result[Unit] =
      Ext.FSL.check(filename)
  }

  def empty[F <: SourceFile](builtin: Builtin[F]): F =
    builtin match {
      case Builtin.Main => FSL(builtin.filename, "")
    }

  def apply(filename: String, ext: Ext, content: String): SourceFile.FSL =
    ext match {
      case Ext.FSL => FSL(filename, content)
    }

  def apply(data: Data): SourceFile.FSL =
    data(Fields.Ext) match {
      case Ext(Ext.FSL) => SourceFile.FSL(data(Fields.Name), data(Fields.Content))
      case other => throw new IllegalArgumentException(s"invalid $other extension")
    }
}
