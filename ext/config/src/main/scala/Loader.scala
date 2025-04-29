package fauna.config

import java.io.{
  FileInputStream,
  FileNotFoundException,
  FileOutputStream,
  OutputStreamWriter
}
import java.nio.file._
import java.util.{ List => JList, Map => JMap }
import org.yaml.snakeyaml.{ DumperOptions, Yaml }
import org.yaml.snakeyaml.error.MarkedYAMLException
import scala.jdk.CollectionConverters._

sealed abstract class FieldMsg(val lines: Seq[String]) {
  override def toString = lines mkString "\n"
}

sealed abstract class Error(msg: Seq[String]) extends FieldMsg(msg)

final case class UnknownError(ex: Throwable)
    extends Error(
      Seq("Unexpected error loading configuration file: ", ex.getMessage)
    )

final case class UnrecognizedSetting(field: String)
    extends Error(Seq(s"`$field`: Unrecognized setting."))

final case class InvalidSetting(field: String, setting: Any, reason: String)
    extends Error(Seq(s"`$field`: Invalid value `$setting`. reason: $reason"))

final case class FileNotFound(path: Path)
    extends Error(Seq(s"Configuration file at ${path.toAbsolutePath} not found."))

final case class FileInvalid(
  problem: String,
  line: Int,
  column: Int,
  snippet: Option[String]
) extends Error(
      Seq(
        "Configuration file is not valid YAML.",
        problem,
        s"line $line, column $column",
        snippet getOrElse ""
      )
    )

final case class MissingRequiredSetting(field: String)
    extends Error(Seq(s"Required configuration setting `$field` absent or null."))

sealed abstract class Warning(msg: Seq[String]) extends FieldMsg(msg)

final case class DeprecatedSetting(field: String)
    extends Warning(
      Seq(s"Deprecated configuration setting `$field`. Will be ignored.")
    )

final case class RenamedSetting(field: String, name: String)
    extends Warning(Seq(s"Renamed configuration setting `$field` is now `$name`."))

object Loader {
  private val yaml = {
    val d = new DumperOptions()
    d.setIndent(2)
    d.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    new Yaml(d)
  }

  private def scalaify(j: Any): Any =
    j match {
      case m: JMap[_, _] => m.asScala.toMap map { case (k, v) => k -> scalaify(v) }
      case l: JList[_]   => l.asScala.toList map { scalaify(_) }
      case o             => o
    }

  private def descalaify(s: Any): Any =
    s match {
      case m: Map[_, _]          => m map { case (k, v) => k -> descalaify(v) } asJava
      case gt: Iterable[_] => (gt map { descalaify(_) } toList) asJava
      case o                     => o
    }

  def deserialize(path: Path): Either[Error, Map[String, Any]] = {
    try {
      val is = new FileInputStream(path.toFile)

      val blob = try {
        yaml.load[AnyRef](is)
      } finally {
        is.close()
      }

      Right(Option(blob) match {
        case Some(obj) => scalaify(obj).asInstanceOf[Map[String, Any]]
        case None      => Map.empty[String, Any]
      })
    } catch {
      case _: FileNotFoundException => Left(FileNotFound(path))
      case e: MarkedYAMLException =>
        val mark = e.getProblemMark
        Left(
          FileInvalid(
            e.getProblem,
            mark.getLine + 1,
            mark.getColumn + 1,
            Option(mark.get_snippet)
          )
        )
      case e: Throwable => Left(UnknownError(e))
    }
  }

  def serialize(path: Path, kvs: Map[String, Any]): Either[Error, Unit] = {
    try {
      val os = new OutputStreamWriter(new FileOutputStream(path.toFile))
      try {
        os.write(yaml.dump(descalaify(kvs)))
      } finally {
        os.close()
      }

      Right(())
    } catch {
      case _: FileNotFoundException => Left(FileNotFound(path))
      case e: Throwable             => Left(UnknownError(e))
    }
  }

  def apply[T <: Configuration](cfg: T) = new Loader(cfg)
}

sealed abstract class ConfigResult[T] {
  def config: T
  def errors: List[Error]
  def warnings: List[Warning]

  val loadingMsg: String

  def toException: Option[Exception]
}

case class Default[T](config: T, warnings: List[Warning], errors: List[Error])
    extends ConfigResult[T] {

  val loadingMsg = s"No configuration file specified; loading defaults..."

  def toException =
    if (errors.nonEmpty) {
      Some(new Exception(s"""
      |Error loading default configuration:
      |${errors map { _.lines.mkString(" - ", "\n   ", "\n") } mkString "\n"}
      |(This is an internal error. Please create a ticket at https://support.fauna.com)
      |""".stripMargin.trim))
    } else {
      None
    }
}

case class FromFile[T](
  path: Path,
  config: T,
  warnings: List[Warning],
  errors: List[Error]
) extends ConfigResult[T] {

  val loadingMsg = s"Loaded configuration from ${path.toAbsolutePath}..."

  def toException =
    if (errors.nonEmpty) {
      Some(new Exception(s"""
      |Error loading configuration from $path:
      |${errors map { _.lines.mkString(" - ", "\n   ", "\n") } mkString "\n"}
      |""".stripMargin.trim))
    } else {
      None
    }
}

class Loader[T <: Configuration](cfg: T) {
  private def setField(field: String, setting: Any): List[FieldMsg] = {
    cfg.fields.get(field) match {
      case None =>
        List(UnrecognizedSetting(field))
      case Some(f) if f.deprecated =>
        List(DeprecatedSetting(field))
      case Some(f) =>
        try {
          f.set(setting)
          f.newName match {
            case Some(name) => List(RenamedSetting(field, name))
            case None       => Nil
          }
        } catch {
          case ex: IllegalArgumentException =>
            List(InvalidSetting(field, setting, ex.getMessage))
        }
    }
  }

  // collate constructs a T, assigns the fields in the keyset of kvs to their values, and produces a Default
  // with the T and any warnings or errors.
  def collate(kvs: Map[String, Any] = Map.empty) = {
    val msgs = kvs flatMap { case (field, setting) => setField(field, setting) }
    val warnings = msgs collect { case m: Warning => m } toList
    val errors = msgs collect { case m: Error     => m } toList

    // a missing field is either absent (kvs.get() produces None) or is present and set to
    // null (kvs.get() produces Some(null))
    val missing = cfg.fields collect {
      case (n, f) if f.required && kvs.getOrElse(n, null) == null =>
        MissingRequiredSetting(n)
    }

    Default(cfg, warnings, errors ++ missing)
  }

  // load unmarshals a yaml file into a collection of key-value pairs and collates them into a T with
  // appropriately-assigned values.  Any parameters set in the file at `path` (e.g. specified from a config file)
  // will take precendence over ones set in the default set.
  def load(path: Path, defaults: Map[String, Any] = Map.empty) =
    Loader.deserialize(path) match {
      case Right(kvs) => {
        val parsed = collate(defaults ++ kvs)
        FromFile(path, parsed.config, parsed.warnings, parsed.errors)
      }
      case Left(err) => FromFile(path, cfg, Nil, List(err))
    }
}
