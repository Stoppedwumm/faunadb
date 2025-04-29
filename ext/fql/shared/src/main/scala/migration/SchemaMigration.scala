package fql.migration

import fql.ast._
import fql.ast.display._
import fql.typer._

sealed trait SchemaMigration {
  // This is a diff-like output for unit tests.
  //
  // FIXME: Add a pretty-printing variant for diffs.
  def debug: String = this match {
    case SchemaMigration.Add(field, ty, default) =>
      s"add ${field.display}: ${ty.display}${default.fold("")(d => s" = ${d.display}")}"
    case SchemaMigration.Drop(field) =>
      s"drop ${field.display}"
    case SchemaMigration.Move(field, to) =>
      s"move ${field.display} -> ${to.display}"
    case SchemaMigration.Split(source, targets) =>
      val ts = targets map { case (name, ty, bf) =>
        s"(${name.display}: ${ty.display}${bf.fold("") { b =>
            s" = ${b.display}"
          }})"
      }
      s"split ${source.display} -> ${ts.mkString(", ")}"
    case SchemaMigration.MoveWildcardConflicts(into) =>
      s"move_conflicts ${into.display}"
    case SchemaMigration.MoveWildcard(field, fields) =>
      s"move_wildcard -> ${field.display} [${fields.mkString(", ")}]"
    case SchemaMigration.AddWildcard =>
      "add_wildcard"
  }
}

object SchemaMigration {
  case class Add(field: Path, ty: Type, default: Option[Expr])
      extends SchemaMigration
  case class Drop(field: Path) extends SchemaMigration

  // NB: the below aren't usable until we can parse migration blocks. They're just
  // here for reference.

  // TODO: What should the span of `field` be? The old schema file will have the same
  // name, so its a bit difficult to point to it. Maybe it should have the span
  // pointing to the migration block?
  case class Move(field: Path, to: Path) extends SchemaMigration

  /** This represents a `split` migration, which splits a field into another field.
    *
    * - If the value matches `discrimator`:
    *   - The value in `field` is left alone.
    *   - `spillField` is filled in with `spillBackfill`.
    * - If the value doesn't match `expected`:
    *   - The value in `field` is moved to `spillField`.
    *   - `field` is filled in with `fieldBackfill`.
    */
  case class Split(
    source: Path,
    targets: Seq[(Path, Type, Option[Expr])]
  ) extends SchemaMigration

  case class MoveWildcardConflicts(into: Path) extends SchemaMigration

  case class MoveWildcard(into: Path, fields: Set[String]) extends SchemaMigration

  case object AddWildcard extends SchemaMigration
}
