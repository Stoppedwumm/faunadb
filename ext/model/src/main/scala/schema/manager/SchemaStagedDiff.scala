package fauna.model.schema.manager

import fauna.atoms.ScopeID
import fauna.model.schema.{
  Result,
  SchemaIndexStatus,
  SchemaManager,
  SchemaSource,
  SchemaStatus,
  StagedIndex
}
import fauna.model.schema.index.CollectionIndex.Status
import fauna.repo.query.Query
import fql.schema.Diff

trait SchemaStagedDiff { self: SchemaManager =>
  def stagedDiff(scope: ScopeID): Query[Result[StagedDiff]] = for {
    status <- SchemaStatus.forScope(scope)
    indexes <- status.activeSchemaVersion match {
      case Some(_) => SchemaIndexStatus.parseIndexStatus(scope).map(Some(_))
      case None    => Query.none
    }

    staged <- SchemaSource.stagedFSLFiles(scope)
    res    <- validate(scope, staged)
  } yield res.map { res => StagedDiff(indexes, res.diffs) }
}

/** Represents a staged schema diff.
  *
  * The `indexes` field serves two purposes:
  * - `None` means there is no staged schema.
  * - `Some(Seq.empty)` means there are staged changes, but no staged
  *   indexes (this should produce the "ready" aggregate state).
  * - `Some(Seq(...))` means there are staged indexes.
  */
case class StagedDiff(indexes: Option[Seq[StagedIndex]], diffs: Seq[Diff]) {

  /** The aggregate index status.
    *
    * None for no staged schema, `Some(SchemaIndexStatus)` if there is a staged status.
    */
  def aggregate = indexes.map(
    _.foldLeft(SchemaIndexStatus.Ready: SchemaIndexStatus)(_ & _.schemaStatus))

  def statusStr = aggregate match {
    case Some(s) => s.asStr
    case None    => "none"
  }

  def pendingSummary: String = {
    val sb = new StringBuilder

    indexes
      .getOrElse(Seq.empty)
      .view
      .filter(_.index.status != Status.Complete)
      .groupBy(_.coll)
      .foreach { case (coll, indexes) =>
        sb ++= "* Collection "
        sb ++= coll
        sb ++= ":"

        indexes.foreach { idx =>
          val icon = idx.index.status match {
            case Status.Building => "~"
            case Status.Complete => sys.error("unreachable")
            case Status.Failed   => "!"
          }

          sb ++= "\n  "
          sb ++= icon
          sb ++= " "
          sb ++= idx.description
          sb ++= ": "
          sb ++= idx.index.status.asStr
        }

        sb ++= "\n"
      }

    sb.result()
  }
}
