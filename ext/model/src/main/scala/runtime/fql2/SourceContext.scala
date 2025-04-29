package fauna.model.runtime.fql2

import fauna.atoms.ScopeID
import fauna.lang.syntax._
import fauna.model.schema.SchemaSource
import fauna.repo.query.Query
import fql.ast.Src

/** Module for looking up an FQL source given a source ID. */
object SourceContext {
  // FIXME: Based on the Auth, either emit or squelch returning a source
  // string for the given src. This should take into account the auth's ability to
  // read any source but the query string itself.
  def lookup(ctx: FQLInterpCtx, src: Src): Query[Option[String]] =
    src match {
      case Src.Inline(_, str)   => Query.some(str)
      case Src.UserFunc(name)   => lookupUserFunc(ctx, name)
      case Src.SourceFile(file) => lookupSchemaFile(ctx.scopeID, file)
      case Src.Null             => Query.none
      case _                    => Query.none
    }

  def lookup(ctx: FQLInterpCtx, srcs: Set[Src]): Query[Map[Src.Id, String]] =
    srcs
      .collect { case id: Src.Id => lookup(ctx, id).mapT(id -> _) }
      .sequence
      .map { _.flatten.toMap }

  private def lookupUserFunc(ctx: FQLInterpCtx, name: String) =
    UserFunction.lookup(ctx, name).map { _.flatMap(_.source) }

  private def lookupSchemaFile(scope: ScopeID, name: String) =
    SchemaSource.get(scope, name) mapT { _.file.content }
}
