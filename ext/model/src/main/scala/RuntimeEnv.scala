package fauna.model

import fauna.atoms.{ CollectionID, ID, IDCompanion, ScopeID }
import fauna.model.runtime.fql2._
import fauna.model.runtime.fql2.stdlib._
import fauna.model.schema.{ CollectionConfig, NativeCollection, ScopedStore }
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.{ FreeVars, Name }
import fql.env.TypeEnv

sealed trait RuntimeEnv {

  def staticTypeEnv: TypeEnv

  def lookupTypeEnv(
    intp: FQLInterpCtx,
    vars: FreeVars,
    tvars: FreeVars): Query[TypeEnv]

  def getGlobal(intp: FQLInterpCtx, name: Name): Query[Option[Value]]

  def getCollection(
    scope: ScopeID,
    coll: CollectionID): Query[Option[CollectionConfig]]

  def idByName[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    name: String): Query[Option[I]]
  def idByAlias[I <: ID[I]: IDCompanion](
    scope: ScopeID,
    alias: String): Query[Option[I]]

  def Store(scope: ScopeID): ScopedStore.Env = new ScopedStore.Env(this, scope)
}

object RuntimeEnv {

  /** Default lookup of cached schema items */
  object Default extends RuntimeEnv {
    lazy val staticTypeEnv = Global.StaticEnv

    def lookupTypeEnv(intp: FQLInterpCtx, vars: FreeVars, tvars: FreeVars) =
      GlobalContext.lookupEnv(intp, vars, tvars)

    def getGlobal(intp: FQLInterpCtx, name: Name) =
      Global.Default.getField(intp, Global.Default, name).map(_.liftValue)

    def getCollection(scope: ScopeID, id: CollectionID) =
      CollectionConfig(scope, id)

    def idByName[I <: ID[I]: IDCompanion](
      scope: ScopeID,
      name: String): Query[Option[I]] =
      Cache.idByName[I](scope, name).map(_.flatMap(_.active))

    def idByAlias[I <: ID[I]: IDCompanion](
      scope: ScopeID,
      name: String): Query[Option[I]] =
      Cache.idByAlias[I](scope, name).map(_.flatMap(_.active))
  }

  /** Does not return user-defined items */
  object Static extends RuntimeEnv {
    lazy val staticTypeEnv = Global.StaticEnv

    def lookupTypeEnv(intp: FQLInterpCtx, vars: FreeVars, tvars: FreeVars) =
      Query.value(staticTypeEnv)

    def getGlobal(intp: FQLInterpCtx, name: Name): Query[Option[Value]] =
      Global.Static.getField(intp, Global.Static, name).map(_.liftValue)

    def getCollection(scope: ScopeID, id: CollectionID) = {
      val cfg: Query[CollectionConfig] = NativeCollection(id)(scope)
      cfg.map(Some(_))
    }

    def idByName[I <: ID[I]: IDCompanion](
      scope: ScopeID,
      name: String): Query[Option[I]] = Query.none
    def idByAlias[I <: ID[I]: IDCompanion](
      scope: ScopeID,
      name: String): Query[Option[I]] = Query.none
  }

  /** Uncached item lookup. This always considers pending writes part of the
    * environment. It should only used for inline index builds.
    *
    * FIXME: only getCollection is uncached for now.
    */
  object InlineIndexEnv extends RuntimeEnv {
    lazy val staticTypeEnv = Global.StaticEnv

    // TODO: make this not cached
    def lookupTypeEnv(intp: FQLInterpCtx, vars: FreeVars, tvars: FreeVars) =
      GlobalContext.lookupEnv(intp, vars, tvars)

    // TODO: make this not cached
    def getGlobal(intp: FQLInterpCtx, name: Name) =
      Global.Default.getField(intp, Global.Default, name).map(_.liftValue)

    // TODO: remove lookupIndexes flag
    def getCollection(scope: ScopeID, id: CollectionID) =
      CollectionConfig.getForSyncIndexBuild(scope, id)

    def idByName[I <: ID[I]: IDCompanion](
      scope: ScopeID,
      name: String): Query[Option[I]] = {
      SchemaNames.idByNameStagedUncached(scope, name)
    }

    def idByAlias[I <: ID[I]: IDCompanion](
      scope: ScopeID,
      name: String): Query[Option[I]] = {
      SchemaNames.idByAliasStagedUncached(scope, name)
    }
  }
}
