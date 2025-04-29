package fql.typer

import fql.ast.Span
import fql.typer.Type.{ Var, VarId }

trait TyperVars { self: Typer =>

  private var _prevVarId: Int = 0

  protected def freshVarId() = {
    _prevVarId += 1
    VarId(_prevVarId)
  }

  def freshVar(span: Span)(implicit level: Type.Level): Var = {
    val v = Var(freshVarId(), level).move(span)
    dbg(s"new var $v")
    v
  }

  private var _prevGrpId: Int = Alt.GrpId.Base.toInt

  protected def freshAltGrpId() = {
    _prevGrpId += 1
    Alt.GrpId(_prevGrpId)
  }

  protected def setVarStateFrom(typer: TyperVars): Unit = {
    this._prevVarId = typer._prevVarId
    this._prevGrpId = typer._prevGrpId
  }

  val rootAlt = Alt.Base

  def freshAltGrp(
    size: Int,
    exclusive: Boolean,
    container: Alt,
    vlevel: Type.Level): Alt.Grp =
    freshAltGrp(size, exclusive, Some(container), container.alevel.incr, vlevel)

  def freshAltInnerGrp(
    size: Int,
    exclusive: Boolean,
    outer: Alt,
    vlevel: Type.Level
  ): Alt.Grp = {
    if (exclusive && !outer.outerGrp.isExclusive) {
      sys.error("Illegal exclusive Alt subgrp under non-exclusive Grp")
    }
    freshAltGrp(size, exclusive, Some(outer), outer.alevel, vlevel)
  }

  def freshAltGrp(
    size: Int,
    exclusive: Boolean,
    parent: Option[Alt],
    alevel: Alt.Level,
    vlevel: Type.Level): Alt.Grp = {
    val grp = Alt.Grp(freshAltGrpId(), size, exclusive, parent, alevel, vlevel)
    parent.foreach { p =>
      if (p.alevel == grp.alevel) p.addChildGrp(grp)
    }
    dbg(s"new alt grp $grp${if (exclusive) "(exclusive)" else ""}")
    grp
  }
}
